"""
llm_classify.py - LLM-based paper reuse classification via OpenRouter.

Provides prompt construction, API calling with retries, and response parsing
for classifying citation edges as PRIMARY / SECONDARY / NEITHER / UNKNOWN.

Adapted from _tmp_find_reuse_friend/llm_utils.py for production Airflow use.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-5.4-nano"

# OpenRouter rejects these IDs; map to a current equivalent (stale Airflow UI params may still use them).
_OPENROUTER_MODEL_ALIASES: Dict[str, str] = {
    "openai/chatgpt-5.3-nano": "openai/gpt-5.4-nano",
}


def normalize_openrouter_model(model: Optional[str]) -> str:
    """Resolve deprecated or invalid OpenRouter model IDs to a supported ID."""
    m = (model or "").strip()
    if not m:
        return DEFAULT_MODEL
    resolved = _OPENROUTER_MODEL_ALIASES.get(m, m)
    if resolved != m:
        logger.info(
            "OpenRouter model %r is not valid; using %r instead.",
            m,
            resolved,
        )
    return resolved


VALID_CLASSIFICATIONS = {"PRIMARY", "SECONDARY", "NEITHER", "UNKNOWN"}
CONFIDENCE_MAP = {"high": 3, "medium": 2, "low": 1}


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------

def get_openrouter_api_key() -> str:
    """Read OPENROUTER_API_KEY from environment. Raises ValueError if absent."""
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY not set. Add it to your .env / Docker environment "
            "and restart the Airflow containers."
        )
    return api_key


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _format_context_excerpts(contexts: List[Dict[str, Any]], max_excerpts: int = 15) -> str:
    """Turn citation_contexts JSONB rows into numbered excerpts for the prompt."""
    excerpts: list[str] = []
    for i, ctx in enumerate(contexts[:max_excerpts], 1):
        text = ctx.get("context", "").strip()
        if not text:
            continue
        method = ctx.get("method", "")
        label = f"Excerpt {i}"
        if method:
            label += f" [{method}]"
        excerpts.append(f"{label}:\n{text}")
    return "\n\n".join(excerpts)


def build_classification_prompt(
    dataset_id: str,
    primary_paper_title: Optional[str],
    citing_paper_title: Optional[str],
    citing_authors: Optional[List[str]],
    citation_contexts: List[Dict[str, Any]],
) -> str:
    """Build the classification prompt for a citation edge."""
    excerpts_text = _format_context_excerpts(citation_contexts)
    if not excerpts_text:
        excerpts_text = "(No context excerpts available)"

    author_str = "Unknown authors"
    if citing_authors:
        names = [a if isinstance(a, str) else str(a) for a in citing_authors[:5]]
        author_str = ", ".join(names)
        if len(citing_authors) > 5:
            author_str += " et al."

    primary_label = primary_paper_title or "(unknown title)"
    citing_label = citing_paper_title or "(unknown title)"

    return (
        f"Analyze these excerpts from a scientific paper and classify its "
        f"relationship to dataset {dataset_id}.\n\n"
        f'Primary paper: "{primary_label}"\n'
        f'Citing paper: "{citing_label}" by {author_str}\n\n'
        f"Context excerpts from the citing paper:\n"
        f"{excerpts_text}\n\n"
        f"Based on ALL the excerpts above, classify the citing paper's "
        f"relationship to the dataset as one of:\n"
        f'- PRIMARY: The authors of the citing paper created and shared this '
        f'dataset (e.g., "we deposited our data", "data are available at", '
        f'"our dataset", "we recorded", "we acquired")\n'
        f'- SECONDARY: The authors used or analyzed an existing dataset '
        f'(e.g., "we downloaded data from", "we used the dataset", '
        f'"data were obtained from", "derived from", "we analyzed data from")\n'
        f'- NEITHER: Not a real reference to using the dataset '
        f'(e.g., general mention of the archive, methodology description)\n'
        f"- UNKNOWN: Cannot determine from the context provided\n\n"
        f"Key guidance: Look for language indicating ownership "
        f'("our data", "we recorded") vs. usage ("we used", "derived from", '
        f'"obtained from"). If there are body text citations, prioritize those '
        f"for classification since they show how the paper actually uses the dataset.\n\n"
        f"Respond with ONLY a raw JSON object (no markdown, no code blocks, no extra text):\n"
        '{"classification": "PRIMARY|SECONDARY|NEITHER|UNKNOWN", '
        '"confidence": "high|medium|low", "reasoning": "Brief explanation"}'
    )


def build_primary_relationship_prompt(
    dataset_id: str,
    paper_title: Optional[str],
    paper_authors: Optional[List[str]],
    citation_contexts: List[Dict[str, Any]],
) -> str:
    """Build the classification prompt for a primary paper -> dataset relationship."""
    excerpts_text = _format_context_excerpts(citation_contexts)
    if not excerpts_text:
        excerpts_text = "(No context excerpts available)"

    author_str = "Unknown authors"
    if paper_authors:
        names = [a if isinstance(a, str) else str(a) for a in paper_authors[:5]]
        author_str = ", ".join(names)
        if len(paper_authors) > 5:
            author_str += " et al."

    title_label = paper_title or "(unknown title)"

    return (
        f"Analyze these excerpts and classify the relationship between the paper "
        f'"{title_label}" by {author_str} and dataset {dataset_id}.\n\n'
        f"Context excerpts:\n{excerpts_text}\n\n"
        f"Classify the paper's relationship to the dataset as one of:\n"
        f'- PRIMARY: The authors of this paper created and shared this dataset\n'
        f'- SECONDARY: The authors used or analyzed an existing dataset\n'
        f'- NEITHER: Not a real reference to using the dataset\n'
        f"- UNKNOWN: Cannot determine from the context provided\n\n"
        f"Key guidance: Look for language indicating ownership "
        f'("our data", "we recorded") vs. usage ("we used", "derived from").\n\n'
        f"Respond with ONLY a raw JSON object (no markdown, no code blocks, no extra text):\n"
        '{"classification": "PRIMARY|SECONDARY|NEITHER|UNKNOWN", '
        '"confidence": "high|medium|low", "reasoning": "Brief explanation"}'
    )


# ---------------------------------------------------------------------------
# OpenRouter API caller
# ---------------------------------------------------------------------------

def _attach_usage(result: Dict[str, Any], raw_response: Optional[dict]) -> Dict[str, Any]:
    """Add OpenAI-style token counts from the API response (OpenRouter-compatible)."""
    u = (raw_response or {}).get("usage") or {}
    result["prompt_tokens"] = int(u.get("prompt_tokens") or 0)
    result["completion_tokens"] = int(u.get("completion_tokens") or 0)
    result["total_tokens"] = int(u.get("total_tokens") or 0)
    return result


def call_openrouter(
    prompt: str,
    api_key: str,
    model: str = DEFAULT_MODEL,
    max_retries: int = 3,
    max_tokens: int = 512,
    temperature: float = 0.1,
    timeout: int = 90,
) -> Dict[str, Any]:
    """
    POST to OpenRouter chat completions with retry / backoff.

    Returns a dict with at least ``classification``, ``confidence``, ``reasoning``,
    and when an HTTP response was received, ``prompt_tokens``, ``completion_tokens``,
    ``total_tokens`` (integers; zeros if usage omitted or request failed before a body).
    """
    model = normalize_openrouter_model(model)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://github.com/Neuro-D3/neurod3",
    }
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }

    last_error: Optional[Exception] = None
    raw_response: Optional[dict] = None

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                OPENROUTER_API_URL,
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited (429), waiting %ss (attempt %d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            raw_response = resp.json()
            break

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            wait = 2 ** attempt
            logger.warning("Network error (%s), retrying in %ss (attempt %d/%d)", exc, wait, attempt + 1, max_retries)
            if attempt < max_retries - 1:
                time.sleep(wait)
            continue

        except requests.RequestException as exc:
            last_error = exc
            body = ""
            if hasattr(exc, "response") and exc.response is not None:
                body = exc.response.text[:300]
            logger.error("Request failed: %s  |  response body: %s", exc, body or "(none)")
            break
    else:
        msg = f"All {max_retries} attempts failed"
        if last_error:
            msg += f": {last_error}"
        return _attach_usage(_unknown_result(msg), None)

    if raw_response is None:
        return _attach_usage(
            _unknown_result(str(last_error) if last_error else "No response"),
            None,
        )

    choices = raw_response.get("choices", [])
    if not choices:
        return _attach_usage(_unknown_result("No choices in API response"), raw_response)

    finish_reason = choices[0].get("finish_reason", "")
    if finish_reason == "length":
        return _attach_usage(
            _unknown_result("Response truncated (finish_reason=length)"),
            raw_response,
        )

    content = choices[0].get("message", {}).get("content", "")
    if not content or len(content.strip()) < 5:
        return _attach_usage(
            _unknown_result("Empty or very short response"),
            raw_response,
        )

    return _attach_usage(parse_classification_response(content), raw_response)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_classification_response(text: str) -> Dict[str, Any]:
    """
    Extract a classification JSON object from raw LLM output.

    Handles bare JSON, markdown-fenced JSON, and various malformed responses
    using multiple fallback strategies.
    """
    if not text:
        return _unknown_result("No response from LLM")

    content = text.strip()

    # Strip markdown code fences
    if content.startswith("```"):
        lines = content.split("\n")
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        content = "\n".join(lines[1:end_idx]).strip()

    # Strategy 1: direct parse
    parsed = _try_json_parse(content)
    if parsed:
        return parsed

    # Strategy 2: extract from markdown block
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        parsed = _try_json_parse(m.group(1))
        if parsed:
            return parsed

    # Strategy 3: find any JSON object containing "classification"
    m = re.search(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", text)
    if m:
        parsed = _try_json_parse(m.group())
        if parsed:
            return parsed

    # Strategy 4: simpler braces search
    m = re.search(r'\{[^{}]*"classification"[^{}]*\}', text)
    if m:
        parsed = _try_json_parse(m.group())
        if parsed:
            return parsed

    # Strategy 5: keyword scan
    upper = text.upper()
    for cls in ("PRIMARY", "SECONDARY", "NEITHER"):
        if cls in upper:
            return {
                "classification": cls,
                "confidence": 1,
                "reasoning": "Extracted from unstructured response",
                "parse_error": True,
            }

    return _unknown_result(f"Failed to parse LLM output: {text[:300]}")


def normalize_confidence(raw: Any) -> int:
    """Map LLM confidence strings/ints to the 1-3 integer scale."""
    if isinstance(raw, int) and 1 <= raw <= 3:
        return raw
    if isinstance(raw, str):
        return CONFIDENCE_MAP.get(raw.lower().strip(), 1)
    return 1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _try_json_parse(text: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "classification" in obj:
            obj["classification"] = obj["classification"].upper().strip().replace(" ", "_")
            if obj["classification"] not in VALID_CLASSIFICATIONS:
                obj["parse_error"] = f"Invalid classification: {obj['classification']}"
            obj["confidence"] = normalize_confidence(obj.get("confidence"))
            return obj
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _unknown_result(reason: str) -> Dict[str, Any]:
    return {
        "classification": "UNKNOWN",
        "confidence": 1,
        "reasoning": reason,
        "parse_error": True,
    }
