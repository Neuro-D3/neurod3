"""
Resolve OpenNeuro dataset -> paper DOIs/titles/authors.

Strategy (best-effort):
- Fetch OpenNeuro dataset metadata via GraphQL (readme/description + publication-like fields when present)
- Extract explicit DOIs from structured metadata when available
- Also extract DOIs from readme/description free text
- Resolve paper metadata via Crossref/OpenAlex (single-fetch helpers) with shared telemetry
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from utils.find_reuse_core import (
    Telemetry,
    extract_dois_from_text,
    normalize_doi,
    resolve_crossref_metadata,
    resolve_zenodo_metadata,
    resolve_openalex_work,
)

logger = logging.getLogger(__name__)

OPENNEURO_GRAPHQL_URL = "https://openneuro.org/crn/graphql"

_DATASET_FIELD_SPECS_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_TYPE_FIELD_NAMES_CACHE: Dict[str, Set[str]] = {}


def _openneuro_graphql(
    session: requests.Session,
    *,
    query: str,
    operation_name: str,
    variables: Dict[str, Any],
    timeout: int,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
    allow_partial: bool = True,
) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "NeuroD3/OpenNeuroPaperMapping",
    }
    payload = {"query": query, "operationName": operation_name, "variables": variables}
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        if min_interval_seconds > 0:
            time.sleep(min_interval_seconds)
        telemetry.total_requests += 1
        try:
            resp = session.post(OPENNEURO_GRAPHQL_URL, json=payload, headers=headers, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                if resp.status_code == 429:
                    telemetry.api_429_count += 1
                else:
                    telemetry.api_5xx_count += 1
                telemetry.api_retry_count += 1
                wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not allow_partial and data.get("errors"):
                raise RuntimeError(f"OpenNeuro GraphQL errors: {data.get('errors')}")
            return data
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            telemetry.api_retry_count += 1
            wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
            time.sleep(wait)
            continue
        except Exception as e:
            last_exc = e
            break
    if last_exc:
        logger.debug("OpenNeuro GraphQL failed op=%s err=%s", operation_name, last_exc)
    return {}


def _get_dataset_field_specs(session: requests.Session, *, telemetry: Telemetry, pacing: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    global _DATASET_FIELD_SPECS_CACHE
    if _DATASET_FIELD_SPECS_CACHE is not None:
        return _DATASET_FIELD_SPECS_CACHE

    introspection_query = """
    query IntrospectDatasetFields {
      __type(name: "Dataset") {
        fields {
          name
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
    """
    data = _openneuro_graphql(
        session,
        query=introspection_query,
        operation_name="IntrospectDatasetFields",
        variables={},
        timeout=30,
        telemetry=telemetry,
        min_interval_seconds=float(pacing["min_interval_seconds"]),
        max_retries=int(pacing["max_retries"]),
        backoff_seconds=float(pacing["backoff_seconds"]),
        allow_partial=True,
    )
    fields = (((data or {}).get("data") or {}).get("__type") or {}).get("fields") or []
    specs: Dict[str, Dict[str, Any]] = {}
    if isinstance(fields, list):
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            if isinstance(name, str) and name:
                specs[name] = f.get("type") or {}

    _DATASET_FIELD_SPECS_CACHE = specs
    return specs


def _get_type_field_names(
    session: requests.Session, type_name: str, *, telemetry: Telemetry, pacing: Dict[str, Any]
) -> Set[str]:
    if type_name in _TYPE_FIELD_NAMES_CACHE:
        return _TYPE_FIELD_NAMES_CACHE[type_name]
    op = f"Introspect{type_name}Fields"
    q = f"""
    query {op} {{
      __type(name: "{type_name}") {{
        fields {{ name }}
      }}
    }}
    """
    data = _openneuro_graphql(
        session,
        query=q,
        operation_name=op,
        variables={},
        timeout=30,
        telemetry=telemetry,
        min_interval_seconds=float(pacing["min_interval_seconds"]),
        max_retries=int(pacing["max_retries"]),
        backoff_seconds=float(pacing["backoff_seconds"]),
        allow_partial=True,
    )
    fields = (((data or {}).get("data") or {}).get("__type") or {}).get("fields") or []
    names: Set[str] = set()
    if isinstance(fields, list):
        for f in fields:
            if isinstance(f, dict):
                n = f.get("name")
                if isinstance(n, str) and n:
                    names.add(n)
    _TYPE_FIELD_NAMES_CACHE[type_name] = names
    return names


def _unwrap_graphql_type(type_spec: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """
    Return (base_type_name, kind_summary).
    kind_summary is a compact string like: SCALAR, LIST(SCALAR), OBJECT, LIST(OBJECT)
    """
    kind = type_spec.get("kind")
    name = type_spec.get("name")
    of1 = type_spec.get("ofType") if isinstance(type_spec.get("ofType"), dict) else None
    if kind == "NON_NULL" and of1:
        return _unwrap_graphql_type(of1)
    if kind == "LIST" and of1:
        base_name, inner = _unwrap_graphql_type(of1)
        return base_name, f"LIST({inner})"
    # Base
    base_name = name if isinstance(name, str) and name else (of1.get("name") if of1 else None)
    return base_name if isinstance(base_name, str) else None, str(kind or "UNKNOWN")


def _extract_dois_from_any(obj: Any, *, out: Set[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, str):
        for d in extract_dois_from_text(obj):
            out.add(d)
        # Also treat pure DOI strings
        d0 = normalize_doi(obj)
        if d0:
            out.add(d0)
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            # if key looks like DOI-ish, try normalizing value
            if isinstance(k, str) and k.lower() in {"doi", "paper_doi", "publicationdoi"}:
                if isinstance(v, str):
                    d0 = normalize_doi(v)
                    if d0:
                        out.add(d0)
            _extract_dois_from_any(v, out=out)
        return
    if isinstance(obj, list):
        for it in obj:
            _extract_dois_from_any(it, out=out)


def _build_dataset_query(
    session: requests.Session, *, telemetry: Telemetry, pacing: Dict[str, Any]
) -> Tuple[str, List[str]]:
    """
    Build a Dataset selection set that is safe across schema versions.
    Returns (selection_set_string, included_field_names).
    """
    specs = _get_dataset_field_specs(session, telemetry=telemetry, pacing=pacing)
    fields = set(specs.keys())

    included: List[str] = []
    lines: List[str] = []

    def include_scalar(field: str) -> None:
        """
        Only include scalars (or list-of-scalars) to avoid GraphQL validation errors.
        """
        if field not in fields:
            return
        _base_name, kind_summary = _unwrap_graphql_type(specs.get(field) or {})
        if kind_summary not in {"SCALAR", "LIST(SCALAR)"}:
            return
        lines.append(field)
        included.append(field)

    include_scalar("name")
    include_scalar("readme")
    include_scalar("metadata")
    include_scalar("summary")
    include_scalar("modalities")
    include_scalar("modality")

    # NOTE: We intentionally do not include the GraphQL `description` field here.
    # It is often an object type (requiring a selection set that varies across schema versions).
    # We already ingest `openneuro_dataset.description` into Postgres and pass it into the resolver.

    # Try publication-like fields if present.
    pub_candidates = [
        "publications",
        "publication",
        "relatedPublications",
        "references",
        "relatedResources",
        "related",
    ]
    for f in pub_candidates:
        if f not in fields:
            continue
        base_name, kind_summary = _unwrap_graphql_type(specs.get(f) or {})
        # If scalar-ish, request directly.
        if kind_summary in {"SCALAR", "LIST(SCALAR)"}:
            lines.append(f)
            included.append(f)
            continue
        # If object-ish, request a safe set of fields if we can introspect them.
        if base_name:
            names = _get_type_field_names(session, base_name, telemetry=telemetry, pacing=pacing)
            wanted = [x for x in ("doi", "url", "title", "name", "identifier", "citation") if x in names]
            if wanted:
                lines.append(f"{f} {{ {' '.join(wanted)} }}")
                included.append(f)
                continue
        # If we can't safely select it, skip to avoid GraphQL validation errors.

    selection = "\n        ".join(lines) if lines else "name"
    return selection, included


def fetch_openneuro_metadata(
    *,
    dataset_id: str,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Dict[str, Any]:
    session = requests.Session()
    pacing = {
        "min_interval_seconds": min_interval_seconds,
        "max_retries": max_retries,
        "backoff_seconds": backoff_seconds,
    }

    selection, _included = _build_dataset_query(session, telemetry=telemetry, pacing=pacing)
    query = f"""
    query DatasetPaperMetadata($id: ID!) {{
      dataset(id: $id) {{
        {selection}
      }}
    }}
    """
    data = _openneuro_graphql(
        session,
        query=query,
        operation_name="DatasetPaperMetadata",
        variables={"id": dataset_id},
        timeout=60,
        telemetry=telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        allow_partial=True,
    )
    dataset = (((data or {}).get("data") or {}).get("dataset")) or {}
    return dataset if isinstance(dataset, dict) else {}


@dataclass
class OpenNeuroPaperResolutionResult:
    papers: List[Dict[str, Any]] = field(default_factory=list)
    telemetry: Dict[str, Any] = field(default_factory=dict)
    reason: Optional[str] = None
    error: Optional[str] = None


def resolve_papers_for_openneuro_dataset(
    *,
    dataset_id: str,
    dataset_title: Optional[str],
    dataset_description: Optional[str],
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> OpenNeuroPaperResolutionResult:
    telemetry = Telemetry()

    meta = fetch_openneuro_metadata(
        dataset_id=dataset_id,
        telemetry=telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    if not meta:
        return OpenNeuroPaperResolutionResult(
            papers=[],
            telemetry=telemetry.to_dict(),
            reason="openneuro_metadata_unavailable",
            error=None,
        )

    dois: Set[str] = set()
    # Structured fields
    _extract_dois_from_any(meta, out=dois)

    # Free text fields
    for text in (
        meta.get("readme"),
        json.dumps(meta.get("description")) if meta.get("description") is not None else None,
        dataset_description,
        dataset_title,
    ):
        if isinstance(text, str) and text.strip():
            for d in extract_dois_from_text(text):
                dois.add(d)

    if not dois:
        return OpenNeuroPaperResolutionResult(
            papers=[],
            telemetry=telemetry.to_dict(),
            reason="no_dois_found",
            error=None,
        )

    # Resolve titles/authors per DOI (single-fetch helpers)
    session = requests.Session()
    out: List[Dict[str, Any]] = []
    for doi in sorted(dois):
        doi_norm = normalize_doi(doi)
        if not doi_norm:
            continue

        paper: Dict[str, Any] = {
            "doi": doi_norm,
            "title": None,
            "openalex_id": None,
            "authors": None,
            "source": "openneuro_metadata",
            "relation_type": "metadata_or_text",
            "paper_metadata_source": None,
        }

        if doi_norm.lower().startswith("10.5281/zenodo."):
            z = resolve_zenodo_metadata(
                session,
                doi_norm,
                telemetry=telemetry,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            if z.get("title"):
                paper["title"] = z.get("title")
                paper["paper_metadata_source"] = "zenodo"
            if z.get("authors"):
                paper["authors"] = z.get("authors")
                paper["paper_metadata_source"] = paper.get("paper_metadata_source") or "zenodo"

        cr = resolve_crossref_metadata(
            session,
            doi_norm,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        if cr.get("title"):
            paper["title"] = cr.get("title")
            paper["paper_metadata_source"] = "crossref"
        if cr.get("authors"):
            paper["authors"] = cr.get("authors")
            paper["paper_metadata_source"] = paper.get("paper_metadata_source") or "crossref"

        if not paper.get("title") or not paper.get("authors") or not paper.get("openalex_id"):
            oa = resolve_openalex_work(
                session,
                doi_norm,
                telemetry=telemetry,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            if not paper.get("title") and oa.get("title"):
                paper["title"] = oa.get("title")
            if oa.get("openalex_id"):
                paper["openalex_id"] = oa.get("openalex_id")
            if not paper.get("authors") and oa.get("authors"):
                paper["authors"] = oa.get("authors")
            if (oa.get("title") or oa.get("openalex_id") or oa.get("authors")) and not paper.get("paper_metadata_source"):
                paper["paper_metadata_source"] = "openalex"

        out.append(paper)

    return OpenNeuroPaperResolutionResult(
        papers=out,
        telemetry=telemetry.to_dict(),
        reason=None,
        error=None,
    )

