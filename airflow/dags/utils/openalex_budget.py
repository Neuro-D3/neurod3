"""
OpenAlex daily-budget check for the paper-mapping DAGs.

OpenAlex meters requests: a free API key gets $1/day (10,000 requests at
$0.0001) and keyless callers share a much smaller per-IP budget; both reset at
midnight UTC and every response carries the running totals in
``X-RateLimit-*`` headers. Spending the budget mid-run used to be found out one
429 at a time; ``check_openalex_budget`` asks once, up front, with a single
one-result request, logs where the day stands, and aborts the run when fewer
than ``min_openalex_requests`` remain.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional

import requests

try:
    from utils.find_reuse_core import (
        openalex_api_key,
        openalex_polite_url,
        openalex_request_headers,
    )
except ImportError:  # pragma: no cover - direct import outside the dags folder
    from dags.utils.find_reuse_core import (  # type: ignore
        openalex_api_key,
        openalex_polite_url,
        openalex_request_headers,
    )

logger = logging.getLogger(__name__)

PROBE_URL = "https://api.openalex.org/works?per-page=1"
DEFAULT_MIN_REMAINING = 200


def parse_budget_headers(headers: Mapping[str, str]) -> Dict[str, Any]:
    """
    Read OpenAlex's X-RateLimit-* headers into plain numbers.

    Returns a dict with ``limit``, ``remaining``, ``used``, ``cost_usd``
    (per request), ``limit_usd``, ``remaining_usd``, ``reset_seconds`` and
    ``resets_at`` (UTC datetime); each is None when the header is absent.
    """
    lowered = {str(k).lower(): v for k, v in headers.items()}

    def num(name: str, cast=float):
        raw = lowered.get(name.lower())
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return cast(str(raw).strip())
        except (TypeError, ValueError):
            return None

    reset_seconds = num("X-RateLimit-Reset", float)
    return {
        "limit": num("X-RateLimit-Limit", int),
        "remaining": num("X-RateLimit-Remaining", int),
        "used": num("X-RateLimit-Credits-Used", int),
        "cost_usd": num("X-RateLimit-Cost-USD", float),
        "limit_usd": num("X-RateLimit-Limit-USD", float),
        "remaining_usd": num("X-RateLimit-Remaining-USD", float),
        "reset_seconds": reset_seconds,
        "resets_at": (
            datetime.now(timezone.utc) + timedelta(seconds=reset_seconds)
            if reset_seconds is not None else None
        ),
    }


def fetch_openalex_budget(session: Optional[requests.Session] = None, timeout: int = 20,
                          telemetry: Any = None) -> Dict[str, Any]:
    """
    One cheap authenticated request to read today's budget.

    Always returns a dict; on any failure ``status`` is None (or the HTTP
    status) and the numbers are None, so callers can log and move on rather
    than fail a run over a monitoring call. A 429 here is itself the answer:
    the budget is already spent.

    The probe is itself a metered request; pass the caller's ``telemetry``
    (find_reuse_core.Telemetry) so it is counted in total_requests.
    """
    s = session or requests.Session()
    url = openalex_polite_url(PROBE_URL)
    out: Dict[str, Any] = {"status": None, "authenticated": bool(openalex_api_key()), "error": None}
    if telemetry is not None:
        telemetry.total_requests += 1
    try:
        resp = s.get(url, headers=openalex_request_headers(url) or None, timeout=timeout)
    except requests.RequestException as exc:
        out["error"] = str(exc)
        out.update(parse_budget_headers({}))
        return out
    out["status"] = resp.status_code
    out.update(parse_budget_headers(resp.headers))
    if resp.status_code >= 400:
        out["error"] = (resp.text or "")[:300]
    return out


def format_budget(budget: Mapping[str, Any]) -> str:
    rem, lim = budget.get("remaining"), budget.get("limit")
    if rem is None or lim is None:
        return "OpenAlex budget unknown" + (f" (HTTP {budget['status']})" if budget.get("status") else "")
    resets = budget.get("resets_at")
    when = f", resets {resets:%H:%M} UTC" if resets else ""
    who = "API key" if budget.get("authenticated") else "NO API KEY (shared per-IP budget)"
    usd = budget.get("remaining_usd")
    usd_s = f" (${usd:.4f})" if usd is not None else ""
    return f"OpenAlex budget: {rem:,} of {lim:,} requests remaining today{usd_s}{when}; {who}"


def check_openalex_budget(params: Optional[Mapping[str, Any]] = None, *, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    Log today's OpenAlex budget and abort the task when it is nearly spent.

    ``params['min_openalex_requests']`` (default 200) is the floor; 0 disables
    the abort but keeps the log line. A probe that cannot be read (network
    error, missing headers) is logged and does not block the run.
    """
    params = params or {}
    try:
        floor = int(params.get("min_openalex_requests", DEFAULT_MIN_REMAINING) or 0)
    except (TypeError, ValueError):
        floor = DEFAULT_MIN_REMAINING

    budget = fetch_openalex_budget(session=session)
    message = format_budget(budget)
    if not budget.get("authenticated"):
        logger.warning("%s. Set OPENALEX_API_KEY (free) to get a 10x larger budget of your own.", message)
    else:
        logger.info(message)

    remaining = budget.get("remaining")
    spent = budget.get("status") == 429 or (remaining is not None and remaining < floor)
    if floor > 0 and spent:
        from airflow.exceptions import AirflowFailException  # imported lazily: not needed in tests
        raise AirflowFailException(
            f"{message}. Below min_openalex_requests={floor}; re-run after the reset, "
            "top up the key's prepaid credit, or lower the floor."
        )
    return budget
