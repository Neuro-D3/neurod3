"""
Run a DAG on specific datasets instead of its normal selection.

The ingestion, paper-mapping and classification DAGs accept a `dataset_ids`
param. Left empty (the default) they behave exactly as before; given ids, they
work on those datasets only. The stack integration test uses this to push one
known dataset per archive through the whole pipeline.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Sequence

logger = logging.getLogger(__name__)

# When ids are requested, listing stops being capped by num_datasets: the
# requested dataset can sit anywhere in the archive's listing.
LIST_ALL = 1_000_000


def requested_dataset_ids(params: Dict[str, Any] | None) -> List[str]:
    """
    The `dataset_ids` param as a clean list: accepts a list or a comma/space
    separated string, strips blanks, keeps order, drops duplicates.
    """
    raw = (params or {}).get("dataset_ids")
    if raw is None:
        return []
    if isinstance(raw, str):
        items: Iterable[Any] = raw.replace(",", " ").split()
    elif isinstance(raw, (list, tuple, set)):
        items = raw
    else:
        items = [raw]
    out: List[str] = []
    for item in items:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    return out


def keep_requested(
    datasets: List[Dict[str, Any]],
    requested: Sequence[str],
    *,
    keys: Sequence[str] = ("dataset_id", "doi"),
    archive: str = "",
) -> List[Dict[str, Any]]:
    """
    Keep only the listed datasets whose id (or DOI, for archives like CRCNS
    whose listing carries the DOI before enrichment resolves the short code)
    matches a requested id. Case-insensitive. Raises when nothing matches, so a
    typo'd or vanished test dataset fails the run loudly instead of ingesting
    nothing and passing.
    """
    if not requested:
        return datasets
    wanted = {r.lower() for r in requested}
    kept: List[Dict[str, Any]] = []
    found: set = set()
    for ds in datasets:
        values = {str(ds.get(k) or "").strip().lower() for k in keys} - {""}
        hit = values & wanted
        if hit:
            kept.append(ds)
            found |= hit
    missing = [r for r in requested if r.lower() not in found]
    label = f"{archive} " if archive else ""
    if missing:
        logger.warning("%sdataset_ids not found in the listing: %s", label, missing)
    if not kept:
        raise ValueError(
            f"None of the requested {label}dataset_ids {list(requested)} are in the archive's "
            f"listing ({len(datasets)} datasets listed)"
        )
    logger.info("%sdataset_ids: kept %d of %d listed datasets", label, len(kept), len(datasets))
    return kept


# Dataset ids across archives: DANDI 000402, OpenNeuro ds004315, CRCNS alm-1 or
# 10.6080/k0ms3qnt, SPARC 157. Anything else is refused rather than quoted.
_SAFE_ID = re.compile(r"^[A-Za-z0-9._/:-]{1,128}$")


def sql_id_list(ids: Sequence[str]) -> str:
    """
    `'a', 'b'` for an SQL IN (...) list. Ids are validated against a strict
    pattern first, so this never needs driver parameters (some selection
    queries contain literal `%` in LIKE patterns, which parameters would break).
    """
    bad = [i for i in ids if not _SAFE_ID.match(str(i))]
    if bad:
        raise ValueError(f"dataset_ids contain characters that are not allowed: {bad}")
    if not ids:
        raise ValueError("sql_id_list needs at least one id")
    return ", ".join("'" + str(i) + "'" for i in ids)
