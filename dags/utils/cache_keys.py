"""
Cache key helpers for S3-friendly object naming.
"""

from __future__ import annotations

import hashlib
from typing import Optional


def doi_hash(doi: str) -> str:
    """
    Stable hash for a normalized DOI string.
    """
    d = (doi or "").strip().lower()
    h = hashlib.sha256(d.encode("utf-8")).hexdigest()
    return h


def paper_cache_key_for_doi(doi: str) -> Optional[str]:
    """
    Return relative cache key like: papers/<doi_hash>/latest.json
    """
    if not doi or not isinstance(doi, str) or not doi.strip():
        return None
    return f"papers/{doi_hash(doi)}/latest.json"

