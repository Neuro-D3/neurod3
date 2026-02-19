"""
Resolve DANDI dandiset -> paper DOIs/titles.

Adapted from ../find_reuse/dandi_primary_papers.py, but scoped for DAG use:
- fetch dandiset version metadata from DANDI API
- extract paper DOIs from relatedResource + description
- resolve titles/IDs via Crossref/OpenAlex
- provide detailed telemetry for throttling/retries/errors
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import re
from typing import Any, Dict, List, Optional, Set

import requests

from utils.find_reuse_core import (
    Telemetry,
    extract_dois_from_text,
    http_get_json,
    normalize_doi,
    resolve_crossref_authors,
    resolve_crossref_title,
    resolve_openalex_authors,
    resolve_openalex_metadata,
)

logger = logging.getLogger(__name__)


DANDI_API_URL = "https://api.dandiarchive.org/api"


# DataCite relation types seen in DANDI metadata
PRIMARY_PAPER_RELATIONS = {
    "dcite:IsDescribedBy",
    "dcite:IsPublishedIn",
    "dcite:IsSupplementTo",
}

SECONDARY_PAPER_RELATIONS = {
    "dcite:Describes",
    "dcite:IsCitedBy",
    "dcite:IsReferencedBy",
    "dcite:Cites",
    "dcite:IsSourceOf",
    "dcite:IsDerivedFrom",
    "dcite:IsPartOf",
}

# Resource types that represent papers (journal articles or preprints).
PAPER_RESOURCE_TYPES = {
    "dcite:JournalArticle",
    "dcite:Preprint",
    "dcite:DataPaper",
    "dcite:ConferencePaper",
    "dcite:ConferenceProceeding",
}


def _has_doi_identifier(resource: Dict[str, Any]) -> bool:
    identifier = resource.get("identifier", "") or ""
    if identifier:
        if isinstance(identifier, str):
            if identifier.startswith(("doi:", "DOI:", "10.")) or "doi.org/" in identifier:
                return True
    url = resource.get("url", "") or ""
    if isinstance(url, str) and url:
        if "doi.org/" in url:
            return True
        if "biorxiv.org/content/10." in url or "medrxiv.org/content/10." in url:
            return True
    return False


def _is_paper_resource(resource: Dict[str, Any]) -> bool:
    resource_type = resource.get("resourceType")
    if resource_type is not None and resource_type not in PAPER_RESOURCE_TYPES:
        return False
    return _has_doi_identifier(resource)


def _extract_doi_from_resource(resource: Dict[str, Any]) -> Optional[str]:
    identifier = resource.get("identifier", "") or ""
    if isinstance(identifier, str) and identifier:
        if identifier.startswith("doi:"):
            return normalize_doi(identifier[4:])
        if identifier.startswith("DOI:"):
            return normalize_doi(identifier[4:])
        if identifier.startswith("10."):
            return normalize_doi(identifier)
        if "doi.org/" in identifier:
            return normalize_doi(identifier.split("doi.org/")[-1])

    url = resource.get("url", "") or ""
    if isinstance(url, str) and url:
        if "doi.org/" in url:
            return normalize_doi(url.split("doi.org/")[-1])
        if "biorxiv.org/content/" in url or "medrxiv.org/content/" in url:
            m = re.search(r"(10\\.[0-9]{4,}/[^\\s/v]+)", url)
            if m:
                return normalize_doi(m.group(1))
    return None


def _get_dandiset_version_metadata(
    session: requests.Session,
    dandiset_id: str,
    version: str,
    *,
    telemetry: Telemetry,
    min_interval_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Optional[Dict[str, Any]]:
    url = f"{DANDI_API_URL}/dandisets/{dandiset_id}/versions/{version}/"
    data = http_get_json(
        session,
        url,
        timeout=30,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        telemetry=telemetry,
    )
    return data


@dataclass
class PaperResolutionResult:
    papers: List[Dict[str, Any]] = field(default_factory=list)
    telemetry: Dict[str, Any] = field(default_factory=dict)
    reason: Optional[str] = None
    error: Optional[str] = None


def resolve_papers_for_dandiset(
    *,
    dandiset_id: str,
    dandiset_title: Optional[str],
    dandiset_description: Optional[str],
    dandiset_version: Optional[str],
    include_secondary_relations: bool = False,
    min_interval_seconds: float = 0.2,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> PaperResolutionResult:
    """
    Resolve paper DOIs + metadata for a single dandiset.

    Sources:
    - DANDI version metadata `relatedResource`
    - DANDI version metadata `description` (and fallback to DB description passed in)
    """
    telemetry = Telemetry()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "NeuroD3/DANDIPaperMapping",
            "Accept": "application/json",
        }
    )

    # Choose which relations to include
    target_relations = set(PRIMARY_PAPER_RELATIONS)
    if include_secondary_relations:
        target_relations |= set(SECONDARY_PAPER_RELATIONS)

    # Determine which version to query
    version = dandiset_version or "draft"
    meta = _get_dandiset_version_metadata(
        session,
        dandiset_id,
        version,
        telemetry=telemetry,
        min_interval_seconds=min_interval_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    if not meta and version != "draft":
        # fallback to draft if published version is missing
        version = "draft"
        meta = _get_dandiset_version_metadata(
            session,
            dandiset_id,
            version,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )

    if not meta:
        return PaperResolutionResult(
            papers=[],
            telemetry=telemetry.to_dict(),
            reason="dandi_metadata_unavailable",
            error=None,
        )

    seen: Set[str] = set()
    papers: List[Dict[str, Any]] = []

    # 1) relatedResource entries
    resources = meta.get("relatedResource", []) or []
    if isinstance(resources, list):
        for r in resources:
            if not isinstance(r, dict):
                continue
            relation = r.get("relation", "") or ""
            if relation not in target_relations:
                continue
            if not _is_paper_resource(r):
                continue
            doi = _extract_doi_from_resource(r)
            if not doi or doi in seen:
                continue
            seen.add(doi)
            papers.append(
                {
                    "doi": doi,
                    "relation_type": relation,
                    "source": "relatedResource",
                    "resource_type": r.get("resourceType"),
                    "name": r.get("name"),
                    "url": r.get("url"),
                    "title": None,
                    "openalex_id": None,
                }
            )

    # 2) description DOIs: prefer DANDI metadata description; fallback to DB description
    desc_meta = meta.get("description")
    desc_text = None
    if isinstance(desc_meta, str) and desc_meta.strip():
        desc_text = desc_meta
    elif isinstance(dandiset_description, str) and dandiset_description.strip():
        desc_text = dandiset_description

    for doi in extract_dois_from_text(desc_text):
        if doi in seen:
            continue
        seen.add(doi)
        papers.append(
            {
                "doi": doi,
                "relation_type": "description",
                "source": "description",
                "resource_type": None,
                "name": None,
                "url": f"https://doi.org/{doi}",
                "title": None,
                "openalex_id": None,
            }
        )

    if not papers:
        return PaperResolutionResult(
            papers=[],
            telemetry=telemetry.to_dict(),
            reason="no_dois_found",
            error=None,
        )

    # 3) Resolve paper titles/ids
    for p in papers:
        doi = p.get("doi")
        if not doi:
            continue

        # Try Crossref for title/authors first (fast, reliable)
        title = resolve_crossref_title(
            session,
            doi,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        authors = resolve_crossref_authors(
            session,
            doi,
            telemetry=telemetry,
            min_interval_seconds=min_interval_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        if title:
            p["title"] = title
            p["paper_metadata_source"] = "crossref"
        if authors:
            p["authors"] = authors
            # Only set if not already set by title path
            p["paper_metadata_source"] = p.get("paper_metadata_source") or "crossref"
        else:
            # Fallback to OpenAlex metadata
            oa = resolve_openalex_metadata(
                session,
                doi,
                telemetry=telemetry,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            oa_authors = resolve_openalex_authors(
                session,
                doi,
                telemetry=telemetry,
                min_interval_seconds=min_interval_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            if oa.get("title"):
                p["title"] = oa.get("title")
            if oa.get("openalex_id"):
                p["openalex_id"] = oa.get("openalex_id")
            if oa_authors:
                p["authors"] = oa_authors
            p["paper_metadata_source"] = "openalex" if (oa.get("title") or oa.get("openalex_id")) else None

        # Keep a normalized DOI for storage safety
        p["doi"] = normalize_doi(doi)

    # Drop any entries that failed DOI normalization
    papers = [p for p in papers if p.get("doi")]

    return PaperResolutionResult(
        papers=papers,
        telemetry=telemetry.to_dict(),
        reason=None,
        error=None,
    )

