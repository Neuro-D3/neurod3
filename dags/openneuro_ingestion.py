"""
OpenNeuro ingestion DAG (GraphQL -> Postgres).

What this DAG does
------------------
- fetch_openneuro_datasets: Pulls dataset ids (and optional name) with pagination.
- enrich_openneuro_current_run: For each dataset id, pulls detailed metadata via
  GraphQL (with schema introspection to avoid invalid fields), normalizes and
  cleans values, and upserts into Postgres.
- create_openneuro_table: Ensures the target table and indexes exist.

Data flow
---------
OpenNeuro GraphQL -> in-memory dataset dict -> normalization -> Postgres upsert.

Field mapping (API -> in-memory -> Postgres)
-------------------------------------------
OpenNeuro.Dataset
  id -------------------------------> dataset_id -> openneuro_dataset.dataset_id
  name -----------------------------> title (if present) -> openneuro_dataset.title
  created --------------------------> created_at -> openneuro_dataset.created_at
  modified/updated/lastModified ----> updated_at -> openneuro_dataset.updated_at
  public ---------------------------> public -> openneuro_dataset.public
  license --------------------------> license -> openneuro_dataset.license
  readme ---------------------------> description (truncated) -> openneuro_dataset.description
  analytics.downloads --------------> downloads -> openneuro_dataset.downloads
  analytics.views ------------------> views -> openneuro_dataset.views
  analytics.downloads --------------> citations (fallback proxy) -> openneuro_dataset.citations
  modalities/modality --------------> modality (normalized) -> openneuro_dataset.modality
  metadata.modalities --------------> modality (preferred, normalized) -> openneuro_dataset.modality
  summary.modalities ---------------> modality fallback (normalized) -> openneuro_dataset.modality
  description.Name -----------------> title fallback -> openneuro_dataset.title
  description.Description ----------> description fallback -> openneuro_dataset.description
  description.License --------------> license fallback -> openneuro_dataset.license

OpenNeuro.Snapshot (selected when needed)
  latest/selected tag --------------> internal snapshot tag for enrichment
  snapshot.description.Name --------> title fallback -> openneuro_dataset.title
  snapshot.description.Description -> description fallback -> openneuro_dataset.description
  snapshot.description.License ----> license fallback -> openneuro_dataset.license
  snapshot.created ----------------> updated_at fallback -> openneuro_dataset.updated_at
  snapshot.readme -----------------> description fallback -> openneuro_dataset.description

Additional notes
----------------
- All string fields are sanitized to remove NUL bytes before upsert.
- Snapshot calls are avoided unless needed (title/description/license/modality fallbacks).
- Title prefers dataset.name, then snapshot.description.Name if meaningful.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.database import get_db_connection, create_unified_datasets_view

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'neurod3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'openneuro_ingestion',
    default_args=default_args,
    description='Fetch and ingest datasets from OpenNeuro GraphQL API',
    schedule='@daily',
    catchup=False,
    tags=['datasets', 'openneuro', 'ingestion'],
    is_paused_upon_creation=False,
    params={
        'num_datasets': 5000,  # Default number of datasets to fetch
        # OpenNeuro is sensitive to high concurrency; keep this lower than DANDI by default.
        'enrichment_max_workers': 5,
    },
)

OPENNEURO_GRAPHQL_URL = "https://openneuro.org/crn/graphql"

_TYPE_FIELD_NAMES_CACHE: Dict[str, set] = {}
_TYPE_FIELD_SPECS_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {}
_TYPE_FIELD_ARGS_CACHE: Dict[str, Dict[str, List[str]]] = {}
_TYPE_FIELD_ARG_SPECS_CACHE: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

# --- OpenNeuro API rate limiting ---
# OpenNeuro can throttle or return flaky resolver errors under high concurrency.
# Keep this conservative by default; can be overridden via env vars.
_OPENNEURO_MAX_INFLIGHT = int(os.getenv("OPENNEURO_MAX_INFLIGHT", "5"))
_OPENNEURO_MIN_INTERVAL_SECONDS = float(os.getenv("OPENNEURO_MIN_INTERVAL_SECONDS", "0.15"))
_OPENNEURO_SEM = threading.BoundedSemaphore(max(1, _OPENNEURO_MAX_INFLIGHT))
_OPENNEURO_RATE_LOCK = threading.Lock()
_OPENNEURO_LAST_REQUEST_AT = 0.0

# --- Debug logging (optional) ---
# Set OPENNEURO_MODALITY_DEBUG=1 to emit a small, capped sample of modality debug logs.
_OPENNEURO_MODALITY_DEBUG = os.getenv("OPENNEURO_MODALITY_DEBUG", "0").strip().lower() in ("1", "true", "yes")
_OPENNEURO_MODALITY_DEBUG_SAMPLES = int(os.getenv("OPENNEURO_MODALITY_DEBUG_SAMPLES", "25"))
_OPENNEURO_MODALITY_DEBUG_LOCK = threading.Lock()
_OPENNEURO_MODALITY_DEBUG_EMITTED = 0

def _get_dataset_field_specs() -> Dict[str, Dict[str, Any]]:
    """
    Best-effort introspection of the OpenNeuro GraphQL schema for the Dataset type.

    We use this to avoid GraphQL validation errors when the schema differs across versions.
    If introspection fails for any reason, we return an empty dict and fall back to safe fields.
    """
    if not hasattr(_get_dataset_field_specs, "_cache"):
        _get_dataset_field_specs._cache = None  # type: ignore[attr-defined]
    if _get_dataset_field_specs._cache is not None:  # type: ignore[attr-defined]
        return _get_dataset_field_specs._cache  # type: ignore[attr-defined]

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

    try:
        data = openneuro_graphql(
            query=introspection_query,
            operation_name="IntrospectDatasetFields",
            variables={},
            timeout=30,
            allow_partial=True,
        )
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("fields", [])
        )
        specs: Dict[str, Dict[str, Any]] = {}
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            if not name:
                continue
            specs[name] = f.get("type") or {}

        _get_dataset_field_specs._cache = specs  # type: ignore[attr-defined]
        return specs
    except Exception as e:
        logger.warning("Could not introspect Dataset fields from OpenNeuro; falling back to safe fields: %s", e)
        _get_dataset_field_specs._cache = {}  # type: ignore[attr-defined]
        return _get_dataset_field_specs._cache  # type: ignore[attr-defined]


def _get_dataset_field_names() -> set:
    return set(_get_dataset_field_specs().keys())


def _get_type_field_names(type_name: str) -> set:
    """
    Best-effort field-name introspection for an arbitrary GraphQL type.
    Cached for the lifetime of the DAG parse/run.
    """
    if type_name in _TYPE_FIELD_NAMES_CACHE:
        return _TYPE_FIELD_NAMES_CACHE[type_name]

    # IMPORTANT: OpenNeuro's Apollo server validates that `operationName` matches a named
    # operation in the GraphQL document. Our client sends `operationName`, so ensure the
    # query uses the same name.
    op = f"Introspect{type_name}Fields"
    introspection_query = f"""
    query {op} {{
      __type(name: "{type_name}") {{
        fields {{
          name
        }}
      }}
    }}
    """

    try:
        data = openneuro_graphql(
            query=introspection_query,
            operation_name=op,
            variables={},
            timeout=30,
            allow_partial=True,
        )
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("fields", [])
        )
        names = {f.get("name") for f in fields if isinstance(f, dict) and f.get("name")}
        _TYPE_FIELD_NAMES_CACHE[type_name] = names
        return names
    except Exception as e:
        logger.warning("Could not introspect %s fields; using empty set: %s", type_name, e)
        _TYPE_FIELD_NAMES_CACHE[type_name] = set()
        return _TYPE_FIELD_NAMES_CACHE[type_name]


def _get_type_field_specs(type_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Best-effort introspection for a GraphQL type, returning a map of field_name -> field_type_spec.

    Field type specs include kind/name/ofType nesting and can be used to decide whether a field
    can be selected directly (scalar) or requires a sub-selection (object).
    """
    if type_name in _TYPE_FIELD_SPECS_CACHE:
        return _TYPE_FIELD_SPECS_CACHE[type_name]

    # IMPORTANT: operationName must match a named operation in the query document.
    op = f"Introspect{type_name}FieldSpecs"
    introspection_query = f"""
    query {op} {{
      __type(name: "{type_name}") {{
        fields {{
          name
          type {{
            kind
            name
            ofType {{
              kind
              name
              ofType {{
                kind
                name
              }}
            }}
          }}
        }}
      }}
    }}
    """

    try:
        data = openneuro_graphql(
            query=introspection_query,
            operation_name=op,
            variables={},
            timeout=30,
            allow_partial=True,
        )
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("fields", [])
        )
        specs: Dict[str, Dict[str, Any]] = {}
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            if not name:
                continue
            specs[name] = f.get("type") or {}

        _TYPE_FIELD_SPECS_CACHE[type_name] = specs
        return specs
    except Exception as e:
        logger.warning("Could not introspect %s field specs; using empty dict: %s", type_name, e)
        _TYPE_FIELD_SPECS_CACHE[type_name] = {}
        return _TYPE_FIELD_SPECS_CACHE[type_name]


def _get_type_field_arg_names(type_name: str) -> Dict[str, List[str]]:
    """
    Best-effort introspection for a GraphQL type, returning map of field_name -> [arg_name, ...].
    """
    if type_name in _TYPE_FIELD_ARGS_CACHE:
        return _TYPE_FIELD_ARGS_CACHE[type_name]

    op = f"Introspect{type_name}FieldArgs"
    introspection_query = f"""
    query {op} {{
      __type(name: "{type_name}") {{
        fields {{
          name
          args {{
            name
          }}
        }}
      }}
    }}
    """

    try:
        data = openneuro_graphql(
            query=introspection_query,
            operation_name=op,
            variables={},
            timeout=30,
            allow_partial=True,
        )
        fields = (
            data.get("data", {})
            .get("__type", {})
            .get("fields", [])
        )
        arg_map: Dict[str, List[str]] = {}
        for f in fields:
            if not isinstance(f, dict) or not f.get("name"):
                continue
            args = f.get("args") or []
            if isinstance(args, list):
                arg_map[f["name"]] = [a.get("name") for a in args if isinstance(a, dict) and a.get("name")]
            else:
                arg_map[f["name"]] = []

        _TYPE_FIELD_ARGS_CACHE[type_name] = arg_map
        return arg_map
    except Exception as e:
        logger.warning("Could not introspect %s field args; using empty dict: %s", type_name, e)
        _TYPE_FIELD_ARGS_CACHE[type_name] = {}
        return _TYPE_FIELD_ARGS_CACHE[type_name]


def _get_type_field_arg_specs(type_name: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Best-effort introspection for a GraphQL type, returning:
      field_name -> { arg_name -> arg_type_spec }
    """
    if type_name in _TYPE_FIELD_ARG_SPECS_CACHE:
        return _TYPE_FIELD_ARG_SPECS_CACHE[type_name]

    op = f"Introspect{type_name}FieldArgSpecs"
    introspection_query = f"""
    query {op} {{
      __type(name: "{type_name}") {{
        fields {{
          name
          args {{
            name
            type {{
              kind
              name
              ofType {{
                kind
                name
                ofType {{
                  kind
                  name
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """

    try:
        data = openneuro_graphql(
            query=introspection_query,
            operation_name=op,
            variables={},
            timeout=30,
            allow_partial=True,
        )
        fields = (data.get("data", {}).get("__type", {}) or {}).get("fields", []) or []
        out: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for f in fields:
            if not isinstance(f, dict) or not f.get("name"):
                continue
            args = f.get("args") or []
            arg_map: Dict[str, Dict[str, Any]] = {}
            if isinstance(args, list):
                for a in args:
                    if isinstance(a, dict) and a.get("name"):
                        arg_map[a["name"]] = a.get("type") or {}
            out[f["name"]] = arg_map

        _TYPE_FIELD_ARG_SPECS_CACHE[type_name] = out
        return out
    except Exception as e:
        logger.warning("Could not introspect %s field arg specs; using empty dict: %s", type_name, e)
        _TYPE_FIELD_ARG_SPECS_CACHE[type_name] = {}
        return _TYPE_FIELD_ARG_SPECS_CACHE[type_name]


def _unwrap_scalar_name(type_spec: Dict[str, Any]) -> Optional[str]:
    """
    Return underlying SCALAR name (e.g. Boolean/String/Int) for a type_spec, else None.
    """
    t = type_spec
    seen = 0
    while isinstance(t, dict) and seen < 10:
        seen += 1
        kind = t.get("kind")
        name = t.get("name")
        of_type = t.get("ofType")
        if kind == "SCALAR" and isinstance(name, str) and name:
            return name
        if kind == "NON_NULL" and isinstance(of_type, dict):
            t = of_type
            continue
        if kind == "LIST" and isinstance(of_type, dict):
            t = of_type
            continue
        break
    return None


def _unwrap_named_type(type_spec: Dict[str, Any]) -> Optional[str]:
    """
    Unwrap NON_NULL/LIST wrappers to get the underlying named type (OBJECT/SCALAR/ENUM name).
    Returns None if no name is available.
    """
    t = type_spec
    seen = 0
    while isinstance(t, dict) and seen < 10:
        seen += 1
        kind = t.get("kind")
        name = t.get("name")
        of_type = t.get("ofType")
        if name:
            return name
        if kind in ("NON_NULL", "LIST") and isinstance(of_type, dict):
            t = of_type
            continue
        break
    return None


def _pick_first_available_field(field_names: set, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in field_names:
            return c
    return None


def _is_scalar_or_list_of_scalar(t: Dict[str, Any]) -> bool:
    """
    Return True if the GraphQL type is a scalar (or list of scalar).
    We only select these fields directly (object fields would require sub-selections).
    """
    if not isinstance(t, dict):
        return False

    kind = t.get("kind")

    of_type = t.get("ofType")

    # SCALAR / ENUM
    if kind in ("SCALAR", "ENUM"):
        return True

    # NON_NULL wrapper
    if kind == "NON_NULL" and isinstance(of_type, dict):
        return _is_scalar_or_list_of_scalar(of_type)

    # LIST wrapper
    if kind == "LIST" and isinstance(of_type, dict):
        return _is_scalar_or_list_of_scalar(of_type)

    # Anything else (OBJECT, INTERFACE, UNION, INPUT_OBJECT)
    return False


def _normalize_openneuro_modalities(raw: Any) -> Optional[str]:
    """
    Map OpenNeuro modality-like values to the canonical modality labels used by the API.
    Stores as a comma-separated string of canonical labels.
    """
    allowed = {
        "Behavioral",
        "Calcium Imaging",
        "Clinical",
        "ECG",
        "EEG",
        "Electrophysiology",
        "fMRI",
        "iEEG",
        "MEG",
        "MRI",
        "NIRS",
        "PET",
        "Survey",
        "X-ray",
    }

    # Normalize input to a flat list of strings
    items: List[str] = []
    if raw is None:
        items = []
    elif isinstance(raw, str):
        # Split comma/semicolon/pipe-delimited strings (common in free-text metadata)
        parts = [p.strip() for p in re.split(r"[,;/|]+", raw) if p and p.strip()]
        items = parts or [raw]
    elif isinstance(raw, list):
        items = [str(x) for x in raw if x is not None]
    else:
        items = [str(raw)]

    mapped: List[str] = []
    for item in items:
        s = item.strip()
        if not s:
            continue
        k = s.lower()

        # Common mappings / heuristics
        # BIDS datatype / scan-type strings commonly seen in OpenNeuro
        if k in ("func", "bold", "sbref") or "_bold" in k or "_sbref" in k:
            mapped.append("fMRI")
        elif k in ("anat", "dwi") or "t1w" in k or "t2w" in k or "flair" in k:
            mapped.append("MRI")
        elif k in ("beh", "behavioral", "behavioural"):
            mapped.append("Behavioral")
        elif "fmri" in k:
            mapped.append("fMRI")
        elif k in ("mri",) or "structural" in k or "diffusion" in k or "dwi" in k:
            mapped.append("MRI")
        elif "eeg" in k:
            mapped.append("EEG")
        elif "ieeg" in k or "intracranial" in k:
            mapped.append("iEEG")
        elif "meg" in k:
            mapped.append("MEG")
        elif "nirs" in k:
            mapped.append("NIRS")
        elif "pet" in k:
            mapped.append("PET")
        elif "ecg" in k:
            mapped.append("ECG")
        elif "xray" in k or "x-ray" in k or "x ray" in k:
            mapped.append("X-ray")
        elif "calcium" in k:
            mapped.append("Calcium Imaging")
        elif "electrophysiology" in k or "ephys" in k:
            mapped.append("Electrophysiology")
        elif "behavior" in k:
            mapped.append("Behavioral")
        elif "clinical" in k:
            mapped.append("Clinical")
        elif "survey" in k or "questionnaire" in k:
            mapped.append("Survey")
        else:
            # If OpenNeuro already uses canonical spelling, keep it.
            # (e.g. "EEG", "fMRI")
            if s in allowed:
                mapped.append(s)

    mapped = sorted(set([m for m in mapped if m in allowed]))
    return ", ".join(mapped) if mapped else None


def _infer_modalities_from_bids_paths(paths: List[str]) -> Optional[str]:
    """
    Infer modalities from a BIDS directory structure.

    We look for BIDS datatype directories (anat/ func/ dwi/ pet/ eeg/ ieeg/ meg/ nirs/ beh/)
    anywhere in the path and map them to the API's canonical modality labels.
    """
    if not paths:
        return None

    # Normalize to lower-case path segments + filename heuristics
    found = set()
    for p in paths:
        if not isinstance(p, str):
            continue
        p_norm = p.replace("\\", "/")
        parts = [seg for seg in p_norm.split("/") if seg]
        parts_l = [seg.lower() for seg in parts]
        filename_l = parts_l[-1] if parts_l else p_norm.lower()

        if "func" in parts_l:
            found.add("fMRI")
        if "anat" in parts_l or "dwi" in parts_l:
            found.add("MRI")
        if "pet" in parts_l:
            found.add("PET")
        if "eeg" in parts_l:
            found.add("EEG")
        if "ieeg" in parts_l:
            found.add("iEEG")
        if "meg" in parts_l:
            found.add("MEG")
        if "nirs" in parts_l:
            found.add("NIRS")
        if "beh" in parts_l:
            found.add("Behavioral")

        # Filename-level BIDS heuristics (helps when listing is shallow)
        if "_bold" in filename_l or "_sbref" in filename_l:
            found.add("fMRI")
        if "_t1w" in filename_l or "_t2w" in filename_l or "_flair" in filename_l or "_dwi" in filename_l:
            found.add("MRI")
        if "_pet" in filename_l:
            found.add("PET")
        # Common file extensions by modality
        if filename_l.endswith((".fif", ".ds")):
            found.add("MEG")
        if filename_l.endswith((".vhdr", ".vmrk", ".eeg", ".edf", ".bdf")):
            found.add("EEG")
        if filename_l.endswith((".snirf",)):
            found.add("NIRS")

    return ", ".join(sorted(found)) if found else None


def _fetch_snapshot_paths_for_bids(dataset_id: str, tag: str) -> List[str]:
    """
    Fetch a (potentially truncated) list of file paths for a snapshot so we can infer BIDS datatypes.

    This is best-effort: OpenNeuro occasionally has resolver issues on snapshot-related fields.
    """
    query = """
    query GetSnapshotFiles($datasetId: ID!, $tag: String!) {
      snapshot(datasetId: $datasetId, tag: $tag) {
        files {
          filename
        }
      }
    }
    """

    data = openneuro_graphql(
        query=query,
        operation_name="GetSnapshotFiles",
        variables={"datasetId": dataset_id, "tag": tag},
        timeout=90,
        allow_partial=True,
    )

    # NOTE: With allow_partial=True, OpenNeuro can return `snapshot: null` alongside errors.
    snap_obj = (data.get("data") or {}).get("snapshot") or {}
    files = snap_obj.get("files", [])
    paths: List[str] = []
    if isinstance(files, list):
        for f in files:
            if isinstance(f, dict) and isinstance(f.get("filename"), str):
                paths.append(f["filename"])

    # Cap to a reasonable number so we don't blow up memory/XCom; we only need to see datatypes.
    return paths[:500]


def _get_latest_snapshot_tag(dataset_id: str) -> Optional[str]:
    """
    Best-effort fetch of a snapshot tag for a dataset.

    Prefer the dataset's `snapshots` field (when available) instead of `latestSnapshot`,
    which has been observed to intermittently fail server-side (resolver ECONNREFUSED).
    """
    tag, _created = _get_latest_snapshot_tag_and_created(dataset_id)
    return tag


def _get_latest_snapshot_tag_and_created(dataset_id: str) -> tuple[Optional[str], Optional[datetime]]:
    """
    Best-effort fetch of a "best" snapshot tag + its created timestamp.
    """
    dataset_specs = _get_dataset_field_specs()
    dataset_fields = set(dataset_specs.keys())

    # Prefer dataset.snapshots(...)
    if "snapshots" in dataset_fields:
        dataset_args = _get_type_field_arg_names("Dataset")
        snapshots_args = dataset_args.get("snapshots", [])

        arg_parts: List[str] = []
        if "first" in snapshots_args:
            arg_parts.append("first: 1")
        elif "last" in snapshots_args:
            arg_parts.append("last: 1")
        arg_str = f"({', '.join(arg_parts)})" if arg_parts else ""

        snapshots_spec = dataset_specs.get("snapshots", {}) or {}
        snapshots_type = _unwrap_named_type(snapshots_spec) or ""
        snapshots_type_fields = _get_type_field_names(snapshots_type) if snapshots_type else set()

        # Build a selection that matches the shape (connection vs list) when possible.
        if snapshots_type == "Snapshot" or ("tag" in snapshots_type_fields and "created" in snapshots_type_fields):
            snapshots_selection = f"snapshots{arg_str} {{ tag created }}"
        elif "edges" in snapshots_type_fields:
            snapshots_selection = f"snapshots{arg_str} {{ edges {{ node {{ tag created }} }} }}"
        elif "nodes" in snapshots_type_fields:
            snapshots_selection = f"snapshots{arg_str} {{ nodes {{ tag created }} }}"
        else:
            # Best-effort fallback
            snapshots_selection = f"snapshots{arg_str} {{ tag created }}"

        query = f"""
        query GetSnapshotTagFromSnapshots($id: ID!) {{
          dataset(id: $id) {{
            {snapshots_selection}
          }}
        }}
        """
        data = openneuro_graphql(
            query=query,
            operation_name="GetSnapshotTagFromSnapshots",
            variables={"id": dataset_id},
            timeout=90,
            allow_partial=True,
        )
        ds = data.get("data", {}).get("dataset") or {}
        snaps = ds.get("snapshots")

        # Parse possible shapes
        candidate = None
        if isinstance(snaps, list) and snaps:
            candidate = snaps[0]
        elif isinstance(snaps, dict):
            if isinstance(snaps.get("nodes"), list) and snaps["nodes"]:
                candidate = snaps["nodes"][0]
            elif isinstance(snaps.get("edges"), list) and snaps["edges"]:
                edge0 = snaps["edges"][0]
                if isinstance(edge0, dict):
                    candidate = edge0.get("node")
            elif "tag" in snaps:
                candidate = snaps

        if isinstance(candidate, dict):
            tag = candidate.get("tag")
            created = _parse_iso8601(candidate.get("created"))
            if isinstance(tag, str) and tag:
                return tag, created

    # Fallback: try dataset.latestSnapshot.tag (best-effort; allow_partial avoids hard-fail)
    if "latestSnapshot" in dataset_fields:
        query = """
        query GetLatestSnapshotTag($id: ID!) {
          dataset(id: $id) {
            latestSnapshot {
              tag
              created
            }
          }
        }
        """
        data = openneuro_graphql(
            query=query,
            operation_name="GetLatestSnapshotTag",
            variables={"id": dataset_id},
            timeout=90,
            allow_partial=True,
        )
        # NOTE: With allow_partial=True, OpenNeuro can return `dataset: null` alongside errors.
        ds_obj = (data.get("data") or {}).get("dataset") or {}
        latest = ds_obj.get("latestSnapshot")
        if isinstance(latest, dict):
            tag = latest.get("tag")
            created = _parse_iso8601(latest.get("created"))
            if isinstance(tag, str) and tag:
                return tag, created

    return None, None


def _get_readme_from_latest_snapshot(dataset_id: str, tag: str) -> Optional[str]:
    """
    Best-effort fetch of README content for a snapshot.
    Prefer snapshot-level `readme` field if the schema supports it.
    """
    snapshot_fields = _get_type_field_names("Snapshot")
    if "readme" not in snapshot_fields:
        return None

    query = """
    query GetSnapshotReadme($datasetId: ID!, $tag: String!) {
      snapshot(datasetId: $datasetId, tag: $tag) {
        readme
      }
    }
    """
    data = openneuro_graphql(
        query=query,
        operation_name="GetSnapshotReadme",
        variables={"datasetId": dataset_id, "tag": tag},
        timeout=90,
        allow_partial=True,
    )
    # NOTE: With allow_partial=True, OpenNeuro can return `snapshot: null` alongside errors.
    snap_obj = (data.get("data") or {}).get("snapshot") or {}
    readme = snap_obj.get("readme")
    return readme if isinstance(readme, str) and readme.strip() else None


def _get_snapshot_description_and_paths(
    dataset_id: str,
    tag: str,
    include_files: bool = False,
) -> tuple[Optional[str], Optional[str], List[str]]:
    """
    Fetch snapshot-level description + files for a dataset snapshot.

    Returns: (description_text_256, license_text, paths[])
    - description_text is derived from snapshot.description.Description (preferred) or snapshot.description.Name.
    - license_text is derived from snapshot.description.License when present.
    """
    snapshot_specs = _get_type_field_specs("Snapshot")
    snapshot_fields = set(snapshot_specs.keys())
    snapshot_args = _get_type_field_arg_names("Snapshot")
    snapshot_arg_specs = _get_type_field_arg_specs("Snapshot")

    selection_parts: List[str] = []

    # --- files (optional; expensive) ---
    if include_files and "files" in snapshot_fields:
        # Determine how to call files() based on its args.
        files_args = snapshot_args.get("files", [])
        files_arg_specs = (snapshot_arg_specs.get("files") or {})
        files_call = "files"
        # Many schemas require a path argument; provide root path if present.
        if "path" in files_args:
            files_call = 'files(path: "")'
        # Some schemas support pagination; request a moderate page if available.
        if "first" in files_args:
            # Merge with existing call
            if "(" in files_call:
                files_call = files_call[:-1] + ", first: 500)"
            else:
                files_call = "files(first: 500)"
        # IMPORTANT:
        # Some schemas support recursive/tree listing, but types vary across deployments.
        # We *must* send the right literal type, otherwise GraphQL validation fails:
        #   "String cannot represent a non string value: true"
        if "recursive" in files_args:
            scalar = _unwrap_scalar_name(files_arg_specs.get("recursive", {})) or ""
            val = "true" if scalar == "Boolean" else "\"true\""
            if "(" in files_call:
                files_call = files_call[:-1] + f", recursive: {val})"
            else:
                files_call = f"files(recursive: {val})"
        if "tree" in files_args:
            scalar = _unwrap_scalar_name(files_arg_specs.get("tree", {})) or ""
            val = "true" if scalar == "Boolean" else "\"true\""
            if "(" in files_call:
                files_call = files_call[:-1] + f", tree: {val})"
            else:
                files_call = f"files(tree: {val})"

        # Determine the path field name for file entries and handle connection shapes.
        files_type = _unwrap_named_type(snapshot_specs.get("files", {})) or "File"
        files_type_specs = _get_type_field_specs(files_type)
        files_type_fields = set(files_type_specs.keys())

        def _pick_path_field(file_obj_type: str) -> str:
            file_specs = _get_type_field_specs(file_obj_type)
            file_fields = set(file_specs.keys())
            for cand in ["filename", "path", "key", "name"]:
                if cand in file_fields:
                    return cand
            return "filename"

        if "edges" in files_type_fields:
            edge_type = _unwrap_named_type(files_type_specs.get("edges", {})) or ""
            edge_specs = _get_type_field_specs(edge_type) if edge_type else {}
            node_type = _unwrap_named_type(edge_specs.get("node", {})) or "File"
            path_field = _pick_path_field(node_type)
            selection_parts.append(
                f"""{files_call} {{
          edges {{
            node {{
              {path_field}
            }}
          }}
        }}"""
            )
        elif "nodes" in files_type_fields:
            nodes_type = _unwrap_named_type(files_type_specs.get("nodes", {})) or "File"
            path_field = _pick_path_field(nodes_type)
            selection_parts.append(
                f"""{files_call} {{
          nodes {{
            {path_field}
          }}
        }}"""
            )
        else:
            # Assume list-of-file objects
            path_field = _pick_path_field(files_type)
            selection_parts.append(
                f"""{files_call} {{
          {path_field}
        }}"""
            )

    # --- description (from dataset_description.json) ---
    desc_text_256: Optional[str] = None
    license_text: Optional[str] = None

    # --- readme (preferred for UI description) ---
    if "readme" in snapshot_fields and _is_scalar_or_list_of_scalar(snapshot_specs.get("readme", {})):
        selection_parts.append("readme")

    if "description" in snapshot_fields:
        desc_type = _unwrap_named_type(snapshot_specs.get("description", {}))
        # If it's a scalar, just fetch it directly
        if _is_scalar_or_list_of_scalar(snapshot_specs.get("description", {})):
            selection_parts.append("description")
        # If it's an object, try to select common fields if present
        elif desc_type:
            desc_specs = _get_type_field_specs(desc_type)
            # OpenNeuro tends to use these keys in dataset_description.json-derived types
            wanted = ["Name", "Description", "License", "DatasetDOI", "HowToAcknowledge", "name", "description", "license"]
            available = [w for w in wanted if w in desc_specs]
            if available:
                selection_parts.append(
                    "description {\n          " + "\n          ".join(available) + "\n        }"
                )

    if not selection_parts:
        return None, None, []

    selection = "\n        ".join(selection_parts)
    query = f"""
    query GetSnapshotDescription($datasetId: ID!, $tag: String!) {{
      snapshot(datasetId: $datasetId, tag: $tag) {{
        {selection}
      }}
    }}
    """

    data = openneuro_graphql(
        query=query,
        operation_name="GetSnapshotDescription",
        variables={"datasetId": dataset_id, "tag": tag},
        timeout=90,
        allow_partial=True,
    )

    snap = data.get("data", {}).get("snapshot", {}) or {}

    # Extract file paths (supports list, connection.edges.node, connection.nodes)
    # Only present when include_files=True.
    files = snap.get("files", [])
    paths: List[str] = []
    if isinstance(files, list):
        for f in files:
            if isinstance(f, dict):
                for cand in ["filename", "path", "key", "name"]:
                    if isinstance(f.get(cand), str):
                        paths.append(f[cand])
                        break
    elif isinstance(files, dict):
        nodes = files.get("nodes")
        edges = files.get("edges")
        if isinstance(nodes, list):
            for n in nodes:
                if isinstance(n, dict):
                    for cand in ["filename", "path", "key", "name"]:
                        if isinstance(n.get(cand), str):
                            paths.append(n[cand])
                            break
        elif isinstance(edges, list):
            for e in edges:
                if not isinstance(e, dict):
                    continue
                n = e.get("node")
                if isinstance(n, dict):
                    for cand in ["filename", "path", "key", "name"]:
                        if isinstance(n.get(cand), str):
                            paths.append(n[cand])
                            break
    paths = paths[:500]


    # Extract description/license: prefer README (256 chars), fall back to dataset_description.json
    readme_raw = snap.get("readme")
    if isinstance(readme_raw, str):
        desc_text_256 = _readme_to_description(readme_raw, max_len=256) or desc_text_256

    desc = snap.get("description")
    if isinstance(desc, dict):
        # Use Description if present; otherwise Name
        if not desc_text_256:
            desc_raw = desc.get("Description") or desc.get("description") or desc.get("Name") or desc.get("name")
            desc_text_256 = _readme_to_description(desc_raw, max_len=256)
        lic = desc.get("License") or desc.get("license")
        if isinstance(lic, str):
            license_text = lic
        elif lic is not None:
            license_text = str(lic)
    elif isinstance(desc, str):
        if not desc_text_256:
            desc_text_256 = _readme_to_description(desc, max_len=256)

    return desc_text_256, license_text, paths


def _get_snapshot_description_name(dataset_id: str, tag: str) -> Optional[str]:
    """
    Best-effort fetch of snapshot description Name (dataset title).
    Returns None if unavailable.
    """
    snapshot_specs = _get_type_field_specs("Snapshot")
    snapshot_fields = set(snapshot_specs.keys())

    if "description" not in snapshot_fields:
        return None

    selection_parts: List[str] = []
    desc_type = _unwrap_named_type(snapshot_specs.get("description", {}))
    # If description is scalar, fetch it directly
    if _is_scalar_or_list_of_scalar(snapshot_specs.get("description", {})):
        selection_parts.append("description")
    elif desc_type:
        desc_specs = _get_type_field_specs(desc_type)
        wanted = ["Name", "name"]
        available = [w for w in wanted if w in desc_specs]
        if available:
            selection_parts.append(
                "description {\n          " + "\n          ".join(available) + "\n        }"
            )

    if not selection_parts:
        return None

    selection = "\n        ".join(selection_parts)
    query = f"""
    query GetSnapshotDescriptionName($datasetId: ID!, $tag: String!) {{
      snapshot(datasetId: $datasetId, tag: $tag) {{
        {selection}
      }}
    }}
    """

    data = openneuro_graphql(
        query=query,
        operation_name="GetSnapshotDescriptionName",
        variables={"datasetId": dataset_id, "tag": tag},
        timeout=90,
        allow_partial=True,
    )

    snap = data.get("data", {}).get("snapshot") or {}
    desc = snap.get("description")
    if isinstance(desc, dict):
        name_val = desc.get("Name") or desc.get("name")
        if isinstance(name_val, str):
            name_val = name_val.strip()
            return name_val if name_val else None
    elif isinstance(desc, str):
        desc = desc.strip()
        return desc if desc else None

    return None


def _get_snapshot_tags(dataset_id: str, limit: int = 50) -> List[tuple[str, Optional[datetime]]]:
    """
    Best-effort fetch of snapshot tags for a dataset.
    Returns list of (tag, created_at) tuples.
    """
    dataset_specs = _get_dataset_field_specs()
    dataset_fields = set(dataset_specs.keys())

    if "snapshots" not in dataset_fields:
        return []

    dataset_args = _get_type_field_arg_names("Dataset")
    snapshots_args = dataset_args.get("snapshots", [])

    arg_parts: List[str] = []
    if "first" in snapshots_args:
        arg_parts.append(f"first: {limit}")
    elif "last" in snapshots_args:
        arg_parts.append(f"last: {limit}")
    arg_str = f"({', '.join(arg_parts)})" if arg_parts else ""

    snapshots_spec = dataset_specs.get("snapshots", {}) or {}
    snapshots_type = _unwrap_named_type(snapshots_spec) or ""
    snapshots_type_fields = _get_type_field_names(snapshots_type) if snapshots_type else set()

    # Build a selection that matches the shape (connection vs list) when possible.
    if snapshots_type == "Snapshot" or ("tag" in snapshots_type_fields and "created" in snapshots_type_fields):
        snapshots_selection = f"snapshots{arg_str} {{ tag created }}"
    elif "edges" in snapshots_type_fields:
        snapshots_selection = f"snapshots{arg_str} {{ edges {{ node {{ tag created }} }} }}"
    elif "nodes" in snapshots_type_fields:
        snapshots_selection = f"snapshots{arg_str} {{ nodes {{ tag created }} }}"
    else:
        snapshots_selection = f"snapshots{arg_str} {{ tag created }}"

    query = f"""
    query GetSnapshotTags($id: ID!) {{
      dataset(id: $id) {{
        {snapshots_selection}
      }}
    }}
    """
    data = openneuro_graphql(
        query=query,
        operation_name="GetSnapshotTags",
        variables={"id": dataset_id},
        timeout=90,
        allow_partial=True,
    )
    ds = data.get("data", {}).get("dataset") or {}
    snaps = ds.get("snapshots")

    candidates: List[dict] = []
    if isinstance(snaps, list):
        candidates = [s for s in snaps if isinstance(s, dict)]
    elif isinstance(snaps, dict):
        if isinstance(snaps.get("nodes"), list):
            candidates = [n for n in snaps["nodes"] if isinstance(n, dict)]
        elif isinstance(snaps.get("edges"), list):
            for e in snaps["edges"]:
                if isinstance(e, dict) and isinstance(e.get("node"), dict):
                    candidates.append(e["node"])
        elif "tag" in snaps:
            candidates = [snaps]

    results: List[tuple[str, Optional[datetime]]] = []
    for c in candidates:
        tag = c.get("tag")
        created = _parse_iso8601(c.get("created")) if isinstance(c.get("created"), str) else None
        if isinstance(tag, str) and tag:
            results.append((tag, created))

    return results


def _readme_to_description(readme: Any, max_len: int = 256) -> Optional[str]:
    if not isinstance(readme, str):
        return None
    # Collapse whitespace/newlines so it's UI-friendly
    text = re.sub(r"\s+", " ", readme).strip()
    if not text:
        return None
    return text[:max_len]


def openneuro_graphql(
    query: str,
    operation_name: str,
    variables: Optional[Dict[str, Any]] = None,
    timeout: int = 90,
    allow_partial: bool = False,
    max_retries: int = 6,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    """
    Perform a GraphQL POST to OpenNeuro's Apollo Server.

    OpenNeuro's GraphQL server has CSRF protections enabled; to avoid 400 responses,
    we must send a non-simple Content-Type and/or Apollo operation headers.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        # Apollo CSRF protection: provide at least one of these headers with a non-empty value.
        "x-apollo-operation-name": operation_name,
        "apollo-require-preflight": "true",
    }

    payload: Dict[str, Any] = {
        "query": query,
        "operationName": operation_name,
        "variables": variables or {},
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            with _OPENNEURO_SEM:
                # Global pacing across threads/processes in this scheduler process
                with _OPENNEURO_RATE_LOCK:
                    global _OPENNEURO_LAST_REQUEST_AT
                    now = time.monotonic()
                    wait = _OPENNEURO_MIN_INTERVAL_SECONDS - (now - _OPENNEURO_LAST_REQUEST_AT)
                    if wait > 0:
                        time.sleep(wait)
                    _OPENNEURO_LAST_REQUEST_AT = time.monotonic()

                resp = requests.post(
                    OPENNEURO_GRAPHQL_URL,
                    json=payload,
                    headers=headers,
                    timeout=timeout,
                )

            # Handle transient HTTP statuses with backoff (rate limits / upstream issues).
            if resp.status_code in (429, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = None
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = None
                if wait is None:
                    wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)

                logger.warning(
                    "OpenNeuro GraphQL transient HTTP %s (attempt %d/%d). Sleeping %.1fs. operation=%s body=%s",
                    resp.status_code,
                    attempt,
                    max_retries,
                    wait,
                    operation_name,
                    resp.text,
                )
                time.sleep(wait)
                continue

            break
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            wait = min(backoff_seconds * (2 ** (attempt - 1)), 60.0)
            logger.warning(
                "OpenNeuro GraphQL request failed (attempt %d/%d). Sleeping %.1fs. operation=%s error=%s",
                attempt,
                max_retries,
                wait,
                operation_name,
                e,
            )
            time.sleep(wait)
    else:
        # Exhausted retries
        raise last_exc or Exception("OpenNeuro GraphQL request failed after retries")

    # Log the body for non-200s to make debugging GraphQL validation errors easy.
    if resp.status_code != 200:
        logger.error(
            "OpenNeuro GraphQL non-200: status=%s operation=%s body=%s",
            resp.status_code,
            operation_name,
            resp.text,
        )

    resp.raise_for_status()

    try:
        data = resp.json()
    except ValueError:
        logger.error(
            "OpenNeuro GraphQL returned non-JSON response: operation=%s body=%s",
            operation_name,
            resp.text,
        )
        raise

    # GraphQL errors can still come back with 200.
    # Some queries may return partial data with errors for a subset of fields/resolvers.
    if isinstance(data, dict) and data.get("errors"):
        if allow_partial:
            logger.warning(
                "OpenNeuro GraphQL returned partial errors (continuing): operation=%s errors=%s",
                operation_name,
                data["errors"],
            )
        else:
            logger.error("OpenNeuro GraphQL errors: operation=%s errors=%s", operation_name, data["errors"])
            raise Exception(f"GraphQL query failed: {data['errors']}")

    return data


def _parse_iso8601(dt_str: str) -> Optional[datetime]:
    """Parse ISO8601 timestamps, return None on failure."""
    if not isinstance(dt_str, str):
        return None
    try:
        if dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        logger.warning("Could not parse timestamp '%s'", dt_str)
        return None


def parse_openneuro_dataset(dataset: Dict[str, Any]) -> Dict[str, Any]:
    """Parse an OpenNeuro dataset from GraphQL response."""
    dataset_id = dataset.get("id", "")

    # Prefer OpenNeuro dataset name (human-readable), fallback to dataset_id.
    # Some schemas expose this under `description.Name` instead of a top-level `name`.
    desc_obj = dataset.get("description") if isinstance(dataset.get("description"), dict) else {}
    title = dataset.get("name") or (desc_obj.get("Name") if isinstance(desc_obj, dict) else None) or dataset_id or None

    # Timestamps: use Dataset.created (available in the list query)
    created_at = _parse_iso8601(dataset.get("created"))

    # Prefer a dataset-level modified field (if present in the schema/query), fallback to created_at.
    updated_at = _parse_iso8601(dataset.get("modified") or dataset.get("updated") or dataset.get("lastModified")) or created_at

    # `DatasetPermissions.public` is not present in the schema; use the dataset-level flag.
    public = bool(dataset.get("public", True))
    
    # Analytics for downloads/views
    analytics = dataset.get("analytics") or {}
    downloads = analytics.get("downloads", 0)
    views = analytics.get("views", 0)

    # Legacy field (no longer maintained). Keep nullable for backward compatibility.
    citations = None
    # Number of associated papers. Not computed for OpenNeuro yet; keep nullable.
    papers = None
    
    # Build URL
    url = f"https://openneuro.org/datasets/{dataset_id}" if dataset_id else ""
    
    # Modality/tags are best populated by the enrich step; keep nullable here unless present.
    modality = None
    if isinstance(desc_obj, dict):
        modality = _normalize_openneuro_modalities(desc_obj.get("Modality"))
    if not modality:
        modality = _normalize_openneuro_modalities(dataset.get("modalities") or dataset.get("modality"))

    description_text = _readme_to_description(dataset.get("readme"))
    license_text = dataset.get("license")
    if isinstance(license_text, list):
        license_text = ", ".join([str(x) for x in license_text if x is not None]) or None
    elif license_text is not None and not isinstance(license_text, str):
        license_text = str(license_text)
    
    return {
        "dataset_id": dataset_id,
        "title": title,
        "modality": modality,
        "citations": citations,
        "papers": papers,
        "url": url,
        "description": description_text,
        "license": license_text,
        "created_at": created_at,
        "updated_at": updated_at,
        "public": public,
        "downloads": downloads,
        "views": views,
    }


def create_openneuro_table(**context):
    """Create the openneuro_dataset table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS openneuro_dataset (
        id SERIAL PRIMARY KEY,
        dataset_id VARCHAR(255) NOT NULL UNIQUE,
        title TEXT,
        modality TEXT,
        -- Legacy field (no longer maintained). Keep nullable for backward compatibility.
        citations INTEGER,
        -- Number of associated papers (nullable by default; not computed yet for OpenNeuro).
        papers INTEGER,
        url TEXT,
        description TEXT,
        license TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        public BOOLEAN DEFAULT true,
        downloads INTEGER DEFAULT 0,
        views INTEGER DEFAULT 0
    );
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                # Allow schema evolution without forcing a full drop/recreate.
                cursor.execute("ALTER TABLE openneuro_dataset ADD COLUMN IF NOT EXISTS license TEXT;")
                cursor.execute("ALTER TABLE openneuro_dataset ADD COLUMN IF NOT EXISTS papers INTEGER;")
                # Indexes (create after columns exist, otherwise upgrades can fail)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_openneuro_dataset_id ON openneuro_dataset(dataset_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_openneuro_modality ON openneuro_dataset(modality);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_openneuro_papers ON openneuro_dataset(papers DESC);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_openneuro_public ON openneuro_dataset(public);")
                conn.commit()
        logger.info("Successfully created openneuro_dataset table (or it already exists)")
    except Exception as e:
        logger.error("Error creating openneuro_dataset table: %s", e)
        raise


def fetch_openneuro_datasets(**context) -> List[Dict[str, Any]]:
    """Fetch datasets from OpenNeuro GraphQL API."""
    num_datasets = context.get('params', {}).get('num_datasets', 50)

    # Build the Dataset node selection based on schema availability.
    # NOTE: We intentionally do NOT request `latestSnapshot` here because OpenNeuro sometimes
    # fails resolving it (server-side ECONNREFUSED), which would break pagination.
    dataset_field_specs = _get_dataset_field_specs()
    dataset_fields = set(dataset_field_specs.keys())

    name_field = "name" if "name" in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get("name", {})) else None






    # Keep the XCom payload small: only fetch identifiers (+ optional name for nicer logs).
    # Detailed metadata (analytics, license, readme, etc.) is fetched per-dataset in the enrich step.
    node_lines = ["id"]
    if name_field:
        node_lines.append(name_field)

    node_selection = "\n            ".join(node_lines)

    # GraphQL query to fetch datasets with pagination
    query = f"""
    query GetDatasets($cursor: String) {{
      datasets(first: 100, after: $cursor) {{
        edges {{
          node {{
            {node_selection}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    
    datasets: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    has_next_page = True
    
    try:
        while len(datasets) < num_datasets and has_next_page:
            logger.info(
                "Fetching OpenNeuro datasets: %d/%d fetched so far",
                len(datasets),
                num_datasets,
            )
            
            data = openneuro_graphql(
                query=query,
                operation_name="GetDatasets",
                variables={"cursor": cursor},
                timeout=30,
                # In case OpenNeuro returns partial errors, don't fail the whole page.
                allow_partial=True,
            )
            
            # Extract datasets from response
            datasets_data = data.get('data', {}).get('datasets', {})
            edges = datasets_data.get('edges', [])
            page_info = datasets_data.get('pageInfo', {})
            
            if not edges:
                logger.info("No more datasets available from OpenNeuro API")
                break
            
            for edge in edges:
                if len(datasets) >= num_datasets:
                    break
                
                node = edge.get('node', {}) or {}
                dataset_id = node.get("id") if isinstance(node, dict) else None
                if not isinstance(dataset_id, str) or not dataset_id:
                    logger.warning(
                        "Skipping dataset with missing identifier: %s", node
                    )
                    continue

                title = None
                if isinstance(node, dict) and name_field and isinstance(node.get(name_field), str):
                    title = node.get(name_field)

                datasets.append(
                    {
                        "dataset_id": dataset_id,
                        "title": title or dataset_id,
                        "url": f"https://openneuro.org/datasets/{dataset_id}",
                    }
                )
                logger.info(
                    "Fetched dataset: %s - %s",
                    dataset_id,
                    title or dataset_id,
                )
            
            # Update pagination info
            has_next_page = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')
        
        logger.info("Successfully fetched %d datasets from OpenNeuro API", len(datasets))
        return datasets
    
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching datasets from OpenNeuro API: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error while fetching OpenNeuro datasets: %s", e)
        raise


def _enrich_single_dataset(ds: Dict[str, Any]) -> tuple:
    """
    Enrich a single dataset by fetching detailed metadata from OpenNeuro GraphQL API.
    Returns a tuple of (enriched_dataset, stats_dict).
    """
    dataset_id = ds.get("dataset_id")
    enriched_ds = ds.copy()
    stats = {
        "enriched_desc": 0,
        "enriched_modality": 0,
        "enriched_metadata": 0,
        "skipped_no_id": 0,
        "request_errors": 0,
    }
    
    if not dataset_id:
        stats["skipped_no_id"] = 1
        return enriched_ds, stats

    # Ensure stable defaults even if fetch returned minimal fields
    if not enriched_ds.get("url"):
        enriched_ds["url"] = f"https://openneuro.org/datasets/{dataset_id}"
    
    dataset_field_specs = _get_dataset_field_specs()
    dataset_fields = set(dataset_field_specs.keys())

    # Only select scalar/list-of-scalar fields directly.
    selectable: List[str] = ["id", "created"]

    for cand in ["name"]:
        if cand in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get(cand, {})):
            selectable.append(cand)
            break

    for cand in ["modified", "updated", "lastModified"]:
        if cand in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get(cand, {})):
            selectable.append(cand)
            break

    if "public" in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get("public", {})):
        selectable.append("public")

    for cand in ["modalities", "modality"]:
        if cand in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get(cand, {})):
            selectable.append(cand)
            break

    # metadata.modalities is the primary source of modality info on OpenNeuro!
    if "metadata" in dataset_fields and not _is_scalar_or_list_of_scalar(dataset_field_specs.get("metadata", {})):
        metadata_type = _unwrap_named_type(dataset_field_specs.get("metadata", {}) or {})
        if metadata_type:
            metadata_specs = _get_type_field_specs(metadata_type)
            if "modalities" in metadata_specs:
                selectable.append("metadata {\n          modalities\n        }")

    if "readme" in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get("readme", {})):
        selectable.append("readme")

    if "license" in dataset_fields and _is_scalar_or_list_of_scalar(dataset_field_specs.get("license", {})):
        selectable.append("license")

    # --- description object (preferred source for modality + rich metadata) ---
    if "description" in dataset_fields and not _is_scalar_or_list_of_scalar(dataset_field_specs.get("description", {})):
        desc_type = _unwrap_named_type(dataset_field_specs.get("description", {}) or {})
        if desc_type:
            desc_specs = _get_type_field_specs(desc_type)
            wanted = ["Name", "Description", "License", "DatasetDOI", "DatasetType", "Modality"]
            available = [w for w in wanted if w in desc_specs]
            if available:
                selectable.append(
                    "description {\n          " + "\n          ".join(available) + "\n        }"
                )

    # --- summary object (preferred source for detailed scan-type tags) ---
    if "summary" in dataset_fields and not _is_scalar_or_list_of_scalar(dataset_field_specs.get("summary", {})):
        summ_type = _unwrap_named_type(dataset_field_specs.get("summary", {}) or {})
        if summ_type:
            summ_specs = _get_type_field_specs(summ_type)
            # `summary.modalities` is the "scan types" array from OpenNeuro (e.g. T1w, bold)
            if "modalities" in summ_specs:
                selectable.append("summary {\n          modalities\n        }")

    # NOTE: Avoid selecting `latestSnapshot` here. It has been observed to intermittently
    # fail server-side (resolver ECONNREFUSED). We fetch snapshot tags separately when needed.
    selectable.append(
        """analytics {
          downloads
          views
        }"""
    )

    selection = "\n        ".join(selectable)

    query = f"""
    query GetDataset($id: ID!) {{
      dataset(id: $id) {{
        {selection}
      }}
    }}
    """
    
    try:
        data = openneuro_graphql(
            query=query,
            operation_name="GetDataset",
            variables={"id": dataset_id},
            timeout=30,
            allow_partial=True,
        )
        
        # OpenNeuro can legitimately return `dataset: null` for some ids.
        # Ensure we always work with a dict.
        dataset_data = data.get('data', {}).get('dataset') or {}
        
        if not dataset_data:
            logger.warning("No data returned for dataset %s", dataset_id)
            return enriched_ds, stats

        # created_at: always set when available (fetch may return only IDs)
        if not enriched_ds.get("created_at") and isinstance(dataset_data.get("created"), str):
            enriched_ds["created_at"] = _parse_iso8601(dataset_data.get("created"))
        
        # Title: prefer dataset name, then description.Name
        if dataset_data.get("name"):
            enriched_ds["title"] = dataset_data.get("name")
        desc_obj = dataset_data.get("description") if isinstance(dataset_data.get("description"), dict) else None
        if not enriched_ds.get("title") and isinstance(desc_obj, dict) and isinstance(desc_obj.get("Name"), str):
            enriched_ds["title"] = desc_obj.get("Name")

        # updated_at: prefer a dataset-level modified timestamp
        if dataset_data.get("modified") or dataset_data.get("updated") or dataset_data.get("lastModified"):
            enriched_ds["updated_at"] = (
                _parse_iso8601(dataset_data.get("modified") or dataset_data.get("updated") or dataset_data.get("lastModified"))
                or enriched_ds.get("updated_at")
            )

        # License: prefer description.License, then top-level license
        if isinstance(desc_obj, dict) and desc_obj.get("License") is not None and not enriched_ds.get("license"):
            lic = desc_obj.get("License")
            if isinstance(lic, list):
                lic = ", ".join([str(x) for x in lic if x is not None]) or None
            elif lic is not None and not isinstance(lic, str):
                lic = str(lic)
            enriched_ds["license"] = lic

        # License (fallback)
        if dataset_data.get("license") is not None:
            lic = dataset_data.get("license")
            if isinstance(lic, list):
                lic = ", ".join([str(x) for x in lic if x is not None]) or None
            elif lic is not None and not isinstance(lic, str):
                lic = str(lic)
            enriched_ds["license"] = lic

        # Description: store README truncated to 256 chars.
        # Prefer dataset-level readme when present; snapshot-level README is fetched later if needed.
        if not enriched_ds.get("description") and isinstance(dataset_data.get("readme"), str):
            desc = _readme_to_description(dataset_data.get("readme"), max_len=256)
            if desc:
                enriched_ds["description"] = desc
                stats["enriched_desc"] = 1
        # Fallback description: description.Description
        if not enriched_ds.get("description") and isinstance(desc_obj, dict) and isinstance(desc_obj.get("Description"), str):
            desc = _readme_to_description(desc_obj.get("Description"), max_len=256)
            if desc:
                enriched_ds["description"] = desc
                stats["enriched_desc"] = 1

        # Modality mapping - check multiple sources in priority order:
        # 1. dataset.metadata.modalities (primary source on OpenNeuro)
        # 2. description.Modality (from dataset_description.json)
        # 3. dataset.modalities/modality (if available as scalar)
        raw_mods = None
        
        # First try metadata.modalities (this is where OpenNeuro stores it!)
        metadata_obj = dataset_data.get("metadata") if isinstance(dataset_data.get("metadata"), dict) else None
        if isinstance(metadata_obj, dict):
            metadata_mods = metadata_obj.get("modalities")
            if metadata_mods:
                raw_mods = metadata_mods
                logger.info("Got modalities from metadata.modalities for %s: %r", dataset_id, metadata_mods)
        
        # Fallback to description.Modality
        if raw_mods is None and isinstance(desc_obj, dict):
            raw_mods = desc_obj.get("Modality")
            if raw_mods:
                logger.info("Got modalities from description.Modality for %s: %r", dataset_id, raw_mods)
        
        # Fallback to dataset-level modalities field
        if raw_mods is None:
            raw_mods = dataset_data.get("modalities") or dataset_data.get("modality")
            if raw_mods:
                logger.info("Got modalities from dataset.modalities for %s: %r", dataset_id, raw_mods)
        
        mod = _normalize_openneuro_modalities(raw_mods)

        # Extract scan types from summary.modalities for potential modality inference
        summ_obj = dataset_data.get("summary") if isinstance(dataset_data.get("summary"), dict) else None
        raw_scan_types = None
        if isinstance(summ_obj, dict):
            raw_tags = summ_obj.get("modalities")
            if isinstance(raw_tags, list):
                flat = [str(x).strip() for x in raw_tags if x is not None and str(x).strip()]
                if flat:
                    raw_scan_types = flat
                    logger.info("Got scan types from summary.modalities for %s: %r", dataset_id, flat)
            elif isinstance(raw_tags, str) and raw_tags.strip():
                raw_scan_types = [p.strip() for p in raw_tags.split(",") if p.strip()]
                logger.info("Got scan types from summary.modalities (string) for %s: %r", dataset_id, raw_scan_types)

        # Modality fallback: if the dataset-level modality isn't present/useful, infer from scan types.
        if not mod and raw_scan_types:
            mod = _normalize_openneuro_modalities(raw_scan_types)

        # Always set modality (even if None) so the key exists
        if mod:
            enriched_ds["modality"] = mod
            stats["enriched_modality"] = 1
            logger.info("Set modality=%r for %s from GraphQL fields", mod, dataset_id)
        else:
            enriched_ds["modality"] = None
            # Log what we got from OpenNeuro for the first few datasets to help debug
            if _OPENNEURO_MODALITY_DEBUG:
                with _OPENNEURO_MODALITY_DEBUG_LOCK:
                    global _OPENNEURO_MODALITY_DEBUG_EMITTED
                    if _OPENNEURO_MODALITY_DEBUG_EMITTED < _OPENNEURO_MODALITY_DEBUG_SAMPLES:
                        logger.info(
                            "OpenNeuro modality debug: dataset=%s desc.Modality=%r dataset.modalities=%r summary.modalities=%r",
                            dataset_id,
                            (desc_obj.get("Modality") if isinstance(desc_obj, dict) else None),
                            dataset_data.get("modalities") or dataset_data.get("modality"),
                            (summ_obj.get("modalities") if isinstance(summ_obj, dict) else None),
                        )
                        _OPENNEURO_MODALITY_DEBUG_EMITTED += 1

        # public flag
        if "public" in dataset_data:
            enriched_ds["public"] = bool(dataset_data.get("public"))

        # Determine a snapshot tag for downstream enrichment (avoid relying on `latestSnapshot`).
        snap_tag = "draft"
        try:
            tag, snap_created = _get_latest_snapshot_tag_and_created(dataset_id)
            if tag:
                snap_tag = tag
            # If updated_at is missing or equals created_at, use snapshot created when it differs.
            if snap_created and (
                (not enriched_ds.get("updated_at")) or (enriched_ds.get("updated_at") == enriched_ds.get("created_at"))
            ):
                if enriched_ds.get("created_at") != snap_created:
                    enriched_ds["updated_at"] = snap_created
        except Exception as e:
            logger.warning("Failed to resolve snapshot tag for %s: %s", dataset_id, e)

        # Prefer snapshot description.Name as title when it's meaningful
        try:
            if dataset_id == "ds000102":
                logger.info("Title debug (pre-snapshot): dataset=%s dataset.name=%r tag=%s", dataset_id, enriched_ds.get("title"), snap_tag)
            snap_title = _get_snapshot_description_name(dataset_id=dataset_id, tag=snap_tag)
            if isinstance(snap_title, str):
                snap_title = snap_title.strip()
            dataset_title = enriched_ds.get("title")
            logger.info(
                "Title debug: dataset=%s dataset.name=%r snapshot.description.Name=%r tag=%s",
                dataset_id,
                dataset_title,
                snap_title,
                snap_tag,
            )
            if snap_title and re.match(r"^ds", snap_title, flags=re.IGNORECASE):
                # If snapshot name is just the dataset id, try tags dynamically (oldest-first)
                try:
                    snapshot_tags = _get_snapshot_tags(dataset_id=dataset_id)
                    # Prefer oldest-first to pick the original published snapshot name
                    snapshot_tags_sorted = sorted(
                        snapshot_tags,
                        key=lambda t: (t[1] is None, t[1] or datetime.min),
                    )
                    for tag, _created in snapshot_tags_sorted:
                        if tag == snap_tag:
                            continue
                        fallback_title = _get_snapshot_description_name(dataset_id=dataset_id, tag=tag)
                        if isinstance(fallback_title, str):
                            fallback_title = fallback_title.strip()
                        logger.info(
                            "Title debug fallback: dataset=%s snapshot.description.Name(%s)=%r",
                            dataset_id,
                            tag,
                            fallback_title,
                        )
                        if fallback_title and not re.match(r"^ds", fallback_title, flags=re.IGNORECASE):
                            enriched_ds["title"] = fallback_title
                            break
                except Exception as e:
                    logger.warning("Failed dynamic snapshot title fallback for %s: %s", dataset_id, e)
            elif snap_title:
                enriched_ds["title"] = snap_title
        except Exception as e:
            logger.warning("Failed to fetch snapshot description Name for %s@%s: %s", dataset_id, snap_tag, e)

        # DEBUG: log what we have before snapshot-derived enrichment
        logger.info(
            "OpenNeuro enrich debug: dataset=%s tag=%s has_desc=%s has_modality=%s",
            dataset_id,
            snap_tag,
            bool(enriched_ds.get("description")),
            bool(enriched_ds.get("modality")),
        )

        # Snapshot-derived enrichment: only for README/description/license now.
        # (Modality no longer comes from file structure.)
        if (not enriched_ds.get("description")) or (not enriched_ds.get("license")):
            tag = snap_tag
            try:
                snap_desc, snap_lic, paths = _get_snapshot_description_and_paths(
                    dataset_id=dataset_id,
                    tag=tag,
                    include_files=False,
                )

                # If draft didn't work, try latest snapshot tag (best effort)
                if (not snap_desc and not paths) and tag == "draft":
                    latest_tag = _get_latest_snapshot_tag(dataset_id)
                    if latest_tag:
                        snap_desc, snap_lic, paths = _get_snapshot_description_and_paths(
                            dataset_id=dataset_id,
                            tag=latest_tag,
                            include_files=False,
                        )

                logger.info(
                    "OpenNeuro snapshot debug: dataset=%s tag=%s desc_len=%s",
                    dataset_id,
                    tag,
                    len(snap_desc) if isinstance(snap_desc, str) else None,
                )

                # Fill description/license from snapshot.description if still missing
                if snap_desc and not enriched_ds.get("description"):
                    enriched_ds["description"] = snap_desc
                    stats["enriched_desc"] = 1
                if snap_lic and not enriched_ds.get("license"):
                    enriched_ds["license"] = snap_lic
            except Exception as e:
                # Don't fail enrichment if snapshot metadata fetching fails.
                logger.warning("Failed to fetch snapshot metadata for %s@%s: %s", dataset_id, tag, e)

        # If description is still missing, try snapshot readme query directly (best effort)
        if not enriched_ds.get("description"):
            tag = snap_tag
            try:
                readme = _get_readme_from_latest_snapshot(dataset_id=dataset_id, tag=tag)
                desc = _readme_to_description(readme, max_len=256) if readme else None
                if desc:
                    enriched_ds["description"] = desc
                    stats["enriched_desc"] = 1
                logger.info(
                    "OpenNeuro readme debug: dataset=%s tag=%s readme_len=%s",
                    dataset_id,
                    tag,
                    len(readme) if isinstance(readme, str) else None,
                )
            except Exception as e:
                logger.warning("Failed to fetch snapshot README for %s@%s: %s", dataset_id, tag, e)

        analytics = dataset_data.get("analytics") or {}
        if isinstance(analytics, dict):
            downloads = int(analytics.get("downloads") or 0)
            views = int(analytics.get("views") or 0)
            enriched_ds["downloads"] = downloads
            enriched_ds["views"] = views
            # citations is deprecated (no longer maintained). Keep nullable.
            enriched_ds["citations"] = None
        
        stats["enriched_metadata"] = 1
        
    except requests.RequestException as e:
        stats["request_errors"] = 1
        logger.warning(
            "Failed to fetch metadata for %s: %s",
            dataset_id,
            e,
        )
    except Exception as e:
        stats["request_errors"] = 1
        logger.error("Error enriching dataset %s: %s", dataset_id, e)
    
    # Final safeguard: ensure all required DB fields exist with safe defaults
    enriched_ds.setdefault("dataset_id", dataset_id)
    enriched_ds.setdefault("title", dataset_id or "Unknown")
    enriched_ds.setdefault("modality", None)
    # Legacy field (no longer maintained). Keep nullable.
    enriched_ds["citations"] = None
    # Number of associated papers (not computed for OpenNeuro yet). Keep nullable.
    enriched_ds.setdefault("papers", None)
    enriched_ds.setdefault("url", f"https://openneuro.org/datasets/{dataset_id}" if dataset_id else "")
    enriched_ds.setdefault("description", None)
    enriched_ds.setdefault("license", None)
    enriched_ds.setdefault("created_at", None)
    enriched_ds.setdefault("updated_at", None)
    enriched_ds.setdefault("public", True)
    enriched_ds.setdefault("downloads", 0)
    enriched_ds.setdefault("views", 0)
    
    return enriched_ds, stats


def _sanitize_string(s: Any) -> Optional[str]:
    """Remove NUL bytes and other problematic characters from strings for Postgres."""
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    # Remove NUL bytes (0x00) which Postgres text fields cannot contain
    return s.replace('\x00', '')


def _upsert_openneuro_datasets(datasets: List[Dict[str, Any]]) -> None:
    """
    Upsert OpenNeuro datasets into Postgres.

    IMPORTANT: Do not store large payloads in XCom. Use the DB for dataset records.
    """
    if not datasets:
        logger.warning("No datasets to upsert")
        return

    insert_sql = """
    INSERT INTO openneuro_dataset (
        dataset_id, title, modality, citations, papers, url, description, license,
        created_at, updated_at, public, downloads, views
    )
    VALUES (
        %(dataset_id)s, %(title)s, %(modality)s, %(citations)s, %(papers)s, %(url)s, %(description)s, %(license)s,
        %(created_at)s, %(updated_at)s, %(public)s, %(downloads)s, %(views)s
    )
    ON CONFLICT (dataset_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        modality = EXCLUDED.modality,
        citations = EXCLUDED.citations,
        papers = COALESCE(EXCLUDED.papers, openneuro_dataset.papers),
        url = EXCLUDED.url,
        description = EXCLUDED.description,
        license = EXCLUDED.license,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        public = EXCLUDED.public,
        downloads = EXCLUDED.downloads,
        views = EXCLUDED.views
    RETURNING (xmax = 0) AS inserted;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            inserted_count = 0
            updated_count = 0
            for dataset in datasets:
                # Ensure all required fields exist with defaults (avoid KeyError during insert)
                if not dataset.get("dataset_id"):
                    logger.warning("Skipping dataset without dataset_id: %s", dataset)
                    continue
                dataset.setdefault("title", dataset.get("dataset_id"))
                dataset.setdefault("modality", None)
                dataset["citations"] = None
                dataset.setdefault("papers", None)
                dataset.setdefault("url", f"https://openneuro.org/datasets/{dataset.get('dataset_id')}")
                dataset.setdefault("description", None)
                dataset.setdefault("license", None)
                dataset.setdefault("created_at", None)
                dataset.setdefault("updated_at", None)
                dataset.setdefault("public", True)
                dataset.setdefault("downloads", 0)
                dataset.setdefault("views", 0)

                # Sanitize all string fields to remove NUL bytes
                dataset["dataset_id"] = _sanitize_string(dataset.get("dataset_id"))
                dataset["title"] = _sanitize_string(dataset.get("title"))
                dataset["modality"] = _sanitize_string(dataset.get("modality"))
                dataset["url"] = _sanitize_string(dataset.get("url"))
                dataset["description"] = _sanitize_string(dataset.get("description"))
                dataset["license"] = _sanitize_string(dataset.get("license"))
                
                cursor.execute(insert_sql, dataset)
                inserted_flag = cursor.fetchone()[0]
                if inserted_flag:
                    inserted_count += 1
                else:
                    updated_count += 1
            conn.commit()

    logger.info(
        "Successfully inserted %d new datasets and updated %d existing datasets",
        inserted_count,
        updated_count,
    )


def enrich_openneuro_current_run(**context) -> List[Dict[str, Any]]:
    """
    Enrich the current run's datasets with detailed metadata using concurrent requests.
    """
    ti = context["ti"]
    datasets: List[Dict[str, Any]] = ti.xcom_pull(task_ids="fetch_openneuro_datasets") or []
    
    params = context.get("params", {}) if isinstance(context.get("params", {}), dict) else {}
    max_workers = params.get("enrichment_max_workers", 5)
    
    total = len(datasets)
    if not datasets:
        logger.info("No datasets from current run to enrich.")
        return []
    
    logger.info("Starting concurrent metadata enrichment for %d datasets (max_workers=%d)", total, max_workers)
    
    enriched: List[Dict[str, Any]] = []
    enriched_desc = 0
    enriched_modality = 0
    enriched_metadata = 0
    skipped_no_id = 0
    request_errors = 0
    
    results: Dict[int, tuple] = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(_enrich_single_dataset, ds): idx for idx, ds in enumerate(datasets)
        }
        
        completed = 0
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            original_ds = datasets[idx]
            completed += 1
            
            try:
                enriched_ds, stats = future.result()
                results[idx] = (enriched_ds, stats)
            except Exception as e:
                logger.error(
                    "Error processing dataset %s (index %d): %s",
                    original_ds.get("dataset_id", "unknown"),
                    idx,
                    e,
                )
                results[idx] = (original_ds, {"request_errors": 1})
            
            if completed == 1 or completed % 50 == 0 or completed == total:
                logger.info(
                    "Enrichment progress: %d/%d (%.1f%%)",
                    completed,
                    total,
                    (completed / total) * 100,
                )
    
    # Process results in original order
    for idx in range(total):
        enriched_ds, stats = results[idx]
        enriched.append(enriched_ds)
        
        enriched_desc += stats.get("enriched_desc", 0)
        enriched_modality += stats.get("enriched_modality", 0)
        enriched_metadata += stats.get("enriched_metadata", 0)
        skipped_no_id += stats.get("skipped_no_id", 0)
        request_errors += stats.get("request_errors", 0)
    
    logger.info(
        "Enrichment complete. Total=%d, "
        "desc_enriched=%d, modality_enriched=%d, metadata_enriched=%d, "
        "skipped_no_id=%d, request_errors=%d",
        total,
        enriched_desc,
        enriched_modality,
        enriched_metadata,
        skipped_no_id,
        request_errors,
    )
    
    # Persist records in the DB to avoid large XCom payloads (which can crash the task runner).
    _upsert_openneuro_datasets(enriched)

    # Return only a small summary (safe for XCom).
    return [
        {
            "total": total,
            "desc_enriched": enriched_desc,
            "modality_enriched": enriched_modality,
            "metadata_enriched": enriched_metadata,
            "skipped_no_id": skipped_no_id,
            "request_errors": request_errors,
        }
    ]


def insert_openneuro_datasets(**context):
    """Insert or update OpenNeuro datasets into the database."""
    ti = context['ti']
    datasets = (
        ti.xcom_pull(task_ids='enrich_openneuro_current_run')
        or ti.xcom_pull(task_ids='fetch_openneuro_datasets')
    )
    # Backwards compatibility: this task used to do the insert.
    # After refactor, enrichment persists to DB and returns only a small summary.
    if datasets and isinstance(datasets, list) and isinstance(datasets[0], dict) and "dataset_id" in datasets[0]:
        _upsert_openneuro_datasets(datasets)
    else:
        logger.info("Insert task skipped (datasets were already persisted during enrichment).")


def create_unified_datasets_view_task(**context):
    """Create or replace the unified_datasets view that combines OpenNeuro and other neuroscience datasets."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                result = create_unified_datasets_view(cursor)
                conn.commit()
                return result
    except Exception as e:
        logger.error("Error creating/replacing unified_datasets view: %s", e)
        raise


def verify_openneuro_data(**context):
    """Verify that OpenNeuro data was inserted correctly."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM openneuro_dataset")
                count = cursor.fetchone()[0]
                
                cursor.execute(
                    "SELECT modality, COUNT(*) "
                    "FROM openneuro_dataset "
                    "WHERE modality IS NOT NULL "
                    "GROUP BY modality "
                    "ORDER BY COUNT(*) DESC "
                    "LIMIT 10"
                )
                modality_stats = cursor.fetchall()
                
                cursor.execute("SELECT SUM(citations) FROM openneuro_dataset")
                total_citations = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT SUM(downloads) FROM openneuro_dataset")
                total_downloads = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT COUNT(*) FROM openneuro_dataset WHERE public = true")
                public_count = cursor.fetchone()[0]
        
        logger.info("Total OpenNeuro datasets in database: %d", count)
        logger.info("Public datasets: %d", public_count)
        logger.info("Total citations: %d", total_citations)
        logger.info("Total downloads: %d", total_downloads)
        logger.info("Top 10 datasets by modality:")
        for modality, modality_count in modality_stats:
            logger.info("  %s: %d", modality, modality_count)
    
    except Exception as e:
        logger.error("Error verifying OpenNeuro data: %s", e)
        raise


# Define tasks
create_openneuro_table_task = PythonOperator(
    task_id='create_openneuro_table',
    python_callable=create_openneuro_table,
    dag=dag,
)

fetch_datasets_task = PythonOperator(
    task_id='fetch_openneuro_datasets',
    python_callable=fetch_openneuro_datasets,
    dag=dag,
)

enrich_openneuro_data_task = PythonOperator(
    task_id='enrich_openneuro_current_run',
    python_callable=enrich_openneuro_current_run,
    dag=dag,
)

insert_datasets_task = PythonOperator(
    task_id='insert_openneuro_datasets',
    python_callable=insert_openneuro_datasets,
    dag=dag,
)

create_view_task = PythonOperator(
    task_id='create_unified_datasets_view',
    python_callable=create_unified_datasets_view_task,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_openneuro_data',
    python_callable=verify_openneuro_data,
    dag=dag,
)

# Set task dependencies
create_openneuro_table_task >> fetch_datasets_task >> enrich_openneuro_data_task >> insert_datasets_task >> create_view_task >> verify_data_task
