#!/usr/bin/env python3
"""
OpenNeuro modality extractor.

- Tries dataset.metadata.modalities first (fast, clean)
- Falls back to inferring modalities from latestSnapshot.files filenames (robust)

Usage:
  python3 openneuro_modality.py ds000001
"""

import sys
import json
import re
import requests
from typing import List, Set, Dict, Any, Optional

OPENNEURO_GRAPHQL_URL = "https://openneuro.org/crn/graphql"


def gql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = requests.post(
        OPENNEURO_GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

    data = resp.json()
    if "errors" in data:
        raise RuntimeError("GraphQL errors:\n" + json.dumps(data["errors"], indent=2))
    return data["data"]


def infer_modalities_from_filenames(filenames: List[str]) -> List[str]:
    """
    BIDS-ish inference from common suffixes/extensions.
    Not perfect, but very good when metadata.modalities is missing.
    """
    mods: Set[str] = set()

    # normalize
    fn = [f.lower() for f in filenames if isinstance(f, str)]

    # EEG / iEEG / MEG
    for f in fn:
        if re.search(r"_eeg\.", f) or "/eeg/" in f:
            mods.add("EEG")
        if re.search(r"_(ieeg|seeg|ecog)\.", f) or "/ieeg/" in f:
            mods.add("iEEG")
        if re.search(r"_meg\.", f) or "/meg/" in f:
            mods.add("MEG")

    # MRI family
    for f in fn:
        # Most MRI data are NIfTI; use suffixes
        if f.endswith(".nii") or f.endswith(".nii.gz"):
            # functional
            if "_bold" in f or "_cbv" in f or "_asl" in f:
                mods.add("fMRI")
            # diffusion
            if "_dwi" in f:
                mods.add("DWI")
            # anatomical
            if "_t1w" in f or "_t2w" in f or "_flair" in f or "_pd" in f:
                mods.add("MRI")  # keep broad unless you want "AnatMRI"
            # fieldmaps
            if "_phasediff" in f or "_magnitude" in f or "_fieldmap" in f:
                mods.add("Fieldmap")

    # PET
    for f in fn:
        if "/pet/" in f or re.search(r"_pet\.", f):
            mods.add("PET")

    # behavioral / physio (common BIDS sidecars)
    for f in fn:
        if re.search(r"_(events|beh)\.tsv$", f) or "/beh/" in f:
            mods.add("Behavioral")
        if re.search(r"_(physio|stim)\.tsv(\.gz)?$", f) or "/func/" in f and "_physio" in f:
            mods.add("Physio")

    # return stable order
    order = ["EEG", "iEEG", "MEG", "fMRI", "DWI", "MRI", "Fieldmap", "PET", "Behavioral", "Physio"]
    return [m for m in order if m in mods] or ["Unknown"]


def get_dataset_modalities(dataset_id: str) -> Dict[str, Any]:
    query = """
    query($id: ID!) {
      dataset(id: $id) {
        id
        name
        public
        publishDate
        metadata {
          modalities
        }
        latestSnapshot {
          id
          readme
          # files can be large; this returns metadata for each file
          files {
            filename
          }
        }
      }
    }
    """

    data = gql(query, {"id": dataset_id})
    ds = data.get("dataset") or {}
    metadata = ds.get("metadata") or {}
    # Introspect Metadata type to list available fields
    meta_fields_query = """
    query IntrospectMetadataFields {
      __type(name: "Metadata") {
        fields {
          name
        }
      }
    }
    """
    meta_fields_data = gql(meta_fields_query, {})
    meta_fields = (
        meta_fields_data.get("__type", {})
        .get("fields", [])
    )
    meta_field_names = [f.get("name") for f in meta_fields if isinstance(f, dict) and f.get("name")]

    # Introspect Dataset type to list available fields
    dataset_fields_query = """
    query IntrospectDatasetFields {
      __type(name: "Dataset") {
        fields {
          name
        }
      }
    }
    """
    dataset_fields_data = gql(dataset_fields_query, {})
    dataset_fields = (
        dataset_fields_data.get("__type", {})
        .get("fields", [])
    )
    dataset_field_names = [f.get("name") for f in dataset_fields if isinstance(f, dict) and f.get("name")]

    # Introspect Snapshot type to check for metadata field
    snapshot_fields_query = """
    query IntrospectSnapshotFields {
      __type(name: "Snapshot") {
        fields {
          name
        }
      }
    }
    """
    snapshot_fields_data = gql(snapshot_fields_query, {})
    snapshot_fields = (
        snapshot_fields_data.get("__type", {})
        .get("fields", [])
    )
    snapshot_field_names = [f.get("name") for f in snapshot_fields if isinstance(f, dict) and f.get("name")]

    # Fetch snapshot metadata / description for the latest snapshot tag if available
    snapshot_metadata = {}
    snapshot_description = {}
    latest_snapshot = ds.get("latestSnapshot") or {}
    latest_snapshot_id = latest_snapshot.get("id") if isinstance(latest_snapshot, dict) else None
    snapshot_tag = None
    if isinstance(latest_snapshot_id, str) and ":" in latest_snapshot_id:
        snapshot_tag = latest_snapshot_id.split(":", 1)[-1]

    if snapshot_tag and "metadata" in snapshot_field_names:
        # Only request metadata subfields that exist
        meta_fields_set = set(meta_field_names)
        wanted = ["datasetName", "datasetId", "datasetUrl"]
        available = [f for f in wanted if f in meta_fields_set]
        if available:
            snapshot_query = f"""
            query GetSnapshotMetadata($datasetId: ID!, $tag: String!) {{
              snapshot(datasetId: $datasetId, tag: $tag) {{
                metadata {{
                  {' '.join(available)}
                }}
              }}
            }}
            """
            snap_data = gql(snapshot_query, {"datasetId": dataset_id, "tag": snapshot_tag})
            snap = snap_data.get("snapshot") or {}
            snapshot_metadata = snap.get("metadata") or {}

    if snapshot_tag and "description" in snapshot_field_names:
        # Introspect Description type to only request available fields
        description_fields_query = """
        query IntrospectDescriptionFields {
          __type(name: "Description") {
            fields {
              name
            }
          }
        }
        """
        description_fields_data = gql(description_fields_query, {})
        description_fields = (
            description_fields_data.get("__type", {})
            .get("fields", [])
        )
        description_field_names = [f.get("name") for f in description_fields if isinstance(f, dict) and f.get("name")]

        # Prefer common fields if they exist
        preferred = ["Name", "Description", "License", "DatasetDOI", "HowToAcknowledge", "name", "description", "license"]
        available_desc = [f for f in preferred if f in set(description_field_names)]
        if available_desc:
            desc_query = f"""
            query GetSnapshotDescription($datasetId: ID!, $tag: String!) {{
              snapshot(datasetId: $datasetId, tag: $tag) {{
                description {{
                  {' '.join(available_desc)}
                }}
              }}
            }}
            """
            desc_data = gql(desc_query, {"datasetId": dataset_id, "tag": snapshot_tag})
            snap = desc_data.get("snapshot") or {}
            snapshot_description = snap.get("description") or {}

    # Fetch all snapshot tags + description.Name for each
    snapshot_descriptions_by_tag: List[Dict[str, Any]] = []
    if "snapshots" in dataset_field_names:
        # Introspect Dataset.snapshots args + type shape
        snapshots_args_query = """
        query IntrospectDatasetSnapshotsArgs {
          __type(name: "Dataset") {
            fields {
              name
              args { name }
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
            snaps_meta = gql(snapshots_args_query, {})
            fields = (
                snaps_meta.get("__type", {})
                .get("fields", [])
            )
            snapshots_field = None
            for f in fields:
                if isinstance(f, dict) and f.get("name") == "snapshots":
                    snapshots_field = f
                    break
            args = snapshots_field.get("args", []) if snapshots_field else []
            arg_names = [a.get("name") for a in args if isinstance(a, dict) and a.get("name")]

            # Build arg string if supported
            arg_parts = []
            if "first" in arg_names:
                arg_parts.append("first: 100")
            elif "last" in arg_names:
                arg_parts.append("last: 100")
            arg_str = f"({', '.join(arg_parts)})" if arg_parts else ""

            # Determine snapshots field type shape (connection vs list)
            snapshots_type = None
            t = snapshots_field.get("type") if snapshots_field else None
            while isinstance(t, dict):
                if t.get("name"):
                    snapshots_type = t.get("name")
                if t.get("kind") in ("NON_NULL", "LIST") and isinstance(t.get("ofType"), dict):
                    t = t.get("ofType")
                    continue
                break

            # Inspect the snapshots type fields to decide edges/nodes/list
            snapshots_type_fields = []
            if snapshots_type:
                type_fields_query = f"""
                query IntrospectSnapshotsTypeFields {{
                  __type(name: "{snapshots_type}") {{
                    fields {{ name }}
                  }}
                }}
                """
                type_fields_data = gql(type_fields_query, {})
                snapshots_type_fields = [
                    f.get("name")
                    for f in (type_fields_data.get("__type", {}) or {}).get("fields", [])
                    if isinstance(f, dict) and f.get("name")
                ]

            if "edges" in snapshots_type_fields:
                snapshots_selection = f"snapshots{arg_str} {{ edges {{ node {{ tag created }} }} }}"
            elif "nodes" in snapshots_type_fields:
                snapshots_selection = f"snapshots{arg_str} {{ nodes {{ tag created }} }}"
            else:
                snapshots_selection = f"snapshots{arg_str} {{ tag created }}"

            snapshots_query = f"""
            query GetSnapshotTags($id: ID!) {{
              dataset(id: $id) {{
                {snapshots_selection}
              }}
            }}
            """

            snap_tags_data = gql(snapshots_query, {"id": dataset_id})
            dataset_node = snap_tags_data.get("dataset") or {}
            snaps = dataset_node.get("snapshots")

            tags: List[Dict[str, Optional[str]]] = []
            if isinstance(snaps, list):
                for s in snaps:
                    if isinstance(s, dict) and isinstance(s.get("tag"), str):
                        tags.append({"tag": s.get("tag"), "created": s.get("created")})
            elif isinstance(snaps, dict):
                if isinstance(snaps.get("nodes"), list):
                    for n in snaps["nodes"]:
                        if isinstance(n, dict) and isinstance(n.get("tag"), str):
                            tags.append({"tag": n.get("tag"), "created": n.get("created")})
                elif isinstance(snaps.get("edges"), list):
                    for e in snaps["edges"]:
                        node = e.get("node") if isinstance(e, dict) else None
                        if isinstance(node, dict) and isinstance(node.get("tag"), str):
                            tags.append({"tag": node.get("tag"), "created": node.get("created")})
                elif isinstance(snaps.get("tag"), str):
                    tags.append({"tag": snaps.get("tag"), "created": snaps.get("created")})

            # Use the same description field introspection to avoid invalid fields.
            desc_name_fields = []
            if "description" in snapshot_field_names:
                desc_field_query = """
                query IntrospectDescriptionFields {
                  __type(name: "Description") {
                    fields { name }
                  }
                }
                """
                desc_fields_data = gql(desc_field_query, {})
                desc_fields = (
                    desc_fields_data.get("__type", {})
                    .get("fields", [])
                )
                desc_field_names = [f.get("name") for f in desc_fields if isinstance(f, dict) and f.get("name")]
                for cand in ("Name", "name"):
                    if cand in desc_field_names:
                        desc_name_fields.append(cand)

            for entry in tags:
                tag = entry.get("tag")
                created = entry.get("created")
                if snapshot_field_names and "description" in snapshot_field_names and desc_name_fields:
                    desc_query = f"""
                    query GetSnapshotDescription($datasetId: ID!, $tag: String!) {{
                      snapshot(datasetId: $datasetId, tag: $tag) {{
                        description {{
                          {' '.join(desc_name_fields)}
                        }}
                      }}
                    }}
                    """
                    desc_data = gql(desc_query, {"datasetId": dataset_id, "tag": tag})
                    snap = desc_data.get("snapshot") or {}
                    desc = snap.get("description") or {}
                    name_val = None
                    if isinstance(desc, dict):
                        name_val = desc.get("Name") or desc.get("name")
                    snapshot_descriptions_by_tag.append(
                        {"tag": tag, "created": created, "description_name": name_val}
                    )
        except Exception as e:
            snapshot_descriptions_by_tag.append({"error": str(e)})

    return {
        "dataset": ds,
        "metadata": metadata,
        "metadata_fields": meta_field_names,
        "dataset_fields": dataset_field_names,
        "snapshot_fields": snapshot_field_names,
        "snapshot_tag": snapshot_tag,
        "snapshot_metadata": snapshot_metadata,
        "snapshot_description": snapshot_description,
        "snapshot_descriptions_by_tag": snapshot_descriptions_by_tag,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 openneuro_modality.py ds000001")
        return 2

    dataset_id = sys.argv[1].strip()
    try:
        out = get_dataset_modalities(dataset_id)
        dataset = out.get("dataset") or {}
        metadata = out.get("metadata") or {}
        print("=== dataset ===")
        print(json.dumps(dataset, indent=2))
        print("\n=== metadata ===")
        print(json.dumps(metadata, indent=2))
        print("\n=== metadata values ===")
        print(json.dumps({
            "datasetName": metadata.get("datasetName"),
            "datasetID": metadata.get("datasetID"),
            "datasetURL": metadata.get("datasetURL"),
        }, indent=2))
        print("\n=== dataset values ===")
        print(json.dumps({
            "id": dataset.get("id"),
            "name": dataset.get("name"),
        }, indent=2))
        print("\n=== metadata fields ===")
        print(json.dumps(out.get("metadata_fields"), indent=2))
        print("\n=== dataset fields ===")
        print(json.dumps(out.get("dataset_fields"), indent=2))
        print("\n=== snapshot fields ===")
        print(json.dumps(out.get("snapshot_fields"), indent=2))
        print("\n=== snapshot tag ===")
        print(json.dumps(out.get("snapshot_tag"), indent=2))
        print("\n=== snapshot metadata ===")
        print(json.dumps(out.get("snapshot_metadata"), indent=2))
        print("\n=== snapshot description ===")
        print(json.dumps(out.get("snapshot_description"), indent=2))
        print("\n=== snapshot descriptions by tag ===")
        print(json.dumps(out.get("snapshot_descriptions_by_tag"), indent=2))
        return 0
    except Exception as e:
        print(f"❌ {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
