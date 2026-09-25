#!/usr/bin/env python3
"""
Build benchmark_pairs.json, the answer key for the reuse classification benchmark.

Source: catalystneuro/find_reuse's human review of its classifier's REUSE calls
(``reuse_confirmation/``). Every pair there was called REUSE by find_reuse's
classifier and then judged by one reviewer, so the key holds two kinds of pair:

  expected REUSE      the reviewer confirmed the reuse      -> measures recall
  expected NOT_REUSE  the reviewer rejected it (neither,    -> measures false
                      mention or primary)                      positives on hard cases

The smoke set is a fixed, stratified subset (10 + 10) chosen by a stable hash,
so it does not change when the file is rebuilt from the same review data.

Usage (from the repo root, with a find_reuse checkout at hand):

    python airflow/dags/reuse_classification_benchmark_test/build_benchmark_pairs.py \
        --find-reuse-dir _tmp_find_reuse_friend
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT_PATH = HERE / "benchmark_pairs.json"

UPSTREAM_REPO = "https://github.com/catalystneuro/find_reuse"

# Smoke set: how many pairs of each (expected, pathway) stratum. Roughly follows
# the full set's mix; direct-pathway negatives are scarce (7), so only 2.
SMOKE_QUOTA = {
    ("REUSE", "direct"): 5,
    ("REUSE", "indirect"): 5,
    ("NOT_REUSE", "indirect"): 8,
    ("NOT_REUSE", "direct"): 2,
}

# Pairs kept out of the smoke set, with the reason. The smoke set should only
# hold pairs whose full text the fetcher can reach, or a smoke run measures the
# fetcher instead of the classifier.
SMOKE_EXCLUDE: dict[tuple[str, str], str] = {
    ("10.1088/1741-2552/ad1787", "000140"): "paywalled (IOP); fetcher returns metadata only",
    ("10.17605/osf.io/pbu8x", "001075"): "OSF record; no source returns any text",
    ("10.7554/elife.111322.1.sa1", "000404"): "eLife review record; fetcher returns metadata only",
}


def _pair_id(doi: str, dataset_id: str) -> str:
    return f"{doi}|{dataset_id}"


def _stable_rank(pair_id: str) -> str:
    return hashlib.sha256(pair_id.encode("utf-8")).hexdigest()


def _expected(call: str) -> str:
    return "REUSE" if call == "reuse" else "NOT_REUSE"


def build(find_reuse_dir: Path) -> dict:
    rc = find_reuse_dir / "reuse_confirmation"
    candidates = json.loads((rc / "reuse_candidates.json").read_text(encoding="utf-8"))
    by_key = {(p["doi"].lower(), p["dandiset"]): p for p in candidates["pairs"]}
    reviewers = [r["username"] for r in json.loads((rc / "reviewers.json").read_text(encoding="utf-8"))]

    pairs = []
    for username in reviewers:
        reviews_path = rc / username / f"{username}-reviews.json"
        if not reviews_path.is_file():
            continue
        reviews = json.loads(reviews_path.read_text(encoding="utf-8"))["reviews"]
        for doi, per_dataset in reviews.items():
            for dataset_id, review in per_dataset.items():
                cand = by_key.get((doi.lower(), dataset_id))
                if cand is None:
                    raise SystemExit(f"Reviewed pair {doi} / {dataset_id} is not in reuse_candidates.json")
                pathway = cand.get("pathway") or "indirect"
                pairs.append({
                    "pair_id": _pair_id(cand["doi"].lower(), dataset_id),
                    "paper_doi": cand["doi"].lower(),
                    "fetched_doi": (cand.get("fetched_doi") or cand["doi"]).lower(),
                    "paper_title": cand.get("title"),
                    "dataset_source": "DANDI",
                    "dataset_id": dataset_id,
                    "dataset_name": cand.get("dandiset_name") or "",
                    "primary_paper_doi": (cand.get("cited_doi") or "").lower(),
                    "pathway": pathway,
                    # indirect = the paper cites the dataset's primary paper
                    # (D3's citation edges); direct = the paper names the dataset.
                    "mode": "citing" if pathway == "indirect" else "direct",
                    "human_call": review["call"],
                    "human_note": review.get("note"),
                    "reviewer": username,
                    "expected": _expected(review["call"]),
                    "find_reuse_reuse_types": cand.get("reuse_types") or [],
                    "find_reuse_same_lab": cand.get("same_lab"),
                    "smoke": False,
                })

    seen = set()
    for p in pairs:
        if p["pair_id"] in seen:
            raise SystemExit(f"Pair reviewed twice: {p['pair_id']}; decide how to merge before building")
        seen.add(p["pair_id"])

    for (expected, pathway), quota in SMOKE_QUOTA.items():
        stratum = sorted(
            (p for p in pairs
             if p["expected"] == expected and p["pathway"] == pathway
             and (p["paper_doi"], p["dataset_id"]) not in SMOKE_EXCLUDE),
            key=lambda p: _stable_rank(p["pair_id"]),
        )
        if len(stratum) < quota:
            raise SystemExit(f"Only {len(stratum)} pairs for smoke stratum {expected}/{pathway}, need {quota}")
        for p in stratum[:quota]:
            p["smoke"] = True

    pairs.sort(key=lambda p: p["pair_id"])
    commit = subprocess.run(
        ["git", "-C", str(find_reuse_dir), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=False,
    ).stdout.strip() or None

    def _counts(ps):
        out: dict = {}
        for p in ps:
            k = f"{p['expected']}/{p['pathway']}"
            out[k] = out.get(k, 0) + 1
        return dict(sorted(out.items()))

    return {
        "description": (
            "Human-reviewed (paper, dandiset) pairs from find_reuse's reuse_confirmation. "
            "Built by build_benchmark_pairs.py; do not edit by hand."
        ),
        "upstream_repo": UPSTREAM_REPO,
        "upstream_commit": commit,
        "upstream_candidates_generated_at": candidates.get("generated_at"),
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "counts": {
            "full": len(pairs),
            "smoke": sum(p["smoke"] for p in pairs),
            "full_by_stratum": _counts(pairs),
            "smoke_by_stratum": _counts([p for p in pairs if p["smoke"]]),
        },
        "smoke_excluded": [
            {"paper_doi": doi, "dataset_id": ds, "reason": why} for (doi, ds), why in sorted(SMOKE_EXCLUDE.items())
        ],
        "pairs": pairs,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--find-reuse-dir", required=True, type=Path)
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    args = parser.parse_args()

    data = build(args.find_reuse_dir)
    args.out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    print(f"Wrote {args.out}: {json.dumps(data['counts'])}")


if __name__ == "__main__":
    main()
