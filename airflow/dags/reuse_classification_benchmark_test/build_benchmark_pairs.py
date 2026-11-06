#!/usr/bin/env python3
"""
Build benchmark_pairs.json, the answer key for the reuse classification benchmark.

Source: catalystneuro/find_reuse's human review of its classifier's REUSE calls
(``reuse_confirmation/``). Every pair there was called REUSE by find_reuse's
classifier and then judged by one reviewer, so the key holds two kinds of pair:

  expected REUSE      the reviewer confirmed the reuse      -> measures recall
  expected NOT_REUSE  the reviewer rejected it (neither,    -> measures false
                      mention or primary)                      positives on hard cases

The smoke set is a fixed, stratified subset (10 + 10). A rebuild keeps the
previous smoke pairs that are still in the key and fills any gaps by a stable
hash, so smoke runs stay comparable as the review data grows.

Usage (from the repo root, with a find_reuse checkout at hand):

    python airflow/dags/reuse_classification_benchmark_test/build_benchmark_pairs.py \
        --find-reuse-dir ../find_reuse
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent

# The same not-a-paper rule the DAGs apply (eLife assessments, OSF registrations,
# NEMAR records): D3 never classifies such a pair, so it cannot be in the key.
import sys  # noqa: E402

sys.path.insert(0, str(HERE.parent))
from utils.find_reuse_core import is_paper  # noqa: E402
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


# Calls that say the reviewer could not decide; they are not ground truth.
UNCLEAR_CALLS = frozenset({"ambiguous_reuse", "unsure"})


def _expected(call: str) -> str:
    return "REUSE" if call == "reuse" else "NOT_REUSE"


def build(find_reuse_dir: Path, previous_smoke: frozenset = frozenset()) -> dict:
    """
    `previous_smoke`: pair ids of the smoke set in the key being replaced. Those
    still eligible stay in it, so smoke runs before and after a rebuild compare
    the same pairs; only the places they leave are refilled by stable hash.
    """
    rc = find_reuse_dir / "reuse_confirmation"
    candidates = json.loads((rc / "reuse_candidates.json").read_text(encoding="utf-8"))
    by_key = {(p["doi"].lower(), p["dandiset"]): p for p in candidates["pairs"]}
    reviewers = [r["username"] for r in json.loads((rc / "reviewers.json").read_text(encoding="utf-8"))]

    # Every reviewer's call per pair. Two people can review the same pair.
    calls: dict[tuple[str, str], list[tuple[str, dict]]] = {}
    for username in reviewers:
        reviews_path = rc / username / f"{username}-reviews.json"
        if not reviews_path.is_file():
            continue
        reviews = json.loads(reviews_path.read_text(encoding="utf-8"))["reviews"]
        for doi, per_dataset in reviews.items():
            for dataset_id, review in per_dataset.items():
                calls.setdefault((doi.lower(), dataset_id), []).append((username, review))

    pairs = []
    # Reviewed pairs left out of the key, by reason. Upstream drops candidates
    # after review (non-paper DOIs, dandisets with no data); their reviews stay.
    # A key holds only clear calls, so a pair a reviewer marked ambiguous or
    # unsure, or that two reviewers called differently (reuse vs not), is out too.
    skipped: dict[str, list[str]] = {
        "not_a_paper": [], "no_longer_a_candidate": [], "unclear_call": [], "reviewers_disagree": [],
    }
    for (doi, dataset_id), reviews in sorted(calls.items()):
        pid = _pair_id(doi, dataset_id)
        if not is_paper(doi):
            skipped["not_a_paper"].append(pid)
            continue
        cand = by_key.get((doi, dataset_id))
        if cand is None:
            skipped["no_longer_a_candidate"].append(pid)
            continue
        verdicts = [r["call"] for _, r in reviews]
        if any(v in UNCLEAR_CALLS for v in verdicts):
            skipped["unclear_call"].append(pid)
            continue
        if len({_expected(v) for v in verdicts}) > 1:
            skipped["reviewers_disagree"].append(pid)
            continue
        pathway = cand.get("pathway") or "indirect"
        notes = [r.get("note") for _, r in reviews if r.get("note")]
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
            # Reviewers agree on reuse vs not; the first call stands for them.
            "human_call": verdicts[0],
            "human_note": " | ".join(notes) or None,
            "reviewer": ", ".join(u for u, _ in reviews),
            "expected": _expected(verdicts[0]),
            "find_reuse_reuse_types": cand.get("reuse_types") or [],
            "find_reuse_same_lab": cand.get("same_lab"),
            "smoke": False,
        })

    for (expected, pathway), quota in SMOKE_QUOTA.items():
        stratum = sorted(
            (p for p in pairs
             if p["expected"] == expected and p["pathway"] == pathway
             and (p["paper_doi"], p["dataset_id"]) not in SMOKE_EXCLUDE),
            key=lambda p: (p["pair_id"] not in previous_smoke, _stable_rank(p["pair_id"])),
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
            "reviewed_but_skipped": {k: len(v) for k, v in skipped.items()},
        },
        "reviewed_but_skipped": {k: sorted(set(v)) for k, v in skipped.items()},
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

    previous_smoke: frozenset = frozenset()
    if args.out.is_file():
        old = json.loads(args.out.read_text(encoding="utf-8"))
        previous_smoke = frozenset(p["pair_id"] for p in old.get("pairs", []) if p.get("smoke"))
    data = build(args.find_reuse_dir, previous_smoke)
    args.out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    print(f"Wrote {args.out}: {json.dumps(data['counts'])}")


if __name__ == "__main__":
    main()
