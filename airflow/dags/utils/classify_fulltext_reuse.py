"""
classify_fulltext_reuse.py - Classify data reuse from a paper's full text.

Vendored from catalystneuro/find_reuse ``src/shared/classify_fulltext_reuse.py``
at commit 3fac8ce14259e467fa40f9d7f0da96f19c1bd1d7 (2026-09-18) for use inside
Airflow tasks. Local changes, kept deliberately small so future syncs are a
diff against that commit:

  - the CLI ``main()`` is dropped (the DAG is the entry point);
  - ``_read_key`` reads the Airflow Variable ``openrouter_api_key`` before the
    environment, matching ``utils.llm_classify.get_openrouter_api_key``;
  - the archive vocabulary import from ``src.indirect_pipeline`` is replaced by
    the same hard-coded fallback upstream ships for standalone use;
  - ``openrouter_credit_remaining`` is carried over from upstream's batch
    runner ``src/shared/run_fulltext_classification.py`` so the DAG can
    preflight the key's balance.

Everything else (labels, prompt, ``PROMPT_VERSION``, quote verification,
parsing) is upstream's verbatim. Bump nothing here without bumping it upstream
first, or the two corpora stop being comparable.

The citation-context pipeline extracts small windows of text around each
citation and asks the model to judge from those. This module takes the opposite
approach: it hands the model the entire paper and asks it to find the evidence
itself, then requires it to quote the passage it judged from.

Requiring a quote is the point. A label on its own cannot be checked, whereas a
quote either appears in the paper or it does not, and a quote that does not
appear is a fabrication the caller can detect and act on.

Two rules govern the output:

  1. A failure is never a classification. Any transport error, malformed
     response, or unrecognized label produces ERROR. It never produces REUSE,
     and it never produces a quiet MENTION that would read as a real judgement.

  2. Every quote is checked against the paper text before it is returned.

Requires DEEPSEEK_API_KEY in the environment or in the project .env file.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import unicodedata
from typing import Any, Optional

import requests

DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# A pinned snapshot, so a re-run months from now is comparable to this one.
DEFAULT_MODEL = "openai/gpt-5.6-luna"

# OpenRouter serves this snapshot from 28 providers at prices spanning 3.5x, and
# picks one per request unless told otherwise. Left alone it routed consecutive
# calls to StreamLake and then Morph, which cost more AND destroyed prefix
# caching, since each provider keeps its own cache. Pinning one provider fixes
# both: it is cheaper, it is reproducible, and the cache actually hits.
# Which OpenRouter backend serves a model, where pinning one matters. Without a
# pin OpenRouter picks per request, so consecutive calls land on different
# backends, the prefix cache never hits, and the price swings. Pinning is only
# meaningful when several providers serve the same weights: the GPT models are
# served by OpenAI, Azure and Bedrock, and naming a backend that does not carry
# the model is a 404 rather than a fallback.
PROVIDER_PINS = {
    'deepseek/deepseek-v4-flash-0731': 'DeepInfra',
    'deepseek/deepseek-v4-flash': 'DeepInfra',
}


def provider_for(model: str) -> Optional[str]:
    """The backend to pin for `model`, or None to let OpenRouter route."""
    return PROVIDER_PINS.get(model)


DEFAULT_PROVIDER = None

# The model thinks before answering, and how long it thinks is a request
# parameter. Left unset it runs at roughly the 'low' budget. Because the paper
# itself is ~26k tokens and the thinking is a few hundred, raising this changes
# the bill by about a tenth while giving the model materially more room on the
# judgement calls that make up most of the disagreement between runs.
VALID_REASONING_EFFORTS = frozenset({'low', 'medium', 'high', 'max'})
DEFAULT_REASONING_EFFORT = 'max'

# The indirect pathway asks how a citing paper relates to a dataset it cites.
VALID_CLASSIFICATIONS = frozenset({'REUSE', 'MENTION', 'NEITHER'})

# The direct pathway starts from a paper that names a dataset identifier outright,
# so the question is not whether the dataset is relevant but who the paper is in
# relation to it: the group that published it, or someone else using it.
VALID_DIRECT_CLASSIFICATIONS = frozenset({'PRIMARY', 'REUSE', 'NEITHER'})

MODE_CITING = 'citing'
MODE_DIRECT = 'direct'
LABELS_FOR_MODE = {
    MODE_CITING: VALID_CLASSIFICATIONS,
    MODE_DIRECT: VALID_DIRECT_CLASSIFICATIONS,
}

# Bumped whenever the prompt or the output schema changes. Cached results carry
# the version they were produced under so a batch run can tell which entries are
# stale rather than silently mixing outputs from two different questions.
PROMPT_VERSION = 6

# How the data was used, once reuse is established. The eight named categories
# are the ones the earlier pipeline used (src/shared/classify_reuse_type.py), kept
# so results from the two remain comparable. OTHER exists because a fixed list
# will meet a paper it does not fit, and forcing such a paper into the nearest
# category loses the fact that it did not fit; the write-in preserves it.
REUSE_TYPES = (
    'TOOL_DEMO',
    'BENCHMARK',
    'AGGREGATION',
    'CONFIRMATORY',
    'NOVEL_ANALYSIS',
    'ML_TRAINING',
    'SIMULATION',
    'TEACHING',
    'OTHER',
)

# Which part of a multimodal dataset was actually reused.
#
# This matters because a large share of DANDI holdings are Patch-seq, which pairs
# electrophysiological recordings with reconstructed morphology and with
# transcriptomics. Those components are hosted in different places. DANDI holds
# the neurophysiology and the behavioral data recorded alongside it; morphology
# reconstructions live in NeuroMorpho, the Allen Cell Types Database, or the
# Brain Image Library, and transcriptomics lives on GEO, CELLxGENE, or NeMO. A
# paper reusing only gene expression or only reconstructions from a Patch-seq
# study has therefore not reused DANDI data, even though it cites the same study.
MODALITIES = (
    'neurophysiology',   # recordings of neural activity
    'behavior',          # behavioral or task data recorded alongside
    'imaging',           # structural images of tissue: MRI, EM volumes, histology
    'morphology',        # reconstructions, dendritic/axonal structure
    'transcriptomics',   # gene expression, RNA-seq, cell-type clustering
    'other',             # anything else the paper describes obtaining
    'unclear',
)

# Modalities DANDI actually hosts. Reuse of anything outside this set is reuse of
# some other repository's holdings, whatever study the data originated with.
# Imaging belongs here: DANDI holds MRI and electron microscopy dandisets, among
# them 000026 (ex vivo brain MRI) and 000402 (the MICrONS volume). Morphology and
# transcriptomics stay out, since the reconstructions and expression data from
# Patch-seq studies live in NeuroMorpho, the Allen Cell Types Database, GEO and
# CELLxGENE even when the recordings from the same study are deposited here.
DANDI_HOSTED_MODALITIES = frozenset({'neurophysiology', 'behavior', 'imaging'})

# Archive vocabulary. Upstream imports this from its citation pipeline
# (src/indirect_pipeline/classify_source_archive.py); D3 does not vendor that
# module, so the canonical set and its alias map are inlined here verbatim.
CANONICAL_ARCHIVES = {
    "DANDI Archive", "CRCNS", "Figshare", "Allen Institute", "IBL",
    "Zenodo", "Dryad", "OSF", "GIN", "OpenNeuro", "EBRAINS", "SPARC",
    "Neural Latents Benchmark", "MICrONS Explorer", "Brain Image Library",
    "NeuroMorpho.org", "GitHub", "Buzsaki Lab", "Lab website",
    "MouseLight", "NEMO Archive", "AWS", "NIRD Research Data Archive",
    "GEO", "CELLxGENE", "Human Cell Atlas", "SEA-AD", "PhysioNet",
}
# Canonical name mapping: {variant: canonical}. Verbatim from upstream
# src/indirect_pipeline/classify_source_archive.py at the vendored commit.
NORMALIZE_MAP = {
    "DANDI": "DANDI Archive",
    "DANDI archive": "DANDI Archive",
    "dandi archive": "DANDI Archive",
    "IBL database": "IBL",
    "IBL (International Brain Laboratory data portal)": "IBL",
    "International Brain Laboratory": "IBL",
    "microns-explorer.org": "MICrONS Explorer",
    "Microns Explorer": "MICrONS Explorer",
    "Neural Latents Benchmark (NLB)": "DANDI Archive",
    "Neural Latents Benchmark": "DANDI Archive",
    "buzsakilab.nyumc.org/datasets/": "Buzsaki Lab",
    "Buzsáki laboratory website": "Buzsaki Lab",
    "Buzsaki Lab data repository": "Buzsaki Lab",
    "Buzsaki Lab website": "Lab website",
    "CellExplorer": "Lab website",
    "Buzsaki Lab": "Lab website",
    "Nemo Archive": "NEMO Archive",
    "institutional repository / lab website": "Lab website",
    "Mouse Light Neuron Browser": "MouseLight",
    # LLM parenthetical variants
    "DANDI Archive (dandiarchive.org)": "DANDI Archive",
    "CRCNS (crcns.org)": "CRCNS",
    "MICrONS Explorer (microns-explorer.org)": "MICrONS Explorer",
    "Figshare (figshare.com)": "Figshare",
    "OSF (osf.io)": "OSF",
    "IBL (International Brain Laboratory)": "IBL",
    "Buzsaki Lab (buzsakilab.com)": "Lab website",
    "GIN (gin.g-node.org)": "GIN",
    "GitHub (github.com - for data, not code)": "GitHub",
    "NCBI Gene Expression Omnibus": "GEO",
    # CELLxGENE, whose spellings vary by case, by punctuation, and by how much
    # of the Chan Zuckerberg branding the paper carries. The multiplication sign
    # in CELL×GENE is U+00D7, which case folding does not reach.
    "CZ CELLxGENE": "CELLxGENE",
    "CZ CELLxGENE Discover Portal": "CELLxGENE",
    "CZ CellxGene Discover": "CELLxGENE",
    "CZI CELLxGENE Portal": "CELLxGENE",
    "CELL×GENE": "CELLxGENE",
    "cellxgene-census": "CELLxGENE",
    "cellxgene.cziscience.com": "CELLxGENE",
    "Human Cell Atlas portal": "Human Cell Atlas",
    "Human Cell Atlas data portal": "Human Cell Atlas",
    "sea-ad.org": "SEA-AD",
    "Seattle Alzheimer’s Disease Brain Cell Atlas": "SEA-AD",
    "Neural Latent Benchmarks": "DANDI Archive",
}

# Names the shared map does not cover. Kept here rather than edited into
# classify_source_archive so the citation pipeline's own vocabulary is untouched.
EXTRA_ALIASES = {
    'neural latent benchmarks': 'DANDI Archive',
    'neural latent benchmark': 'DANDI Archive',
    'neural latents benchmarks': 'DANDI Archive',
    'nlb': 'DANDI Archive',
}

ARCHIVE_UNCLEAR = 'unclear'

# The model sometimes names more than one source in a single string, as
# "DANDI Archive and GitHub" or "DANDI Archive; GitHub", because a paper drew
# on several. Splitting is only used to answer one question, whether DANDI is
# among them, so it can be permissive: a stray fragment cannot be mistaken for
# DANDI, and the alternative of matching the whole string exactly loses every
# compound answer to the "other archive" bucket.
ARCHIVE_SEPARATORS = re.compile(r'\s*(?:;|,|\+|/|&|\band\b|\bplus\b)\s*', re.I)


def archive_parts(value: str | None) -> list[str]:
    """Every archive named in `value`, normalized, in the order given."""
    if not value or not isinstance(value, str):
        return []
    seen, out = set(), []
    for part in ARCHIVE_SEPARATORS.split(value):
        name = normalize_archive(part)
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out or ([normalize_archive(value)] if normalize_archive(value) else [])


def names_dandi(value: str | None) -> bool:
    """
    True when DANDI is one of the archives named, alone or among others.

    A paper that says it took data from DANDI and GitHub took data from DANDI.
    Anything naming an archive without DANDI is other, whether one or several.
    """
    return 'DANDI Archive' in archive_parts(value)


def normalize_archive(value: str | None) -> Optional[str]:
    """
    Map a model-supplied archive name onto the canonical vocabulary.

    Returns None when nothing usable was given, the canonical name when it is
    recognized, and the cleaned free-text otherwise. Unrecognized names are kept
    rather than discarded: a real archive the vocabulary has not seen yet is
    more useful than a null, and it shows up as a candidate for the map.
    """
    if not value or not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.lower() in {'null', 'none', 'n/a', ARCHIVE_UNCLEAR}:
        return None
    # The alias map is consulted first, and deliberately so. Some names appear in
    # both it and the canonical set, and when they do the map is the one that
    # carries the correction: "Neural Latents Benchmark" is a canonical-looking
    # name for datasets that are actually hosted on DANDI, and checking the
    # canonical set first returned it unchanged and undercounted DANDI reuse.
    if cleaned.lower() in EXTRA_ALIASES:
        return EXTRA_ALIASES[cleaned.lower()]
    if cleaned in NORMALIZE_MAP:
        return NORMALIZE_MAP[cleaned]
    lowered = {k.lower(): v for k, v in NORMALIZE_MAP.items()}
    if cleaned.lower() in lowered:
        return lowered[cleaned.lower()]
    if cleaned in CANONICAL_ARCHIVES:
        return cleaned
    for canonical in CANONICAL_ARCHIVES:
        if cleaned.lower() == canonical.lower():
            return canonical
    return cleaned

# The model is a reasoning model, so completion tokens cover thinking as well as
# the answer. Too small a budget truncates mid-JSON and yields an ERROR. At the
# 'max' effort setting the thinking alone ran to 2,400 tokens on a sample paper,
# so the budget is well clear of that rather than a few hundred tokens above it.
DEFAULT_MAX_TOKENS = 16384

# Verified against the live API at 800K characters (~136K tokens). Real papers
# in this corpus top out near 140K characters, so truncation should not trigger;
# it exists so that an unusually large input degrades visibly rather than by
# being silently rejected.
DEFAULT_MAX_INPUT_CHARS = 600_000


# One HTTP session per thread, rather than a fresh connection per request.
#
# `requests.post` builds and discards a Session on every call, so each of the
# ~14,000 classification requests paid for a new TCP connect and TLS handshake
# against the same host. A session per thread reuses one connection for all the
# requests that thread makes. Thread-local rather than shared because
# requests.Session is not documented as thread-safe, and a thread only has one
# request in flight at a time so a small pool per thread is ample.
_sessions = threading.local()


def _session() -> requests.Session:
    session = getattr(_sessions, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=4, pool_maxsize=4, max_retries=0)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        _sessions.session = session
    return session


class ClassificationError(Exception):
    """Raised only by helpers; the public entry point returns ERROR instead."""


# HTTP statuses that will not improve by trying the next paper. A spent key or a
# revoked credential fails identically for every remaining item, so a batch of
# 11,580 produces 11,580 copies of the same error and, worse, overwrites good
# cached results with each one.
FATAL_HTTP_STATUSES = frozenset({401, 402, 403})


# --------------------------------------------------------------------------- #
# API key
# --------------------------------------------------------------------------- #

def _read_key(name: str) -> Optional[str]:
    """
    Read an API key for Airflow use.

    Upstream falls back to a project ``.env`` file. Inside Airflow the sources
    are, in priority order: the Airflow Variable of the same name in lower case
    (``openrouter_api_key`` / ``deepseek_api_key``, rotatable from the UI), then
    the process environment (docker-compose ``.env``).
    """
    value = _airflow_variable(name.lower())
    if value:
        return value
    value = (os.environ.get(name) or "").strip()
    return value or None


def _airflow_variable(key: str) -> Optional[str]:
    """Read an Airflow Variable, or None when Airflow is absent or has no such key."""
    try:  # Airflow 3 task SDK
        from airflow.sdk import Variable
        value = Variable.get(key, default=None)
    except Exception:
        try:  # Airflow 2.x / metadata-DB access outside a task
            from airflow.models import Variable
            value = Variable.get(key, default_var=None)
        except Exception:  # Airflow not importable (tests) or DB unreachable
            return None
    value = (value or "").strip() if isinstance(value, str) else None
    return value or None


def uses_openrouter(model: str) -> bool:
    """OpenRouter slugs are namespaced ('vendor/model'); DeepSeek's own are not."""
    return '/' in model


def get_api_key_for(model: str) -> Optional[str]:
    """Return the key for whichever service serves `model`."""
    if uses_openrouter(model):
        return _read_key('OPENROUTER_API_KEY')
    return _read_key('DEEPSEEK_API_KEY')


def get_deepseek_api_key() -> Optional[str]:
    """Backwards-compatible accessor for the DeepSeek key."""
    return _read_key('DEEPSEEK_API_KEY')


# --------------------------------------------------------------------------- #
# Quote verification
# --------------------------------------------------------------------------- #

# Characters that text extractors and language models disagree about. The model
# routinely straightens curly quotes and dashes when transcribing, so a strict
# substring test reports hallucinations that did not happen.
_CHAR_FOLDING = {
    '‘': "'", '’': "'", '‚': "'", '‛': "'",
    '“': '"', '”': '"', '„': '"', '‟': '"',
    '‐': '-', '‑': '-', '‒': '-', '–': '-',
    '—': '-', '―': '-', '−': '-',
    ' ': ' ', ' ': ' ', ' ': ' ', ' ': ' ',
    '​': '', '‌': '', '‍': '', '﻿': '',
    '…': '...',
}


def _fold(text: str) -> str:
    """Fold characters that extraction and transcription disagree about."""
    text = unicodedata.normalize('NFKC', text)
    return ''.join(_CHAR_FOLDING.get(ch, ch) for ch in text)


def _collapse_space(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()


def _alnum(text: str) -> str:
    """Letters and digits only, lowercased. Used by the last matching tier."""
    return re.sub(r'[^a-z0-9]+', '', unicodedata.normalize('NFKC', text).lower())


def _strip_space(text: str) -> str:
    return re.sub(r'\s+', '', text)


def verify_quote(quote: str, paper_text: str) -> dict:
    """
    Locate `quote` in `paper_text`, tolerating benign transcription differences.

    Progressively looser matching, recording which tier succeeded so the caller
    can tell a byte-perfect quote from one that merely survived normalization:

      exact                - present verbatim
      normalized           - present after folding quotes, dashes, and spaces
      case_insensitive     - present apart from capitalization
      spacing_insensitive  - present ignoring whitespace entirely
      punctuation_insensitive - present ignoring punctuation too
      not_found            - absent, meaning the model fabricated it

    Every tier preserves the order of non-whitespace characters, so a quote that
    matches is genuinely present rather than merely built from the same words.

    The case-insensitive tier is not pedantry. A model quoting from the middle
    of a sentence routinely drops the lead-in and capitalizes the new first
    word: the paper reads "Notably, a recent study also reported..." and the
    quote comes back as "A recent study also reported...". Reporting that as a
    possible fabrication trains the reader to ignore the warning, which costs
    more than the small looseness of ignoring case.
    """
    result = {
        'quote': quote,
        'chars': len(quote or ''),
        'match_type': 'not_found',
        'verbatim': False,
        'offset': None,
    }
    if not quote or not paper_text:
        return result

    offset = paper_text.find(quote)
    if offset != -1:
        result.update(match_type='exact', verbatim=True, offset=offset)
        return result

    folded_paper = _collapse_space(_fold(paper_text))
    folded_quote = _collapse_space(_fold(quote))
    offset = folded_paper.find(folded_quote)
    if offset != -1:
        result.update(match_type='normalized', offset=offset)
        return result

    offset = folded_paper.lower().find(folded_quote.lower())
    if offset != -1:
        result.update(match_type='case_insensitive', offset=offset)
        return result

    stripped_paper = _strip_space(_fold(paper_text)).lower()
    stripped_quote = _strip_space(_fold(quote)).lower()
    if stripped_quote and stripped_quote in stripped_paper:
        result.update(match_type='spacing_insensitive',
                      offset=stripped_paper.find(stripped_quote))
        return result

    # Last tier: compare letters and digits only.
    #
    # Text extraction leaves punctuation artifacts that the model silently
    # cleans up when transcribing: stripped superscript citation markers leave
    # doubled commas, and spacing around brackets and periods varies by
    # extractor. Measured over a sample of 446 quotes this matcher had called
    # fabrications, 56% were in fact present once punctuation was ignored.
    #
    # This still requires every letter and digit of the quote in the original
    # order, so it cannot match a paraphrase, an abridgement, or a reordering.
    # What it forgives is punctuation the paper and the model disagree about.
    alnum_paper = _alnum(paper_text)
    alnum_quote = _alnum(quote)
    if alnum_quote and alnum_quote in alnum_paper:
        result.update(match_type='punctuation_insensitive',
                      offset=alnum_paper.find(alnum_quote))
    return result


# --------------------------------------------------------------------------- #
# Prompt
# --------------------------------------------------------------------------- #

def build_prompt(
    paper_text: str,
    dataset_id: str = '',
    dataset_name: str = '',
    dataset_description: str = '',
    primary_paper_doi: str = '',
    mode: str = MODE_CITING,
    matched_patterns: Optional[list] = None,
) -> str:
    """
    Build the classification prompt around the full paper text.

    `mode` selects the question. MODE_CITING asks how a paper that cites a
    dataset's primary publication relates to the data. MODE_DIRECT asks about a
    paper that names the dataset identifier outright, where the live question is
    whether this is the group that published it or someone else using it.
    """
    if dataset_id or dataset_name:
        target = "THE DATASET DESCRIBED BELOW"
        described = "THE DATASET IN QUESTION\n"
        if dataset_id:
            described += f"Identifier: {dataset_id}\n"
        if dataset_name:
            described += f"Name: {dataset_name}\n"
        if dataset_description:
            described += f"Description: {dataset_description[:2000]}\n"
        if primary_paper_doi:
            described += f"Primary paper DOI: {primary_paper_doi}\n"
    else:
        target = "ANY EXTERNALLY-PUBLISHED DATASET THAT THE AUTHORS DID NOT COLLECT"
        described = ""

    archive_list = '\n'.join(f'  - {a}' for a in sorted(CANONICAL_ARCHIVES))
    label_union = ' | '.join(
        f'"{label}"' for label in sorted(LABELS_FOR_MODE.get(mode, VALID_CLASSIFICATIONS)))

    if mode == MODE_DIRECT:
        found = ''
        if matched_patterns:
            shown = ', '.join(str(m) for m in list(matched_patterns)[:6])
            found = (f"\nThe identifier was detected in this paper's text as: {shown}\n")
        opening = (
            f"You are reading the full text of a scientific paper that names the dataset "
            f"identifier below somewhere in its text. Your job is to determine the paper's "
            f"RELATIONSHIP to that dataset.\n{found}"
        )
        quote_guidance = (
            "For PRIMARY, quote the passage showing these authors produced or "
            "deposited the data. For REUSE, quote the passage showing they obtained "
            "data they did not collect. For NEITHER, quote whatever passage is most "
            "relevant, or use an empty list if genuinely nothing is relevant."
        )
        classifications = """CLASSIFICATIONS
- PRIMARY: This paper is the one that produced and published the dataset. The authors collected the data and deposited it; the identifier appears because they are pointing readers to their own deposit. Signals: a data availability statement saying the authors' data "are available at" the identifier, the paper describing the experiments that generated the recordings, or heavy author overlap with the dataset's contributors.
- REUSE: The authors obtained and analyzed data they did not collect. Downloading, re-analyzing, re-processing, training on, or benchmarking against someone else's deposit all count.
- NEITHER: The identifier appears without either relationship. Use this when the reference is a bibliography entry for another study, a passing mention of the archive, a mention of the dataset as related work the authors did not touch, or a parsing artifact where the matched string is not really this dataset.

HOW TO DECIDE
The identifier is present, so the question is not whether the dataset is relevant but who these authors are with respect to it. Decide PRIMARY versus REUSE from whether the text shows the authors GENERATING the data or OBTAINING it.

PRIMARY reads like: "data are available at [identifier]", "we deposited", "our recordings have been made available". The paper will describe performing the experiments that produced the data.

REUSE reads like: "data were downloaded from", "we analyzed the publicly available dataset", "obtained from the X archive", "we used the dataset of [author]".

A paper can be PRIMARY for its own deposit while also reusing other datasets; judge only the identifier named above. A review or commentary that merely lists the identifier is NEITHER, not REUSE."""
    else:
        opening = (
            f"You are reading the full text of a scientific paper to determine whether "
            f"its authors REUSED DATA from {target}.\n"
        )
        quote_guidance = (
            "For REUSE, quote the passage showing the authors obtained the data. "
            "For MENTION, quote the passage showing the citation is background rather "
            "than reuse. For NEITHER, quote whatever passage is most relevant, or use "
            "an empty list if genuinely nothing is relevant."
        )
        classifications = """CLASSIFICATIONS
- REUSE: The authors obtained and analyzed the actual DATA (recordings, images, behavioral traces, scans, spike trains, etc.) that they did not collect themselves. Downloading, re-analyzing, re-processing, training on, or benchmarking against the data all count.
- MENTION: The paper cites or references the work in any way without reusing its data. This covers discussion as background, prior findings, methodology or comparison, and it also covers a bare entry in the reference list that is never discussed in the body.
- NEITHER: The paper does not cite or reference the work at all. Use this only when the citation link itself is wrong.

HOW TO DECIDE
Data reuse is the rare case. Most references to other work are background mentions. Default to MENTION and output REUSE only when the text shows the authors actually handled the data.

MENTION and NEITHER are separated by whether the citation exists. How substantial it is does not matter, and neither does whether the two papers are on the same topic. Search the text for the work's DOI, its title, and its authors, including inside the reference list. If you find any of them anywhere, the answer is MENTION even when the surrounding subject matter looks unrelated, because papers routinely cite work outside their own topic. Answer NEITHER only when you have searched and found no trace of the work at all.

Evidence of REUSE typically appears in Methods, Data Availability, Results, or Acknowledgements, and reads like: "data were downloaded from", "we analyzed the publicly available dataset", "obtained from the X archive", "we used the dataset of [author]", "accession number", or a description of re-processing someone else's recordings.

A review, perspective, commentary, or editorial article does NOT reuse data, even when it describes datasets in detail. Classify those as MENTION."""

    # The paper comes first, and everything that varies comes after it. Providers
    # cache by prompt prefix, so this makes the bulk of the prompt cacheable:
    # a second question about the same paper reuses ~99% of it at a fraction of
    # the price. With the paper last, only the instruction block cached, which
    # measured at 6.4% of tokens.
    return f"""FULL TEXT OF THE PAPER
{paper_text}

=== END OF PAPER. THE TASK FOLLOWS. ===

{opening}
{described}
{classifications}

IF AND ONLY IF THE CLASSIFICATION IS REUSE, ALSO ANSWER THE FOLLOWING
For every other classification, set same_lab, same_lab_confidence, source_archive, reused_modalities, reuse_type and reuse_type_other to null.

SAME LAB OR A DIFFERENT ONE?
Compare the author list of this paper against the authors of the work whose data was reused. Set same_lab to true when the same group is reusing or extending its own data, and false when a different group is using someone else's. Judge this only from author overlap and statements like "our previously published dataset" or "data we collected in [citation]". Shared authorship is NOT itself evidence of reuse: decide the classification first, from the text, and answer this separately. Give same_lab_confidence from 1 to 10.

WHICH ARCHIVE SUPPLIED THE DATA?
Name where the data was actually obtained, using one of these names exactly when it applies:
{archive_list}
Use the name of another repository if the paper names one that is not on this list. Use "unclear" if the paper does not say where the data came from. Do not guess from the subject matter: name an archive only if the paper mentions it.

WHICH PART OF THE DATA WAS REUSED? — THE DECISIVE QUESTION FOR THIS STUDY
Many neuroscience datasets are multimodal, and their components are hosted in different places. Patch-seq studies in particular pair electrophysiological recordings with reconstructed cell morphology and with transcriptomics. The recordings and the behavior recorded alongside them live in the neurophysiology archive; morphological reconstructions live in NeuroMorpho, the Allen Cell Types Database, or the Brain Image Library; gene expression lives on GEO, CELLxGENE, or NeMO.

Identify which parts THIS paper actually obtained and analyzed, as a list from:
  - neurophysiology: recordings of neural activity. Intracellular or extracellular electrophysiology, patch-clamp traces, spike trains, firing rates, membrane or intrinsic properties, LFP, EEG, ECoG, or calcium/voltage imaging of activity.
  - behavior: behavioral, task, or positional data recorded alongside the neural data.
  - imaging: structural images of tissue rather than recordings of activity. MRI of any kind, diffusion imaging, electron microscopy volumes, light-sheet or whole-brain volumes, histology, and the segmentations derived from them.
  - morphology: reconstructed cell shape, dendritic or axonal structure, anatomical reconstructions.
  - transcriptomics: gene expression, RNA-seq counts, transcriptomic cell-type labels or clusters.
  - other: anything else obtained that none of the above covers.
  - unclear: the paper does not say which part it used.

Judge this from what the paper says it analyzed, NOT from what the cited dataset contains. This is the most common error to avoid: a paper that cites a Patch-seq study but analyzes only its gene expression or only its reconstructions has reused transcriptomics or morphology alone. Do not list neurophysiology merely because the original study also recorded it. List neurophysiology only when the text shows this paper handled the recordings themselves.

HOW WAS THE DATA USED?
Say what the authors did with it, choosing the ONE category that best describes their primary purpose:
  - TOOL_DEMO: demonstrating a new analysis tool, software package, or processing pipeline, with the dataset serving as example data. The scientific question is secondary to showcasing the method.
  - BENCHMARK: measuring the performance of an algorithm, decoder, or model against other methods on a shared dataset.
  - AGGREGATION: combining this dataset with others for statistical power, cross-dataset comparison, or a pooled analysis.
  - CONFIRMATORY: the authors have their own experiments and use this dataset to replicate, validate, or confirm their findings.
  - NOVEL_ANALYSIS: asking a new scientific question of the existing data that the original authors did not address.
  - ML_TRAINING: training a machine learning or deep learning model on the data.
  - SIMULATION: validating, constraining, or parameterizing a computational model or simulation with real data.
  - TEACHING: educational use, such as a tutorial, course, or textbook example.
  - OTHER: none of the above genuinely fits.

Choose OTHER only when the paper's use really falls outside all eight, and then put a short noun phrase naming the actual use in reuse_type_other, for example "quality control reference" or "stimulus set reuse". Leave reuse_type_other null for the eight named categories. If several categories apply, name the primary one, the main reason the authors went to this dataset.

QUOTE THE PROVENANCE SEPARATELY
Alongside the reuse evidence, quote the passage(s) that establish WHERE the data came from: the archive name, an accession or dataset identifier, a DOI, a URL, or a data availability statement. These go in source_quotes and are subject to the same character-for-character rule. If the paper never says where the data came from, return an empty list rather than inferring it.

EVIDENCE QUOTES — THIS IS THE CRITICAL REQUIREMENT
You MUST quote the exact passage(s) from the paper that drove your judgement.
- Copy the text CHARACTER FOR CHARACTER from the paper above. Do not paraphrase, summarize, correct, abridge, or join separated sentences.
- Each quote should be one or two complete sentences: long enough to stand on its own, short enough to be precise.
- Give 1 to 3 quotes. {quote_guidance}
- If you cannot find a supporting passage, return an empty list rather than inventing one. A fabricated quote is far worse than no quote.

OUTPUT
Return ONLY a JSON object, no markdown fences and no commentary:
{{
  "classification": {label_union},
  "confidence": <integer 1-10>,
  "evidence_quotes": ["<exact quote>", ...],
  "same_lab": <true | false | null>,
  "same_lab_confidence": <integer 1-10, or null>,
  "source_archive": "<archive name, \\"unclear\\", or null>",
  "reused_modalities": ["neurophysiology" | "behavior" | "imaging" | "morphology" | "transcriptomics" | "other" | "unclear", ...],
  "reuse_type": "<TOOL_DEMO | BENCHMARK | AGGREGATION | CONFIRMATORY | NOVEL_ANALYSIS | ML_TRAINING | SIMULATION | TEACHING | OTHER, or null>",
  "reuse_type_other": "<short phrase, only when reuse_type is OTHER; otherwise null>",
  "source_quotes": ["<exact quote establishing where the data came from>", ...],
  "reasoning": "<2-4 sentences explaining the judgement>"
}}

"""


# --------------------------------------------------------------------------- #
# Result helpers
# --------------------------------------------------------------------------- #

def _error_result(kind: str, message: str, **extra) -> dict:
    """
    Build an ERROR result.

    ERROR is a distinct outcome from every real classification. It must never be
    collapsed into REUSE, and it must never be collapsed into MENTION either,
    since a silent MENTION would be indistinguishable from a genuine negative
    and would quietly bias the corpus.
    """
    result = {
        'classification': 'ERROR',
        'fatal': False,
        'confidence': 0,
        'evidence_quotes': [],
        'quote_warnings': [],
        'hallucinated_quote_count': 0,
        'same_lab': None,
        'same_lab_confidence': None,
        'source_archive': None,
        'reused_modalities': [],
        'reuse_type': None,
        'reuse_type_other': None,
        'reused_neurophysiology': None,
        'reused_dandi_hosted': None,
        'source_quotes': [],
        'reasoning': None,
        'mode': None,
        'prompt_version': PROMPT_VERSION,
        'error': message,
        'error_kind': kind,
        'usage': None,
        'truncation': None,
    }
    result.update(extra)
    return result


def parse_strict(content: str, valid_labels: frozenset = VALID_CLASSIFICATIONS) -> dict:
    """
    Parse the model's JSON response, rejecting anything unrecognized.

    Deliberately strict. The shared `parse_json_response` falls back to scanning
    for a label keyword anywhere in the text, which would turn an error page
    containing the word "REUSE" into a REUSE verdict. Here, a response that is
    not well-formed JSON carrying a valid label is an error.

    Raises ClassificationError when the response cannot be trusted.
    """
    if not content or not content.strip():
        raise ClassificationError('empty response')

    text = content.strip()
    if text.startswith('```'):
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # One narrow rescue: a single JSON object embedded in prose. Anything
        # looser risks inventing a verdict out of surrounding text.
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if not match:
            raise ClassificationError(f'response is not JSON: {text[:200]}')
        try:
            parsed = json.loads(match.group())
        except json.JSONDecodeError:
            raise ClassificationError(f'response is not JSON: {text[:200]}')

    if not isinstance(parsed, dict):
        raise ClassificationError(f'response is not a JSON object: {text[:200]}')

    raw_label = parsed.get('classification')
    if not isinstance(raw_label, str):
        raise ClassificationError(f'missing classification field: {text[:200]}')

    label = raw_label.strip().upper().replace(' ', '_')
    if label not in valid_labels:
        raise ClassificationError(f'unrecognized classification {label!r}')
    parsed['classification'] = label
    return parsed


def _verify_quote_list(
    raw: Any, paper_text: str, field: str, warnings: list[str],
) -> tuple[list[dict], int]:
    """
    Verify one list of model-supplied quotes, returning records and a fabrication count.

    Shared by the reuse evidence and the provenance quotes so both are held to
    the same standard: a quote that cannot be located is reported, not trusted.
    """
    if isinstance(raw, str):          # a lone quote instead of a list
        raw = [raw]
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        warnings.append(f'{field} was {type(raw).__name__}, not a list')
        raw = []

    records, hallucinated = [], 0
    for quote in raw:
        if not isinstance(quote, str):
            warnings.append(
                f'ignored non-string {field} entry of type {type(quote).__name__}')
            continue
        record = verify_quote(quote, paper_text)
        records.append(record)
        if record['match_type'] == 'not_found':
            hallucinated += 1
            warnings.append(
                f'{field} quote not found in paper (possible fabrication): '
                f'{quote[:120]!r}')
        elif record['match_type'] != 'exact':
            warnings.append(
                f"{field} quote matched only after {record['match_type']} "
                f'normalization: {quote[:80]!r}')
    return records, hallucinated


def _parse_reuse_type(raw: Any, other: Any, warnings: list[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Validate the reuse type and its write-in.

    An unrecognized category is recorded as OTHER with the model's own wording
    carried over, since that wording is the answer to the question the fixed
    list failed to cover. Dropping it would lose information that cannot be
    recovered without paying for the call again.
    """
    if raw is None:
        return None, None
    if not isinstance(raw, str):
        warnings.append(f'reuse_type was {type(raw).__name__}, not a string')
        return None, None

    value = raw.strip().upper().replace(' ', '_').replace('-', '_')
    write_in = other.strip() if isinstance(other, str) and other.strip() else None

    if value not in REUSE_TYPES:
        warnings.append(f'unrecognized reuse_type {raw!r} recorded as OTHER')
        return 'OTHER', write_in or raw.strip()[:120]

    if value == 'OTHER':
        if not write_in:
            warnings.append('reuse_type OTHER with no write-in description')
        return 'OTHER', write_in
    if write_in:
        # A write-in alongside a named category is a contradiction; the category
        # is the answer we asked for, so keep it and note the extra text.
        warnings.append(f'reuse_type {value} came with a write-in {write_in[:60]!r}; ignored')
    return value, None


def _parse_modalities(raw: Any, warnings: list[str]) -> list[str]:
    """Validate the modality list against the vocabulary, dropping anything unrecognized."""
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        if raw is not None:
            warnings.append(f'reused_modalities was {type(raw).__name__}, not a list')
        return []

    seen, out = set(), []
    for item in raw:
        if not isinstance(item, str):
            continue
        value = item.strip().lower().replace(' ', '_')
        # Accept the obvious synonyms rather than losing a correct answer to wording.
        value = {'electrophysiology': 'neurophysiology', 'ephys': 'neurophysiology',
                 'physiology': 'neurophysiology', 'transcriptomic': 'transcriptomics',
                 'gene_expression': 'transcriptomics', 'genetics': 'transcriptomics',
                 'behaviour': 'behavior', 'morphological': 'morphology',
                 'mri': 'imaging', 'fmri': 'imaging', 'structural_imaging': 'imaging',
                 'electron_microscopy': 'imaging', 'em': 'imaging', 'histology': 'imaging',
                 'anatomy': 'imaging', 'anatomical': 'imaging'}.get(value, value)
        if value not in MODALITIES:
            warnings.append(f'unrecognized modality {item!r} dropped')
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _is_empty_completion(raw: Any) -> bool:
    """
    True only for a well-formed response whose message content is empty.

    Deliberately narrow. An explicit error envelope or a missing choices array
    is a real failure that should be reported immediately, not retried three
    times with backoff first. The case worth retrying is the one that looks
    entirely successful and simply has no answer in it.
    """
    if not isinstance(raw, dict) or raw.get('error'):
        return False
    choices = raw.get('choices') or []
    if not choices:
        return False
    return not ((choices[0].get('message') or {}).get('content') or '').strip()


def _truncate(paper_text: str, max_chars: int) -> tuple[str, Optional[dict]]:
    """
    Trim an oversized paper, keeping the head and the tail.

    Data availability statements sit at the end of a paper, so a head-only trim
    would remove the passages most likely to prove reuse.
    """
    if len(paper_text) <= max_chars:
        return paper_text, None

    head = int(max_chars * 0.6)
    tail = max_chars - head
    marker = '\n\n[... MIDDLE OF PAPER OMITTED FOR LENGTH ...]\n\n'
    trimmed = paper_text[:head] + marker + paper_text[-tail:]
    info = {
        'truncated': True,
        'original_chars': len(paper_text),
        'kept_chars': len(trimmed),
        'dropped_chars': len(paper_text) - max_chars,
        'strategy': 'head_tail',
    }
    print(f"  WARNING: paper is {len(paper_text):,} chars, truncating to "
          f"{max_chars:,} (head+tail); {info['dropped_chars']:,} chars dropped",
          file=sys.stderr, flush=True)
    return trimmed, info


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def classify_paper_reuse(
    paper_text: str,
    dataset_id: str = '',
    dataset_name: str = '',
    dataset_description: str = '',
    primary_paper_doi: str = '',
    paper_doi: str = '',
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    max_input_chars: int = DEFAULT_MAX_INPUT_CHARS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = 0.0,
    timeout: int = 300,
    max_retries: int = 3,
    mode: str = MODE_CITING,
    matched_patterns: Optional[list] = None,
    provider: Optional[str] = DEFAULT_PROVIDER,
    reasoning_effort: Optional[str] = DEFAULT_REASONING_EFFORT,
) -> dict:
    """
    Classify data reuse by sending the whole paper to DeepSeek.

    Args:
        paper_text: Full text of the paper. Must be the article body; an
            abstract cannot support this judgement, since reuse is described in
            Methods and Data Availability.
        dataset_id / dataset_name / dataset_description / primary_paper_doi:
            Optional. When supplied, the question is whether this paper reused
            that specific dataset. When omitted, it is whether the paper reused
            any dataset its authors did not collect.
        paper_doi: Recorded on the result for traceability only.

    Returns a dict with:
        classification            REUSE | MENTION | NEITHER | ERROR
        confidence                1-10, or 0 for ERROR
        evidence_quotes           list of verified quote records
        quote_warnings            human-readable notes about unverifiable quotes
        hallucinated_quote_count  quotes not found in the paper at all
        source_archive, reasoning, usage, truncation, error, error_kind
    """
    if not paper_text or not paper_text.strip():
        return _error_result('empty_input', 'No paper text supplied',
                             paper_doi=paper_doi)

    key = api_key or get_api_key_for(model)
    if not key:
        needed = 'OPENROUTER_API_KEY' if uses_openrouter(model) else 'DEEPSEEK_API_KEY'
        return _error_result(
            'no_api_key',
            f'No API key for {model}. Set {needed} in the environment or .env.',
            paper_doi=paper_doi, fatal=True)

    if mode not in LABELS_FOR_MODE:
        return _error_result('bad_mode', f'unknown mode {mode!r}', paper_doi=paper_doi)
    valid_labels = LABELS_FOR_MODE[mode]

    sent_text, truncation = _truncate(paper_text, max_input_chars)
    prompt = build_prompt(sent_text, dataset_id, dataset_name,
                          dataset_description, primary_paper_doi,
                          mode=mode, matched_patterns=matched_patterns)

    payload = {
        'model': model,
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': max_tokens,
        # Greedy decoding. At 0.1 a quarter of REUSE labels changed between two
        # runs of the same prompt on the same text, which made the corpus
        # unreproducible for no benefit: there is no creativity wanted here, only
        # the model's best reading of a fixed document.
        'temperature': temperature,
        'response_format': {'type': 'json_object'},
    }
    if reasoning_effort:
        if reasoning_effort not in VALID_REASONING_EFFORTS:
            raise ValueError(
                f'reasoning_effort must be one of {sorted(VALID_REASONING_EFFORTS)}, '
                f'got {reasoning_effort!r}')
        payload['reasoning_effort'] = reasoning_effort
    headers = {
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json',
    }

    if uses_openrouter(model):
        api_url = OPENROUTER_API_URL
        headers['HTTP-Referer'] = 'https://github.com/catalystneuro/find_reuse'
        provider = provider or provider_for(model)
        if provider:
            # Without this OpenRouter picks a provider per request. Consecutive
            # calls then land on different backends, so the prefix cache never
            # hits and the price varies by up to 3.5x between them.
            payload['provider'] = {'order': [provider], 'allow_fallbacks': False}
    else:
        api_url = DEEPSEEK_API_URL

    raw = None
    last_error = None
    for attempt in range(max_retries):
        try:
            response = _session().post(api_url, headers=headers,
                                     json=payload, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f'HTTP {response.status_code}'
                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))
                    continue
                return _error_result('http_error',
                                     f'{last_error} after {max_retries} attempts',
                                     paper_doi=paper_doi, truncation=truncation)
            if response.status_code != 200:
                body = response.text[:300]
                return _error_result(
                    'http_error', f'HTTP {response.status_code}: {body}',
                    paper_doi=paper_doi, truncation=truncation,
                    fatal=response.status_code in FATAL_HTTP_STATUSES)
            raw = response.json()

            # A reasoning model sometimes spends its thinking budget and then
            # emits no content at all. The envelope is a valid 200 with a
            # finish_reason of 'stop', so nothing looks wrong, but there is no
            # answer to parse. It is transient, and retrying gets one: this
            # accounted for 91 of 122 errors in a full corpus run, every one of
            # which succeeded on a second attempt.
            if not _is_empty_completion(raw) or attempt == max_retries - 1:
                break
            last_error = 'empty content despite HTTP 200'
            time.sleep(2 ** attempt)
            continue
        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = f'{type(e).__name__}: {e}'
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return _error_result('network_error', last_error,
                                 paper_doi=paper_doi, truncation=truncation)
        except requests.RequestException as e:
            return _error_result('request_error', f'{type(e).__name__}: {e}',
                                 paper_doi=paper_doi, truncation=truncation)
        except Exception as e:  # includes malformed JSON in the envelope
            return _error_result('unexpected_error', f'{type(e).__name__}: {e}',
                                 paper_doi=paper_doi, truncation=truncation)

    if raw is None:
        return _error_result('no_response',
                             last_error or 'No response from API',
                             paper_doi=paper_doi, truncation=truncation)

    if isinstance(raw, dict) and raw.get('error'):
        return _error_result('api_error', str(raw['error'])[:300],
                             paper_doi=paper_doi, truncation=truncation)

    choices = raw.get('choices') or []
    if not choices:
        return _error_result('no_choices', 'Response contained no choices',
                             paper_doi=paper_doi, truncation=truncation)

    finish_reason = choices[0].get('finish_reason')
    content = (choices[0].get('message') or {}).get('content') or ''
    usage = raw.get('usage')

    if finish_reason == 'length':
        return _error_result(
            'truncated_response',
            f'Model output hit the {max_tokens}-token cap before finishing; '
            'raise max_tokens',
            paper_doi=paper_doi, usage=usage, truncation=truncation)

    try:
        parsed = parse_strict(content, valid_labels)
    except ClassificationError as e:
        return _error_result('parse_error', str(e), paper_doi=paper_doi,
                             usage=usage, truncation=truncation)

    # Verify both quote lists against the text the model was actually shown.
    warnings: list[str] = []
    verified, hallucinated = _verify_quote_list(
        parsed.get('evidence_quotes'), sent_text, 'evidence_quotes', warnings)
    source_quotes, source_hallucinated = _verify_quote_list(
        parsed.get('source_quotes'), sent_text, 'source_quotes', warnings)
    hallucinated += source_hallucinated

    confidence = parsed.get('confidence')
    if not isinstance(confidence, int) or not 1 <= confidence <= 10:
        warnings.append(f'confidence {confidence!r} out of range, recorded as-is')

    # same_lab and the archive only mean anything for REUSE. Carrying a value
    # through on MENTION or NEITHER would invite it being counted later.
    is_reuse = parsed['classification'] == 'REUSE'
    same_lab = parsed.get('same_lab') if is_reuse else None
    if not isinstance(same_lab, bool):
        if is_reuse and same_lab is not None:
            warnings.append(f'same_lab was {same_lab!r}, not a boolean; recorded as unknown')
        same_lab = None

    same_lab_confidence = parsed.get('same_lab_confidence') if is_reuse else None
    if not isinstance(same_lab_confidence, int) or not 1 <= same_lab_confidence <= 10:
        same_lab_confidence = None

    archive = normalize_archive(parsed.get('source_archive')) if is_reuse else None
    if is_reuse and archive and archive not in CANONICAL_ARCHIVES:
        warnings.append(f'archive {archive!r} is not in the canonical vocabulary')

    modalities = _parse_modalities(parsed.get('reused_modalities'), warnings) \
        if is_reuse else []

    reuse_type, reuse_type_other = _parse_reuse_type(
        parsed.get('reuse_type'), parsed.get('reuse_type_other'), warnings) \
        if is_reuse else (None, None)
    if is_reuse and reuse_type is None:
        warnings.append('REUSE with no reuse_type')
    # Left as None rather than False for non-reuse rows, so "we did not ask" stays
    # distinguishable from "we asked and the answer was no".
    reused_neuro = ('neurophysiology' in modalities) if is_reuse else None
    reused_dandi = (
        bool(DANDI_HOSTED_MODALITIES.intersection(modalities)) if is_reuse else None)
    if is_reuse and not modalities:
        warnings.append('REUSE with no usable modality; treated as unclear')

    return {
        'classification': parsed['classification'],
        'confidence': confidence,
        'evidence_quotes': verified,
        'source_quotes': source_quotes,
        'quote_warnings': warnings,
        'hallucinated_quote_count': hallucinated,
        'same_lab': same_lab,
        'same_lab_confidence': same_lab_confidence,
        'source_archive': archive,
        'reused_modalities': modalities,
        'reuse_type': reuse_type,
        'reuse_type_other': reuse_type_other,
        'reused_neurophysiology': reused_neuro,
        'reused_dandi_hosted': reused_dandi,
        'reasoning': parsed.get('reasoning'),
        'mode': mode,
        'prompt_version': PROMPT_VERSION,
        'paper_doi': paper_doi,
        'input_chars': len(sent_text),
        'truncation': truncation,
        'usage': usage,
        'model': model,
        'provider': (raw.get('provider') if isinstance(raw, dict) else None) or provider,
        'error': None,
        'error_kind': None,
    }


# --------------------------------------------------------------------------- #
# Credit preflight (from upstream src/shared/run_fulltext_classification.py)
# --------------------------------------------------------------------------- #

def openrouter_credit_remaining(model: str = DEFAULT_MODEL) -> Optional[float]:
    """
    Remaining credit on the OpenRouter key, or None if not applicable/unknown.

    Cheap to ask and worth asking: a key that is already spent turns a whole run
    into a single repeated 403, and finding that out after the fact costs more
    than the check. Returns None for models served directly by DeepSeek, when
    no key is configured, or when OpenRouter does not answer.
    """
    if not uses_openrouter(model):
        return None
    key = _read_key('OPENROUTER_API_KEY')
    if not key:
        return None
    try:
        resp = _session().get('https://openrouter.ai/api/v1/key',
                              headers={'Authorization': f'Bearer {key}'},
                              timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json().get('data') or {}
        return data.get('limit_remaining')
    except Exception:
        return None
