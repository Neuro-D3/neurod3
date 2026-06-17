"""
Object-storage access for the find_reuse cache.

All reads and writes of cache objects (paper full text, classification and
dataset snapshots, the seed manifest) go through this module. The backing store
is configured by a single environment variable, ``FIND_REUSE_BUCKET_URL``, an
fsspec URL such as ``gs://neuro-d3-staging-find-reuse-cache``. GCS is the store
in staging; because access is via fsspec, ``file://`` and ``memory://`` URLs
work unchanged for local development and tests, and any other fsspec-supported
store is a configuration change rather than a code change.

Credentials are ambient. On the Airflow VM and its containers, gcsfs picks up
the attached service account from the metadata server; on a workstation it picks
up ``gcloud auth application-default login``. ``GOOGLE_APPLICATION_CREDENTIALS``
is the standard escape hatch for hosts where neither applies.

Keys passed to these functions are always relative to the configured bucket URL
(for example ``papers/<sha>/latest.json``, as produced by
``utils.cache_keys.paper_cache_key_for_doi``). The key only, never a full URL,
is what callers persist (for example in ``papers.fulltext_object_key``), so rows
stay portable across providers.

Connection and authentication errors propagate to the caller. This module does
not retry and does not fall back to a different store.
"""

from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from typing import Any, Tuple
from urllib.parse import urlsplit

import fsspec

BUCKET_URL_ENV = "FIND_REUSE_BUCKET_URL"

# Serialization used for every JSON object written through this module. Fixed and
# deterministic so a given object always produces the same bytes and the same
# sha256, which is what makes hash-based idempotency (skip upload when the stored
# object already matches) reliable.
_JSON_KWARGS = {"sort_keys": True, "ensure_ascii": False, "separators": (",", ":")}


class BucketConfigError(RuntimeError):
    """Raised when the bucket URL is not configured."""


def _bucket_url() -> str:
    """Return the configured bucket URL with any trailing slash removed."""
    url = os.environ.get(BUCKET_URL_ENV)
    if not url or not url.strip():
        raise BucketConfigError(
            f"{BUCKET_URL_ENV} is not set. Point it at the cache bucket, "
            "e.g. gs://neuro-d3-staging-find-reuse-cache (or memory://... for tests)."
        )
    return url.strip().rstrip("/")


@lru_cache(maxsize=None)
def _filesystem(url: str) -> fsspec.AbstractFileSystem:
    """Return the fsspec filesystem for a bucket URL's protocol, cached per URL."""
    protocol = urlsplit(url).scheme or "file"
    if protocol in ("file", "local"):
        # Object stores create intermediate prefixes implicitly; the local
        # filesystem does not, so writing a nested key needs the parent dirs
        # created first. auto_mkdir handles that for file:// dev and tests.
        return fsspec.filesystem(protocol, auto_mkdir=True)
    return fsspec.filesystem(protocol)


def _full_path(key: str) -> str:
    """Join a relative cache key onto the configured bucket URL."""
    return f"{_bucket_url()}/{key.lstrip('/')}"


def bucket_name() -> str:
    """
    Return the bucket name from the configured URL, for storing alongside object
    keys (for example in ``papers.fulltext_bucket``). For ``gs://`` this is the
    GCS bucket; for ``memory://`` it is the in-memory namespace.
    """
    url = _bucket_url()
    split = urlsplit(url)
    if split.netloc:
        return split.netloc
    # file:// URLs carry the path in `path`; fall back to its first segment.
    return split.path.lstrip("/").split("/", 1)[0]


def exists(key: str) -> bool:
    """Return whether an object exists at ``key``."""
    return _filesystem(_bucket_url()).exists(_full_path(key))


def get_bytes(key: str) -> bytes:
    """Return the raw bytes at ``key``. Raises ``FileNotFoundError`` if absent."""
    return _filesystem(_bucket_url()).cat_file(_full_path(key))


def put_bytes(key: str, body: bytes) -> Tuple[str, int]:
    """
    Write ``body`` to ``key``. Returns the ``(sha256_hex, byte_count)`` of the
    written content, for integrity tracking by callers.
    """
    _filesystem(_bucket_url()).pipe_file(_full_path(key), body)
    return hashlib.sha256(body).hexdigest(), len(body)


def get_json(key: str) -> Any:
    """Return the JSON object stored at ``key``. Raises ``FileNotFoundError`` if absent."""
    return json.loads(get_bytes(key).decode("utf-8"))


def put_json(key: str, obj: Any) -> Tuple[str, int]:
    """
    Serialize ``obj`` to JSON (deterministically) and write it to ``key``.
    Returns the ``(sha256_hex, byte_count)`` of the written bytes.
    """
    body = json.dumps(obj, **_JSON_KWARGS).encode("utf-8")
    return put_bytes(key, body)
