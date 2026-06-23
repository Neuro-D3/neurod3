"""
Unit tests for ``utils.bucket``.

All tests run against ``memory://`` or a ``file://`` temp directory, so they
exercise the real fsspec read/write path with no live cloud calls.
"""

import hashlib
import json

import pytest

from utils import bucket


@pytest.fixture
def memory_bucket(monkeypatch):
    """Point the module at a unique in-memory bucket per test."""
    url = f"memory://test-{bucket.__name__}"
    monkeypatch.setenv(bucket.BUCKET_URL_ENV, url)
    fs = bucket._filesystem(url)
    # MemoryFileSystem state is process-global; clear anything a prior test left.
    for path in fs.find(url):
        fs.rm_file(path)
    return url


def test_put_get_bytes_roundtrip(memory_bucket):
    sha, n = bucket.put_bytes("papers/abc/latest.json", b"hello world")
    assert n == 11
    assert sha == hashlib.sha256(b"hello world").hexdigest()
    assert bucket.get_bytes("papers/abc/latest.json") == b"hello world"


def test_put_get_json_roundtrip(memory_bucket):
    obj = {"doi": "10.1/x", "full_text": "body", "authors": ["b", "a"]}
    sha, n = bucket.put_json("papers/x/latest.json", obj)
    assert bucket.get_json("papers/x/latest.json") == obj
    # Hash and size correspond to the module's deterministic serialization.
    expected = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    assert sha == hashlib.sha256(expected).hexdigest()
    assert n == len(expected)


def test_put_json_is_deterministic(memory_bucket):
    """Same object, different key insertion order, yields the same hash."""
    sha1, _ = bucket.put_json("k1", {"a": 1, "b": 2})
    sha2, _ = bucket.put_json("k2", {"b": 2, "a": 1})
    assert sha1 == sha2


def test_exists(memory_bucket):
    assert bucket.exists("missing/key.json") is False
    bucket.put_bytes("present/key.json", b"x")
    assert bucket.exists("present/key.json") is True


def test_get_missing_key_raises(memory_bucket):
    with pytest.raises(FileNotFoundError):
        bucket.get_bytes("nope/missing.json")


def test_leading_slash_in_key_is_normalized(memory_bucket):
    bucket.put_bytes("/papers/y/latest.json", b"data")
    assert bucket.get_bytes("papers/y/latest.json") == b"data"


def test_missing_env_raises(monkeypatch):
    monkeypatch.delenv(bucket.BUCKET_URL_ENV, raising=False)
    with pytest.raises(bucket.BucketConfigError):
        bucket.exists("any/key")


@pytest.mark.parametrize(
    "url,expected",
    [
        ("gs://neuro-d3-staging-find-reuse-cache", "neuro-d3-staging-find-reuse-cache"),
        ("gs://my-bucket/sub/prefix", "my-bucket"),
        ("memory://fr-cache", "fr-cache"),
    ],
)
def test_bucket_name(monkeypatch, url, expected):
    monkeypatch.setenv(bucket.BUCKET_URL_ENV, url)
    assert bucket.bucket_name() == expected


def test_trailing_slash_in_url_is_stripped(monkeypatch):
    monkeypatch.setenv(bucket.BUCKET_URL_ENV, "memory://fr-cache/")
    bucket.put_bytes("papers/z.json", b"q")
    assert bucket.get_bytes("papers/z.json") == b"q"


def test_file_url_roundtrip(monkeypatch, tmp_path):
    """A file:// URL exercises the same path against the local filesystem."""
    monkeypatch.setenv(bucket.BUCKET_URL_ENV, f"file://{tmp_path}")
    sha, n = bucket.put_json("papers/local/latest.json", {"ok": True})
    assert bucket.get_json("papers/local/latest.json") == {"ok": True}
    assert (tmp_path / "papers" / "local" / "latest.json").exists()
    assert n > 0 and len(sha) == 64
