# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""The storage API through the native facade, on the potato backend."""

import os
import threading

import pytest
import requests

from conftest import free_port
from native_client import NativeClient, NativeError


def expect_error(code: str, status: int, func, *args, **kwargs):
    with pytest.raises(NativeError) as e:
        func(*args, **kwargs)
    assert (e.value.code, e.value.status) == (code, status)


# Buckets

def test_buckets(native):
    native.create_bucket("b2")
    assert native.list_buckets() == ["b2", "bkt"]
    expect_error("bucket_exists", 409, native.create_bucket, "b2")
    native.delete_bucket("b2")
    assert native.list_buckets() == ["bkt"]
    expect_error("no_such_bucket", 404, native.delete_bucket, "b2")


def test_invalid_bucket_name(native):
    expect_error("invalid_argument", 400, native.create_bucket, "a%7Eb")


def test_delete_non_empty_bucket(native):
    native.put("bkt", "k", b"x")
    expect_error("bucket_not_empty", 409, native.delete_bucket, "bkt")
    native.delete("bkt", "k")
    native.delete_bucket("bkt")


def test_open_session_makes_bucket_non_empty(native):
    s = native.open_session("bkt", "a/k")
    expect_error("bucket_not_empty", 409, native.delete_bucket, "bkt")
    native.abort("bkt", "a/k", s)
    native.delete_bucket("bkt")


def test_deleted_objects_leave_bucket_empty(native):
    native.put("bkt", "a/b/c", b"x")
    native.put("bkt", "/x", b"x")
    native.delete("bkt", "a/b/c")
    native.delete("bkt", "/x")
    native.delete_bucket("bkt")


def test_no_such_bucket(native):
    expect_error("no_such_bucket", 404, native.put, "nope", "k", b"x")
    expect_error("no_such_bucket", 404, native.read, "nope", "k")
    expect_error("no_such_bucket", 404, native.list, "nope")
    expect_error("no_such_bucket", 404, native.open_session, "nope", "k")


# Objects

def test_put_get(native):
    info = native.put("bkt", "key", b"hello", metadata={"Color": "red"})
    assert info["key"] == "key"
    assert info["size"] == 5
    assert info["metadata"] == {"color": "red"}
    r = native.get("bkt", "key")
    assert r.content == b"hello"
    assert r.headers["X-Labaz-Size"] == "5"
    assert r.headers["X-Labaz-Generation"] == str(info["generation"])
    assert r.headers["X-Labaz-Meta-color"] == "red"


def test_head(native):
    info = native.put("bkt", "key", b"hello", metadata={"m": "v"})
    r = native.head("bkt", "key")
    assert r.headers["X-Labaz-Size"] == "5"
    assert r.headers["X-Labaz-Generation"] == str(info["generation"])
    assert r.headers["X-Labaz-Meta-m"] == "v"
    expect_error("", 404, native.head, "bkt", "nope")  # HEAD has no body


def test_empty_object(native):
    assert native.put("bkt", "empty", b"")["size"] == 0
    assert native.read("bkt", "empty") == b""
    s = native.open_session("bkt", "empty2")
    assert native.commit("bkt", "empty2", s, [])["size"] == 0
    assert native.read("bkt", "empty2") == b""


def test_large_object(native):
    data = os.urandom(5 * 1024 * 1024 + 123)
    native.put("bkt", "large", data)
    assert native.read("bkt", "large") == data


def test_delete(native):
    native.put("bkt", "key", b"x")
    native.delete("bkt", "key")
    expect_error("no_such_key", 404, native.read, "bkt", "key")
    expect_error("no_such_key", 404, native.delete, "bkt", "key")


def test_overwrite(native):
    g1 = native.put("bkt", "key", b"first", metadata={"v": "1"})["generation"]
    g2 = native.put("bkt", "key", b"second!", metadata={"v": "2"})["generation"]
    assert g2 > g1
    r = native.get("bkt", "key")
    assert r.content == b"second!"
    assert r.headers["X-Labaz-Meta-v"] == "2"


def test_generation_precondition(native):
    expect_error("precondition_failed", 412, native.put, "bkt", "key", b"x", generation_match=1)
    g1 = native.put("bkt", "key", b"x", generation_match=0)["generation"]
    expect_error("precondition_failed", 412, native.put, "bkt", "key", b"y", generation_match=0)
    expect_error("precondition_failed", 412, native.put, "bkt", "key", b"y", generation_match=g1 + 1)
    native.put("bkt", "key", b"y", generation_match=g1)
    assert native.read("bkt", "key") == b"y"


def test_generation_unique_after_recreate(native):
    g1 = native.put("bkt", "key", b"x")["generation"]
    native.delete("bkt", "key")
    g2 = native.put("bkt", "key", b"x")["generation"]
    assert g2 != g1


@pytest.mark.parametrize("rng, expected, content_range", [
    ("bytes=0-0", b"0", "bytes 0-0/10"),
    ("bytes=2-5", b"2345", "bytes 2-5/10"),
    ("bytes=7-", b"789", "bytes 7-9/10"),
    ("bytes=-3", b"789", "bytes 7-9/10"),
    ("bytes=-30", b"0123456789", "bytes 0-9/10"),
    ("bytes=5-100", b"56789", "bytes 5-9/10"),
])
def test_range(native, rng, expected, content_range):
    native.put("bkt", "key", b"0123456789")
    r = native.get("bkt", "key", range=rng)
    assert r.status_code == 206
    assert r.content == expected
    assert r.headers["Content-Range"] == content_range


@pytest.mark.parametrize("rng", ["bytes=10-", "bytes=10-20", "bytes=-0"])
def test_unsatisfiable_range(native, rng):
    native.put("bkt", "key", b"0123456789")
    expect_error("invalid_range", 416, native.read, "bkt", "key", range=rng)


def test_bad_range(native):
    native.put("bkt", "key", b"0123456789")
    expect_error("bad_request", 400, native.read, "bkt", "key", range="pages=1-2")


# Keys

@pytest.mark.parametrize("key", [
    "a/b/c",
    "/leading",
    "a//b",
    ".", "..", "./x", "x/../y",
    "tilde~m", "~e", "per%cent", "sp ace", "q?uery#hash",
    "ünïcödé",
    "x" * 1024,
    "/".join(["y" * 300] * 3),
])
def test_key_round_trip(native, key):
    native.put("bkt", key, key.encode())
    assert native.read("bkt", key) == key.encode()
    assert native.list_keys("bkt") == [key]
    native.delete("bkt", key)
    assert native.list_keys("bkt") == []


@pytest.mark.parametrize("key", ["dir/", "a/b/"])
def test_key_ending_with_slash(native, key):
    expect_error("invalid_argument", 400, native.put, "bkt", key, b"x")
    expect_error("invalid_argument", 400, native.open_session, "bkt", key)
    assert native.list_keys("bkt") == []


def test_file_and_directory_keys_coexist(native):
    for k in ["a", "a/b", "a/b/c"]:
        native.put("bkt", k, k.encode())
    for k in ["a", "a/b", "a/b/c"]:
        assert native.read("bkt", k) == k.encode()


def test_flat_namespace(native):
    native.put("bkt", "sstables/x/data", b"1")
    native.delete("bkt", "sstables/x/data")
    assert native.list_keys("bkt") == []
    native.put("bkt", "sstables/x/index", b"2")
    assert native.read("bkt", "sstables/x/index") == b"2"


def test_list_order_and_prefix(native):
    keys = ["a", "a/b", "a/c", "a.b", "a0", "ab", "b", "a-", "a/b/c", "~", "%", "a//x"]
    for k in keys:
        native.put("bkt", k, b"")
    assert native.list_keys("bkt") == sorted(keys, key=lambda k: k.encode())
    assert native.list_keys("bkt", prefix="a/") == ["a//x", "a/b", "a/b/c", "a/c"]
    assert native.list_keys("bkt", prefix="a/b") == ["a/b", "a/b/c"]
    assert native.list_keys("bkt", prefix="zzz") == []


def test_list_pagination(native):
    keys = sorted(f"{d}/{i:02}" for d in ["x", "y", "z"] for i in range(7))
    for k in keys:
        native.put("bkt", k, b"")
    for page in [1, 2, 5, 100]:
        assert native.list_keys("bkt", page=page) == keys
    r = native.list("bkt", start_after="y/03", max_keys=2)
    assert [o["key"] for o in r["objects"]] == ["y/04", "y/05"]
    assert r["truncated"]
    r = native.list("bkt", start_after="z/06")
    assert r["objects"] == [] and not r["truncated"]


def test_list_reports_object_info(native):
    native.put("bkt", "k", b"12345", metadata={"m": "v"})
    [o] = native.list("bkt")["objects"]
    assert (o["key"], o["size"], o["metadata"]) == ("k", 5, {"m": "v"})


# Sessions

def test_session(native):
    s = native.open_session("bkt", "obj", metadata={"m": "v"})
    assert native.put_part("bkt", "obj", s, 3, b"ccc") == {"number": 3, "size": 3}
    native.put_part("bkt", "obj", s, 1, b"a")
    native.put_part("bkt", "obj", s, 2, b"")
    d = native.describe_session("bkt", "obj", s)
    assert (d["bucket"], d["key"], d["metadata"]) == ("bkt", "obj", {"m": "v"})
    assert [(p["number"], p["size"]) for p in d["parts"]] == [(1, 1), (2, 0), (3, 3)]
    info = native.commit("bkt", "obj", s, [1, 2, 3])
    assert (info["size"], info["metadata"]) == (4, {"m": "v"})
    assert native.read("bkt", "obj") == b"accc"


def test_session_commit_all_parts_by_default(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 2, b"2")
    native.put_part("bkt", "obj", s, 10, b"10")
    native.put_part("bkt", "obj", s, 1, b"1")
    assert native.commit("bkt", "obj", s)["size"] == 4
    assert native.read("bkt", "obj") == b"1210"


def test_session_part_order_is_commit_order(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"1")
    native.put_part("bkt", "obj", s, 2, b"2")
    native.commit("bkt", "obj", s, [2, 1])
    assert native.read("bkt", "obj") == b"21"


def test_session_resent_part_replaces(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"old")
    native.put_part("bkt", "obj", s, 1, b"new!")
    assert [p["size"] for p in native.describe_session("bkt", "obj", s)["parts"]] == [4]
    native.commit("bkt", "obj", s, [1])
    assert native.read("bkt", "obj") == b"new!"


def test_session_commit_subset(native):
    s = native.open_session("bkt", "obj")
    for n in range(1, 5):
        native.put_part("bkt", "obj", s, n, str(n).encode())
    native.commit("bkt", "obj", s, [1, 3])
    assert native.read("bkt", "obj") == b"13"


def test_session_commit_errors(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"1")
    expect_error("no_such_part", 404, native.commit, "bkt", "obj", s, [1, 2])
    expect_error("invalid_argument", 400, native.commit, "bkt", "obj", s, [1, 1])
    # The session survives a failed commit.
    native.commit("bkt", "obj", s, [1])


def test_session_commit_retry(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"1")
    first = native.commit("bkt", "obj", s, [1])
    assert native.commit("bkt", "obj", s, [1]) == first
    expect_error("no_such_session", 404, native.put_part, "bkt", "obj", s, 2, b"2")
    expect_error("no_such_session", 404, native.describe_session, "bkt", "obj", s)


def test_session_commit_precondition(native):
    g = native.put("bkt", "obj", b"x")["generation"]
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"y")
    expect_error("precondition_failed", 412, native.commit, "bkt", "obj", s, [1], generation_match=0)
    native.commit("bkt", "obj", s, [1], generation_match=g)
    assert native.read("bkt", "obj") == b"y"


def test_session_abort(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"1")
    native.abort("bkt", "obj", s)
    expect_error("no_such_session", 404, native.describe_session, "bkt", "obj", s)
    expect_error("no_such_session", 404, native.commit, "bkt", "obj", s, [1])
    expect_error("no_such_session", 404, native.abort, "bkt", "obj", s)
    expect_error("no_such_key", 404, native.read, "bkt", "obj")


def test_concurrent_sessions_of_one_object(native):
    s1 = native.open_session("bkt", "obj")
    s2 = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s1, 1, b"one")
    native.put_part("bkt", "obj", s2, 1, b"two")
    native.commit("bkt", "obj", s2)
    assert native.read("bkt", "obj") == b"two"
    native.commit("bkt", "obj", s1)
    assert native.read("bkt", "obj") == b"one"


def test_session_of_another_object(native):
    native.create_bucket("other")
    s = native.open_session("bkt", "obj")
    for bucket, key in [("bkt", "obj2"), ("other", "obj")]:
        expect_error("no_such_session", 404, native.put_part, bucket, key, s, 1, b"x")
        expect_error("no_such_session", 404, native.describe_session, bucket, key, s)
        expect_error("no_such_session", 404, native.commit, bucket, key, s)
        expect_error("no_such_session", 404, native.abort, bucket, key, s)
    native.abort("bkt", "obj", s)


@pytest.mark.parametrize("session", ["nope", "0.zz", "99.1", "0.1", "../x", ""])
def test_bad_session_ids(native, session):
    expect_error("no_such_session", 404, native.describe_session, "bkt", "obj", session)


def test_bad_parts_list(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"1")
    for parts in ["1,x", "1,,2", "-1"]:
        expect_error("bad_request", 400, native.request, "PATCH", "/bkt/obj", params={"session": s, "parts": parts})
    native.commit("bkt", "obj", s, [1])


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
def test_part_without_session(native, method):
    expect_error("bad_request", 400, native.request, method, "/bkt/obj", params={"part": 1}, data=b"x")


def test_session_without_part(native):
    s = native.open_session("bkt", "obj")
    expect_error("bad_request", 400, native.request, "PUT", "/bkt/obj", params={"session": s}, data=b"x")


@pytest.mark.parametrize("method", ["GET", "DELETE"])
def test_unexpected_part(native, method):
    s = native.open_session("bkt", "obj")
    expect_error("bad_request", 400, native.request, method, "/bkt/obj", params={"session": s, "part": 1})
    assert native.describe_session("bkt", "obj", s)["session"] == s


def test_commit_without_session(native):
    expect_error("bad_request", 400, native.request, "PATCH", "/bkt/obj")


def test_session_overwrites_atomically(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"new")
    native.put("bkt", "obj", b"old")
    native.commit("bkt", "obj", s, [1])
    assert native.read("bkt", "obj") == b"new"


def test_range_across_parts(native):
    s = native.open_session("bkt", "obj")
    native.put_part("bkt", "obj", s, 1, b"0123")
    native.put_part("bkt", "obj", s, 2, b"")
    native.put_part("bkt", "obj", s, 3, b"45")
    native.put_part("bkt", "obj", s, 4, b"6789")
    native.commit("bkt", "obj", s, [1, 2, 3, 4])
    data = b"0123456789"
    for first in range(10):
        for last in range(first, 10):
            assert native.read("bkt", "obj", range=f"bytes={first}-{last}") == data[first:last + 1]


# Copy

def multipart(native, key, chunks):
    s = native.open_session("bkt", key)
    for n, c in enumerate(chunks, 1):
        native.put_part("bkt", key, s, n, c)
    return native.commit("bkt", key, s)


def test_copy_object(native):
    src = native.put("bkt", "src", b"payload", metadata={"m": "src"})
    info = native.copy("bkt", "dst", "src", metadata={"m": "dst"})
    assert info["size"] == 7
    assert info["generation"] != src["generation"]
    assert native.read("bkt", "dst") == b"payload"
    assert native.head("bkt", "dst").headers["X-Labaz-Meta-m"] == "dst"
    assert native.read("bkt", "src") == b"payload"


def test_copy_shares_files(native, tmp_path):
    native.put("bkt", "src", b"payload")
    native.copy("bkt", "dst", "src")
    links = [f.stat().st_nlink for f in (tmp_path / "server0" / "data").rglob("part.*")]
    assert links == [2, 2]


def test_copy_outlives_source(native):
    multipart(native, "src", [b"01", b"23", b"45"])
    native.copy("bkt", "dst", "src")
    native.put("bkt", "src", b"new")
    assert native.read("bkt", "dst") == b"012345"
    native.delete("bkt", "src")
    assert native.read("bkt", "dst") == b"012345"


def test_copy_range(native):
    data = b"0123456789"
    multipart(native, "src", [b"0123", b"", b"45", b"6789"])
    for first in range(10):
        for last in range(first, 10):
            native.copy("bkt", "dst", "src", range=f"bytes={first}-{last}")
            assert native.read("bkt", "dst") == data[first:last + 1]
    native.copy("bkt", "dst", "src", range="bytes=7-")
    assert native.read("bkt", "dst") == b"789"
    native.copy("bkt", "dst", "src", range="bytes=-3")
    assert native.read("bkt", "dst") == b"789"


def test_copy_of_copy(native):
    multipart(native, "a", [b"0123", b"4567"])
    native.copy("bkt", "b", "a", range="bytes=2-5")
    native.copy("bkt", "c", "b", range="bytes=1-2")
    assert native.read("bkt", "c") == b"34"
    assert native.read("bkt", "c", range="bytes=1-1") == b"4"


def test_copy_onto_itself(native):
    native.put("bkt", "obj", b"0123456789")
    native.copy("bkt", "obj", "obj", range="bytes=2-4")
    assert native.read("bkt", "obj") == b"234"


def test_copy_empty_object(native):
    native.put("bkt", "src", b"")
    assert native.copy("bkt", "dst", "src")["size"] == 0
    assert native.read("bkt", "dst") == b""
    expect_error("invalid_range", 416, native.copy, "bkt", "dst", "src", range="bytes=0-0")


def test_copy_parts(native):
    native.put("bkt", "src", b"0123456789")
    s = native.open_session("bkt", "dst")
    assert native.copy_part("bkt", "dst", s, 1, "src", range="bytes=5-") == {"number": 1, "size": 5}
    native.put_part("bkt", "dst", s, 2, b"-")
    native.copy_part("bkt", "dst", s, 3, "src", range="bytes=0-4")
    native.copy_part("bkt", "dst", s, 4, "src")
    native.copy_part("bkt", "dst", s, 4, "src", range="bytes=0-0")  # replaces part 4
    assert [p["size"] for p in native.describe_session("bkt", "dst", s)["parts"]] == [5, 1, 5, 1]
    native.put("bkt", "src", b"changed")
    native.commit("bkt", "dst", s, [1, 2, 3])
    assert native.read("bkt", "dst") == b"56789-01234"


def test_copy_part_dropped_on_abort(native, tmp_path):
    native.put("bkt", "src", b"payload")
    s = native.open_session("bkt", "dst")
    native.copy_part("bkt", "dst", s, 1, "src")
    native.abort("bkt", "dst", s)
    links = [f.stat().st_nlink for f in (tmp_path / "server0" / "data").rglob("part.*")]
    assert links == [1]


def test_copy_preconditions(native):
    src = native.put("bkt", "src", b"payload")
    expect_error("precondition_failed", 412, native.copy, "bkt", "dst", "src",
                 src_generation_match=src["generation"] + 1)
    expect_error("no_such_key", 404, native.read, "bkt", "dst")
    native.copy("bkt", "dst", "src", src_generation_match=src["generation"])
    expect_error("precondition_failed", 412, native.copy, "bkt", "dst", "src", generation_match=0)
    s = native.open_session("bkt", "dst")
    expect_error("precondition_failed", 412, native.copy_part, "bkt", "dst", s, 1, "src",
                 src_generation_match=src["generation"] + 1)
    assert native.describe_session("bkt", "dst", s)["parts"] == []


def test_copy_missing_source(native):
    expect_error("no_such_key", 404, native.copy, "bkt", "dst", "nope")
    expect_error("no_such_key", 404, native.read, "bkt", "dst")
    s = native.open_session("bkt", "dst")
    expect_error("no_such_key", 404, native.copy_part, "bkt", "dst", s, 1, "nope")
    expect_error("invalid_argument", 400, native.copy, "bkt", "dst", "")


def test_copy_odd_keys(native):
    src = "a&b=c/%2F?#+ ü"
    native.put("bkt", src, b"x")
    native.copy("bkt", "dst", src)
    assert native.read("bkt", "dst") == b"x"


@pytest.mark.parametrize("params", [
    {"copy-range": "bytes=0-1"},
    {"copy-if-generation-match": "1"},
])
def test_copy_params_without_key(native, params):
    expect_error("bad_request", 400, native.request, "PUT", "/bkt/obj", params=params, data=b"x")


@pytest.mark.parametrize("method", ["GET", "DELETE"])
def test_copy_key_not_on_put(native, method):
    native.put("bkt", "obj", b"x")
    expect_error("bad_request", 400, native.request, method, "/bkt/obj", params={"copy-key": "obj"})
    assert native.read("bkt", "obj") == b"x"


# Protocol

def test_unknown_route(native):
    # Seastar's own "not found" reply.
    expect_error("", 404, native.request, "OPTIONS", "/bkt/obj")


def test_empty_bucket_in_path(native):
    expect_error("bad_request", 400, native.request, "POST", "/")


# Concurrency and sharding

def test_overwrite_under_reads(native):
    """A reader sees either version, never a mix or an error."""
    a, b = b"a" * 100_000, b"b" * 50_000
    native.put("bkt", "obj", a)
    stop = threading.Event()
    errors = []

    def reader():
        c = NativeClient(native.url)
        while not stop.is_set():
            try:
                data = c.read("bkt", "obj")
                if data not in (a, b):
                    errors.append(f"bad content of length {len(data)}")
            except Exception as e:
                errors.append(repr(e))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        for i in range(50):
            native.put("bkt", "obj", a if i % 2 else b)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert errors == []


# Persistence

def test_objects_survive_restart(run_labaz, tmp_path):
    data_dir = tmp_path / "data"
    port = free_port()
    args = ["--facade", f"native:http={port}", "--data-dir", str(data_dir)]
    server = run_labaz(args)
    c = NativeClient(f"http://127.0.0.1:{port}")
    c.create_bucket("bkt")
    info = c.put("bkt", "a/b", b"payload", metadata={"m": "v"})
    assert server.stop() == 0

    server.start()
    c = NativeClient(f"http://127.0.0.1:{port}")
    assert c.list_buckets() == ["bkt"]
    assert c.read("bkt", "a/b") == b"payload"
    [o] = c.list("bkt")["objects"]
    assert o == info
