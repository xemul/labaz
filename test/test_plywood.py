# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""The storage API through the plywood facade, on the potato backend. What
every facade does alike is tested in test_objects.py."""

import hashlib
import os
import zlib

import pytest

from conftest import free_port
from plywood_client import PlywoodClient, PlywoodError


def expect_error(code: str, status: int, func, *args, **kwargs):
    with pytest.raises(PlywoodError) as e:
        func(*args, **kwargs)
    assert (e.value.code, e.value.status) == (code, status)


# Buckets

def test_invalid_bucket_name(plywood):
    expect_error("invalid_argument", 400, plywood.create_bucket, "a%7Eb")


def test_open_session_makes_bucket_non_empty(plywood):
    s = plywood.open_session("bkt", "a/k")
    expect_error("bucket_not_empty", 409, plywood.delete_bucket, "bkt")
    plywood.abort("bkt", "a/k", s)
    plywood.delete_bucket("bkt")


def test_no_such_bucket_for_session(plywood):
    expect_error("no_such_bucket", 404, plywood.open_session, "nope", "k")


# Objects

def test_put_get(plywood):
    info = plywood.put("bkt", "key", b"hello", metadata={"Color": "red"})
    assert info["key"] == "key"
    assert info["size"] == 5
    assert info["metadata"] == {"color": "red"}
    r = plywood.get("bkt", "key")
    assert r.content == b"hello"
    assert r.headers["X-Labaz-Size"] == "5"
    assert r.headers["X-Labaz-Generation"] == str(info["generation"])
    assert r.headers["X-Labaz-Meta-color"] == "red"


def test_head(plywood):
    info = plywood.put("bkt", "key", b"hello", metadata={"m": "v"})
    r = plywood.head("bkt", "key")
    assert r.headers["X-Labaz-Size"] == "5"
    assert r.headers["X-Labaz-Generation"] == str(info["generation"])
    assert r.headers["X-Labaz-Meta-m"] == "v"
    expect_error("", 404, plywood.head, "bkt", "nope")  # HEAD has no body


def test_empty_session_object(plywood):
    s = plywood.open_session("bkt", "empty2")
    assert plywood.commit("bkt", "empty2", s, [])["size"] == 0
    assert plywood.read("bkt", "empty2") == b""


def test_overwrite(plywood):
    g1 = plywood.put("bkt", "key", b"first", metadata={"v": "1"})["generation"]
    g2 = plywood.put("bkt", "key", b"second!", metadata={"v": "2"})["generation"]
    assert g2 > g1
    r = plywood.get("bkt", "key")
    assert r.content == b"second!"
    assert r.headers["X-Labaz-Meta-v"] == "2"


def test_upload_kind(plywood):
    assert plywood.put("bkt", "single", b"x")["multipart"] is False
    assert plywood.copy("bkt", "copy", "single")["multipart"] is False
    s = plywood.open_session("bkt", "multi")
    plywood.put_part("bkt", "multi", s, 1, b"x")
    assert plywood.commit("bkt", "multi", s, [1])["multipart"] is True
    assert plywood.commit("bkt", "multi", s, [1])["multipart"] is True  # a retry
    listed = {o["key"]: o["multipart"] for o in plywood.list("bkt")["objects"]}
    assert listed == {"single": False, "copy": False, "multi": True}


def test_generation_precondition(plywood):
    expect_error("precondition_failed", 412, plywood.put, "bkt", "key", b"x", generation_match=1)
    g1 = plywood.put("bkt", "key", b"x", generation_match=0)["generation"]
    expect_error("precondition_failed", 412, plywood.put, "bkt", "key", b"y", generation_match=0)
    expect_error("precondition_failed", 412, plywood.put, "bkt", "key", b"y", generation_match=g1 + 1)
    plywood.put("bkt", "key", b"y", generation_match=g1)
    assert plywood.read("bkt", "key") == b"y"


def test_generation_unique_after_recreate(plywood):
    g1 = plywood.put("bkt", "key", b"x")["generation"]
    plywood.delete("bkt", "key")
    g2 = plywood.put("bkt", "key", b"x")["generation"]
    assert g2 != g1


def test_bad_range(plywood):
    plywood.put("bkt", "key", b"0123456789")
    expect_error("bad_request", 400, plywood.read, "bkt", "key", range="pages=1-2")


# Keys

@pytest.mark.parametrize("key", ["dir/", "a/b/"])
def test_session_key_ending_with_slash(plywood, key):
    expect_error("invalid_argument", 400, plywood.open_session, "bkt", key)
    assert plywood.list_keys("bkt") == []


def test_list_reports_object_info(plywood):
    plywood.put("bkt", "k", b"12345", metadata={"m": "v"})
    [o] = plywood.list("bkt")["objects"]
    assert (o["key"], o["size"], o["metadata"]) == ("k", 5, {"m": "v"})


# Sessions

def test_session_of_small_parts(plywood):
    # Parts of any size, as plywood takes, which S3 does not.
    s = plywood.open_session("bkt", "obj", metadata={"m": "v"})
    assert plywood.put_part("bkt", "obj", s, 3, b"ccc") == {"number": 3, "size": 3, "checksums": {}}
    plywood.put_part("bkt", "obj", s, 1, b"a")
    plywood.put_part("bkt", "obj", s, 2, b"")
    d = plywood.describe_session("bkt", "obj", s)
    assert (d["bucket"], d["key"], d["metadata"]) == ("bkt", "obj", {"m": "v"})
    assert [(p["number"], p["size"]) for p in d["parts"]] == [(1, 1), (2, 0), (3, 3)]
    info = plywood.commit("bkt", "obj", s, [1, 2, 3])
    assert (info["size"], info["metadata"]) == (4, {"m": "v"})
    assert plywood.read("bkt", "obj") == b"accc"


def test_session_commit_all_parts_by_default(plywood):
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 2, b"2")
    plywood.put_part("bkt", "obj", s, 10, b"10")
    plywood.put_part("bkt", "obj", s, 1, b"1")
    assert plywood.commit("bkt", "obj", s)["size"] == 4
    assert plywood.read("bkt", "obj") == b"1210"


def test_session_part_order_is_commit_order(plywood):
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 1, b"1")
    plywood.put_part("bkt", "obj", s, 2, b"2")
    plywood.commit("bkt", "obj", s, [2, 1])
    assert plywood.read("bkt", "obj") == b"21"


def test_session_commit_duplicate_part(plywood):
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 1, b"1")
    expect_error("invalid_argument", 400, plywood.commit, "bkt", "obj", s, [1, 1])
    # The session survives a failed commit.
    plywood.commit("bkt", "obj", s, [1])


def test_session_commit_generation_match(plywood):
    g = plywood.put("bkt", "obj", b"x")["generation"]
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 1, b"y")
    expect_error("precondition_failed", 412, plywood.commit, "bkt", "obj", s, [1], generation_match=g + 1)
    plywood.commit("bkt", "obj", s, [1], generation_match=g)
    assert plywood.read("bkt", "obj") == b"y"


def test_bad_parts_list(plywood):
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 1, b"1")
    for parts in ["1,x", "1,,2", "-1"]:
        expect_error("bad_request", 400, plywood.request, "PATCH", "/bkt/obj", params={"session": s, "parts": parts})
    plywood.commit("bkt", "obj", s, [1])


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
def test_part_without_session(plywood, method):
    expect_error("bad_request", 400, plywood.request, method, "/bkt/obj", params={"part": 1}, data=b"x")


def test_session_without_part(plywood):
    s = plywood.open_session("bkt", "obj")
    expect_error("bad_request", 400, plywood.request, "PUT", "/bkt/obj", params={"session": s}, data=b"x")


@pytest.mark.parametrize("method", ["GET", "DELETE"])
def test_unexpected_part(plywood, method):
    s = plywood.open_session("bkt", "obj")
    expect_error("bad_request", 400, plywood.request, method, "/bkt/obj", params={"session": s, "part": 1})
    assert plywood.describe_session("bkt", "obj", s)["session"] == s


def test_commit_without_session(plywood):
    expect_error("bad_request", 400, plywood.request, "PATCH", "/bkt/obj")


def test_range_across_small_parts(plywood):
    s = plywood.open_session("bkt", "obj")
    plywood.put_part("bkt", "obj", s, 1, b"0123")
    plywood.put_part("bkt", "obj", s, 2, b"")
    plywood.put_part("bkt", "obj", s, 3, b"45")
    plywood.put_part("bkt", "obj", s, 4, b"6789")
    plywood.commit("bkt", "obj", s, [1, 2, 3, 4])
    data = b"0123456789"
    for first in range(10):
        for last in range(first, 10):
            assert plywood.read("bkt", "obj", range=f"bytes={first}-{last}") == data[first:last + 1]


# Copy

def multipart(plywood, key, chunks):
    s = plywood.open_session("bkt", key)
    for n, c in enumerate(chunks, 1):
        plywood.put_part("bkt", key, s, n, c)
    return plywood.commit("bkt", key, s)


def test_copy_range(plywood):
    data = b"0123456789"
    multipart(plywood, "src", [b"0123", b"", b"45", b"6789"])
    for first in range(10):
        for last in range(first, 10):
            plywood.copy("bkt", "dst", "src", range=f"bytes={first}-{last}")
            assert plywood.read("bkt", "dst") == data[first:last + 1]
    plywood.copy("bkt", "dst", "src", range="bytes=7-")
    assert plywood.read("bkt", "dst") == b"789"
    plywood.copy("bkt", "dst", "src", range="bytes=-3")
    assert plywood.read("bkt", "dst") == b"789"


def test_copy_of_copy(plywood):
    multipart(plywood, "a", [b"0123", b"4567"])
    plywood.copy("bkt", "b", "a", range="bytes=2-5")
    plywood.copy("bkt", "c", "b", range="bytes=1-2")
    assert plywood.read("bkt", "c") == b"34"
    assert plywood.read("bkt", "c", range="bytes=1-1") == b"4"


def test_copy_range_onto_itself(plywood):
    plywood.put("bkt", "obj", b"0123456789")
    plywood.copy("bkt", "obj", "obj", range="bytes=2-4")
    assert plywood.read("bkt", "obj") == b"234"


def test_copy_range_of_empty_object(plywood):
    plywood.put("bkt", "src", b"")
    expect_error("invalid_range", 416, plywood.copy, "bkt", "dst", "src", range="bytes=0-0")


def test_copy_part_open_ranges(plywood):
    plywood.put("bkt", "src", b"0123456789")
    s = plywood.open_session("bkt", "dst")
    assert plywood.copy_part("bkt", "dst", s, 1, "src", range="bytes=5-") == {"number": 1, "size": 5, "checksums": {}}
    plywood.copy_part("bkt", "dst", s, 2, "src", range="bytes=-3")
    plywood.commit("bkt", "dst", s, [1, 2])
    assert plywood.read("bkt", "dst") == b"56789789"


def test_copy_preconditions(plywood):
    src = plywood.put("bkt", "src", b"payload")
    expect_error("precondition_failed", 412, plywood.copy, "bkt", "dst", "src",
                 src_generation_match=src["generation"] + 1)
    expect_error("no_such_key", 404, plywood.read, "bkt", "dst")
    plywood.copy("bkt", "dst", "src", src_generation_match=src["generation"])
    s = plywood.open_session("bkt", "dst")
    expect_error("precondition_failed", 412, plywood.copy_part, "bkt", "dst", s, 1, "src",
                 src_generation_match=src["generation"] + 1)
    assert plywood.describe_session("bkt", "dst", s)["parts"] == []


@pytest.mark.parametrize("params", [
    {"copy-range": "bytes=0-1"},
    {"copy-if-generation-match": "1"},
])
def test_copy_params_without_key(plywood, params):
    expect_error("bad_request", 400, plywood.request, "PUT", "/bkt/obj", params=params, data=b"x")


@pytest.mark.parametrize("method", ["GET", "DELETE"])
def test_copy_key_not_on_put(plywood, method):
    plywood.put("bkt", "obj", b"x")
    expect_error("bad_request", 400, plywood.request, method, "/bkt/obj", params={"copy-key": "obj"})
    assert plywood.read("bkt", "obj") == b"x"


# Checksums

ALGORITHMS = ["crc32", "crc32c", "crc64nvme", "md5", "sha1", "sha256"]
CRCS = ["crc32", "crc32c", "crc64nvme"]


def reflected_crc(data: bytes, poly: int, width: int) -> bytes:
    mask = (1 << width) - 1
    crc = mask
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
    return (crc ^ mask).to_bytes(width // 8, "big")


def digest(alg: str, data: bytes) -> str:
    """The reference value of a checksum, in hex."""
    if alg == "crc32":
        return zlib.crc32(data).to_bytes(4, "big").hex()
    if alg == "crc32c":
        return reflected_crc(data, 0x82F63B78, 32).hex()
    if alg == "crc64nvme":
        return reflected_crc(data, 0x9A6C9329AC4BC9B5, 64).hex()
    return hashlib.new(alg, data).hexdigest()


def digests(algs, data: bytes) -> dict:
    return {alg: digest(alg, data) for alg in algs}


def test_checksum_check_values(plywood):
    info = plywood.put("bkt", "obj", b"123456789", checksums=ALGORITHMS)
    assert info["checksums"] == {
        "crc32": "cbf43926",
        "crc32c": "e3069283",
        "crc64nvme": "ae8b14860a799888",
        "md5": "25f9e794323b453885f5181f1b624d0b",
        "sha1": "f7c3bc1d808e04732adf679965ccc34ca7ae3441",
        "sha256": "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225",
    }
    assert info["checksums"] == digests(ALGORITHMS, b"123456789")


def test_no_checksums_by_default(plywood):
    info = plywood.put("bkt", "obj", b"data")
    assert info["checksums"] == {}
    assert info["parts"] == [{"number": 1, "size": 4, "checksums": {}}]


def test_checksums_reported(plywood):
    data = os.urandom(10_000)
    info = plywood.put("bkt", "obj", data, checksums=["crc32c", "md5"])
    expected = digests(["crc32c", "md5"], data)
    assert info["checksums"] == expected
    assert info["parts"] == [{"number": 1, "size": len(data), "checksums": expected}]
    headers = plywood.head("bkt", "obj").headers
    assert {alg: headers[f"X-Labaz-Checksum-{alg}"] for alg in expected} == expected
    assert "X-Labaz-Checksum-sha1" not in headers
    assert plywood.get("bkt", "obj").headers["X-Labaz-Checksum-crc32c"] == expected["crc32c"]
    [o] = plywood.list("bkt")["objects"]
    assert o == info


@pytest.mark.parametrize("sizes", [
    [1, 1],
    [3, 0, 5],
    [4096, 7, 65537, 0, 1, 100_000],
])
def test_multipart_checksums(plywood, sizes):
    chunks = [os.urandom(n) for n in sizes]
    s = plywood.open_session("bkt", "obj", checksums=ALGORITHMS)
    for n, c in enumerate(chunks, 1):
        assert plywood.put_part("bkt", "obj", s, n, c)["checksums"] == digests(ALGORITHMS, c)
    assert plywood.describe_session("bkt", "obj", s)["checksums"] == sorted(ALGORITHMS, key=ALGORITHMS.index)
    info = plywood.commit("bkt", "obj", s)
    # CRCs are combined from the parts' ones; MD5 and SHA cannot be.
    assert info["checksums"] == digests(CRCS, b"".join(chunks))
    assert info["parts"] == [{"number": n, "size": len(c), "checksums": digests(ALGORITHMS, c)}
                             for n, c in enumerate(chunks, 1)]
    assert plywood.read("bkt", "obj") == b"".join(chunks)


def test_part_order_in_checksums(plywood):
    s = plywood.open_session("bkt", "obj", checksums=CRCS)
    plywood.put_part("bkt", "obj", s, 1, b"first")
    plywood.put_part("bkt", "obj", s, 2, b"second")
    plywood.put_part("bkt", "obj", s, 3, b"third")
    info = plywood.commit("bkt", "obj", s, [3, 1])
    assert info["checksums"] == digests(CRCS, b"thirdfirst")
    assert [p["number"] for p in info["parts"]] == [3, 1]


def test_single_part_session_checksums(plywood):
    s = plywood.open_session("bkt", "obj", checksums=ALGORITHMS)
    plywood.put_part("bkt", "obj", s, 1, b"one")
    plywood.put_part("bkt", "obj", s, 2, b"two")
    assert plywood.commit("bkt", "obj", s, [2])["checksums"] == digests(ALGORITHMS, b"two")


def test_empty_object_checksums(plywood):
    s = plywood.open_session("bkt", "obj", checksums=ALGORITHMS)
    info = plywood.commit("bkt", "obj", s, [])
    assert info["parts"] == []
    assert info["checksums"] == digests(ALGORITHMS, b"")


def test_copy_checksums_of_single_part_source(plywood):
    plywood.put("bkt", "src", b"0123456789", checksums=ALGORITHMS)
    info = plywood.copy("bkt", "dst", "src", checksums=ALGORITHMS)
    assert info["checksums"] == digests(ALGORITHMS, b"0123456789")
    # A range covering the whole source is the whole source.
    info = plywood.copy("bkt", "dst", "src", range="bytes=0-", checksums=["md5"])
    assert info["checksums"] == digests(["md5"], b"0123456789")


def test_copy_checksums_of_multipart_source(plywood):
    chunks = [b"0123", b"", b"45", b"6789"]
    s = plywood.open_session("bkt", "src", checksums=ALGORITHMS)
    for n, c in enumerate(chunks, 1):
        plywood.put_part("bkt", "src", s, n, c)
    plywood.commit("bkt", "src", s)
    info = plywood.copy("bkt", "dst", "src", checksums=ALGORITHMS)
    assert info["parts"][0]["checksums"] == digests(ALGORITHMS, b"0123456789")
    assert info["checksums"] == digests(ALGORITHMS, b"0123456789")


def test_copy_checksums_the_source_lacks(plywood):
    plywood.put("bkt", "src", b"0123456789", checksums=["crc32"])
    info = plywood.copy("bkt", "dst", "src", checksums=["crc32", "crc32c", "sha256"])
    assert info["checksums"] == digests(["crc32", "crc32c", "sha256"], b"0123456789")
    plywood.put("bkt", "src", b"0123456789")
    info = plywood.copy("bkt", "dst", "src", checksums=["md5"])
    assert info["checksums"] == digests(["md5"], b"0123456789")


@pytest.mark.parametrize("range,data", [
    ("bytes=3-7", b"34567"),
    ("bytes=0-3", b"0123"),
    ("bytes=-1", b"9"),
    ("bytes=0-", b"0123456789"),
])
def test_copy_range_checksums(plywood, range, data):
    s = plywood.open_session("bkt", "src", checksums=["crc32c"])
    for n, c in enumerate([b"0123", b"45", b"6789"], 1):
        plywood.put_part("bkt", "src", s, n, c)
    plywood.commit("bkt", "src", s)
    info = plywood.copy("bkt", "dst", "src", range=range, checksums=ALGORITHMS)
    assert info["parts"][0]["checksums"] == digests(ALGORITHMS, data)


def test_copied_parts_in_object_checksums(plywood):
    plywood.put("bkt", "src", b"0123456789", checksums=CRCS)
    s = plywood.open_session("bkt", "obj", checksums=CRCS)
    plywood.put_part("bkt", "obj", s, 1, b"abc")
    plywood.copy_part("bkt", "obj", s, 2, "src", range="bytes=-2")
    plywood.copy_part("bkt", "obj", s, 3, "src")
    info = plywood.commit("bkt", "obj", s)
    assert info["checksums"] == digests(CRCS, b"abc890123456789")
    assert plywood.read("bkt", "obj") == b"abc890123456789"


def test_verify_object(plywood):
    data = b"payload"
    plywood.put("bkt", "obj", data, verify=digests(["crc32c", "sha1"], data))
    expect_error("bad_digest", 400, plywood.put, "bkt", "obj", b"other", verify={"md5": digest("md5", data)})
    assert plywood.read("bkt", "obj") == data
    # Verifying does not make the storage keep the checksum.
    assert plywood.head("bkt", "obj").headers.get("X-Labaz-Checksum-crc32c") is None


def test_verify_part(plywood):
    s = plywood.open_session("bkt", "obj", checksums=["crc32"])
    plywood.put_part("bkt", "obj", s, 1, b"good", verify={"crc32": digest("crc32", b"good")})
    expect_error("bad_digest", 400, plywood.put_part, "bkt", "obj", s, 2, b"bad",
                 verify={"crc64nvme": digest("crc64nvme", b"good")})
    expect_error("bad_digest", 400, plywood.put_part, "bkt", "obj", s, 1, b"bad",
                 verify={"crc32": digest("crc32", b"good")})
    parts = plywood.describe_session("bkt", "obj", s)["parts"]
    assert parts == [{"number": 1, "size": 4, "checksums": digests(["crc32"], b"good")}]


def test_bad_checksum_headers(plywood):
    expect_error("bad_request", 400, plywood.put, "bkt", "obj", b"x", checksums=["crc16"])
    expect_error("bad_request", 400, plywood.open_session, "bkt", "obj", checksums=["md5", "nope"])
    expect_error("bad_request", 400, plywood.put, "bkt", "obj", b"x", verify={"crc16": "00"})
    expect_error("bad_request", 400, plywood.put, "bkt", "obj", b"x", verify={"md5": "xyz"})
    expect_error("no_such_key", 404, plywood.read, "bkt", "obj")
    plywood.put("bkt", "src", b"x")
    expect_error("bad_request", 400, plywood.request, "PUT", "/bkt/dst", params={"copy-key": "src"},
                 headers={"X-Labaz-Checksum-md5": digest("md5", b"x")})


# Protocol

def test_unknown_route(plywood):
    # Seastar's own "not found" reply.
    expect_error("", 404, plywood.request, "OPTIONS", "/bkt/obj")


def test_empty_bucket_in_path(plywood):
    expect_error("bad_request", 400, plywood.request, "POST", "/")


# Persistence

def test_objects_survive_restart(run_labaz, tmp_path):
    data_dir = tmp_path / "data"
    port = free_port()
    args = ["--facade", f"plywood:http={port}", "--data-dir", str(data_dir)]
    server = run_labaz(args)
    c = PlywoodClient(f"http://127.0.0.1:{port}")
    c.create_bucket("bkt")
    info = c.put("bkt", "a/b", b"payload", metadata={"m": "v"}, checksums=["crc32c", "md5"])
    s = c.open_session("bkt", "a/c")
    c.put_part("bkt", "a/c", s, 1, b"part")
    multi = c.commit("bkt", "a/c", s)
    assert server.stop() == 0

    server.start()
    c = PlywoodClient(f"http://127.0.0.1:{port}")
    assert c.list_buckets() == ["bkt"]
    assert c.read("bkt", "a/b") == b"payload"
    assert c.list("bkt")["objects"] == [info, multi]
