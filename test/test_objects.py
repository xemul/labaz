# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""What every facade does the same way, run through each of them (see the
`client` fixture), on the potato backend."""

import os
import socket
import threading
import urllib.parse

import pytest

from conftest import free_port
from facade_error import FacadeError
from facades import FACADES, make_client
from test_plywood import digest


def expect_error(client, errc: str | None, status: int, func, *args, **kwargs):
    """Expects func() to fail with `status` and the facade's code for the
    storage error code `errc`; None skips the code, e.g. for HEAD."""
    with pytest.raises(FacadeError) as e:
        func(*args, **kwargs)
    assert e.value.status == status
    if errc is not None:
        assert e.value.code == client.code(errc)


# Buckets

def test_buckets(client):
    client.create_bucket("bkt2")
    assert client.list_buckets() == ["bkt", "bkt2"]
    expect_error(client, "bucket_exists", 409, client.create_bucket, "bkt2")
    client.delete_bucket("bkt2")
    assert client.list_buckets() == ["bkt"]
    expect_error(client, "no_such_bucket", 404, client.delete_bucket, "bkt2")


def test_delete_non_empty_bucket(client):
    client.put("bkt", "k", b"x")
    expect_error(client, "bucket_not_empty", 409, client.delete_bucket, "bkt")
    client.delete("bkt", "k")
    client.delete_bucket("bkt")


def test_deleted_objects_leave_bucket_empty(client):
    client.put("bkt", "a/b/c", b"x")
    client.put("bkt", "/x", b"x")
    client.delete("bkt", "a/b/c")
    client.delete("bkt", "/x")
    client.delete_bucket("bkt")


def test_no_such_bucket(client):
    expect_error(client, "no_such_bucket", 404, client.put, "nope", "k", b"x")
    expect_error(client, "no_such_bucket", 404, client.read, "nope", "k")
    expect_error(client, "no_such_bucket", 404, client.list, "nope")
    expect_error(client, "no_such_bucket", 404, client.delete, "nope", "k")


# Objects

def test_put_get(client):
    client.put("bkt", "key", b"hello", metadata={"Color": "red"})
    o = client.get("bkt", "key")
    assert (o.data, o.size, o.status) == (b"hello", 5, 200)
    assert o.metadata == {"color": "red"}


def test_head(client):
    client.put("bkt", "key", b"hello", metadata={"m": "v"})
    o = client.head("bkt", "key")
    assert (o.size, o.metadata) == (5, {"m": "v"})
    expect_error(client, None, 404, client.head, "bkt", "nope")  # HEAD has no body


def test_head_error_has_no_body(client):
    # Raw HTTP: clients buffer what comes after the headers of a reply to
    # HEAD and drop it, which would hide a body sent there.
    with socket.create_connection(urllib.parse.urlparse(client.url).netloc.split(":")) as s:
        s.settimeout(5)
        s.sendall(b"HEAD /bkt/nope HTTP/1.1\r\nHost: x\r\n\r\n"
                  b"GET /bkt/nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        data = b""
        while chunk := s.recv(65536):
            data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 404 ")
    # The reply to GET follows the headers of the reply to HEAD.
    assert rest.startswith(b"HTTP/1.1 404 ")


def test_empty_object(client):
    client.put("bkt", "empty", b"")
    assert client.head("bkt", "empty").size == 0
    assert client.read("bkt", "empty") == b""


def test_large_object(client):
    data = os.urandom(5 * 1024 * 1024 + 123)
    client.put("bkt", "large", data)
    assert client.read("bkt", "large") == data


def test_delete(client):
    client.put("bkt", "key", b"x")
    client.delete("bkt", "key")
    expect_error(client, "no_such_key", 404, client.read, "bkt", "key")
    if client.name == "s3":
        client.delete("bkt", "key")  # deleting a missing key succeeds in S3
    else:
        expect_error(client, "no_such_key", 404, client.delete, "bkt", "key")


def test_overwrite(client):
    client.put("bkt", "key", b"first", metadata={"v": "1"})
    client.put("bkt", "key", b"second!", metadata={"v": "2"})
    o = client.get("bkt", "key")
    assert (o.data, o.metadata) == (b"second!", {"v": "2"})


@pytest.mark.parametrize("rng, expected, content_range", [
    ("bytes=0-0", b"0", "bytes 0-0/10"),
    ("bytes=2-5", b"2345", "bytes 2-5/10"),
    ("bytes=7-", b"789", "bytes 7-9/10"),
    ("bytes=-3", b"789", "bytes 7-9/10"),
    ("bytes=-30", b"0123456789", "bytes 0-9/10"),
    ("bytes=5-100", b"56789", "bytes 5-9/10"),
])
def test_range(client, rng, expected, content_range):
    client.put("bkt", "key", b"0123456789")
    o = client.get("bkt", "key", range=rng)
    assert (o.status, o.data, o.content_range) == (206, expected, content_range)


@pytest.mark.parametrize("rng", ["bytes=10-", "bytes=10-20", "bytes=-0"])
def test_unsatisfiable_range(client, rng):
    client.put("bkt", "key", b"0123456789")
    expect_error(client, "invalid_range", 416, client.read, "bkt", "key", range=rng)


# Keys

@pytest.mark.parametrize("key", [
    "a/b/c",
    "/leading",
    "a//b",
    ".", "..", "./x", "x/../y",
    "tilde~m", "~e", "per%cent", "sp ace", "q?uery#hash", "pl+us", "amp&er",
    "ünïcödé",
    "x" * 1024,
    "/".join(["y" * 300] * 3),
])
def test_key_round_trip(client, key):
    client.put("bkt", key, key.encode())
    assert client.read("bkt", key) == key.encode()
    assert client.list_keys("bkt") == [key]
    client.delete("bkt", key)
    assert client.list_keys("bkt") == []


@pytest.mark.parametrize("key", ["dir/", "a/b/"])
def test_key_ending_with_slash(client, key):
    # Not supported, which S3 does support.
    expect_error(client, "invalid_argument", 400, client.put, "bkt", key, b"x")
    assert client.list_keys("bkt") == []


def test_file_and_directory_keys_coexist(client):
    for k in ["a", "a/b", "a/b/c"]:
        client.put("bkt", k, k.encode())
    for k in ["a", "a/b", "a/b/c"]:
        assert client.read("bkt", k) == k.encode()


def test_flat_namespace(client):
    client.put("bkt", "sstables/x/data", b"1")
    client.delete("bkt", "sstables/x/data")
    assert client.list_keys("bkt") == []
    client.put("bkt", "sstables/x/index", b"2")
    assert client.read("bkt", "sstables/x/index") == b"2"


# Listing

def test_list_order_and_prefix(client):
    keys = ["a", "a/b", "a/c", "a.b", "a0", "ab", "b", "a-", "a/b/c", "~", "%", "a//x"]
    for k in keys:
        client.put("bkt", k, b"")
    assert client.list_keys("bkt") == sorted(keys, key=lambda k: k.encode())
    assert client.list_keys("bkt", prefix="a/") == ["a//x", "a/b", "a/b/c", "a/c"]
    assert client.list_keys("bkt", prefix="a/b") == ["a/b", "a/b/c"]
    assert client.list_keys("bkt", prefix="zzz") == []


def test_list_pagination(client):
    keys = sorted(f"{d}/{i:02}" for d in ["x", "y", "z"] for i in range(7))
    for k in keys:
        client.put("bkt", k, b"")
    for page in [1, 2, 5, 100]:
        assert client.list_keys("bkt", page=page) == keys
    objects, truncated = client.list("bkt", start_after="y/03", max_keys=2)
    assert [o["key"] for o in objects] == ["y/04", "y/05"]
    assert truncated
    objects, truncated = client.list("bkt", start_after="z/06")
    assert objects == [] and not truncated


def test_list_reports_object_info(client):
    client.put("bkt", "k", b"12345")
    objects, _ = client.list("bkt")
    assert objects == [{"key": "k", "size": 5}]


# Checksums

def test_verify_object(client):
    data = b"payload"
    client.put("bkt", "obj", data, verify={"crc32c": digest("crc32c", data)})
    client.put("bkt", "obj", data, verify={"md5": digest("md5", data)})
    expect_error(client, "bad_digest", 400, client.put, "bkt", "obj", b"other", verify={"md5": digest("md5", data)})
    expect_error(client, "bad_digest", 400, client.put, "bkt", "obj", b"other", verify={"sha256": digest("sha256", data)})
    assert client.read("bkt", "obj") == data


# Concurrency

def test_overwrite_under_reads(client):
    """A reader sees either version, never a mix or an error."""
    a, b = b"a" * 100_000, b"b" * 50_000
    client.put("bkt", "obj", a)
    stop = threading.Event()
    errors = []

    def reader():
        c = make_client(client.name, client.url)
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
            client.put("bkt", "obj", a if i % 2 else b)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert errors == []


# Persistence

@pytest.mark.parametrize("facade", FACADES)
def test_objects_survive_restart(run_labaz, tmp_path, facade):
    port = free_port()
    server = run_labaz(["--facade", f"{facade}:http={port}", "--data-dir", str(tmp_path / "data")])
    c = make_client(facade, f"http://127.0.0.1:{port}")
    c.create_bucket("bkt")
    c.put("bkt", "a/b", b"payload", metadata={"m": "v"})
    assert server.stop() == 0

    server.start()
    c = make_client(facade, f"http://127.0.0.1:{port}")
    assert c.list_buckets() == ["bkt"]
    o = c.get("bkt", "a/b")
    assert (o.data, o.metadata) == (b"payload", {"m": "v"})
