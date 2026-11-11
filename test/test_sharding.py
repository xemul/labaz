# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""Requests about objects that other shards own are routed to them."""

import os

import pytest

from conftest import free_port
from plywood_client import PlywoodClient
from test_plywood import digest, expect_error

SHARDS = 4
# Connections are spread over the shards, so several of them reach
# several shards.
CONNECTIONS = 8


def start(run_labaz, data_dir, smp: int) -> tuple:
    port = free_port()
    server = run_labaz(["--facade", f"plywood:http={port}", "--data-dir", str(data_dir), "--smp", str(smp)])
    return server, f"http://127.0.0.1:{port}"


def write_objects(url) -> dict:
    """Writes objects through several connections, so through several
    shards, and returns them."""
    cs = clients(url)
    cs[0].create_bucket("bkt")
    objects = {}
    for i in range(32):
        key = f"dir/obj-{i}"
        objects[key] = os.urandom(997 * i)
        cs[i % len(cs)].put("bkt", key, objects[key], checksums=["crc32c"])
    # Many buffers to send, and parts sent through different connections.
    chunks = [os.urandom(1 << 20) for _ in range(3)]
    s = cs[0].open_session("bkt", "big")
    for n, chunk in enumerate(chunks, 1):
        cs[n].put_part("bkt", "big", s, n, chunk)
    cs[-1].commit("bkt", "big", s)
    objects["big"] = b"".join(chunks)
    return objects


@pytest.fixture
def objects(run_labaz, tmp_path):
    """The URL of a server of SHARDS shards, and the objects written to it."""
    _, url = start(run_labaz, tmp_path / "data", SHARDS)
    return url, write_objects(url)


def clients(url):
    return [PlywoodClient(url) for _ in range(CONNECTIONS)]


def test_reads(objects):
    url, objs = objects
    for c in clients(url):
        for key, data in objs.items():
            assert c.read("bkt", key) == data
        assert c.read("bkt", "big", range="bytes=1000000-2100000") == objs["big"][1000000:2100001]


def test_heads_and_listing(objects):
    url, objs = objects
    for c in clients(url):
        for key, data in objs.items():
            assert int(c.head("bkt", key).headers["X-Labaz-Size"]) == len(data)
        assert sorted(c.list_keys("bkt")) == sorted(objs)


def test_deletes(objects):
    url, objs = objects
    cs = clients(url)
    keys = sorted(objs)
    for i, key in enumerate(keys[::2]):
        cs[i % len(cs)].delete("bkt", key)
    for c in cs:
        for i, key in enumerate(keys):
            if i % 2 == 0:
                expect_error("no_such_key", 404, c.read, "bkt", key)
            else:
                assert c.read("bkt", key) == objs[key]


def test_sessions(objects):
    url, objs = objects
    for i, c in enumerate(clients(url)):
        key = f"copy-{i}"
        s = c.open_session("bkt", key, checksums=["crc32c"])
        c.copy_part("bkt", key, s, 1, "dir/obj-5", range="bytes=10-99")
        c.copy_part("bkt", key, s, 2, "dir/obj-7")
        assert [p["number"] for p in c.describe_session("bkt", key, s)["parts"]] == [1, 2]
        c.commit("bkt", key, s)
        assert c.read("bkt", key) == objs["dir/obj-5"][10:100] + objs["dir/obj-7"]
        s = c.open_session("bkt", key)
        c.abort("bkt", key, s)
        expect_error("no_such_session", 404, c.describe_session, "bkt", key, s)


def test_uploads(objects):
    url, objs = objects
    cs = clients(url)
    for i, c in enumerate(cs):
        for j in range(8):
            key = f"new-{i}-{j}"
            data = os.urandom(1000 * j)
            info = c.put("bkt", key, data, checksums=["crc32c", "md5"])
            assert info["checksums"] == {"crc32c": digest("crc32c", data), "md5": digest("md5", data)}
            assert cs[(i + j) % len(cs)].read("bkt", key) == data


def test_verification_on_other_shards(objects):
    url, objs = objects
    for i, c in enumerate(clients(url)):
        key = f"verified-{i}"
        c.put("bkt", key, b"good", verify={"sha256": digest("sha256", b"good")})
        expect_error("bad_digest", 400, c.put, "bkt", key, b"bad", verify={"sha256": digest("sha256", b"good")})
        assert c.read("bkt", key) == b"good"


def test_restart_with_other_shard_count(run_labaz, tmp_path):
    server, url = start(run_labaz, tmp_path / "data", SHARDS)
    objs = write_objects(url)
    assert server.stop() == 0
    _, url = start(run_labaz, tmp_path / "data", 3)
    for c in clients(url):
        for key, data in objs.items():
            assert c.read("bkt", key) == data
