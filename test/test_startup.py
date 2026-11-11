# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

import subprocess
from concurrent.futures import ThreadPoolExecutor

import requests

from conftest import DEFAULT_SEASTAR_ARGS, free_port


def test_http(http_url):
    r = requests.get(f"{http_url}/some/path")
    assert r.status_code == 404


def test_https(https_url, tls_cert):
    cert, _ = tls_cert
    r = requests.get(f"{https_url}/some/path", verify=str(cert))
    assert r.status_code == 404


def test_http_and_https_together(run_labaz, tls_cert):
    cert, key = tls_cert
    http_port, https_port = free_port(), free_port()
    run_labaz(["--http-port", str(http_port), "--https-port", str(https_port),
               "--tls-cert", str(cert), "--tls-key", str(key)])
    assert requests.get(f"http://127.0.0.1:{http_port}/").status_code == 404
    assert requests.get(f"https://127.0.0.1:{https_port}/", verify=str(cert)).status_code == 404


def test_keepalive(http_url):
    with requests.Session() as s:
        for _ in range(20):
            assert s.get(f"{http_url}/k").status_code == 404


def test_parallel_connections(http_url):
    def one(i):
        # A session per call, so that every request uses its own connection
        # and connections spread over shards.
        with requests.Session() as s:
            return s.get(f"{http_url}/p/{i}").status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        assert set(pool.map(one, range(64))) == {404}


def test_sigterm_stops_cleanly(run_labaz):
    port = free_port()
    server = run_labaz(["--http-port", str(port)])
    assert server.stop() == 0
    assert "Stopping" in server.log()


def run_bad_config(binary, args):
    return subprocess.run([str(binary), *DEFAULT_SEASTAR_ARGS, *args],
                          capture_output=True, text=True, timeout=30)


def test_no_ports(labaz_binary):
    r = run_bad_config(labaz_binary, [])
    assert r.returncode != 0
    assert "--http-port" in r.stdout + r.stderr


def test_https_without_cert(labaz_binary):
    r = run_bad_config(labaz_binary, ["--https-port", str(free_port())])
    assert r.returncode != 0
    assert "--tls-cert" in r.stdout + r.stderr


def test_cert_without_key(labaz_binary, tls_cert):
    cert, _ = tls_cert
    r = run_bad_config(labaz_binary, ["--http-port", str(free_port()), "--tls-cert", str(cert)])
    assert r.returncode != 0
    assert "--tls-key" in r.stdout + r.stderr
