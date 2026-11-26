# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

import subprocess
from concurrent.futures import ThreadPoolExecutor

import requests

from conftest import DEFAULT_SEASTAR_ARGS, free_port


def test_http(http_url):
    r = requests.get(f"{http_url}/some/path")
    assert r.status_code == 404
    assert r.json()["error"] == "no_such_bucket"


def test_https(https_url, tls_cert):
    cert, _ = tls_cert
    r = requests.get(f"{https_url}/some/path", verify=str(cert))
    assert r.status_code == 404


def test_http_and_https_together(run_labaz, tls_cert):
    cert, key = tls_cert
    http_port, https_port = free_port(), free_port()
    run_labaz(["--facade", f"plywood:http={http_port},https={https_port}",
               "--tls-cert", str(cert), "--tls-key", str(key)])
    assert requests.get(f"http://127.0.0.1:{http_port}/").status_code == 200
    assert requests.get(f"https://127.0.0.1:{https_port}/", verify=str(cert)).status_code == 200


def test_two_facades_share_storage(run_labaz):
    port1, port2 = free_port(), free_port()
    run_labaz(["--facade", f"plywood:http={port1}", "--facade", f"plywood:http={port2}"])
    assert requests.put(f"http://127.0.0.1:{port1}/shared").status_code == 200
    assert requests.get(f"http://127.0.0.1:{port2}/").json()["buckets"] == ["shared"]


def test_keepalive(http_url):
    with requests.Session() as s:
        for _ in range(20):
            assert s.get(f"{http_url}/k").status_code == 404


def test_parallel_connections(http_url):
    def one(i):
        # A session per call, so that every request uses its own connection.
        with requests.Session() as s:
            return s.get(f"{http_url}/p/{i}").status_code

    with ThreadPoolExecutor(max_workers=16) as pool:
        assert set(pool.map(one, range(64))) == {404}


def test_sigterm_stops_cleanly(run_labaz):
    port = free_port()
    server = run_labaz(["--facade", f"plywood:http={port}"])
    assert server.stop() == 0
    assert "Stopping" in server.log()


def run_bad_config(binary, args, tmp_path):
    return subprocess.run([str(binary), *DEFAULT_SEASTAR_ARGS, "--data-dir", str(tmp_path), *args],
                          capture_output=True, text=True, timeout=30)


def check_bad_config(binary, args, tmp_path, message):
    r = run_bad_config(binary, args, tmp_path)
    assert r.returncode != 0
    assert message in r.stdout + r.stderr


def test_no_facade(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, [], tmp_path, "--facade")


def test_unknown_facade(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", f"carrot:http={free_port()}"], tmp_path, "unknown facade 'carrot'")


def test_unknown_backend(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", f"plywood:http={free_port()}", "--backend", "turnip"],
                     tmp_path, "unknown backend 'turnip'")


def test_facade_without_ports(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", "plywood"], tmp_path, "neither an http nor an https port")


def test_bad_facade_parameter(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", "plywood:http=1,color=red"], tmp_path, "unknown parameter 'color'")


def test_bad_port(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", "plywood:http=70000"], tmp_path, "bad port '70000'")


def test_https_without_cert(labaz_binary, tmp_path):
    check_bad_config(labaz_binary, ["--facade", f"plywood:https={free_port()}"], tmp_path, "--tls-cert")


def test_cert_without_key(labaz_binary, tls_cert, tmp_path):
    cert, _ = tls_cert
    check_bad_config(labaz_binary, ["--facade", f"plywood:http={free_port()}", "--tls-cert", str(cert)],
                     tmp_path, "--tls-key")


def test_potato_without_data_dir(labaz_binary):
    r = subprocess.run([str(labaz_binary), *DEFAULT_SEASTAR_ARGS, "--facade", f"plywood:http={free_port()}"],
                       capture_output=True, text=True, timeout=30)
    assert r.returncode != 0
    assert "--data-dir" in r.stdout + r.stderr


def object_round_trips(s3_port, gcs_port):
    s3_url, gcs_url = f"http://127.0.0.1:{s3_port}", f"http://127.0.0.1:{gcs_port}"
    assert requests.put(f"{s3_url}/bkt").status_code == 200
    assert requests.put(f"{s3_url}/bkt/k", data=b"data").status_code == 200
    assert requests.get(f"{s3_url}/bkt/k").content == b"data"
    r = requests.post(f"{gcs_url}/upload/storage/v1/b/bkt/o", params={"uploadType": "media", "name": "g"}, data=b"data")
    assert r.status_code == 200
    assert requests.get(f"{gcs_url}/storage/v1/b/bkt/o/g", params={"alt": "media"}).content == b"data"


def test_trace_logs(run_labaz):
    s3_port, gcs_port = free_port(), free_port()
    server = run_labaz(["--facade", f"s3:http={s3_port}", "--facade", f"gcs:http={gcs_port}",
                        "--logger-log-level", "s3=trace:gcs=trace:potato=trace"])
    object_round_trips(s3_port, gcs_port)
    server.stop()
    log = server.log()
    assert "GET /bkt/k" in log
    assert "replied 200 in" in log
    assert "bkt/k: data sent in" in log
    assert "GET /storage/v1/b/bkt/o/g" in log
    assert "bkt/g: data sent in" in log
    assert "commit session bkt/k (write): lock waited for" in log
    assert "read bkt/g (read): lock held for" in log


def test_no_trace_logs_by_default(run_labaz):
    s3_port, gcs_port = free_port(), free_port()
    server = run_labaz(["--facade", f"s3:http={s3_port}", "--facade", f"gcs:http={gcs_port}"])
    object_round_trips(s3_port, gcs_port)
    server.stop()
    assert "TRACE" not in server.log()
