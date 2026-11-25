# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

import pytest

# A single shard: routing requests between shards is not there yet.
DEFAULT_SEASTAR_ARGS = ["--smp", "1", "-m", "512M", "--overprovisioned"]
READY_LINE = "labaz: ready"
START_TIMEOUT = 30
STOP_TIMEOUT = 30


def pytest_addoption(parser):
    parser.addoption("--labaz", default=os.environ.get("LABAZ_BINARY"),
                     help="path to the labaz binary (or set LABAZ_BINARY)")


@pytest.fixture(scope="session")
def labaz_binary(pytestconfig) -> Path:
    path = pytestconfig.getoption("--labaz")
    if not path:
        pytest.skip("no labaz binary given; pass --labaz=PATH or set LABAZ_BINARY")
    path = Path(path).resolve()
    if not os.access(path, os.X_OK):
        pytest.fail(f"labaz binary {path} is not executable")
    return path


@pytest.fixture(scope="session")
def tls_cert(tmp_path_factory) -> tuple[Path, Path]:
    """A self-signed certificate for 127.0.0.1/localhost, as (cert, key)."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl is not installed")
    d = tmp_path_factory.mktemp("tls")
    cert, key = d / "cert.pem", d / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(key), "-out", str(cert), "-days", "1",
         "-subj", "/CN=localhost",
         "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost"],
        check=True, capture_output=True)
    return cert, key


def seastar_args(args: list[str]) -> list[str]:
    """DEFAULT_SEASTAR_ARGS, except for options that `args` sets itself."""
    ret = []
    it = iter(DEFAULT_SEASTAR_ARGS)
    for opt in it:
        value = next(it) if not opt.startswith("--overprovisioned") else None
        if opt not in args:
            ret += [opt] if value is None else [opt, value]
    return ret


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LabazServer:
    """One labaz process. Output goes to a log file that is printed when a
    test fails."""

    def __init__(self, binary: Path, workdir: Path, args: list[str]):
        self.binary = binary
        self.args = args
        self.workdir = workdir
        self.log_path = workdir / "labaz.log"
        self.proc: subprocess.Popen | None = None

    def start(self):
        log = open(self.log_path, "a")
        log_start = log.tell()
        self.proc = subprocess.Popen([str(self.binary), *seastar_args(self.args), *self.args],
                                     stdout=log, stderr=subprocess.STDOUT)
        log.close()
        deadline = time.monotonic() + START_TIMEOUT
        while time.monotonic() < deadline:
            if READY_LINE in self.log()[log_start:]:
                return
            if self.proc.poll() is not None:
                raise RuntimeError(f"labaz exited with {self.proc.returncode} before "
                                   f"becoming ready:\n{self.log()}")
            time.sleep(0.05)
        self.proc.kill()
        self.proc.wait()
        raise RuntimeError(f"labaz did not become ready in {START_TIMEOUT}s:\n{self.log()}")

    def stop(self) -> int:
        """Sends SIGTERM and returns the exit code."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                raise RuntimeError(f"labaz did not stop in {STOP_TIMEOUT}s:\n{self.log()}")
        return self.proc.returncode

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")


@pytest.fixture
def run_labaz(labaz_binary, tmp_path):
    """Factory: run_labaz(args) starts a server with the given extra
    arguments and returns it. Unless the arguments name a --data-dir, the
    server gets one in its work directory. Every server still running at
    the end of the test is stopped and must exit cleanly."""
    servers = []

    def start(args: list[str]) -> LabazServer:
        workdir = tmp_path / f"server{len(servers)}"
        workdir.mkdir()
        if "--data-dir" not in args:
            args = [*args, "--data-dir", str(workdir / "data")]
        server = LabazServer(labaz_binary, workdir, args)
        servers.append(server)
        server.start()
        return server

    yield start

    for server in servers:
        rc = server.stop()
        print(f"--- {server.log_path} ---\n{server.log()}")
        assert rc == 0, f"labaz exited with {rc}"


@pytest.fixture
def http_url(run_labaz) -> str:
    port = free_port()
    run_labaz(["--facade", f"plywood:http={port}"])
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def https_url(run_labaz, tls_cert) -> str:
    cert, key = tls_cert
    port = free_port()
    run_labaz(["--facade", f"plywood:https={port}", "--tls-cert", str(cert), "--tls-key", str(key)])
    return f"https://127.0.0.1:{port}"


@pytest.fixture
def plywood(http_url):
    """A client of a fresh server, with a bucket named 'bkt' created."""
    from plywood_client import PlywoodClient
    c = PlywoodClient(http_url)
    c.create_bucket("bkt")
    return c


@pytest.fixture(params=["plywood", "s3"])
def client(request, run_labaz):
    """A client of a fresh server, through each facade in turn (see
    facades.py), with a bucket named 'bkt' created."""
    from facades import S3_KEY, make_client
    port = free_port()
    args = ["--facade", f"{request.param}:http={port}"]
    if request.param == "s3":
        args += ["--s3-key", S3_KEY]
    run_labaz(args)
    c = make_client(request.param, f"http://127.0.0.1:{port}")
    c.create_bucket("bkt")
    return c


def start_s3(run_labaz, *args: str) -> str:
    port = free_port()
    run_labaz(["--facade", f"s3:http={port}", *args])
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def s3_url(run_labaz) -> str:
    """The URL of a fresh server's s3 facade that does not check signatures,
    for tests that send raw HTTP requests."""
    return start_s3(run_labaz)


@pytest.fixture
def signed_s3_url(run_labaz) -> str:
    """The URL of a fresh server's s3 facade that checks that requests are
    signed with S3_KEY, as boto3 clients of facades.s3_client() sign them."""
    from facades import S3_KEY
    return start_s3(run_labaz, "--s3-key", S3_KEY)


@pytest.fixture
def s3(request):
    """A boto3 client of a fresh server's s3 facade, with a bucket named
    'bkt' created. The server checks the requests' signatures, unless the
    test sends raw HTTP requests too: then it is s3_url's server."""
    from facades import s3_client
    c = s3_client(request.getfixturevalue("s3_url" if "s3_url" in request.fixturenames else "signed_s3_url"))
    c.create_bucket(Bucket="bkt")
    return c
