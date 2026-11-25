# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""The gcs facade: what GCS does its own way, through google-cloud-storage,
and the wire format, through plain HTTP requests. What every facade does
alike is tested in test_objects.py."""

import base64
import os

import google.api_core.exceptions as gexc
import pytest
import requests

from test_plywood import digest

CHUNK = 256 * 1024
# The library uploads more than this with a resumable upload.
RESUMABLE = 8 * 1024 * 1024 + 1


def b64(hex_digest: str) -> str:
    return base64.b64encode(bytes.fromhex(hex_digest)).decode()


def error_of(r: requests.Response) -> tuple[int, str]:
    return r.status_code, r.json()["error"]["errors"][0]["reason"]


# Buckets

# The library refuses names that do not begin and end with a letter or a digit itself.
@pytest.mark.parametrize("name", ["ab", "x" * 64, "Upper", "sp ace", "a$b"])
def test_invalid_bucket_name(gcs, name):
    with pytest.raises(gexc.BadRequest):
        gcs.create_bucket(name)


def test_bucket_resource(gcs):
    b = gcs.get_bucket("bkt")
    assert (b.name, b.location, b.storage_class) == ("bkt", "US", "STANDARD")
    with pytest.raises(gexc.NotFound):
        gcs.get_bucket("nope")
    with pytest.raises(gexc.Conflict):
        gcs.create_bucket("bkt")


def test_bucket_requests_need_project(gcs_url, gcs):
    r = requests.post(f"{gcs_url}/storage/v1/b", json={"name": "bkt2"})
    assert error_of(r) == (400, "required")
    assert error_of(requests.get(f"{gcs_url}/storage/v1/b")) == (400, "required")


def test_list_buckets_pages(gcs):
    for b in ["bb1", "bb2", "cc1"]:
        gcs.create_bucket(b)
    assert [b.name for b in gcs.list_buckets(page_size=1)] == ["bb1", "bb2", "bkt", "cc1"]
    assert [b.name for b in gcs.list_buckets(prefix="bb")] == ["bb1", "bb2"]


# Objects

def test_object_resource(gcs):
    data = b"payload"
    blob = gcs.bucket("bkt").blob("a/b")
    blob.metadata = {"Key": "value"}
    blob.upload_from_string(data)
    o = gcs.bucket("bkt").get_blob("a/b")
    assert (o.name, o.size, o.metageneration, o.metadata) == ("a/b", len(data), 1, {"Key": "value"})
    assert (o.md5_hash, o.crc32c) == (b64(digest("md5", data)), b64(digest("crc32c", data)))
    assert (o.content_type, o.storage_class) == ("application/octet-stream", "STANDARD")
    assert o.generation == blob.generation and o.updated is not None
    # The library downloads from the mediaLink.
    assert o.download_as_bytes() == data


def test_generations(gcs):
    blob = gcs.bucket("bkt").blob("k")
    blob.upload_from_string(b"1")
    g1 = blob.generation
    blob.upload_from_string(b"2")
    assert blob.generation > g1
    # There are no other versions to read.
    with pytest.raises(gexc.NotFound):
        gcs.bucket("bkt").blob("k", generation=g1).download_as_bytes()


@pytest.mark.parametrize("name", [".", "..", "new\nline", "x" * 1025])
def test_invalid_object_names(gcs, name):
    with pytest.raises(gexc.BadRequest):
        gcs.bucket("bkt").blob(name).upload_from_string(b"x")


def test_read_conditions(gcs_url, gcs):
    blob = gcs.bucket("bkt").blob("k")
    blob.upload_from_string(b"data")
    g = blob.generation
    assert blob.download_as_bytes(if_generation_match=g, if_metageneration_match=1) == b"data"
    for conditions in [{"if_generation_match": g + 1}, {"if_generation_match": 0}, {"if_metageneration_match": 2}]:
        with pytest.raises(gexc.PreconditionFailed):
            blob.download_as_bytes(**conditions)
        with pytest.raises(gexc.PreconditionFailed):
            blob.reload(**conditions)
    for params in [{"ifGenerationNotMatch": g}, {"ifMetagenerationNotMatch": 1}]:
        r = requests.get(f"{gcs_url}/storage/v1/b/bkt/o/k", params={"alt": "media", **params})
        assert (r.status_code, r.content) == (304, b"")


def test_download_hashes(gcs_url, gcs):
    data = b"0123456789"
    gcs.bucket("bkt").blob("k").upload_from_string(data)
    r = requests.get(f"{gcs_url}/download/storage/v1/b/bkt/o/k", params={"alt": "media"})
    assert r.headers["x-goog-hash"] == f"crc32c={b64(digest('crc32c', data))},md5={b64(digest('md5', data))}"
    assert r.headers["x-goog-stored-content-length"] == "10"
    r = requests.get(f"{gcs_url}/download/storage/v1/b/bkt/o/k", params={"alt": "media"}, headers={"Range": "bytes=2-4"})
    assert (r.status_code, r.content, r.headers["Content-Range"]) == (206, b"234", "bytes 2-4/10")
    assert "x-goog-hash" not in r.headers


def test_error_reply(gcs_url, gcs):
    r = requests.get(f"{gcs_url}/storage/v1/b/bkt/o/no%2Fsuch")
    assert r.status_code == 404
    err = r.json()["error"]
    assert (err["code"], err["message"]) == (404, "No such object: bkt/no/such")
    assert err["errors"] == [{"message": "No such object: bkt/no/such", "domain": "global", "reason": "notFound"}]


# Uploads

def test_upload_types(gcs_url, gcs):
    """Simple, multipart and resumable uploads, the last one in one chunk
    and in several. An object of several chunks has no MD5."""
    r = requests.post(f"{gcs_url}/upload/storage/v1/b/bkt/o", params={"uploadType": "media", "name": "media"}, data=b"m")
    assert (r.status_code, r.json()["size"], r.json()["md5Hash"]) == (200, "1", b64(digest("md5", b"m")))
    gcs.bucket("bkt").blob("multipart").upload_from_string(b"mp")
    one = os.urandom(RESUMABLE)
    blob = gcs.bucket("bkt").blob("one", chunk_size=64 * CHUNK)
    blob.upload_from_string(one, if_generation_match=0)
    assert (blob.size, blob.md5_hash, blob.crc32c) == (len(one), b64(digest("md5", one)), b64(digest("crc32c", one)))
    several = os.urandom(RESUMABLE)
    blob = gcs.bucket("bkt").blob("several", chunk_size=16 * CHUNK)
    blob.upload_from_string(several)
    assert (blob.size, blob.md5_hash, blob.crc32c) == (len(several), None, b64(digest("crc32c", several)))
    for name, data in [("media", b"m"), ("multipart", b"mp"), ("one", one), ("several", several)]:
        assert gcs.bucket("bkt").blob(name).download_as_bytes() == data


def test_upload_checksums(gcs):
    blob = gcs.bucket("bkt").blob("k")
    blob.md5_hash = b64(digest("md5", b"other"))
    with pytest.raises(gexc.BadRequest):
        blob.upload_from_string(b"data", checksum=None)
    blob = gcs.bucket("bkt").blob("k")
    blob.crc32c = b64(digest("crc32c", b"other"))
    with pytest.raises(gexc.BadRequest):
        blob.upload_from_string(b"data", checksum=None)
    assert gcs.bucket("bkt").get_blob("k") is None
    blob = gcs.bucket("bkt").blob("k")
    blob.crc32c = b64(digest("crc32c", b"data"))
    blob.upload_from_string(b"data", checksum=None)


def test_upload_preconditions(gcs):
    blob = gcs.bucket("bkt").blob("k")
    blob.upload_from_string(b"1", if_generation_match=0)
    with pytest.raises(gexc.PreconditionFailed):
        blob.upload_from_string(b"2", if_generation_match=0)
    blob.upload_from_string(b"3", if_generation_match=blob.generation)
    big = gcs.bucket("bkt").blob("k", chunk_size=CHUNK)
    with pytest.raises(gexc.PreconditionFailed):
        big.upload_from_string(os.urandom(RESUMABLE), if_generation_match=0)
    assert gcs.bucket("bkt").blob("k").download_as_bytes() == b"3"


def session(gcs, name="k", **kwargs) -> str:
    return gcs.bucket("bkt").blob(name).create_resumable_upload_session(**kwargs)


def put(url, content_range: str | None, data: bytes = b"") -> requests.Response:
    headers = {"Content-Range": content_range} if content_range else {}
    return requests.put(url, data=data, headers=headers, allow_redirects=False)


def test_resumable_upload(gcs):
    """Chunks come in order; what the session holds is reported in a 308's
    Range, which a session with nothing has none of."""
    url = session(gcs)
    r = put(url, "bytes */*")
    assert (r.status_code, "Range" in r.headers) == (308, False)
    data = os.urandom(CHUNK + 100)
    r = put(url, f"bytes 0-{CHUNK - 1}/*", data[:CHUNK])
    assert (r.status_code, r.headers["Range"]) == (308, f"bytes=0-{CHUNK - 1}")
    assert put(url, "bytes */*").headers["Range"] == f"bytes=0-{CHUNK - 1}"
    r = put(url, f"bytes {CHUNK}-{CHUNK + 99}/{CHUNK + 100}", data[CHUNK:])
    assert (r.status_code, r.json()["size"]) == (200, str(CHUNK + 100))
    generation = r.json()["generation"]
    # Asking again, or sending the last chunk again, gets the object again.
    assert put(url, "bytes */*").json()["generation"] == generation
    assert put(url, f"bytes {CHUNK}-{CHUNK + 99}/{CHUNK + 100}", data[CHUNK:]).json()["generation"] == generation
    assert error_of(put(url, f"bytes {CHUNK + 100}-{CHUNK + 199}/*", data[:100])) == (400, "invalid")
    assert gcs.bucket("bkt").blob("k").download_as_bytes() == data


@pytest.mark.parametrize("content_range, size", [
    ("bytes 100-199/*", 100),            # not where the session is
    ("bytes 0-99/*", 100),               # not the last, but not a multiple of 256 KiB
    ("bytes 0-99/50", 100),              # past the declared size
    ("bytes 0-99/*", 50),                # not the declared length
    ("bytes */10", 0),                   # not what the session holds
    ("bytes */*", 1),                    # data with no range
    ("bytes 0-99", 100),
    ("bytes 9-0/*", 0),
    ("0-99/*", 100),
])
def test_resumable_bad_chunks(gcs, content_range, size):
    url = session(gcs)
    assert error_of(put(url, content_range, b"x" * size)) == (400, "invalid")
    assert "Range" not in put(url, "bytes */*").headers


def test_resumable_empty_object(gcs):
    r = put(session(gcs), "bytes */0")
    assert (r.status_code, r.json()["size"]) == (200, "0")
    assert gcs.bucket("bkt").blob("k").download_as_bytes() == b""


def test_resumable_cancel(gcs):
    url = session(gcs)
    put(url, f"bytes 0-{CHUNK - 1}/*", b"x" * CHUNK)
    assert requests.delete(url).status_code == 499
    assert error_of(put(url, "bytes */*")) == (404, "notFound")
    assert error_of(requests.delete(url)) == (404, "notFound")
    assert gcs.bucket("bkt").get_blob("k") is None


def test_resumable_session_of_another_object(gcs):
    url = session(gcs)
    assert error_of(put(url.replace("name=k", "name=other"), "bytes */*")) == (404, "notFound")


def test_resumable_precondition(gcs):
    gcs.bucket("bkt").blob("k").upload_from_string(b"old")
    url = session(gcs, if_generation_match=0)
    assert "ifGenerationMatch=0" in url
    assert error_of(put(url, "bytes 0-2/3", b"new")) == (412, "conditionNotMet")
    assert gcs.bucket("bkt").blob("k").download_as_bytes() == b"old"


def test_bad_upload_type(gcs_url, gcs):
    r = requests.post(f"{gcs_url}/upload/storage/v1/b/bkt/o", params={"uploadType": "nope", "name": "k"}, data=b"x")
    assert error_of(r) == (400, "invalid")


# Copies

def test_rewrite(gcs):
    """A copy is made in one call, and takes the source's metadata unless
    given its own. Its checksums are of the data, also when the source is
    a composite object, which has no MD5."""
    data = os.urandom(RESUMABLE)
    src = gcs.bucket("bkt").blob("src", chunk_size=16 * CHUNK)
    src.metadata = {"m": "src"}
    src.upload_from_string(data)
    assert src.md5_hash is None
    dst = gcs.bucket("bkt").blob("dst")
    token, rewritten, size = dst.rewrite(src)
    assert (token, rewritten, size) == (None, len(data), len(data))
    assert (dst.metadata, dst.md5_hash, dst.crc32c) == ({"m": "src"}, b64(digest("md5", data)), src.crc32c)
    dst = gcs.bucket("bkt").blob("dst")
    dst.metadata = {"m": "dst"}
    dst.rewrite(src)
    assert gcs.bucket("bkt").get_blob("dst").metadata == {"m": "dst"}
    assert dst.download_as_bytes() == data


def test_copy_to(gcs):
    src = gcs.bucket("bkt").blob("src")
    src.metadata = {"m": "v"}
    src.upload_from_string(b"data")
    dst = gcs.bucket("bkt").copy_blob(src, gcs.bucket("bkt"), "dst")
    assert (dst.name, dst.metadata, dst.download_as_bytes()) == ("dst", {"m": "v"}, b"data")
    gcs.create_bucket("other")
    with pytest.raises(gexc.GoogleAPICallError) as e:
        gcs.bucket("bkt").copy_blob(src, gcs.bucket("other"), "dst")
    assert e.value.code == 501


def test_copy_conditions(gcs):
    src = gcs.bucket("bkt").blob("src")
    src.upload_from_string(b"data")
    g = src.generation
    dst = gcs.bucket("bkt").blob("dst")
    dst.rewrite(src, if_source_generation_match=g, if_source_metageneration_match=1, if_generation_match=0)
    for conditions in [{"if_source_generation_match": g + 1}, {"if_source_generation_not_match": g},
                       {"if_source_metageneration_match": 2}, {"if_source_metageneration_not_match": 1},
                       {"if_generation_match": 0}]:
        with pytest.raises(gexc.PreconditionFailed):
            gcs.bucket("bkt").blob("dst").rewrite(src, **conditions)
    with pytest.raises(gexc.NotFound):
        gcs.bucket("bkt").blob("dst").rewrite(gcs.bucket("bkt").blob("nope"))


def test_rewrite_token(gcs_url, gcs):
    gcs.bucket("bkt").blob("src").upload_from_string(b"data")
    r = requests.post(f"{gcs_url}/storage/v1/b/bkt/o/src/rewriteTo/b/bkt/o/dst", params={"rewriteToken": "x"}, json={})
    assert error_of(r) == (400, "invalid")


def test_compose(gcs):
    parts = [b"one", b"two", b"three"]
    sources = []
    for n, data in enumerate(parts):
        b = gcs.bucket("bkt").blob(f"part{n}")
        b.upload_from_string(data)
        sources.append(b)
    dst = gcs.bucket("bkt").blob("dst")
    dst.metadata = {"m": "v"}
    dst.compose(sources, if_source_generation_match=[b.generation for b in sources])
    data = b"".join(parts)
    assert (dst.size, dst.crc32c, dst.md5_hash, dst.metadata) == (len(data), b64(digest("crc32c", data)), None, {"m": "v"})
    assert dst.download_as_bytes() == data
    # A source may be used more than once, or be the destination itself.
    dst.compose([dst, sources[0], sources[0]])
    assert dst.download_as_bytes() == data + b"oneone"


def test_compose_errors(gcs_url, gcs):
    b = gcs.bucket("bkt").blob("a")
    b.upload_from_string(b"a")
    dst = gcs.bucket("bkt").blob("dst")
    with pytest.raises(gexc.PreconditionFailed):
        dst.compose([b], if_source_generation_match=[b.generation + 1])
    with pytest.raises(gexc.NotFound):
        dst.compose([b, gcs.bucket("bkt").blob("nope")])
    dst.upload_from_string(b"old")
    with pytest.raises(gexc.PreconditionFailed):
        dst.compose([b], if_generation_match=0)
    assert dst.download_as_bytes() == b"old"
    url = f"{gcs_url}/storage/v1/b/bkt/o/dst/compose"
    # There is no other version of a source to use.
    r = requests.post(url, json={"sourceObjects": [{"name": "a", "generation": str(b.generation + 1)}]})
    assert error_of(r) == (404, "notFound")
    r = requests.post(url, json={"sourceObjects": [{"name": "a", "generation": str(b.generation)}]})
    assert r.status_code == 200
    assert error_of(requests.post(url, json={"sourceObjects": []})) == (400, "required")
    assert error_of(requests.post(url, json={"sourceObjects": [{"name": "a"}] * 33})) == (400, "invalid")



def test_list_delimiter(gcs):
    for k in ["a", "b/1", "b/2", "b/c/1", "c/1", "d"]:
        gcs.bucket("bkt").blob(k).upload_from_string(b"")
    it = gcs.list_blobs("bkt", delimiter="/")
    assert [b.name for b in it] == ["a", "d"]
    assert it.prefixes == {"b/", "c/"}
    it = gcs.list_blobs("bkt", delimiter="/", prefix="b/", page_size=1)
    assert [b.name for b in it] == ["b/1", "b/2"]
    assert it.prefixes == {"b/c/"}


def test_list_start_offset(gcs):
    for k in ["a", "b", "c"]:
        gcs.bucket("bkt").blob(k).upload_from_string(b"")
    assert [b.name for b in gcs.list_blobs("bkt", start_offset="b")] == ["b", "c"]
    assert [b.name for b in gcs.list_blobs("bkt", start_offset="bb")] == ["c"]


def test_list_reply(gcs_url, gcs):
    r = requests.get(f"{gcs_url}/storage/v1/b/bkt/o")
    assert r.json() == {"kind": "storage#objects"}
    assert error_of(requests.get(f"{gcs_url}/storage/v1/b/bkt/o", params={"maxResults": "0"})) == (400, "invalid")
    assert error_of(requests.get(f"{gcs_url}/storage/v1/b/bkt/o", params={"pageToken": "!"})) == (400, "invalid")


# The wire

@pytest.mark.parametrize("method, path, params, body", [
    ("GET", "/storage/v1/b/bkt/o", {"endOffset": "x"}, None),
    ("GET", "/storage/v1/b/bkt/o", {"versions": "true"}, None),
    ("GET", "/storage/v1/b/bkt/o", {"matchGlob": "*"}, None),
    ("DELETE", "/storage/v1/b/bkt/o/k", {"ifGenerationMatch": "1"}, None),
    ("POST", "/upload/storage/v1/b/bkt/o", {"uploadType": "resumable", "name": "k"}, {"md5Hash": "AAAAAAAAAAAAAAAAAAAAAA=="}),
    ("POST", "/upload/storage/v1/b/bkt/o", {"uploadType": "media", "name": "k", "kmsKeyName": "x"}, None),
    ("POST", "/upload/storage/v1/b/bkt/o", {"uploadType": "media", "name": "k", "ifGenerationNotMatch": "1"}, None),
])
def test_not_implemented(gcs_url, gcs, method, path, params, body):
    gcs.bucket("bkt").blob("k").upload_from_string(b"x")
    r = requests.request(method, f"{gcs_url}{path}", params=params, json=body)
    assert error_of(r) == (501, "notImplemented")
    assert gcs.bucket("bkt").blob("k").download_as_bytes() == b"x"
