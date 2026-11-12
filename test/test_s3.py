# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""The s3 facade: what S3 does its own way, through boto3, and the wire
format, through plain HTTP requests. What every facade does alike is tested
in test_objects.py."""

import base64
import datetime
import hashlib
import os
import xml.etree.ElementTree as ET

import botocore
import pytest
import requests

from facades import PART_SIZE, s3_client
from test_plywood import digest

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def expect_error(code: str, status: int, func, *args, **kwargs):
    with pytest.raises(botocore.exceptions.ClientError) as e:
        func(*args, **kwargs)
    assert (e.value.response["Error"]["Code"], e.value.response["ResponseMetadata"]["HTTPStatusCode"]) == (code, status)


def b64(hex_digest: str) -> str:
    return base64.b64encode(bytes.fromhex(hex_digest)).decode()


def md5_etag(data: bytes) -> str:
    return f'"{hashlib.md5(data).hexdigest()}"'


# Buckets

@pytest.mark.parametrize("name", ["ab", "x" * 64, "Upper", "under_score", "-dash", "dash-", "a..b", "192.168.1.1"])
def test_invalid_bucket_name(s3, name):
    expect_error("InvalidBucketName", 400, s3.create_bucket, Bucket=name)


def test_head_bucket(s3):
    assert s3.head_bucket(Bucket="bkt")["ResponseMetadata"]["HTTPStatusCode"] == 200
    expect_error("404", 404, s3.head_bucket, Bucket="nope")


def test_create_bucket_with_configuration(s3):
    s3.create_bucket(Bucket="bkt2", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    assert [b["Name"] for b in s3.list_buckets()["Buckets"]] == ["bkt", "bkt2"]


# Objects

def test_etag_is_md5(s3):
    data = b"1234567890"
    assert s3.put_object(Bucket="bkt", Key="k", Body=data)["ETag"] == md5_etag(data)
    assert s3.head_object(Bucket="bkt", Key="k")["ETag"] == md5_etag(data)
    assert s3.get_object(Bucket="bkt", Key="k")["ETag"] == md5_etag(data)
    [o] = s3.list_objects_v2(Bucket="bkt")["Contents"]
    assert o["ETag"] == md5_etag(data)
    s3.put_object(Bucket="bkt", Key="empty", Body=b"")
    assert s3.head_object(Bucket="bkt", Key="empty")["ETag"] == md5_etag(b"")


def test_last_modified(s3):
    before = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    s3.put_object(Bucket="bkt", Key="k", Body=b"x")
    after = datetime.datetime.now(datetime.timezone.utc)
    mtime = s3.head_object(Bucket="bkt", Key="k")["LastModified"]
    assert before <= mtime <= after
    [o] = s3.list_objects_v2(Bucket="bkt")["Contents"]
    assert o["LastModified"].replace(microsecond=0) == mtime


def test_delete_missing_key(s3):
    assert s3.delete_object(Bucket="bkt", Key="nope")["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_unparsable_range_is_ignored(s3):
    s3.put_object(Bucket="bkt", Key="k", Body=b"0123456789")
    for rng in ["pages=1-2", "bytes=1-2,4-5", "bytes=x-"]:
        r = s3.get_object(Bucket="bkt", Key="k", Range=rng)
        assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert r["Body"].read() == b"0123456789"


def test_head_range(s3):
    s3.put_object(Bucket="bkt", Key="k", Body=b"0123456789")
    r = s3.head_object(Bucket="bkt", Key="k", Range="bytes=2-5")
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert (r["ContentLength"], r["ContentRange"]) == (4, "bytes 2-5/10")


def test_if_none_match_star(s3):
    s3.put_object(Bucket="bkt", Key="k", Body=b"1", IfNoneMatch="*")
    expect_error("PreconditionFailed", 412, s3.put_object, Bucket="bkt", Key="k", Body=b"2", IfNoneMatch="*")
    assert s3.get_object(Bucket="bkt", Key="k")["Body"].read() == b"1"


def test_key_too_long(s3):
    expect_error("KeyTooLongError", 400, s3.put_object, Bucket="bkt", Key="x" * 1025, Body=b"")


# Conditional reads

@pytest.fixture
def versioned(s3):
    """An object, with its ETag and Last-Modified, and dates before and
    after the latter."""
    s3.put_object(Bucket="bkt", Key="k", Body=b"0123456789")
    h = s3.head_object(Bucket="bkt", Key="k")
    second = datetime.timedelta(seconds=1)
    return h["ETag"], h["LastModified"], h["LastModified"] - second, h["LastModified"] + second


def read_with(s3, method: str, **conditions) -> int:
    """The status of a GetObject or HeadObject with the conditions."""
    try:
        return getattr(s3, method)(Bucket="bkt", Key="k", **conditions)["ResponseMetadata"]["HTTPStatusCode"]
    except botocore.exceptions.ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"]


@pytest.mark.parametrize("method", ["get_object", "head_object"])
def test_conditions(s3, versioned, method):
    tag, mtime, before, after = versioned
    other = '"0123456789abcdef0123456789abcdef"'
    cases = [
        ({"IfMatch": tag}, 200),
        ({"IfMatch": tag.strip('"')}, 200),
        ({"IfMatch": "*"}, 200),
        ({"IfMatch": f"{other}, {tag}"}, 200),
        ({"IfMatch": other}, 412),
        ({"IfMatch": f"W/{tag}"}, 412),  # If-Match compares strongly
        ({"IfNoneMatch": tag}, 304),
        ({"IfNoneMatch": f"W/{tag}"}, 304),
        ({"IfNoneMatch": "*"}, 304),
        ({"IfNoneMatch": other}, 200),
        ({"IfModifiedSince": before}, 200),
        ({"IfModifiedSince": mtime}, 304),
        ({"IfModifiedSince": after}, 304),
        ({"IfUnmodifiedSince": before}, 412),
        ({"IfUnmodifiedSince": mtime}, 200),
        ({"IfUnmodifiedSince": after}, 200),
        # If-Match goes before If-Unmodified-Since, If-None-Match before If-Modified-Since.
        ({"IfMatch": tag, "IfUnmodifiedSince": before}, 200),
        ({"IfMatch": other, "IfUnmodifiedSince": after}, 412),
        ({"IfNoneMatch": tag, "IfModifiedSince": before}, 304),
        ({"IfNoneMatch": other, "IfModifiedSince": after}, 200),
        # A failed If-Match goes before a not-modified.
        ({"IfMatch": other, "IfNoneMatch": tag}, 412),
    ]
    for conditions, status in cases:
        assert read_with(s3, method, **conditions) == status, conditions


def test_condition_failure_replies(s3_url, versioned):
    tag, mtime, _, _ = versioned
    r = requests.get(f"{s3_url}/bkt/k", headers={"If-None-Match": tag})
    assert (r.status_code, r.content, r.headers["ETag"]) == (304, b"", tag)
    assert r.headers["Last-Modified"] == mtime.strftime("%a, %d %b %Y %H:%M:%S GMT")
    r = requests.get(f"{s3_url}/bkt/k", headers={"If-Match": '"other"'})
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (412, "PreconditionFailed")


@pytest.mark.parametrize("date", ["Wed, 12 Oct 2009 17:50:00 GMT", "Wednesday, 12-Oct-09 17:50:00 GMT",
                                  "Wed Oct 12 17:50:00 2009"])
def test_condition_date_formats(s3_url, versioned, date):
    assert requests.get(f"{s3_url}/bkt/k", headers={"If-Unmodified-Since": date}).status_code == 412
    assert requests.get(f"{s3_url}/bkt/k", headers={"If-Modified-Since": date}).status_code == 200


def test_condition_bad_dates_are_ignored(s3_url, versioned):
    for header in ["If-Modified-Since", "If-Unmodified-Since"]:
        assert requests.get(f"{s3_url}/bkt/k", headers={header: "yesterday"}).status_code == 200


def test_conditions_with_range(s3, versioned):
    tag, _, _, _ = versioned
    r = s3.get_object(Bucket="bkt", Key="k", Range="bytes=2-4", IfMatch=tag)
    assert (r["ResponseMetadata"]["HTTPStatusCode"], r["Body"].read()) == (206, b"234")
    assert read_with(s3, "get_object", Range="bytes=2-4", IfMatch='"other"') == 412
    # The conditions go before the range.
    assert read_with(s3, "get_object", Range="bytes=20-", IfMatch='"other"') == 412
    assert read_with(s3, "get_object", Range="bytes=20-", IfMatch=tag) == 416


def test_download_file(s3, tmp_path):
    # boto3's managed download reads ranges with If-Match.
    data = os.urandom(20 * 1024 * 1024 + 7)
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.write_bytes(data)
    s3.upload_file(str(src), "bkt", "k")
    s3.download_file("bkt", "k", str(dst))
    assert dst.read_bytes() == data


# Checksums

@pytest.mark.parametrize("alg", ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"])
def test_checksum(s3, alg):
    data = b"payload"
    arg = f"Checksum{alg.upper()}"
    r = s3.put_object(Bucket="bkt", Key="k", Body=data, **{arg: b64(digest(alg, data))})
    assert r[arg] == b64(digest(alg, data))
    assert s3.head_object(Bucket="bkt", Key="k", ChecksumMode="ENABLED")[arg] == b64(digest(alg, data))
    assert s3.get_object(Bucket="bkt", Key="k", ChecksumMode="ENABLED")[arg] == b64(digest(alg, data))
    expect_error("BadDigest", 400, s3.put_object, Bucket="bkt", Key="k", Body=b"other", **{arg: b64(digest(alg, data))})


def test_checksum_algorithm_computed(s3):
    data = b"payload"
    r = s3.put_object(Bucket="bkt", Key="k", Body=data, ChecksumAlgorithm="SHA256")
    assert r["ChecksumSHA256"] == b64(digest("sha256", data))


def test_default_checksum(s3_url):
    # A client that does not send checksums gets S3's default one.
    s3 = s3_client(s3_url, request_checksum_calculation="when_required", response_checksum_validation="when_required")
    s3.create_bucket(Bucket="bkt")
    data = b"payload"
    r = s3.put_object(Bucket="bkt", Key="k", Body=data)
    assert r["ChecksumCRC64NVME"] == b64(digest("crc64nvme", data))
    assert r["ChecksumType"] == "FULL_OBJECT"
    h = s3.head_object(Bucket="bkt", Key="k", ChecksumMode="ENABLED")
    assert h["ChecksumCRC64NVME"] == b64(digest("crc64nvme", data))
    assert "ChecksumCRC64NVME" not in s3.head_object(Bucket="bkt", Key="k")


def test_no_checksums_of_ranges(s3):
    s3.put_object(Bucket="bkt", Key="k", Body=b"0123456789")
    r = s3.get_object(Bucket="bkt", Key="k", Range="bytes=1-2", ChecksumMode="ENABLED")
    assert not [k for k in r if k.startswith("Checksum") and k != "ChecksumType"]


def test_content_md5(s3):
    data = b"payload"
    s3.put_object(Bucket="bkt", Key="k", Body=data, ContentMD5=b64(digest("md5", data)))
    expect_error("BadDigest", 400, s3.put_object, Bucket="bkt", Key="k", Body=b"x", ContentMD5=b64(digest("md5", data)))
    expect_error("InvalidDigest", 400, s3.put_object, Bucket="bkt", Key="k", Body=b"x", ContentMD5="nope")


def test_bad_checksum_headers(s3_url):
    url = f"{s3_url}/bkt/k"
    requests.put(f"{s3_url}/bkt")
    crc = b64(digest("crc32", b"x"))
    for headers in [{"x-amz-checksum-crc16": crc},
                    {"x-amz-checksum-md5": b64(digest("md5", b"x"))},
                    {"x-amz-checksum-crc32": "not base64!"},
                    {"x-amz-checksum-crc32": crc, "x-amz-checksum-crc32c": b64(digest("crc32c", b"x"))},
                    {"x-amz-checksum-crc32": crc, "x-amz-sdk-checksum-algorithm": "SHA256"},
                    {"x-amz-sdk-checksum-algorithm": "MD5"}]:
        r = requests.put(url, data=b"x", headers=headers)
        assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (400, "InvalidRequest"), headers


# Multipart uploads

def upload(s3, key: str, parts: list[bytes], **create) -> tuple[str, list[dict]]:
    """Uploads the parts of a new upload, returning its id and the parts to
    complete it with."""
    u = s3.create_multipart_upload(Bucket="bkt", Key=key, **create)["UploadId"]
    # Or boto3 sends its default CRC32 with the parts, which S3 refuses
    # for an upload of another algorithm.
    alg = {"ChecksumAlgorithm": create["ChecksumAlgorithm"]} if "ChecksumAlgorithm" in create else {}
    listed = []
    for n, data in enumerate(parts, 1):
        r = s3.upload_part(Bucket="bkt", Key=key, UploadId=u, PartNumber=n, Body=data, **alg)
        listed.append({"PartNumber": n, "ETag": r["ETag"]})
    return u, listed


def complete(s3, key: str, u: str, parts: list[dict], **kwargs) -> dict:
    return s3.complete_multipart_upload(Bucket="bkt", Key=key, UploadId=u, MultipartUpload={"Parts": parts}, **kwargs)


def multipart_etag(parts: list[bytes]) -> str:
    md5s = b"".join(hashlib.md5(p).digest() for p in parts)
    return f'"{hashlib.md5(md5s).hexdigest()}-{len(parts)}"'


@pytest.mark.parametrize("sizes", [[0], [10], [PART_SIZE, 1], [PART_SIZE, PART_SIZE + 1, 0]])
def test_multipart_etag(s3, sizes):
    parts = [os.urandom(n) for n in sizes]
    u, listed = upload(s3, "k", parts)
    assert [p["ETag"] for p in listed] == [md5_etag(p) for p in parts]
    assert complete(s3, "k", u, listed)["ETag"] == multipart_etag(parts)
    assert s3.head_object(Bucket="bkt", Key="k")["ETag"] == multipart_etag(parts)
    [o] = s3.list_objects_v2(Bucket="bkt")["Contents"]
    assert o["ETag"] == multipart_etag(parts)


def composite(alg: str, parts: list[bytes]) -> str:
    """The base64 COMPOSITE checksum of the parts."""
    sums = b"".join(bytes.fromhex(digest(alg, p)) for p in parts)
    return f"{base64.b64encode(bytes.fromhex(digest(alg, sums))).decode()}-{len(parts)}"


@pytest.mark.parametrize("alg", [None, "crc32", "crc32c", "crc64nvme", "sha1", "sha256"])
def test_multipart_checksum(s3, alg):
    parts = [os.urandom(PART_SIZE), os.urandom(1000)]
    create = {"ChecksumAlgorithm": alg.upper()} if alg else {}
    u, listed = upload(s3, "k", parts, **create)
    r = complete(s3, "k", u, listed)
    # The default type of each algorithm, and CRC64NVME by default.
    alg = alg or "crc64nvme"
    arg = f"Checksum{alg.upper()}"
    if alg == "crc64nvme":
        expected, type = b64(digest(alg, b"".join(parts))), "FULL_OBJECT"
    else:
        expected, type = composite(alg, parts), "COMPOSITE"
    assert (r[arg], r["ChecksumType"]) == (expected, type)
    h = s3.head_object(Bucket="bkt", Key="k", ChecksumMode="ENABLED")
    assert (h[arg], h["ChecksumType"]) == (expected, type)


@pytest.mark.parametrize("alg, type", [("SHA256", "FULL_OBJECT"), ("CRC64NVME", "COMPOSITE")])
def test_multipart_invalid_checksum_type(s3, alg, type):
    expect_error("InvalidRequest", 400, s3.create_multipart_upload, Bucket="bkt", Key="k",
                 ChecksumAlgorithm=alg, ChecksumType=type)


def test_part_checksums(s3):
    u = s3.create_multipart_upload(Bucket="bkt", Key="k", ChecksumAlgorithm="SHA256")["UploadId"]
    r = s3.upload_part(Bucket="bkt", Key="k", UploadId=u, PartNumber=1, Body=b"x", ChecksumSHA256=b64(digest("sha256", b"x")))
    assert r["ChecksumSHA256"] == b64(digest("sha256", b"x"))
    expect_error("BadDigest", 400, s3.upload_part, Bucket="bkt", Key="k", UploadId=u, PartNumber=1, Body=b"y",
                 ChecksumSHA256=b64(digest("sha256", b"x")))
    expect_error("InvalidRequest", 400, s3.upload_part, Bucket="bkt", Key="k", UploadId=u, PartNumber=1, Body=b"x",
                 ChecksumCRC32=b64(digest("crc32", b"x")))
    # An upload with the default algorithm takes parts with any checksum.
    u = s3.create_multipart_upload(Bucket="bkt", Key="k2")["UploadId"]
    s3.upload_part(Bucket="bkt", Key="k2", UploadId=u, PartNumber=1, Body=b"x", ChecksumCRC32=b64(digest("crc32", b"x")))
    expect_error("BadDigest", 400, s3.upload_part, Bucket="bkt", Key="k2", UploadId=u, PartNumber=1, Body=b"y",
                 ChecksumCRC32=b64(digest("crc32", b"x")))


@pytest.mark.parametrize("number", [0, 10001])
def test_part_number_range(s3, number):
    u = s3.create_multipart_upload(Bucket="bkt", Key="k")["UploadId"]
    expect_error("InvalidArgument", 400, s3.upload_part, Bucket="bkt", Key="k", UploadId=u, PartNumber=number, Body=b"x")


def test_complete_errors(s3):
    u, listed = upload(s3, "k", [b"a" * PART_SIZE, b"small", b"last"])
    p1, p2, p3 = listed
    expect_error("InvalidPartOrder", 400, complete, s3, "k", u, [p2, p1])
    expect_error("InvalidPartOrder", 400, complete, s3, "k", u, [p1, p1])
    expect_error("EntityTooSmall", 400, complete, s3, "k", u, [p1, p2, p3])
    expect_error("InvalidPart", 400, complete, s3, "k", u, [{"PartNumber": 1, "ETag": p2["ETag"]}])
    expect_error("InvalidPart", 400, complete, s3, "k", u, [{**p1, "ChecksumCRC64NVME": b64(digest("crc64nvme", b"x"))}])
    expect_error("InvalidRequest", 400, complete, s3, "k", u, [p1, p3], MpuObjectSize=5)
    # Unquoted ETags are fine too.
    complete(s3, "k", u, [p1, {"PartNumber": 3, "ETag": p3["ETag"].strip('"')}], MpuObjectSize=PART_SIZE + 4)
    assert s3.get_object(Bucket="bkt", Key="k")["Body"].read() == b"a" * PART_SIZE + b"last"


def test_complete_object_checksum(s3):
    parts = [os.urandom(PART_SIZE), b"tail"]
    u, listed = upload(s3, "k", parts)
    expect_error("BadDigest", 400, complete, s3, "k", u, listed, ChecksumCRC64NVME=b64(digest("crc64nvme", b"x")))
    complete(s3, "k", u, listed, ChecksumCRC64NVME=b64(digest("crc64nvme", b"".join(parts))))


@pytest.mark.parametrize("body", [b"", b"<nope", b"<CompleteMultipartUpload/>",
                                  b"<CompleteMultipartUpload><Part><ETag>x</ETag></Part></CompleteMultipartUpload>"])
def test_complete_malformed(s3_url, s3, body):
    u = s3.create_multipart_upload(Bucket="bkt", Key="k")["UploadId"]
    r = requests.post(f"{s3_url}/bkt/k", params={"uploadId": u}, data=body)
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (400, "MalformedXML")


def test_list_parts(s3):
    u, _ = upload(s3, "k", [b"1", b"22", b"333", b"4444"], ChecksumAlgorithm="CRC32C")
    r = s3.list_parts(Bucket="bkt", Key="k", UploadId=u, MaxParts=2)
    assert [(p["PartNumber"], p["Size"]) for p in r["Parts"]] == [(1, 1), (2, 2)]
    assert (r["IsTruncated"], r["NextPartNumberMarker"], r["MaxParts"]) == (True, 2, 2)
    assert r["Parts"][1]["ETag"] == md5_etag(b"22")
    assert r["Parts"][1]["ChecksumCRC32C"] == b64(digest("crc32c", b"22"))
    assert (r["ChecksumAlgorithm"], r["ChecksumType"]) == ("CRC32C", "COMPOSITE")
    r = s3.list_parts(Bucket="bkt", Key="k", UploadId=u, PartNumberMarker=2)
    assert ([p["PartNumber"] for p in r["Parts"]], r["IsTruncated"]) == ([3, 4], False)


def test_abort(s3):
    u, _ = upload(s3, "k", [b"x"])
    assert s3.abort_multipart_upload(Bucket="bkt", Key="k", UploadId=u)["ResponseMetadata"]["HTTPStatusCode"] == 204
    expect_error("NoSuchUpload", 404, s3.abort_multipart_upload, Bucket="bkt", Key="k", UploadId=u)


def test_upload_file(s3, tmp_path):
    # boto3's managed upload: parts of 8 MiB, with CRC32s.
    data = os.urandom(20 * 1024 * 1024 + 7)
    path = tmp_path / "data"
    path.write_bytes(data)
    s3.upload_file(str(path), "bkt", "k")
    h = s3.head_object(Bucket="bkt", Key="k", ChecksumMode="ENABLED")
    assert (h["ContentLength"], h["ChecksumType"]) == (len(data), "COMPOSITE")
    assert h["ETag"].endswith('-3"')
    assert s3.get_object(Bucket="bkt", Key="k")["Body"].read() == data


# CopyObject

def copy(s3, key: str, src: str, **kwargs) -> dict:
    return s3.copy_object(Bucket="bkt", Key=key, CopySource={"Bucket": "bkt", "Key": src}, **kwargs)


def test_copy_etag(s3):
    """A copy is a single upload: its ETag is the MD5 of its data, whatever
    the source's is."""
    parts = [os.urandom(PART_SIZE), b"tail"]
    u, listed = upload(s3, "mp", parts)
    complete(s3, "mp", u, listed)
    r = copy(s3, "c1", "mp")["CopyObjectResult"]
    assert r["ETag"] == md5_etag(b"".join(parts))
    assert s3.head_object(Bucket="bkt", Key="c1")["ETag"] == md5_etag(b"".join(parts))
    s3.put_object(Bucket="bkt", Key="single", Body=b"hello")
    assert copy(s3, "c2", "single")["CopyObjectResult"]["ETag"] == md5_etag(b"hello")


def test_copy_checksum(s3):
    """A copy keeps the source's checksum algorithm, unless asked for
    another one, as a FULL_OBJECT checksum."""
    parts = [os.urandom(PART_SIZE), b"tail"]
    data = b"".join(parts)
    u, listed = upload(s3, "crc32", parts, ChecksumAlgorithm="CRC32")
    complete(s3, "crc32", u, listed)
    s3.put_object(Bucket="bkt", Key="sha256", Body=data, ChecksumAlgorithm="SHA256")
    # Not put_object(), whose body boto3 sends with a CRC32.
    u, listed = upload(s3, "default", parts)
    complete(s3, "default", u, listed)
    for src, kwargs, alg in [("crc32", {}, "crc32"), ("sha256", {}, "sha256"), ("default", {}, "crc64nvme"),
                             ("crc32", {"ChecksumAlgorithm": "SHA1"}, "sha1"),
                             ("default", {"ChecksumAlgorithm": "CRC32C"}, "crc32c")]:
        arg = f"Checksum{alg.upper()}"
        r = copy(s3, "dst", src, **kwargs)["CopyObjectResult"]
        assert (r[arg], r["ChecksumType"]) == (b64(digest(alg, data)), "FULL_OBJECT")
        h = s3.head_object(Bucket="bkt", Key="dst", ChecksumMode="ENABLED")
        assert (h[arg], h["ChecksumType"]) == (b64(digest(alg, data)), "FULL_OBJECT")


def test_copy_metadata_directive(s3):
    s3.put_object(Bucket="bkt", Key="src", Body=b"x", Metadata={"m": "src"})
    copy(s3, "dst", "src", Metadata={"m": "ignored"})
    assert s3.head_object(Bucket="bkt", Key="dst")["Metadata"] == {"m": "src"}
    copy(s3, "dst", "src", MetadataDirective="COPY")
    assert s3.head_object(Bucket="bkt", Key="dst")["Metadata"] == {"m": "src"}
    copy(s3, "dst", "src", MetadataDirective="REPLACE", Metadata={"n": "dst"})
    assert s3.head_object(Bucket="bkt", Key="dst")["Metadata"] == {"n": "dst"}
    copy(s3, "dst", "src", MetadataDirective="REPLACE")
    assert s3.head_object(Bucket="bkt", Key="dst")["Metadata"] == {}
    expect_error("InvalidArgument", 400, copy, s3, "dst", "src", MetadataDirective="MOVE")


def test_copy_onto_itself_needs_replace(s3):
    s3.put_object(Bucket="bkt", Key="k", Body=b"x", Metadata={"m": "v"})
    expect_error("InvalidRequest", 400, copy, s3, "k", "k")
    expect_error("InvalidRequest", 400, copy, s3, "k", "k", ChecksumAlgorithm="SHA256")
    copy(s3, "k", "k", MetadataDirective="REPLACE")
    h = s3.head_object(Bucket="bkt", Key="k")
    assert (h["Metadata"], s3.get_object(Bucket="bkt", Key="k")["Body"].read()) == ({}, b"x")


@pytest.mark.parametrize("conditions, ok", [
    ({"CopySourceIfMatch": "{etag}"}, True),
    ({"CopySourceIfMatch": '"other"'}, False),
    ({"CopySourceIfMatch": "*"}, True),
    ({"CopySourceIfNoneMatch": "{etag}"}, False),
    ({"CopySourceIfNoneMatch": '"other"'}, True),
    ({"CopySourceIfModifiedSince": "{past}"}, True),
    ({"CopySourceIfModifiedSince": "{future}"}, False),
    ({"CopySourceIfUnmodifiedSince": "{future}"}, True),
    ({"CopySourceIfUnmodifiedSince": "{past}"}, False),
    # As HTTP evaluates them: If-Match wins over If-Unmodified-Since, and
    # If-None-Match over If-Modified-Since.
    ({"CopySourceIfMatch": "{etag}", "CopySourceIfUnmodifiedSince": "{past}"}, True),
    ({"CopySourceIfNoneMatch": '"other"', "CopySourceIfModifiedSince": "{future}"}, True),
    ({"CopySourceIfNoneMatch": "{etag}", "CopySourceIfModifiedSince": "{past}"}, False),
])
def test_copy_source_conditions(s3, conditions, ok):
    etag = s3.put_object(Bucket="bkt", Key="src", Body=b"x")["ETag"]
    now = datetime.datetime.now(datetime.timezone.utc)
    values = {"{etag}": etag, "{past}": now - datetime.timedelta(days=1), "{future}": now + datetime.timedelta(days=1)}
    kwargs = {k: values.get(v, v) for k, v in conditions.items()}
    if ok:
        copy(s3, "dst", "src", **kwargs)
        assert s3.get_object(Bucket="bkt", Key="dst")["Body"].read() == b"x"
    else:
        # Whichever condition fails, unlike a read's 304.
        expect_error("PreconditionFailed", 412, copy, s3, "dst", "src", **kwargs)
        expect_error("NoSuchKey", 404, s3.get_object, Bucket="bkt", Key="dst")


@pytest.mark.parametrize("source, key", [
    ("/bkt/src", "src"),
    ("bkt/src", "src"),
    ("/bkt/a%20b%2Bc", "a b+c"),
    ("/bkt/a+b", "a+b"),
    ("/bkt/d/e", "d/e"),
])
def test_copy_source_header(s3_url, s3, source, key):
    s3.put_object(Bucket="bkt", Key=key, Body=b"x")
    r = requests.put(f"{s3_url}/bkt/dst", headers={"x-amz-copy-source": source})
    assert r.status_code == 200, r.content
    assert ET.fromstring(r.content).tag == f"{NS}CopyObjectResult"
    assert s3.get_object(Bucket="bkt", Key="dst")["Body"].read() == b"x"


@pytest.mark.parametrize("source, code", [
    ("bkt", "InvalidArgument"),
    ("/bkt/", "InvalidArgument"),
    ("/bkt", "InvalidArgument"),
    ("//src", "InvalidArgument"),
    ("/bkt/dir/", "InvalidArgument"),
    ("/bkt/" + "x" * 1025, "KeyTooLongError"),
])
def test_copy_bad_source(s3_url, s3, source, code):
    r = requests.put(f"{s3_url}/bkt/dst", headers={"x-amz-copy-source": source})
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (400, code)


# Listing

@pytest.fixture
def tree(s3):
    keys = ["a", "b/1", "b/2", "b/c/1", "c/1", "d"]
    for k in keys:
        s3.put_object(Bucket="bkt", Key=k, Body=b"")
    return keys


def pages(s3, method: str, **kwargs) -> list[dict]:
    return list(s3.get_paginator(method).paginate(Bucket="bkt", **kwargs))


def keys_and_prefixes(pages: list[dict]) -> tuple[list[str], list[str]]:
    return ([o["Key"] for p in pages for o in p.get("Contents", [])],
            [c["Prefix"] for p in pages for c in p.get("CommonPrefixes", [])])


def test_list_delimiter(s3, tree):
    r = s3.list_objects_v2(Bucket="bkt", Delimiter="/")
    assert [o["Key"] for o in r["Contents"]] == ["a", "d"]
    assert [c["Prefix"] for c in r["CommonPrefixes"]] == ["b/", "c/"]
    assert (r["KeyCount"], r["Delimiter"]) == (4, "/")
    r = s3.list_objects_v2(Bucket="bkt", Delimiter="/", Prefix="b/")
    assert [o["Key"] for o in r["Contents"]] == ["b/1", "b/2"]
    assert [c["Prefix"] for c in r["CommonPrefixes"]] == ["b/c/"]
    r = s3.list_objects_v2(Bucket="bkt", Delimiter="/1")
    assert [o["Key"] for o in r["Contents"]] == ["a", "b/2", "d"]
    assert [c["Prefix"] for c in r["CommonPrefixes"]] == ["b/1", "b/c/1", "c/1"]


@pytest.mark.parametrize("page", [1, 2, 3, 1000])
def test_list_delimiter_pages(s3, tree, page):
    for method in ["list_objects_v2", "list_objects"]:
        ps = pages(s3, method, Delimiter="/", PaginationConfig={"PageSize": page})
        assert keys_and_prefixes(ps) == (["a", "d"], ["b/", "c/"])
        assert all(len(p.get("Contents", [])) + len(p.get("CommonPrefixes", [])) <= page for p in ps)


def test_list_v2_tokens(s3, tree):
    r = s3.list_objects_v2(Bucket="bkt", MaxKeys=2)
    assert ([o["Key"] for o in r["Contents"]], r["IsTruncated"], r["KeyCount"]) == (["a", "b/1"], True, 2)
    r = s3.list_objects_v2(Bucket="bkt", MaxKeys=2, ContinuationToken=r["NextContinuationToken"])
    assert [o["Key"] for o in r["Contents"]] == ["b/2", "b/c/1"]
    r = s3.list_objects_v2(Bucket="bkt", MaxKeys=2, StartAfter="b/c/1")
    assert ([o["Key"] for o in r["Contents"]], r["IsTruncated"], r["StartAfter"]) == (["c/1", "d"], False, "b/c/1")
    assert "NextContinuationToken" not in r
    expect_error("InvalidArgument", 400, s3.list_objects_v2, Bucket="bkt", ContinuationToken="!!")


def test_list_v1_markers(s3, tree):
    r = s3.list_objects(Bucket="bkt", MaxKeys=2)
    assert ([o["Key"] for o in r["Contents"]], r["IsTruncated"]) == (["a", "b/1"], True)
    assert "NextMarker" not in r  # only with a delimiter
    r = s3.list_objects(Bucket="bkt", Marker="b/1", MaxKeys=2)
    assert ([o["Key"] for o in r["Contents"]], r["Marker"]) == (["b/2", "b/c/1"], "b/1")
    r = s3.list_objects(Bucket="bkt", Delimiter="/", MaxKeys=2)
    assert (keys_and_prefixes([r]), r["NextMarker"]) == ((["a"], ["b/"]), "b/")
    assert all("Owner" in o for o in s3.list_objects(Bucket="bkt")["Contents"])


def test_list_max_keys(s3, tree):
    r = s3.list_objects_v2(Bucket="bkt", MaxKeys=0)
    assert ("Contents" not in r, r["KeyCount"]) == (True, 0)
    assert s3.list_objects_v2(Bucket="bkt", MaxKeys=5000)["MaxKeys"] == 1000


def test_list_url_encoding(s3):
    keys = ["sp ace", "pl+us", "amp&er", "ünï", "lt<gt>"]
    for k in keys:
        s3.put_object(Bucket="bkt", Key=k, Body=b"")
    # boto3 asks for url encoding and decodes the keys itself.
    assert [o["Key"] for o in s3.list_objects_v2(Bucket="bkt")["Contents"]] == sorted(keys, key=str.encode)
    assert [o["Key"] for o in s3.list_objects(Bucket="bkt")["Contents"]] == sorted(keys, key=str.encode)


def test_list_raw_xml(s3_url, s3, tree):
    r = requests.get(f"{s3_url}/bkt", params={"list-type": "2", "prefix": "b/", "delimiter": "/"})
    assert r.headers["Content-Type"] == "application/xml"
    root = ET.fromstring(r.content)
    assert root.tag == f"{NS}ListBucketResult"
    assert [e.text for e in root.iter(f"{NS}Key")] == ["b/1", "b/2"]
    assert root.find(f"{NS}Contents/{NS}StorageClass").text == "STANDARD"
    assert root.find(f"{NS}Contents/{NS}Owner") is None  # only with fetch-owner


@pytest.mark.parametrize("params", [{"list-type": "3"}, {"encoding-type": "base64"}, {"max-keys": "-1"}])
def test_list_bad_params(s3_url, s3, params):
    r = requests.get(f"{s3_url}/bkt", params=params)
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (400, "InvalidArgument")


# The wire

def test_content_length(s3_url, s3):
    data = b"x" * 100_000
    s3.put_object(Bucket="bkt", Key="k", Body=data)
    r = requests.get(f"{s3_url}/bkt/k")
    assert (r.headers["Content-Length"], r.content) == (str(len(data)), data)
    assert "Transfer-Encoding" not in r.headers
    assert r.headers["Content-Type"] == "binary/octet-stream"
    r = requests.get(f"{s3_url}/bkt/k", headers={"Range": "bytes=10-19"})
    assert (r.status_code, r.headers["Content-Length"], r.content) == (206, "10", data[10:20])
    r = requests.head(f"{s3_url}/bkt/k")
    assert (r.headers["Content-Length"], r.content) == (str(len(data)), b"")


def test_error_reply(s3_url, s3):
    r = requests.get(f"{s3_url}/bkt/no/such")
    assert (r.status_code, r.headers["Content-Type"]) == (404, "application/xml")
    root = ET.fromstring(r.content)
    assert root.tag == "Error"
    assert root.find("Code").text == "NoSuchKey"
    assert root.find("Message").text == "The specified key does not exist."
    assert root.find("Resource").text == "/bkt/no/such"
    assert root.find("RequestId").text == r.headers["x-amz-request-id"]


@pytest.mark.parametrize("method, path, headers", [
    ("PUT", "/bkt/k?tagging", {}),
    ("GET", "/bkt/k?tagging", {}),
    ("PUT", "/bkt/k", {"x-amz-tagging": "a=b"}),
    ("PUT", "/bkt/k", {"x-amz-copy-source": "/bkt2/k"}),
    ("PUT", "/bkt/k", {"x-amz-copy-source": "/bkt/k?versionId=1", "x-amz-metadata-directive": "REPLACE"}),
    ("PUT", "/bkt/k", {"x-amz-copy-source": "/bkt/k", "x-amz-metadata-directive": "REPLACE", "x-amz-tagging": "a=b"}),
    ("PUT", "/bkt/k", {"If-Match": '"x"'}),
    ("PUT", "/bkt/k", {"If-None-Match": '"x"'}),
    ("PUT", "/bkt/k", {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}),
    ("PUT", "/bkt/k", {"Content-Encoding": "aws-chunked"}),
    ("POST", "/bkt/k?uploads", {"x-amz-tagging": "a=b"}),
    ("POST", "/bkt/k?uploads", {"x-amz-checksum-algorithm": "CRC32", "x-amz-checksum-type": "FULL_OBJECT"}),
    ("PUT", "/bkt/k?partNumber=1&uploadId=x", {"x-amz-copy-source": "/bkt/src"}),
    ("GET", "/bkt?uploads", {}),
    ("GET", "/bkt/k?versionId=1", {}),
    ("GET", "/bkt?versioning", {}),
    ("POST", "/bkt?delete", {}),
])
def test_not_implemented(s3_url, s3, method, path, headers):
    s3.put_object(Bucket="bkt", Key="k", Body=b"x")
    r = requests.request(method, f"{s3_url}{path}", headers=headers, data=b"")
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (501, "NotImplemented")
    assert s3.get_object(Bucket="bkt", Key="k")["Body"].read() == b"x"
