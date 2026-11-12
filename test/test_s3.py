# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""The s3 facade: what S3 does its own way, through boto3, and the wire
format, through plain HTTP requests. What every facade does alike is tested
in test_objects.py."""

import base64
import datetime
import hashlib
import xml.etree.ElementTree as ET

import botocore
import pytest
import requests

from facades import s3_client
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
    ("PUT", "/bkt/k", {"x-amz-copy-source": "/bkt/src"}),
    ("PUT", "/bkt/k", {"If-Match": '"x"'}),
    ("PUT", "/bkt/k", {"If-None-Match": '"x"'}),
    ("PUT", "/bkt/k", {"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}),
    ("PUT", "/bkt/k", {"Content-Encoding": "aws-chunked"}),
    ("POST", "/bkt/k?uploads", {}),
    ("PUT", "/bkt/k?partNumber=1&uploadId=x", {}),
    ("GET", "/bkt/k", {"If-None-Match": '"x"'}),
    ("GET", "/bkt/k", {"If-Modified-Since": "Wed, 12 Oct 2009 17:50:00 GMT"}),
    ("GET", "/bkt/k?versionId=1", {}),
    ("GET", "/bkt?versioning", {}),
    ("POST", "/bkt?delete", {}),
])
def test_not_implemented(s3_url, s3, method, path, headers):
    s3.put_object(Bucket="bkt", Key="k", Body=b"x")
    r = requests.request(method, f"{s3_url}{path}", headers=headers, data=b"")
    assert (r.status_code, ET.fromstring(r.content).find("Code").text) == (501, "NotImplemented")
    assert s3.get_object(Bucket="bkt", Key="k")["Body"].read() == b"x"
