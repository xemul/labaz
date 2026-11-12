# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""One client interface over every facade, for tests of what all of them
do the same way. Each facade's client raises FacadeError, whose code and
status are the facade's own; error() gives them for a storage error code.

Multipart uploads are plywood's sessions. As S3 requires, every part of
an upload but the last must have at least PART_SIZE bytes."""

import base64
from dataclasses import dataclass, field

import boto3
import botocore
from botocore.config import Config

from facade_error import FacadeError
from plywood_client import PlywoodClient

FACADES = ["plywood", "s3"]
PART_SIZE = 5 << 20


@dataclass
class Object:
    data: bytes = b""
    size: int = 0
    metadata: dict = field(default_factory=dict)
    status: int = 200
    content_range: str | None = None


def s3_client(url: str, **config) -> "botocore.client.S3":
    """A boto3 client of the s3 facade at `url`. Requests are not signed
    with real credentials, as the facade does not check them, and are not
    retried."""
    cfg = {"s3": {"addressing_style": "path"}, "retries": {"total_max_attempts": 1}} | config
    return boto3.client("s3", endpoint_url=url, aws_access_key_id="labaz", aws_secret_access_key="labaz",
                        region_name="us-east-1", config=Config(**cfg))


# Storage error codes as (code, status) of the plywood facade.
PLYWOOD_ERRORS = {
    "no_such_bucket": ("no_such_bucket", 404),
    "no_such_key": ("no_such_key", 404),
    "no_such_session": ("no_such_session", 404),
    "no_such_part": ("no_such_part", 404),
    "bucket_exists": ("bucket_exists", 409),
    "bucket_not_empty": ("bucket_not_empty", 409),
    "invalid_argument": ("invalid_argument", 400),
    "invalid_range": ("invalid_range", 416),
    "precondition_failed": ("precondition_failed", 412),
    "bad_digest": ("bad_digest", 400),
}


class PlywoodFacade:
    name = "plywood"

    def __init__(self, url: str):
        self.url = url
        self.c = PlywoodClient(url)

    def error(self, errc: str) -> tuple[str, int]:
        return PLYWOOD_ERRORS[errc]

    def create_bucket(self, bucket):
        self.c.create_bucket(bucket)

    def delete_bucket(self, bucket):
        self.c.delete_bucket(bucket)

    def list_buckets(self) -> list[str]:
        return self.c.list_buckets()

    def put(self, bucket, key, data: bytes, metadata: dict | None = None, verify: dict | None = None):
        self.c.put(bucket, key, data, metadata=metadata, verify=verify)

    @staticmethod
    def _object(headers, data=b"", status=200) -> Object:
        prefix = "x-labaz-meta-"
        return Object(data=data, size=int(headers["X-Labaz-Size"]), status=status,
                      content_range=headers.get("Content-Range"),
                      metadata={k[len(prefix):]: v for k, v in headers.items() if k.lower().startswith(prefix)})

    def get(self, bucket, key, range: str | None = None) -> Object:
        r = self.c.get(bucket, key, range=range)
        return self._object(r.headers, r.content, r.status_code)

    def read(self, bucket, key, range: str | None = None) -> bytes:
        return self.c.read(bucket, key, range=range)

    def head(self, bucket, key) -> Object:
        return self._object(self.c.head(bucket, key).headers)

    def delete(self, bucket, key):
        self.c.delete(bucket, key)

    def copy(self, bucket, key, src_key, metadata: dict | None = None, must_not_exist: bool = False):
        """Copies the whole source, with the given metadata."""
        self.c.copy(bucket, key, src_key, metadata=metadata, generation_match=0 if must_not_exist else None)

    def list(self, bucket, prefix="", start_after="", max_keys=None) -> tuple[list[dict], bool]:
        """([{"key", "size"}], truncated)"""
        r = self.c.list(bucket, prefix=prefix, start_after=start_after, max_keys=max_keys)
        return [{"key": o["key"], "size": o["size"]} for o in r["objects"]], r["truncated"]

    def list_keys(self, bucket, prefix="", page=1000) -> list[str]:
        return self.c.list_keys(bucket, prefix=prefix, page=page)

    # multipart uploads

    def open_upload(self, bucket, key, metadata: dict | None = None) -> str:
        return self.c.open_session(bucket, key, metadata=metadata)

    def upload_part(self, bucket, key, upload: str, number: int, data: bytes):
        self.c.put_part(bucket, key, upload, number, data)

    def copy_part(self, bucket, key, upload: str, number: int, src_key, range: str | None = None):
        """range: "bytes=first-last" of the source."""
        self.c.copy_part(bucket, key, upload, number, src_key, range=range)

    def list_parts(self, bucket, key, upload: str) -> list[tuple[int, int]]:
        """[(number, size)]"""
        return [(p["number"], p["size"]) for p in self.c.describe_session(bucket, key, upload)["parts"]]

    def complete(self, bucket, key, upload: str, parts: list[int], must_not_exist: bool = False):
        """Returns what the facade replies with, the same for a retry."""
        return self.c.commit(bucket, key, upload, parts, generation_match=0 if must_not_exist else None)

    def abort(self, bucket, key, upload: str):
        self.c.abort(bucket, key, upload)


# Storage error codes as (code, status) of the s3 facade.
S3_ERRORS = {
    "no_such_bucket": ("NoSuchBucket", 404),
    "no_such_key": ("NoSuchKey", 404),
    "no_such_session": ("NoSuchUpload", 404),
    "no_such_part": ("InvalidPart", 400),
    "bucket_exists": ("BucketAlreadyOwnedByYou", 409),
    "bucket_not_empty": ("BucketNotEmpty", 409),
    "invalid_argument": ("InvalidArgument", 400),
    "invalid_range": ("InvalidRange", 416),
    "precondition_failed": ("PreconditionFailed", 412),
    "bad_digest": ("BadDigest", 400),
}


class S3Facade:
    name = "s3"

    def __init__(self, url: str):
        self.url = url
        self.c = s3_client(url)
        # The ETags of uploaded parts, which completing an upload lists.
        self.etags = {}

    def error(self, errc: str) -> tuple[str, int]:
        return S3_ERRORS[errc]

    def _call(self, method, **kwargs) -> dict:
        try:
            return getattr(self.c, method)(**kwargs)
        except botocore.exceptions.ClientError as e:
            err = e.response["Error"]
            raise FacadeError(e.response["ResponseMetadata"]["HTTPStatusCode"], err["Code"], err.get("Message", ""))

    def create_bucket(self, bucket):
        self._call("create_bucket", Bucket=bucket)

    def delete_bucket(self, bucket):
        self._call("delete_bucket", Bucket=bucket)

    def list_buckets(self) -> list[str]:
        return [b["Name"] for b in self._call("list_buckets")["Buckets"]]

    def put(self, bucket, key, data: bytes, metadata: dict | None = None, verify: dict | None = None):
        """verify: {algorithm: hex digest} the body must have; md5 goes as
        Content-MD5, the others as x-amz-checksum-<algorithm>."""
        kwargs = {}
        for alg, value in (verify or {}).items():
            b64 = base64.b64encode(bytes.fromhex(value)).decode()
            if alg == "md5":
                kwargs["ContentMD5"] = b64
            else:
                kwargs[f"Checksum{alg.upper()}"] = b64
        self._call("put_object", Bucket=bucket, Key=key, Body=data, Metadata=metadata or {}, **kwargs)

    def get(self, bucket, key, range: str | None = None) -> Object:
        r = self._call("get_object", Bucket=bucket, Key=key, **({"Range": range} if range else {}))
        return Object(data=r["Body"].read(), size=r["ContentLength"], metadata=r["Metadata"],
                      status=r["ResponseMetadata"]["HTTPStatusCode"], content_range=r.get("ContentRange"))

    def read(self, bucket, key, range: str | None = None) -> bytes:
        return self.get(bucket, key, range).data

    def head(self, bucket, key) -> Object:
        r = self._call("head_object", Bucket=bucket, Key=key)
        return Object(size=r["ContentLength"], metadata=r["Metadata"])

    def delete(self, bucket, key):
        self._call("delete_object", Bucket=bucket, Key=key)

    def copy(self, bucket, key, src_key, metadata: dict | None = None, must_not_exist: bool = False):
        kwargs = {"IfNoneMatch": "*"} if must_not_exist else {}
        self._call("copy_object", Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": src_key},
                   MetadataDirective="REPLACE", Metadata=metadata or {}, **kwargs)

    def list(self, bucket, prefix="", start_after="", max_keys=None) -> tuple[list[dict], bool]:
        kwargs = {"Prefix": prefix, "StartAfter": start_after}
        if max_keys is not None:
            kwargs["MaxKeys"] = max_keys
        r = self._call("list_objects_v2", Bucket=bucket, **kwargs)
        return [{"key": o["Key"], "size": o["Size"]} for o in r.get("Contents", [])], r["IsTruncated"]

    def list_keys(self, bucket, prefix="", page=1000) -> list[str]:
        keys, token = [], None
        while True:
            kwargs = {"ContinuationToken": token} if token else {}
            r = self._call("list_objects_v2", Bucket=bucket, Prefix=prefix, MaxKeys=page, **kwargs)
            keys += [o["Key"] for o in r.get("Contents", [])]
            if not r["IsTruncated"]:
                return keys
            token = r["NextContinuationToken"]

    # multipart uploads

    def open_upload(self, bucket, key, metadata: dict | None = None) -> str:
        return self._call("create_multipart_upload", Bucket=bucket, Key=key, Metadata=metadata or {})["UploadId"]

    def upload_part(self, bucket, key, upload: str, number: int, data: bytes):
        r = self._call("upload_part", Bucket=bucket, Key=key, UploadId=upload, PartNumber=number, Body=data)
        self.etags[(upload, number)] = r["ETag"]

    def copy_part(self, bucket, key, upload: str, number: int, src_key, range: str | None = None):
        kwargs = {"CopySourceRange": range} if range else {}
        r = self._call("upload_part_copy", Bucket=bucket, Key=key, UploadId=upload, PartNumber=number,
                       CopySource={"Bucket": bucket, "Key": src_key}, **kwargs)
        self.etags[(upload, number)] = r["CopyPartResult"]["ETag"]

    def list_parts(self, bucket, key, upload: str) -> list[tuple[int, int]]:
        r = self._call("list_parts", Bucket=bucket, Key=key, UploadId=upload)
        return [(p["PartNumber"], p["Size"]) for p in r.get("Parts", [])]

    def complete(self, bucket, key, upload: str, parts: list[int], must_not_exist: bool = False):
        kwargs = {"IfNoneMatch": "*"} if must_not_exist else {}
        listed = [{"PartNumber": n, "ETag": self.etags.get((upload, n), '"none"')} for n in parts]
        r = self._call("complete_multipart_upload", Bucket=bucket, Key=key, UploadId=upload,
                       MultipartUpload={"Parts": listed}, **kwargs)
        return r["ETag"]

    def abort(self, bucket, key, upload: str):
        self._call("abort_multipart_upload", Bucket=bucket, Key=key, UploadId=upload)


def make_client(facade: str, url: str):
    return {"plywood": PlywoodFacade, "s3": S3Facade}[facade](url)
