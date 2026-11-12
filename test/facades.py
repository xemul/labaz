# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""One client interface over every facade, for tests of what all of them
do the same way. Each facade's client raises FacadeError, whose code is
the facade's own; code() names it from the storage error code."""

import base64
from dataclasses import dataclass, field

import boto3
import botocore
from botocore.config import Config

from facade_error import FacadeError
from plywood_client import PlywoodClient

FACADES = ["plywood", "s3"]


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


class PlywoodFacade:
    name = "plywood"

    def __init__(self, url: str):
        self.url = url
        self.c = PlywoodClient(url)

    def code(self, errc: str) -> str:
        return errc

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

    def list(self, bucket, prefix="", start_after="", max_keys=None) -> tuple[list[dict], bool]:
        """([{"key", "size"}], truncated)"""
        r = self.c.list(bucket, prefix=prefix, start_after=start_after, max_keys=max_keys)
        return [{"key": o["key"], "size": o["size"]} for o in r["objects"]], r["truncated"]

    def list_keys(self, bucket, prefix="", page=1000) -> list[str]:
        return self.c.list_keys(bucket, prefix=prefix, page=page)


# Storage error codes as the s3 facade reports them.
S3_CODES = {
    "no_such_bucket": "NoSuchBucket",
    "no_such_key": "NoSuchKey",
    "bucket_exists": "BucketAlreadyOwnedByYou",
    "bucket_not_empty": "BucketNotEmpty",
    "invalid_argument": "InvalidArgument",
    "invalid_range": "InvalidRange",
    "precondition_failed": "PreconditionFailed",
    "bad_digest": "BadDigest",
}


class S3Facade:
    name = "s3"

    def __init__(self, url: str):
        self.url = url
        self.c = s3_client(url)

    def code(self, errc: str) -> str:
        return S3_CODES[errc]

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


def make_client(facade: str, url: str):
    return {"plywood": PlywoodFacade, "s3": S3Facade}[facade](url)
