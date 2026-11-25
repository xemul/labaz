# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""One client interface over every facade, for tests of what all of them
do the same way. Each facade's client raises FacadeError, whose code and
status are the facade's own; error() gives them for a storage error code.

Multipart uploads are plywood's sessions. As S3 requires, every part of
an upload but the last must have at least PART_SIZE bytes. Not every
facade has everything: `features` lists what of the following a client
has, and tests that need any of them skip the others:

- "parts": parts that may come in any order, be sent again or left out,
  and be listed; GCS's resumable uploads append chunks in order.
- "copy": copies of objects.
- "parts" with "copy": parts copied from objects.
- "head": HEAD requests (GCS's JSON API has none)."""

import base64
import json
from dataclasses import dataclass, field
from urllib.parse import quote

import boto3
import botocore
import google.api_core.exceptions
import google.resumable_media
import requests
from botocore.config import Config
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage as gcs_storage
from google.resumable_media.requests import Download

from facade_error import FacadeError
from plywood_client import PlywoodClient

FACADES = ["plywood", "s3", "gcs"]
ALL_FEATURES = {"parts", "copy", "head"}
PART_SIZE = 5 << 20


@dataclass
class Object:
    data: bytes = b""
    size: int = 0
    metadata: dict = field(default_factory=dict)
    status: int = 200
    content_range: str | None = None


def s3_client(url: str, **config) -> "botocore.client.S3":
    """A boto3 client of the s3 facade at `url`. Requests are signed with
    the key labaz:labaz, which servers that check signatures are given, and
    are not retried."""
    cfg = {"s3": {"addressing_style": "path"}, "retries": {"total_max_attempts": 1}} | config
    return boto3.client("s3", endpoint_url=url, aws_access_key_id="labaz", aws_secret_access_key="labaz",
                        region_name="us-east-1", config=Config(**cfg))


# The key the s3 facade's test servers check signatures against, as
# --s3-key, and that s3_client() signs with.
S3_KEY = "labaz:labaz"


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
    features = ALL_FEATURES

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

    def open_upload(self, bucket, key, metadata: dict | None = None, must_not_exist: bool = False) -> str:
        """must_not_exist goes with complete() as well."""
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
    features = ALL_FEATURES

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

    def open_upload(self, bucket, key, metadata: dict | None = None, must_not_exist: bool = False) -> str:
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


def gcs_client(url: str) -> gcs_storage.Client:
    """A google-cloud-storage client of the gcs facade at `url`."""
    return gcs_storage.Client(project="labaz", credentials=AnonymousCredentials(),
                              client_options={"api_endpoint": url})


# Storage error codes as (reason, status) of the gcs facade.
GCS_ERRORS = {
    "no_such_bucket": ("notFound", 404),
    "no_such_key": ("notFound", 404),
    "no_such_session": ("notFound", 404),
    "bucket_exists": ("conflict", 409),
    "bucket_not_empty": ("conflict", 409),
    "invalid_argument": ("invalid", 400),
    "invalid_range": ("requestedRangeNotSatisfiable", 416),
    "precondition_failed": ("conditionNotMet", 412),
    "bad_digest": ("invalid", 400),
}


def gcs_error(r: requests.Response) -> FacadeError:
    try:
        err = r.json()["error"]
        return FacadeError(r.status_code, err["errors"][0]["reason"], err["message"])
    except (ValueError, KeyError, IndexError):
        return FacadeError(r.status_code, "", r.text)


class GcsFacade:
    """Through google-cloud-storage. An upload is a resumable upload, whose
    chunks are sent as such (the library has no call for one chunk), and
    whose parts must come in order."""
    name = "gcs"
    features = {"copy"}

    def __init__(self, url: str):
        self.url = url
        self.c = gcs_client(url)
        # The uploads opened with must_not_exist.
        self.must_not_exist = set()
        # The sizes of the chunks sent, and the last part, not sent yet, by
        # (bucket, key, upload).
        self.parts = {}

    def error(self, errc: str) -> tuple[str, int]:
        return GCS_ERRORS[errc]

    def _call(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except google.api_core.exceptions.GoogleAPICallError as e:
            # Errors of uploads come without their details.
            if not e.errors and e.response is not None:
                raise gcs_error(e.response)
            reason = e.errors[0].get("reason", "") if e.errors else ""
            raise FacadeError(e.code, reason, e.message)
        except google.resumable_media.InvalidResponse as e:
            raise gcs_error(e.response)

    def _blob(self, bucket, key):
        return self.c.bucket(bucket).blob(key)

    def create_bucket(self, bucket):
        self._call(self.c.create_bucket, bucket)

    def delete_bucket(self, bucket):
        self._call(self.c.bucket(bucket).delete)

    def list_buckets(self) -> list[str]:
        return self._call(lambda: [b.name for b in self.c.list_buckets()])

    def put(self, bucket, key, data: bytes, metadata: dict | None = None, verify: dict | None = None):
        """verify: {algorithm: hex digest} the body must have, md5 or crc32c."""
        blob = self._blob(bucket, key)
        blob.metadata = metadata
        for alg, value in (verify or {}).items():
            setattr(blob, {"md5": "md5_hash", "crc32c": "crc32c"}[alg], base64.b64encode(bytes.fromhex(value)).decode())
        # The library would compute a checksum of its own, over the one given.
        self._call(blob.upload_from_string, data, checksum=None if verify else "auto")

    def _download(self, bucket, key, range: str | None) -> requests.Response:
        url = f"{self.url}/download/storage/v1/b/{bucket}/o/{quote(key, safe='')}?alt=media"
        d = Download(url, headers={"Range": range} if range else {}, checksum=None)
        return self._call(d.consume, self.c._http)

    def get(self, bucket, key, range: str | None = None) -> Object:
        r = self._download(bucket, key, range)
        o = self.head(bucket, key)
        return Object(data=r.content, size=o.size, metadata=o.metadata, status=r.status_code,
                      content_range=r.headers.get("Content-Range"))

    def read(self, bucket, key, range: str | None = None) -> bytes:
        return self._download(bucket, key, range).content

    def head(self, bucket, key) -> Object:
        blob = self._blob(bucket, key)
        self._call(blob.reload)
        return Object(size=blob.size, metadata=blob.metadata or {})

    def delete(self, bucket, key):
        self._call(self._blob(bucket, key).delete)

    def copy(self, bucket, key, src_key, metadata: dict | None = None, must_not_exist: bool = False):
        """With rewriteTo, as ScyllaDB copies."""
        dst = self._blob(bucket, key)
        if metadata is not None:
            dst.metadata = metadata
        self._call(dst.rewrite, self._blob(bucket, src_key), if_generation_match=0 if must_not_exist else None)

    def list(self, bucket, prefix="", start_after="", max_keys=None) -> tuple[list[dict], bool]:
        # startOffset is where to start from, the key included.
        it = self.c.list_blobs(bucket, prefix=prefix or None, start_offset=start_after + "\0" if start_after else None,
                               page_size=max_keys)
        page = self._call(lambda: next(it.pages))
        return [{"key": b.name, "size": b.size} for b in page], it.next_page_token is not None

    def list_keys(self, bucket, prefix="", page=1000) -> list[str]:
        return self._call(lambda: [b.name for b in self.c.list_blobs(bucket, prefix=prefix or None, page_size=page)])

    # uploads

    def _upload_url(self, bucket, key, upload: str) -> str:
        url = (f"{self.url}/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
               f"&name={quote(key, safe='')}&upload_id={quote(upload, safe='')}")
        if upload in self.must_not_exist:
            url += "&ifGenerationMatch=0"
        return url

    def _send(self, method, bucket, key, upload: str, data: bytes = b"", content_range: str | None = None):
        headers = {"Content-Range": content_range} if content_range else {}
        r = requests.request(method, self._upload_url(bucket, key, upload), data=data, headers=headers,
                             allow_redirects=False)
        if r.status_code >= 400 and r.status_code != 499:
            raise gcs_error(r)
        return r

    def open_upload(self, bucket, key, metadata: dict | None = None, must_not_exist: bool = False) -> str:
        blob = self._blob(bucket, key)
        blob.metadata = metadata
        url = self._call(blob.create_resumable_upload_session, if_generation_match=0 if must_not_exist else None)
        upload = dict(p.split("=", 1) for p in url.split("?", 1)[1].split("&"))["upload_id"]
        if must_not_exist:
            self.must_not_exist.add(upload)
        return upload

    def upload_part(self, bucket, key, upload: str, number: int, data: bytes):
        """Sends the part before, now that it is not the last: only the last
        chunk may be of any size."""
        sent, pending = self.parts.setdefault((bucket, key, upload), ([], []))
        assert number == len(sent) + len(pending) + 1, "the parts of a resumable upload come in order"
        self._held(bucket, key, upload)
        if pending:
            first = sum(sent)
            self._send("PUT", bucket, key, upload, pending[0], f"bytes {first}-{first + len(pending[0]) - 1}/*")
            sent.append(len(pending.pop()))
        pending.append(data)

    def _held(self, bucket, key, upload: str) -> int:
        """What the server holds of the upload, which must be there."""
        r = self._send("PUT", bucket, key, upload, content_range="bytes */*")
        return int(r.headers["Range"].split("-")[1]) + 1 if "Range" in r.headers else 0

    def list_parts(self, bucket, key, upload: str) -> list[tuple[int, int]]:
        sent, pending = self.parts.get((bucket, key, upload), ([], []))
        assert self._held(bucket, key, upload) == sum(sent)
        return [(n, size) for n, size in enumerate(sent + [len(d) for d in pending], 1)]

    def complete(self, bucket, key, upload: str, parts: list[int], must_not_exist: bool = False):
        """Returns the object's generation: what a completion of a completed
        upload replies with again. must_not_exist goes with open_upload()."""
        sent, pending = self.parts.get((bucket, key, upload), ([], []))
        assert parts == list(range(1, len(sent) + len(pending) + 1)), "a resumable upload completes with all its chunks"
        first = sum(sent)
        if pending and pending[0]:
            total = first + len(pending[0])
            r = self._send("PUT", bucket, key, upload, pending[0], f"bytes {first}-{total - 1}/{total}")
        else:
            r = self._send("PUT", bucket, key, upload, content_range=f"bytes */{first}")
        return r.json()["generation"]

    def abort(self, bucket, key, upload: str):
        self._send("DELETE", bucket, key, upload)


def make_client(facade: str, url: str):
    return {"plywood": PlywoodFacade, "s3": S3Facade, "gcs": GcsFacade}[facade](url)
