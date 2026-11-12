# The S3 protocol

`--facade s3:http=<port>[,https=<port>]`

The Amazon S3 REST API, path-style (`/{bucket}/{key}`). The goal is to behave
as S3 does; this page lists what is supported so far and where Labaz differs.
A request for something that is not supported fails with
`501 NotImplemented`, rather than being served differently from S3.

Requests are not authenticated: the `Authorization` header and the
`x-amz-content-sha256` value are not checked, so any client credentials
work.

Errors are S3's XML documents, with S3's codes and messages:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Resource>/bkt/key</Resource><RequestId>0-1f</RequestId></Error>
```

Every reply has an `x-amz-request-id` header. Replies to `HEAD` have no body.

| Storage error | Status | Code |
|---|---|---|
| no such bucket | 404 | `NoSuchBucket` |
| no such key | 404 | `NoSuchKey` |
| bucket exists | 409 | `BucketAlreadyOwnedByYou` |
| bucket not empty | 409 | `BucketNotEmpty` |
| precondition failed | 412 | `PreconditionFailed` |
| invalid argument | 400 | `InvalidArgument` |
| invalid range | 416 | `InvalidRange` |
| checksum mismatch | 400 | `BadDigest` |

## Buckets

| Operation | Request | Notes |
|---|---|---|
| ListBuckets | `GET /` | `CreationDate` is the epoch: the storage does not keep it |
| CreateBucket | `PUT /{bucket}` | S3's bucket naming rules apply (`InvalidBucketName`); the `CreateBucketConfiguration` body is ignored |
| HeadBucket | `HEAD /{bucket}` | |
| DeleteBucket | `DELETE /{bucket}` | `BucketNotEmpty` also while an upload to it is in progress |
| ListObjects | `GET /{bucket}` | see below |
| ListObjectsV2 | `GET /{bucket}?list-type=2` | see below |

## Objects

| Operation | Request | Notes |
|---|---|---|
| PutObject | `PUT /{bucket}/{key}` | `x-amz-meta-*`, checksums (below), `If-None-Match: *` |
| GetObject | `GET /{bucket}/{key}` | a single `Range: bytes=...`; an unparsable `Range` is ignored, as S3 does |
| HeadObject | `HEAD /{bucket}/{key}` | the same headers as GetObject, `Range` included |
| DeleteObject | `DELETE /{bucket}/{key}` | 204, also if the key does not exist |

GetObject and HeadObject reply with `Content-Length`, `ETag`, `Last-Modified`,
`Accept-Ranges: bytes`, `Content-Type: binary/octet-stream` and the
`x-amz-meta-*` headers.

The `ETag` of an object is S3's: the quoted hex MD5 of its data, or for an
object of several parts, the MD5 of the parts' MD5s followed by `-<parts>`.
Objects written without an MD5 (through another facade) get an opaque ETag.

Keys are up to 1024 bytes (`KeyTooLongError`). Keys ending with `/` are not
supported and are refused with `InvalidArgument`.

## Checksums

Every object gets an MD5 (for its ETag) and, as S3 does by default, a
CRC64NVME.

- `x-amz-checksum-<algorithm>: <base64>` on PutObject, for `crc32`, `crc32c`,
  `crc64nvme`, `sha1` or `sha256`, is verified against the body and the
  checksum is kept. At most one such header is allowed.
- `x-amz-sdk-checksum-algorithm: <ALGORITHM>` makes the object keep that
  checksum.
- `Content-MD5: <base64>` is verified against the body.
- The PutObject reply has the `x-amz-checksum-<algorithm>` of the algorithm
  asked for, or of `crc64nvme` if none was, and `x-amz-checksum-type:
  FULL_OBJECT`.
- GetObject and HeadObject with `x-amz-checksum-mode: ENABLED` reply with the
  checksums the object keeps (all of them, not only the one asked for at
  upload), unless only a range is asked for.

## Listing

ListObjects (V1) and ListObjectsV2 support `prefix`, `delimiter`
(`CommonPrefixes`), `max-keys` (up to 1000), `encoding-type=url`, and
`marker` (V1) or `start-after` and `continuation-token` (V2). Keys are listed
in byte order. Entries have `Key`, `LastModified`, `ETag`, `Size`,
`StorageClass` (`STANDARD`), and `Owner` in V1 or with `fetch-owner=true`.

## Not supported yet

- Authentication (SigV4) and aws-chunked request bodies.
- Multipart uploads, CopyObject and UploadPartCopy.
- Object tagging.
- DeleteObjects, versioning and every other bucket sub-resource.
- Conditional reads (`If-Match`, `If-None-Match`, `If-Modified-Since`,
  `If-Unmodified-Since`), conditional writes other than `If-None-Match: *`,
  `response-*` overrides, `partNumber` reads.
- Stored system metadata: `Content-Type`, `Cache-Control` and the like
  given on PutObject are ignored.
