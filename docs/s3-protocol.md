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
| no such session | 404 | `NoSuchUpload` |
| no such part | 400 | `InvalidPart` |
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

The `ETag` of an object is S3's: the quoted hex MD5 of its data for
PutObject, and for a multipart upload the MD5 of the parts' MD5s followed by
`-<number of parts>`, whatever the number. Objects written without MD5s
(through another facade) get an opaque ETag.

Keys are up to 1024 bytes (`KeyTooLongError`). Keys ending with `/` are not
supported and are refused with `InvalidArgument`.

## Multipart uploads

| Operation | Request | Notes |
|---|---|---|
| CreateMultipartUpload | `POST /{bucket}/{key}?uploads` | `x-amz-meta-*`, `x-amz-checksum-algorithm`, `x-amz-checksum-type` (below) |
| UploadPart | `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` | part numbers 1 to 10000; checksums as for PutObject; the reply's `ETag` is the part's MD5 |
| CompleteMultipartUpload | `POST /{bucket}/{key}?uploadId={id}` | `If-None-Match: *`, `x-amz-mp-object-size`, `x-amz-checksum-<algorithm>` of the whole object |
| AbortMultipartUpload | `DELETE /{bucket}/{key}?uploadId={id}` | 204 |
| ListParts | `GET /{bucket}/{key}?uploadId={id}` | `max-parts`, `part-number-marker`; a part's `LastModified` is the epoch: the storage does not keep it |

An upload id is a storage session id. CompleteMultipartUpload follows S3's
rules: the parts are listed in ascending order (`InvalidPartOrder`), exist
with the listed ETags and checksums (`InvalidPart`), and all but the last
have at least 5 MiB (`EntityTooSmall`). Parts uploaded but not listed are
dropped. Completing a completed upload again gives the same reply.

## Checksums

Every object gets an MD5 (for its ETag) and, as S3 does by default, a
CRC64NVME. Its S3 checksum algorithm is the one asked for at upload, or
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
  object's checksum, unless only a range is asked for.
- A multipart upload's checksum is of S3's default type for its algorithm:
  `COMPOSITE` (the checksum of the parts' checksums, followed by
  `-<number of parts>`) for CRC32, CRC32C, SHA1 and SHA256, and
  `FULL_OBJECT` (of the whole data) for CRC64NVME. Asking for `FULL_OBJECT`
  with CRC32 or CRC32C fails with `NotImplemented`.
- A part's checksum must be of its upload's algorithm, unless the upload has
  the default one: then a part's checksum of another algorithm is verified,
  but not kept.

## Listing

ListObjects (V1) and ListObjectsV2 support `prefix`, `delimiter`
(`CommonPrefixes`), `max-keys` (up to 1000), `encoding-type=url`, and
`marker` (V1) or `start-after` and `continuation-token` (V2). Keys are listed
in byte order. Entries have `Key`, `LastModified`, `ETag`, `Size`,
`StorageClass` (`STANDARD`), and `Owner` in V1 or with `fetch-owner=true`.

## Not supported yet

- Authentication (SigV4) and aws-chunked request bodies.
- CopyObject and UploadPartCopy.
- ListMultipartUploads: the storage does not list sessions.
- Object tagging.
- DeleteObjects, versioning and every other bucket sub-resource.
- Conditional reads (`If-Match`, `If-None-Match`, `If-Modified-Since`,
  `If-Unmodified-Since`). boto3's `download_file` sends `If-Match` with its
  ranged reads, so it fails.
- Conditional writes other than `If-None-Match: *`, `response-*` overrides,
  `partNumber` reads.
- Stored system metadata: `Content-Type`, `Cache-Control` and the like
  given on PutObject are ignored.
