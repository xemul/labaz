# The GCS protocol

`--facade gcs:http=<port>[,https=<port>]`

The Google Cloud Storage JSON API: `/storage/v1/...`, with uploads at
`/upload/storage/v1/...` and downloads at `/download/storage/v1/...` as
well. The goal is to behave as GCS does; this page lists what is supported
so far and where Labaz differs. A request for something that is not
supported fails with `501` and the reason `notImplemented`, rather than being
served differently from GCS.

Requests are not authenticated: the `Authorization` header is not checked.

Errors are GCS's JSON documents:

```json
{"error": {"code": 404, "message": "No such object: bkt/key",
           "errors": [{"message": "No such object: bkt/key", "domain": "global", "reason": "notFound"}]}}
```

| Storage error | Status | Reason |
|---|---|---|
| no such bucket, key or upload session | 404 | `notFound` |
| bucket exists, bucket not empty | 409 | `conflict` |
| precondition failed | 412 | `conditionNotMet` |
| invalid argument, checksum mismatch | 400 | `invalid` |
| invalid range | 416 | `requestedRangeNotSatisfiable` |

## Buckets

| Operation | Request | Notes |
|---|---|---|
| insert | `POST /storage/v1/b?project=...` | `project` is required and ignored; of the bucket resource only `name` is used. GCS's bucket naming rules apply |
| get | `GET /storage/v1/b/{bucket}` | |
| list | `GET /storage/v1/b?project=...` | `prefix`, `maxResults`, `pageToken` |
| delete | `DELETE /storage/v1/b/{bucket}` | `conflict` also while an upload to it is in progress |

A bucket resource has a `location` of `US`, a `storageClass` of `STANDARD`,
and the epoch as `timeCreated`: the storage does not keep any of them.

## Objects

| Operation | Request | Notes |
|---|---|---|
| get | `GET /storage/v1/b/{bucket}/o/{object}` | the object resource, or with `alt=media` the data |
| get (data) | `GET /download/storage/v1/b/{bucket}/o/{object}?alt=media` | |
| list | `GET /storage/v1/b/{bucket}/o` | see below |
| delete | `DELETE /storage/v1/b/{bucket}/o/{object}` | 204, or `notFound` |
| insert | `POST /upload/storage/v1/b/{bucket}/o?uploadType=...` | see below |
| rewrite | `POST /storage/v1/b/{bucket}/o/{object}/rewriteTo/b/{bucket}/o/{object}` | see below |
| copy | `POST /storage/v1/b/{bucket}/o/{object}/copyTo/b/{bucket}/o/{object}` | see below |
| compose | `POST /storage/v1/b/{bucket}/o/{object}/compose` | see below |

Object names are a single URL-encoded path segment (`a%2Fb`). They are up to
1024 bytes, may not be `.` or `..` or contain line breaks, as GCS's rules
say, and may not end with `/`, which Labaz does not support (`invalid`).

The object resource has `name`, `bucket`, `generation`, `metageneration`,
`size`, `md5Hash`, `crc32c`, `etag`, `timeCreated`, `updated`, `metadata`,
`selfLink` and `mediaLink`.

- Objects are never changed in place, so `metageneration` is always 1.
- `contentType` is always `application/octet-stream`: given ones, and the
  other system metadata (`cacheControl`, `contentEncoding`, ...), are
  ignored.
- Uploaded and copied objects have an MD5 and a CRC32C. An object of several
  parts (uploaded in several chunks, or composed) has no `md5Hash`, as GCS's
  composite objects have none: the storage computes checksums part by part,
  and an MD5 does not combine. Objects written through other facades have
  the checksums they were written with.
- There are no old versions: `generation` names the current one or nothing
  (`notFound`).

Reads take `ifGenerationMatch` and `ifMetagenerationMatch` (`412` when they
do not hold) and `ifGenerationNotMatch` and `ifMetagenerationNotMatch` (`304`
with no body). Data reads take a single `Range: bytes=...` (`206` with
`Content-Range`; an unparsable one is ignored) and reply with
`x-goog-generation`, `x-goog-metageneration`,
`x-goog-stored-content-length` and, for the whole object, `x-goog-hash`.

Listing supports `prefix`, `delimiter` (`prefixes`), `maxResults` (up to
1000), `pageToken` and `startOffset`. Names are listed in byte order. `items`
and `prefixes` are left out when empty, as GCS does.

## Uploads

Every upload takes `ifGenerationMatch`, which is checked when the object is
published; `0` means the object must not exist.

- `uploadType=media`: the body is the data, the name is in `name`.
- `uploadType=multipart`: a `multipart/related` body of the object resource
  (`name`, `metadata`) and the data, which is read into memory. The
  resource's `md5Hash` and `crc32c`, if any, are verified (`invalid`).
- `uploadType=resumable`: starts a resumable upload of the object resource
  in the body (if any) and replies with its session URI in `Location`: the
  same path with `upload_id`, and `name` and `ifGenerationMatch`, so that
  the URI has all the session needs. A session is a storage session, kept
  in memory: sessions do not survive a restart.

Chunks of a resumable upload are `PUT` to the session URI with a
`Content-Range`, which follows GCS's rules (`invalid` otherwise):

- `bytes <first>-<last>/*` sends a chunk, which must start where the session
  is, have the declared length, and, unless it is the last one, be a
  multiple of 256 KiB.
- `bytes <first>-<last>/<total>` sends a chunk too, and completes the upload
  when it reaches the total; no byte may be at or past it.
- `bytes */<total>` completes the upload, which must hold `total` bytes.
  `bytes */0` makes an empty object.
- `bytes */*` asks where the upload is.
- A `PUT` without a `Content-Range` sends all of the data at once.

An incomplete upload is answered with `308` and, once it holds any data, a
`Range: bytes=0-<last>`. Completing it publishes the object and replies with
its resource; asking again, or sending the last chunk again, gets the same
resource. `DELETE` on the session URI cancels the upload with `499`.

## Copies

`rewriteTo` and `copyTo` copy an object of the same bucket; another bucket
fails with `notImplemented`.

- The copy takes the `metadata` of the request's object resource, or else
  the source's. Its checksums are those of the data, so a copy of a
  composite object has an MD5 again.
- `ifSourceGenerationMatch`, `ifSourceGenerationNotMatch`,
  `ifSourceMetagenerationMatch` and `ifSourceMetagenerationNotMatch` fail the
  copy with `412` when they do not hold; `sourceGeneration` other than the
  current one with `notFound`. If the source changes after they were
  checked, they are checked again against its new version.
- `ifGenerationMatch` applies to the destination.
- `rewriteTo` copies all at once: its reply is `done`, with the resource,
  and has no `rewriteToken`. A request with one is `invalid`.

`compose` makes a composite object of up to 32 objects of the bucket, in
order; a source may be listed more than once, and may be the destination.
A source's `generation` other than its current one gives `notFound`, and its
`objectPreconditions.ifGenerationMatch` not holding gives `412`. Of the
`destination` resource, only `metadata` is used. The object has a CRC32C,
combined from the sources', and no MD5.

## Not supported yet

- `moveTo`, and copying to another bucket.
- Authentication.
- Checksums given with a copy.
- Patching objects or buckets, object versions, ACLs, `endOffset`,
  `matchGlob` and `versions` in listings, preconditions on deletes, and the
  preconditions other than `ifGenerationMatch` on uploads.
- Checksums given with a resumable upload.
- Stored system metadata: `contentType` and the like are ignored.
- The XML API.
