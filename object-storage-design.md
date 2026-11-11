# S3- and GCS-compatible object storage: top-level design

This document is the starting point for developing a standalone object storage
server that speaks both the S3 and the GCS protocols. Its first user is the
ScyllaDB test suite, where it replaces the Versity S3 gateway and
fake-gcs-server, but it is meant to be usable as a generic object store.

Each item is marked **Decided** (agreed in the design discussion) or
**Proposed** (suggested, not yet confirmed). Statements about ScyllaDB come
from reading its source and carry file references relative to the scylla
repository. Statements about how real S3 and GCS behave come from general
knowledge of those services and were not checked against their current
documentation.

## 1. Goals

- **Generic.** A standalone object store, not a Scylla-specific mock.
  (Decided)
- **Unified.** One protocol-neutral core; S3 and GCS are facades that only
  translate. (Decided)
- **One bucket, both facades.** A bucket is reachable through the S3 and the
  GCS facade at the same time. (Decided)
- **Credentials enforced.** The storage verifies credentials itself and
  cooperates with a credentials provider, which may be a mock or a production
  one and may differ per facade. (Decided)
- **Fault injection built in.** The storage can inject faults by itself,
  through a plugin or an option. (Decided)
- **Pluggable storage backend.** The first backend is a simple posix one.
  (Decided)
- **Seastar.** The server is a Seastar application using Seastar I/O
  primitives. (Decided)
- **Restart durability.** Objects and in-progress uploads survive a server
  restart, recovered from filesystem state. (Decided)

## 2. Architecture

```
        S3 clients                         GCS clients
            |                                   |
   +------------------+               +------------------+
   |    S3 facade     |               |    GCS facade    |
   | SigV4 check      |               | bearer token /   |
   | XML, ETags,      |               | anonymous mode   |
   | multipart rules  |               | JSON, resumable  |
   +--------+---------+               | upload rules     |
            |                         +--------+---------+
            |    S3 credentials                |    GCS credentials
            |    provider (plug)               |    provider (plug)
            +----------------+-----------------+
                             |
                +------------+-------------+
                | fault injection (plug)   |   same interface as the core,
                +------------+-------------+   can fail before or after apply
                             |
                +------------+-------------+
                |           core           |   buckets, identity checks,
                |                          |   preconditions, neutral errors
                +------------+-------------+
                             |
                +------------+-------------+
                |  storage backend (plug)  |   sessions, parts, objects,
                |  first: "dull posix"     |   listing, durability
                +--------------------------+
```

Responsibilities:

- **Facade:** wire protocol, authentication, protocol-specific limits and
  error rendering. Holds no durable state of its own.
- **Core:** thin. Bucket and identity checks, preconditions, neutral error
  codes, and the seat for fault injection.
- **Backend:** all durable state.

## 3. What ScyllaDB needs from each facade

This is the minimum scope for the facades. Sources:
`utils/s3/client.cc`, `utils/gcp/object_storage.cc`,
`sstables/object_storage_client.cc`, `sstables/storage.cc`, and the test
harness under `test/pylib/`.

### 3.1 S3 facade

All requests are path-style (`/{bucket}/{key}`), over HTTP or HTTPS.

| Operation | Details Scylla relies on |
|---|---|
| PutObject | `x-amz-meta-*` user metadata; `ETag` in the reply; zero-length bodies; replaces an existing key |
| GetObject | Whole object (200) and `Range` (206). A ranged reply must carry a correct `Content-Range`, otherwise Scylla fails the read |
| HeadObject | 200/404; `Content-Length`, `Last-Modified`, `ETag`, `x-amz-meta-*` |
| DeleteObject | 204; 404 is accepted as "already gone" |
| CopyObject | `x-amz-copy-source`, `x-amz-metadata-directive: REPLACE`, `x-amz-tagging` |
| CreateMultipartUpload | `POST ?uploads`, metadata and `x-amz-tagging`; `UploadId` in XML reply |
| UploadPart | `PUT ?partNumber=&uploadId=`; `ETag` header in the reply |
| UploadPartCopy | Same with `x-amz-copy-source`, with and without `x-amz-copy-source-range`; `CopyPartResult/ETag` in the body |
| CompleteMultipartUpload | `POST ?uploadId=` with the part list; `CompleteMultipartUploadResult/ETag`; Scylla also parses an `<Error>` inside a 200 reply |
| AbortMultipartUpload | `DELETE ?uploadId=` → 204 |
| ListObjectsV2 | `list-type=2`, `prefix`, `max-keys`, `continuation-token`; Scylla reads `Contents/Key`, `IsTruncated`, `NextContinuationToken`; no delimiter is sent |
| Object tagging | `GET/PUT/DELETE ?tagging`. Production code only sets the `x-amz-tagging` header; the standalone calls are used by tests |
| Bucket create/delete | `PUT /bucket` → 200, `DELETE /bucket` → 204 |

Errors are XML `<Error><Code><Message>`. Codes that change Scylla's
behaviour: `NoSuchKey`, `NoSuchBucket`, `NoSuchUpload` (mapped to ENOENT),
`AccessDenied` (EACCES), `SlowDown` and HTTP 429/503 (throttling backoff),
`RequestTimeTooSkewed` (re-sign and retry), `ExpiredToken` (drop credentials,
re-obtain, retry).

Limits Scylla assumes: 5 MiB minimum part, 5 GiB maximum part, 10,000 parts
per upload (`utils/s3/client.hh:37-44`).

How Scylla uploads:

- **Plain sink:** parts of 5 MiB or more uploaded in parallel in the
  background, then Complete; Abort on failure.
- **Jumbo sink:** uploads intermediate objects, stitches them into the final
  object with whole-object UploadPartCopy, then deletes them
  (`utils/s3/client.cc:1596` onwards).
- **Empty object:** falls back to a plain PUT (`utils/s3/client.cc:1110`).
- **Large copy:** objects over 5 GiB are copied with ranged UploadPartCopy.

### 3.2 GCS facade

JSON API under `/storage/v1/...`.

| Operation | Details Scylla relies on |
|---|---|
| Initiate resumable upload | `POST /upload/storage/v1/b/{b}/o?uploadType=resumable&name=`, JSON body with `metadata`; session URI in `Location` |
| Upload chunk | `PUT` to the session URI with `Content-Range: bytes a-b/total`, `bytes a-b/*`, or `bytes */total` to finalize with no data; 308 with `Range: bytes=0-N` while incomplete, 200/201 when done |
| Status query | `PUT` with `Content-Range: bytes */*` → 308 with `Range`, or 200/201 |
| Cancel upload | `DELETE` on the session URI → 499 |
| Get object resource | `GET /storage/v1/b/{b}/o/{name}`; Scylla reads `name`, `size`, `generation`, `metadata` |
| Get object data | `...?alt=media&ifGenerationMatch=` with `Range`; 200 or 206 |
| List | `GET /storage/v1/b/{b}/o` with `prefix`, `maxResults`, `pageToken`; `nextPageToken` in the reply |
| Delete | `DELETE /storage/v1/b/{b}/o/{name}` → 200/204 |
| Copy | `POST .../rewriteTo/b/{b}/o/{name}` with the `rewriteToken` loop and a metadata body |
| Compose | `POST .../compose` with `sourceObjects` and `destination` |
| Bucket create/delete | `POST /storage/v1/b?project=`, `DELETE /storage/v1/b/{b}` |

The client also implements `moveTo`; whether anything calls it was not
checked.

How Scylla uploads:

- **Sink:** sequential chunks, flushed at 8 MiB and aligned to 256 KiB
  (`utils/gcp/object_storage.cc:41-42`, `:189`). On a 308 whose `Range` is
  behind, it resends from the reported offset (`:897` onwards).
- **Empty object:** finalizes a session with `bytes */0` and no data.
- **File upload (backup):** uploads `{name}-temp-{off}-{end}` objects in
  parallel, merges them with `compose`, then deletes them
  (`sstables/object_storage_client.cc:385-432`).

The GCS facade must validate `Content-Range` strictly. fake-gcs-server does
not, which hid a real bug; the tests currently compensate with a validating
proxy (`test/pylib/gcs_upload_validator.py`), which is a good reference for
the rules.

### 3.3 Semantics both facades must provide

- **Flat key namespace.** Deleting the last object under a prefix must not
  affect later writes under that prefix. The Versity gateway breaks this
  (SCYLLADB-5060); the harness works around it by creating a placeholder
  object named `sstables/`.
- **Zero-length objects.** Scylla's `refs/nodes/{host}/{gen}` reference
  markers are empty.
- **Replace on write.** Writing to an existing key replaces the object
  atomically. Proven only by `test_object_reupload`
  (`test/boost/s3_test.cc:974`), which does it with plain PUT, multipart and
  the jumbo sink; whether production paths rely on it was not checked.
- **Listing consistent after write.** Sstable deletion lists
  `{prefix}/{sstable_id}/refs/` to count references. This is an inference
  from the code, not a tested requirement.

### 3.4 What the test harness does directly

- **boto3 against the S3 endpoint:** create/delete bucket, PutObject,
  GetObject, HeadObject, `upload_file`, paginated ListObjectsV2, bulk
  DeleteObjects.
- **PutBucketPolicy:** one alternator export test, which tolerates failure.
- **boto3 against the GCS endpoint:** the harness empties GCS buckets through
  the S3 XML API. With one bucket visible through both facades, this can go
  through the S3 facade.
- **GCS bucket management:** through the JSON API with `requests`.

## 4. Credentials

### 4.1 How Scylla authenticates to S3

Scylla configures an identity per endpoint, never permissions on a bucket.
The endpoint config (`object_storage_endpoints`) has `name` (URL),
`aws_region` and `iam_role_arn`; there is no key pair in it. Credentials come
from a fixed chain, first source that yields a key wins
(`utils/s3/client.cc:120-123`):

1. Environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`.
2. EC2 instance metadata at `169.254.169.254`.
3. STS AssumeRole at `sts.<aws_region>.amazonaws.com` with `iam_role_arn`.

Every request is then signed with SigV4 header authentication:

```
Authorization: AWS4-HMAC-SHA256 Credential=<key id>/<date>/<region>/s3/aws4_request,
               SignedHeaders=..., Signature=...
x-amz-date: <now>
x-amz-content-sha256: UNSIGNED-PAYLOAD
x-amz-security-token: <session token>      (only with temporary credentials)
```

Sources 2 and 3 have fixed AWS addresses and cannot be pointed at a custom
issuer through configuration. They are tested only against a separate mock
(`test/pylib/s3_server_mock.py`) and never reach the storage.

### 4.2 S3 facade (Decided)

- The storage is given one or more access-key/secret pairs and the region
  name clients sign for.
- Only the S3 facade decodes and verifies signatures. The core receives at
  most an authenticated identity.
- Verification needs the request exactly as sent (method, path, query, signed
  headers) and a clock check that can answer `RequestTimeTooSkewed`.
- For the Scylla tests one root pair is enough: the harness generates it and
  hands it to the server, to Scylla and to boto3.

To check early: boto3 signs differently from Scylla. It sends a real payload
hash, and recent versions may use chunked uploads with trailing checksums.
The validator should be tested against the boto3 version in the Scylla
toolchain.

### 4.3 GCS facade

- In the tests Scylla runs with `credentials_file: none`, builds the client
  without credentials and sends no `Authorization` header
  (`utils/gcp/object_storage.cc:553`).
- The facade starts with an "accept anonymous" mode so that today's tests run
  unchanged. (Proposed)
- With real credentials Scylla sends `Authorization: Bearer <token>` and, on
  a 401, refreshes the token and retries. Enforcing this needs a token issuer
  and a harness change to give Scylla a credentials file.
- Of Scylla's token flows, only the service-account one (honours `token_uri`
  from the credentials file) and the metadata-server one (overridable host)
  can be pointed at a non-Google issuer. The user refresh-token flow is
  hard-wired to `oauth2.googleapis.com` (`utils/gcp/gcp_credentials.cc`).

### 4.4 Parked for later discussion

- Whether the credentials provider also issues credentials (STS, instance
  metadata, OAuth token endpoint) or only validates them.
- The authorization model beyond "any authenticated identity may do
  anything". Nothing in Scylla or its tests exercises per-bucket or
  per-object permissions.

## 5. Object model in the core

The core model is the superset of both protocols. (Proposed)

| Capability | S3 facade | GCS facade |
|---|---|---|
| Buckets, flat keys, zero-length objects, replace on write | yes | yes |
| Size, modification time, user metadata | `x-amz-meta-*`, `Last-Modified` | `metadata`, `size` |
| Facade-private attributes (opaque to the core) | `ETag` | none needed by Scylla |
| Per-object generation counter | not used by Scylla | `generation`, `ifGenerationMatch` |
| Object tags | `x-amz-tagging`, `?tagging` | not used |
| Ranged read | `Range` → 206 + `Content-Range` | same, with `alt=media` |
| Prefix listing with a resume point | ListObjectsV2 | `pageToken` |
| Conditional write | `If-None-Match` / `If-Match` | `ifGenerationMatch` (0 = must not exist) |

Real S3 and GCS both replace atomically on overwrite and both offer opt-in
create-only writes; Scylla sends no such precondition on writes.

## 6. Uploads

### 6.1 Why one model fits both

A GCS resumable upload is a restricted case of an S3 multipart upload:

| | S3 multipart | GCS resumable |
|---|---|---|
| Data shape | Numbered parts | One linear byte stream |
| Ordering | Any order, parallel, a part can be re-sent | Strictly sequential |
| Size rule | Parts except the last ≥ 5 MiB | Chunks except the last multiples of 256 KiB |
| Finish | Explicit, with a part list | Implicit, when the declared total is reached |

So the backend stores an upload as a session of parts, each part a file
written through a Seastar file output stream, tied together at commit by a
meta "seal" file that enumerates them. Reading an object consults that file.
(Decided)

### 6.2 Backend upload API (Decided in outline; signatures Proposed)

```cpp
// One per upload. The id is durable before it is returned.
future<session_id> open_session(bucket, key, object_attrs);

// S3: number = the client's part number; a re-send replaces the earlier part.
// GCS: number = next sequence number.
// The part belongs to the session only once seal() has succeeded.
future<part_writer> write_part(session_id, unsigned number);

struct part_writer {
    output_stream<char>& out();                    // file output stream underneath
    uint64_t size() const;                         // bytes accepted so far
    future<part_info> seal(bytes facade_private);  // close and record the part
};

future<session_info> describe_session(session_id); // parts, sizes, private data

// Writes the seal file listing the parts in order, then atomically publishes
// it as the object.
future<object_info> commit_parts(session_id, std::vector<unsigned> numbers,
                                 precondition, bytes facade_private);

future<> abort_session(session_id);
```

### 6.3 How the facades drive it

**S3:**

- CreateMultipartUpload → `open_session`.
- UploadPart → `write_part(n)`, pipe the body, `seal(etag)`.
- UploadPartCopy → `write_part(n)` filled from the source object's range.
- ListParts → `describe_session`.
- CompleteMultipartUpload → check ETags and the 5 MiB rule with
  `describe_session`, then `commit_parts(listed numbers)`.
- AbortMultipartUpload → `abort_session`.
- PutObject and CopyObject → a one-part session committed immediately.

**GCS:**

- Initiate → `open_session`.
- Chunk PUT → write into the session's current part.
- Status query → sum of part sizes from the session state.
- Finalize → `commit_parts(all parts in order)`; a `bytes */0` finalize
  commits an empty part list and must produce an empty object.
- Cancel → `abort_session`.
- `rewriteTo` → a one-part session filled from the source.
- `compose` → one session with one part per source object.

**GCS part lifetime.** Every chunk is its own PUT request, so a part must not
be tied to a request body; that would create a file per chunk. The facade
keeps the `part_writer` in its session state across requests: (Proposed)

- **Body ends cleanly:** flush, keep the stream open, the next PUT continues
  the same part.
- **Body breaks midway, session idle too long, or server restart:** seal the
  part at the size it reached; the next PUT opens a new part.

This matches Seastar's file sink, which requires every write to start at a
disk-aligned position and pads and truncates only an unaligned tail on flush
(`seastar/src/core/fstream.cc:440-481`). A flush at a 256 KiB chunk boundary
is always aligned, so the stream stays usable; a broken body leaves an
unaligned tail, so that case must close the part.

### 6.4 Reading

- Every object has a seal file, including single-part ones, since user
  metadata, tags, generation and facade-private data need a home anyway.
- A ranged read picks the starting part by cumulative size and streams across
  part boundaries.
- After an overwrite or delete, a reader that has loaded the old seal file
  but not yet opened its parts would race with removal; a reference count on
  the loaded seal object is enough in a single process.

### 6.5 Future work

- A compaction module that squashes parts together. (Far future, Decided)
- Hard links for whole-part copies, so `compose` and jumbo-sink stitching
  need no data copy. (Proposed)

## 7. Posix backend

The first backend is deliberately simple: it works on individual files and
maps object names to filesystem names in the simplest way that works, even
1-1. (Decided)

### 7.1 Session layout and restart recovery (Proposed)

```
sessions/<session_id>/
    session.meta                  bucket, key, attributes, facade kind
    <number>.<unique>.tmp         part being written
    <number>.<unique>.<private>   sealed part; <private> = facade's opaque data, hex
    object.meta                   seal file, present only once commit has started
```

- The session id is the directory name; the S3 `UploadId` and the GCS session
  URI carry it, so a request finds its session after a restart without an
  index.
- `session.meta` is written and flushed before the id is returned.
- Sealing a part is: flush the file, rename `.tmp` to the final name, sync
  the directory. The size is the file size; the S3 ETag travels in the name.
- There is no per-session append-only log, because small appends hit the
  alignment restriction described in 6.3.

Restart scan, per session directory:

| Found | S3 session | GCS session |
|---|---|---|
| Sealed part | Kept | Kept |
| `.tmp` part | Deleted; never acknowledged, the client retries | Sealed at its size rounded down to 256 KiB; the client queries status and resumes |
| Same number twice | Newest sealed one wins | cannot happen |
| `object.meta` present | Commit rolled forward | Commit rolled forward |
| No `session.meta` | Directory removed | Directory removed |

The GCS rounding is safe because every acknowledged chunk ended on a 256 KiB
boundary and was flushed before the 308 reply. It also discards padding left
by a crash between the sink's padded tail write and its truncate.

Commit:

1. Write `object.meta` into the session directory and flush. Point of no
   return.
2. Publish the directory as the object's current content.
3. Delete parts the commit did not list.

A crash after step 1 is redone at startup. A retried commit of an already
published session must report success, since the client may not have seen the
first reply.

Guarantees: nothing acknowledged is lost on a process restart. The same holds
for power loss provided every acknowledgement follows a flush (the file
stream's `flush()` syncs the file) and every rename is followed by a
directory sync.

### 7.2 Name mapping: what the simple scheme must still handle

- **An object is a directory** holding `object.meta` and part files. Internal
  names need a reserved marker so they cannot collide with a key component.
- **Empty parent directories.** Removing a parent when its last object goes
  races with a concurrent write under the same prefix. Either never remove
  parents or retry the create.
- **Keys ending in a slash.** The harness creates a placeholder object named
  `sstables/`. Either accept such keys or drop that workaround from the
  harness for this storage.

### 7.3 Documented limits of this backend

- **Long keys:** a path component is limited to 255 bytes; S3 allows
  1024-byte keys. Scylla's keys are short.
- **Key `a` together with key `a/b`:** works only if the object marker is a
  suffix on the directory name. Scylla was not found to do this, based on the
  key formats in `sstables/object_storage_client.cc` and
  `sstables/storage.cc`.
- **Sorted listing:** S3 and GCS return keys in byte order across the bucket.
  A directory walk can produce it by sorting each directory's entries with
  `/` appended to subdirectory names. Whether any Scylla consumer depends on
  the order was not checked.

These are lifted by a different backend, not by complicating this one.

## 8. Fault injection

The injector sits between the facades and the core and exposes the same
interface, so it can fail a call before or after forwarding it. (Proposed
placement; the capability itself is Decided.)

The existing S3 proxy (`test/pylib/s3_proxy.py`) defines the minimum to
reproduce:

- **Failure kinds:** a retryable HTTP status with a named S3 error code; an
  error embedded in a 200 reply for copy, part upload and complete; a
  connection reset instead of a reply.
- **Fail after apply:** the request is executed and failure is still
  reported, so the client retries something that already happened.
- **Bounded per-resource policy:** fail a given path N times, then succeed.
- **Expired token:** injected as an error.

Rendering an injected fault (status code, error name, reset) is facade work,
so the injector needs a way to tell the facade which fault to render.

The GCS boost tests also cover stalled uploads, a session falling behind and
unacknowledged chunks (`test/lib/gcs_fixture.cc`,
`test/boost/gcp_object_storage_test.cc`). How they inject those was not read;
the GCS injection points are still open.

## 9. Scylla test integration: reference points

| What | Where |
|---|---|
| S3 server wrapper (Versity gateway, root key pair, env variables) | `test/pylib/s3mock_server.py` |
| GCS server wrapper (fake-gcs-server, validator) | `test/pylib/object_storage.py` |
| S3 fault-injection proxy | `test/pylib/s3_proxy.py`, `test/pylib/start_s3_proxy.py` |
| STS / instance-metadata mock | `test/pylib/s3_server_mock.py` |
| GCS upload validator | `test/pylib/gcs_upload_validator.py` |
| S3 client tests | `test/boost/s3_test.cc` |
| GCS client tests | `test/boost/gcp_object_storage_test.cc`, `test/lib/gcs_fixture.cc` |
| Common upload tests | `test/boost/object_storage_upload_test.cc` |
| Cluster tests | `test/cluster/object_store/` |
| Alternator export/import | `test/alternator/test_export.py`, `test/alternator/test_import.py` |

Environment variables the harness publishes: `S3_SERVER_ADDRESS_FOR_TEST`,
`S3_SERVER_PORT_FOR_TEST`, `S3_BUCKET_FOR_TEST`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `GS_SERVER_ADDRESS_FOR_TEST`, `GS_BUCKET_FOR_TEST`,
`GS_CREDENTIALS_FILE`.

## 10. Open questions

1. Credentials provider scope: validation only, or issuance too (section
   4.4).
2. Authorization model for a generic store (section 4.4).
3. GCS credential enforcement: token issuer and harness changes (section
   4.3).
4. GCS fault-injection points (section 8).
5. Whether a session's later calls must come from the identity that opened
   it.
6. Session expiry: S3 keeps sessions until aborted, GCS expires them after
   about a week.
7. boto3 signing and checksum modes the S3 facade must accept (section 4.2).
8. Whether any Scylla production path relies on replace-on-write or on
   listing order (sections 3.3, 7.3).
