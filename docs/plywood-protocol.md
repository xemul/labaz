# The plywood protocol

`--facade plywood:http=<port>[,https=<port>]`

A testing-only HTTP/JSON protocol that maps 1:1 to the storage API
(`src/storage.hh`). It follows that API as it evolves and makes no attempt at
compatibility with anything.

An object is addressed as `/{bucket}/{key}`; the key may contain `/` and is
percent-decoded. Keys ending with `/` are not supported and are refused.

Errors are returned as `{"error": "<code>", "message": "..."}` where the code
is one of the storage error codes or `bad_request`:

| Status | Codes |
|---|---|
| 400 | `invalid_argument`, `bad_request`, `bad_digest` (the body does not match its `X-Labaz-Checksum-<algorithm>`) |
| 404 | `no_such_bucket`, `no_such_key`, `no_such_session`, `no_such_part` |
| 409 | `bucket_exists`, `bucket_not_empty` |
| 412 | `precondition_failed` |
| 416 | `invalid_range` |

A request that matches no route gets Seastar's own 404 reply.

## Buckets

| Request | Reply |
|---|---|
| `GET /` | `{"buckets": [names]}` |
| `PUT /{bucket}` | 200 |
| `DELETE /{bucket}` | 204; `bucket_not_empty` while it has objects or sessions |
| `GET /{bucket}?prefix=&start-after=&max-keys=` | `{"objects": [object info], "truncated": bool}`, keys in byte order; `max-keys` defaults to 1000 |

## Objects

An object info is
`{"key", "size", "generation", "mtime_ns", "metadata": {}, "checksums": {}, "parts": [part info]}`,
and a part info is `{"number", "size", "checksums": {}}`.

| Request | Reply |
|---|---|
| `PUT /{bucket}/{key}` | object info. The body is the data. Optional headers: `X-Labaz-Meta-<name>` (user metadata; names are lower-cased), `X-Labaz-If-Generation-Match` (0: must not exist), `X-Labaz-Checksums`, `X-Labaz-Checksum-<algorithm>` (see below) |
| `GET /{bucket}/{key}` | the data; 206 with `Content-Range` if a `Range: bytes=...` header was given (single range only). Headers `X-Labaz-Size`, `X-Labaz-Generation`, `X-Labaz-Meta-<name>`, `X-Labaz-Checksum-<algorithm>` |
| `HEAD /{bucket}/{key}` | the same headers, no body |
| `DELETE /{bucket}/{key}` | 204 |

A `PUT` is a one-part session committed at once.

## Upload sessions

A session belongs to one object; using its id with another object's path is
`no_such_session`. An object can have several sessions at a time.

| Request | Reply |
|---|---|
| `POST /{bucket}/{key}` | `{"session": id}`. Optional `X-Labaz-Meta-<name>` headers |
| `PUT /{bucket}/{key}?session={id}&part={n}` | `{"number", "size"}`. The body is the part's data. A part with the same number is replaced |
| `GET /{bucket}/{key}?session={id}` | `{"session", "bucket", "key", "metadata", "parts": [part info]}` |
| `PATCH /{bucket}/{key}?session={id}[&parts={n},{n},...]` | object info. Commits all sealed parts in number order, or the listed ones in the listed order (an empty list makes an empty object); the others are dropped. No body. Optional `X-Labaz-If-Generation-Match`. Committing again returns the same reply |
| `DELETE /{bucket}/{key}?session={id}` | 204 |

`part` is only valid together with `session`, on `PUT`; anywhere else, and a
`PUT ?session` without it, is `bad_request`.

## Checksums

Algorithms: `crc32` (zlib's), `crc32c`, `crc64nvme`, `md5`, `sha1`, `sha256`.
Values are hex; CRCs are big-endian.

- `X-Labaz-Checksums: <algorithm>,...` on `POST` (open a session) and on a
  `PUT` of an object (including a copy) selects the checksums the storage
  computes for every part. The session description lists them.
- A part info has its part's checksums.
- An object info's `checksums` (and the `X-Labaz-Checksum-<algorithm>`
  headers of `GET` and `HEAD`) are of the object's data as a whole, as far as
  they derive from its parts' ones: those of its only part, those of no data
  for an object of no parts, and otherwise the CRCs that every part has,
  combined. MD5 and SHA of an object of several parts are not reported.
- A copied part has the checksums of its data like a written one. Those
  that derive from the source's parts (when copying the whole source) are
  taken from them, the rest are computed by reading the copied data.
- `X-Labaz-Checksum-<algorithm>: <hex>` on a `PUT` with a body (an object or
  a part) is verified against the body, which is refused with `bad_digest`
  if it does not match. It does not make the storage keep that checksum. A
  copy has no body, so these headers are `bad_request` on it.

## Copy

A `PUT` of an object or of a session part can take its data from another
object of the same bucket instead of the body:

| Request | Reply |
|---|---|
| `PUT /{bucket}/{key}?copy-key={src}[&copy-range=bytes=a-b][&copy-if-generation-match={g}]` | object info. The source's data (or the range of it) becomes the object. Metadata and the destination's `X-Labaz-If-Generation-Match` come from the headers, as for a plain `PUT` |
| `PUT /{bucket}/{key}?session={id}&part={n}&copy-key={src}[&copy-range=...][&copy-if-generation-match={g}]` | `{"number", "size"}`. The source's data (or the range of it) becomes the part |

`copy-range` has the syntax of the `Range` header. `copy-if-generation-match`
is checked against the source. `copy-range` and `copy-if-generation-match`
require `copy-key`, which is only valid on `PUT`.

A test client is in `test/plywood_client.py`.
