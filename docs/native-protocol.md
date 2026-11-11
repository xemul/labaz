# The native protocol

`--facade native:http=<port>[,https=<port>]`

A testing-only HTTP/JSON protocol that maps 1:1 to the storage API
(`src/storage.hh`). It follows that API as it evolves and makes no attempt at
compatibility with anything.

An object is addressed as `/{bucket}/{key}`; the key may contain `/` and is
percent-decoded. Keys ending with `/` are not supported and are refused.

Errors are returned as `{"error": "<code>", "message": "..."}` where the code
is one of the storage error codes or `bad_request`:

| Status | Codes |
|---|---|
| 400 | `invalid_argument`, `bad_request` |
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
`{"key", "size", "generation", "mtime_ns", "metadata": {}}`.

| Request | Reply |
|---|---|
| `PUT /{bucket}/{key}` | object info. The body is the data. Optional headers: `X-Labaz-Meta-<name>` (user metadata; names are lower-cased), `X-Labaz-If-Generation-Match` (0: must not exist) |
| `GET /{bucket}/{key}` | the data; 206 with `Content-Range` if a `Range: bytes=...` header was given (single range only). Headers `X-Labaz-Size`, `X-Labaz-Generation`, `X-Labaz-Meta-<name>` |
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

A test client is in `test/native_client.py`.
