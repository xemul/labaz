# Storage backends

A backend is selected with `--backend <name>`. Every backend implements the
`labaz::storage` interface (`src/storage.hh`). Facades talk to
`labaz::storage_service`, which routes the calls about an object to the
backend instance of the shard owning it.

## potato

> **Not intended for production use.**

`--backend potato --data-dir <dir>` (the default backend)

Stores data in files, in the simplest way possible. It exists to be easy to
understand and debug, so it trades everything else for that.

Layout under `<dir>`: an object is a directory named by the SHA-1 of its key.

```
<bucket>/<sha1(key)>/meta.json        object meta (JSON): the key, attributes, how it was uploaded, parts and slices
<bucket>/<sha1(key)>/data.<gen>/      the object's parts
<bucket>/<sha1(key)>/session.<id>/    an upload session's parts
.staging/                             links of copies between shards
```

- Keys are not encoded and do not map to paths, so any key works, of any
  length, and keys `a` and `a/b` coexist. The key is kept in `meta.json`;
  `jq .key */meta.json` in a bucket directory lists them.
- A commit renames the session directory into a `data.<gen>` directory,
  writes the object meta to a temporary file and renames it over the
  current one. That rename is the switch to the new version: a failure
  before it leaves the current version intact. The previous version's data
  directory is removed afterwards.
- A reader opens all part files under a shard-wide read lock, which commits,
  deletes and aborts take for write, so a concurrent overwrite or delete
  cannot remove them under it; once open, the files stay readable.
- An object directory is removed once it holds neither an object nor a
  session.
- Sealed part files are never modified. An object, and a session part, is a
  list of slices: byte ranges of part files. A copy hardlinks the source's
  part files into the session (one link per slice) under the read lock, and
  records the slices that the copied range covers, so no data is copied and
  the copy does not depend on the source afterwards.
- When another shard owns the source, the copy runs in two phases, so that
  no shard waits for another's lock while holding its own: the source's
  shard links its files into `.staging/` under its lock, then the
  destination's shard links them from there into the session under its
  lock, as it links a source of its own, and removes the staged links. Shard 0 empties `.staging/` on start. Bucket names
  cannot start with a dot, so it is never taken for a bucket.
- Checksums of a part are computed while it is written. A copied part gets
  those it can derive from the source's parts' checksums when it copies the
  whole source (all of a single part's, the combined CRCs of several parts),
  and the rest are computed by reading its links after they are made, so
  data is only read if the session wants checksums that cannot be derived.
  The meta keeps the object's parts (sizes and checksums) besides its
  slices.
- Every call about an object is made on the shard owning it
  (`storage_service` routes them there; only a copy reaches its source's
  shard itself), so sessions can live in the memory of the shard that
  opened them. All shards share the data directory, so a restart
  with another number of shards finds every object; only in-progress
  sessions are lost, as on any restart.

Limitations, by design:

- In-progress upload sessions live in memory and are lost on restart.
  Committed objects survive a restart.
- No crash safety: a crash in the middle of a commit or a delete can leave
  stray files. Nothing is synced beyond what the file streams do.
- Keys ending with `/` are refused (as by the storage API in general).
- Listing reads the meta of every object in the bucket, whatever the prefix.
- Deleting a bucket checks for emptiness without excluding concurrent
  writes into it.
- Committed sessions are remembered in memory (for idempotent commit
  retries) until restart.
