# Storage backends

A backend is selected with `--backend <name>`. Every backend implements the
`labaz::storage` interface (`src/storage.hh`); facades only talk to that
interface.

## potato

> **Not intended for production use.**

`--backend potato --data-dir <dir>` (the default backend)

Stores data in files, in the simplest way possible. It exists to be easy to
understand and debug, so it trades everything else for that.

Layout under `<dir>`: an object is a directory named by the SHA-1 of its key.

```
<bucket>/<sha1(key)>/meta.json        object meta (JSON): the key, attributes and part files
<bucket>/<sha1(key)>/data.<gen>/      the object's parts
<bucket>/<sha1(key)>/session.<id>/    an upload session's parts
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
- Every call is served on the shard it is made on; there is no routing
  between shards. Sessions live in the memory of the shard that opened them,
  so all calls of a session must come to that shard. Until the facades route
  requests, run with `--smp 1`.

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
