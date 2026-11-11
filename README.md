# Labaz

An object storage server that speaks the S3 and GCS protocols, built on
[Seastar](https://github.com/scylladb/seastar). See
[object-storage-design.md](object-storage-design.md) for the design.

At this stage it serves only the testing-only [plywood facade](docs/plywood-protocol.md)
on top of the [potato backend](docs/backends.md).

## Building

Seastar is a git submodule and is built along with Labaz.
Its build dependencies must be installed (see `seastar/install-dependencies.sh`).

```sh
git submodule update --init --recursive
cmake --preset dev          # or: debug, release
ninja -C build/dev
```

The presets use clang and put the build into `build/<preset>/`.

## Running

```sh
build/dev/labaz --smp 1 -m 1G \
    --backend potato --data-dir /var/tmp/labaz \
    --facade plywood:http=8080,https=8443 --tls-cert cert.pem --tls-key key.pem
```

- `--facade <name>:http=<port>,https=<port>,address=<addr>` serves one
  protocol; it can be repeated, and all facades share the same storage. At
  least one port per facade is required; `address` defaults to
  `--listen-address` (127.0.0.1). HTTPS ports use `--tls-cert`/`--tls-key`.
- `--backend <name>` selects the storage; see [docs/backends.md](docs/backends.md).
- `--help` lists the available facades and backends with their options.

Every object is owned by one shard (`src/sharder.*`). The storage runs the
calls about an object on that shard, whichever shard received the request,
and streams the data between the two.

Once all facades listen, the server prints `labaz: ready` to stdout. It stops
on SIGINT or SIGTERM.

## Testing

Tests are written in Python with pytest and run the built binary:

```sh
ninja -C build/dev check
# or directly:
python3 -m pytest test --labaz=build/dev/labaz
```

They need `pytest`, `requests` and the `openssl` command.

## Layout

| Path | What |
|---|---|
| `src/storage.*` | The storage interface, routing calls to the owner shard, and neutral error codes |
| `src/facade.hh` | The facade (protocol) interface |
| `src/sharder.*` | Which shard owns an object |
| `src/streams.*` | Reading streams of other shards |
| `src/checksum.*` | Checksums |
| `src/registry.cc` | The list of built-in backends and facades |
| `src/potato/` | The potato backend |
| `src/plywood/` | The plywood facade |
| `src/http_frontend.*` | Per-facade HTTP(S) servers on all shards |

## License

Apache-2.0, see [LICENSE](LICENSE).
