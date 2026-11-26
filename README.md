# Labaz

An object storage server that speaks the S3 and GCS protocols, built on
[Seastar](https://github.com/scylladb/seastar). See
[object-storage-design.md](object-storage-design.md) for the design.

At this stage it serves a part of the [S3 API](docs/s3-protocol.md), a part
of the [GCS JSON API](docs/gcs-protocol.md) and the testing-only
[plywood facade](docs/plywood-protocol.md), on top of the
[potato backend](docs/backends.md).

## Building

Seastar is a git submodule and is built along with Labaz.
Its build dependencies must be installed (see `seastar/install-dependencies.sh`), and
Labaz's own: RapidJSON, GnuTLS, zlib and pugixml (on Fedora: `dnf install
rapidjson-devel gnutls-devel zlib-ng-compat-devel pugixml-devel`).

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

## Container

```sh
podman build -f container/Containerfile -t labaz .
podman run --rm -p 7070:7070 -p 4443:4443 -e LABAZ_S3_KEY=<access-key>:<secret> labaz
```

The image serves the S3 facade on port 7070 and the GCS facade on 4443, on
all addresses, with the potato backend's data in the `/var/lib/labaz`
volume. Environment variables configure it: `LABAZ_S3_PORT` and
`LABAZ_GCS_PORT` (empty for no such facade), `LABAZ_S3_KEY` and
`LABAZ_S3_REGION`, `LABAZ_SMP` (1) and `LABAZ_MEMORY` (1G); arguments go to
`labaz` as they are. See [container/entrypoint.sh](container/entrypoint.sh).
For now the image is built in ScyllaDB's toolchain image
(`docker.io/scylladb/scylla-toolchain`), which has Seastar's build
dependencies; `--build-arg BUILDER=<image>` picks another. Pushes to `master`
and of `v*` tags build the image on GitHub and publish it as
`ghcr.io/<owner>/labaz:latest` and `ghcr.io/<owner>/labaz:<tag>`
(`.github/workflows/container.yml`).

## Testing

Tests are written in Python with pytest and run the built binary:

```sh
ninja -C build/dev check
# or directly:
python3 -m pytest test --labaz=build/dev/labaz
```

They need `pytest`, `requests`, `boto3`, `google-cloud-storage` and the
`openssl` command.

## Layout

| Path | What |
|---|---|
| `src/storage.*` | The storage interface, routing calls to the owner shard, and neutral error codes |
| `src/facade.hh` | The facade (protocol) interface |
| `src/sharder.*` | Which shard owns an object |
| `src/streams.*` | Reading streams of other shards |
| `src/checksum.*` | Checksums, and their hex and base64 forms |
| `src/http_util.*`, `src/listing.*` | What the facades share: URL encoding, dates, `Range` headers, listings with a delimiter, request tracing, sending object data |
| `src/registry.cc` | The list of built-in backends and facades |
| `src/potato/` | The potato backend |
| `src/plywood/` | The plywood facade |
| `src/s3/` | The S3 facade; see [docs/s3-protocol.md](docs/s3-protocol.md) |
| `src/gcs/` | The GCS facade; see [docs/gcs-protocol.md](docs/gcs-protocol.md) |
| `src/http_frontend.*` | Per-facade HTTP(S) servers on all shards |
| `container/` | The container image |

## License

Apache-2.0, see [LICENSE](LICENSE).
