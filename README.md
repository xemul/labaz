# Labaz

An object storage server that speaks the S3 and GCS protocols, built on
[Seastar](https://github.com/scylladb/seastar). See
[object-storage-design.md](object-storage-design.md) for the design.

At this stage it is a skeleton: an HTTP(S) server with no routes.

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
build/dev/labaz --smp 2 -m 1G \
    --http-port 8080 \
    --https-port 8443 --tls-cert cert.pem --tls-key key.pem
```

At least one of `--http-port` and `--https-port` is required. Once
listening, the server prints a `labaz: listening ...` line to stdout. It stops
on SIGINT or SIGTERM.

## Testing

Tests are written in Python with pytest and run the built binary:

```sh
ninja -C build/dev check
# or directly:
python3 -m pytest test --labaz=build/dev/labaz
```

They need `pytest`, `requests` and the `openssl` command.

## License

Apache-2.0, see [LICENSE](LICENSE).
