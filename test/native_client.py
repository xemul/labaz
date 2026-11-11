# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0

"""A client for the native facade (docs/native-protocol.md)."""

from urllib.parse import quote

import requests


class NativeError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code


class NativeClient:
    def __init__(self, url: str, **kwargs):
        self.url = url
        self.s = requests.Session()
        self.kwargs = kwargs

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        r = self.s.request(method, self.url + path, **self.kwargs, **kwargs)
        if r.status_code >= 400:
            try:
                err = r.json()
                raise NativeError(r.status_code, err["error"], err["message"])
            except (ValueError, KeyError):
                raise NativeError(r.status_code, "", r.text)
        return r

    @staticmethod
    def _key(key: str) -> str:
        # Dot segments are encoded too, or they get normalized away.
        return "/".join("%2E" * len(c) if c in (".", "..") else quote(c, safe="")
                        for c in key.split("/"))

    @staticmethod
    def _meta_headers(metadata: dict | None) -> dict:
        return {f"X-Labaz-Meta-{k}": v for k, v in (metadata or {}).items()}

    def _obj(self, bucket: str, key: str) -> str:
        return f"/{bucket}/{self._key(key)}"

    # buckets

    def create_bucket(self, bucket: str):
        self.request("PUT", f"/{bucket}")

    def delete_bucket(self, bucket: str):
        self.request("DELETE", f"/{bucket}")

    def list_buckets(self) -> list[str]:
        return self.request("GET", "/").json()["buckets"]

    # objects

    def put(self, bucket: str, key: str, data: bytes, metadata: dict | None = None,
            generation_match: int | None = None) -> dict:
        headers = self._meta_headers(metadata)
        if generation_match is not None:
            headers["X-Labaz-If-Generation-Match"] = str(generation_match)
        return self.request("PUT", self._obj(bucket, key), data=data, headers=headers).json()

    def get(self, bucket: str, key: str, range: str | None = None) -> requests.Response:
        headers = {"Range": range} if range else {}
        return self.request("GET", self._obj(bucket, key), headers=headers)

    def read(self, bucket: str, key: str, range: str | None = None) -> bytes:
        return self.get(bucket, key, range).content

    @staticmethod
    def _copy_params(src_key: str, range: str | None, src_generation_match: int | None) -> dict:
        params = {"copy-key": src_key}
        if range is not None:
            params["copy-range"] = range
        if src_generation_match is not None:
            params["copy-if-generation-match"] = src_generation_match
        return params

    def copy(self, bucket: str, key: str, src_key: str, range: str | None = None,
             src_generation_match: int | None = None, metadata: dict | None = None,
             generation_match: int | None = None) -> dict:
        headers = self._meta_headers(metadata)
        if generation_match is not None:
            headers["X-Labaz-If-Generation-Match"] = str(generation_match)
        return self.request("PUT", self._obj(bucket, key), headers=headers,
                            params=self._copy_params(src_key, range, src_generation_match)).json()

    def head(self, bucket: str, key: str) -> requests.Response:
        return self.request("HEAD", self._obj(bucket, key))

    def delete(self, bucket: str, key: str):
        self.request("DELETE", self._obj(bucket, key))

    def list(self, bucket: str, prefix: str = "", start_after: str = "",
             max_keys: int | None = None) -> dict:
        params = {"prefix": prefix, "start-after": start_after}
        if max_keys is not None:
            params["max-keys"] = max_keys
        return self.request("GET", f"/{bucket}", params=params).json()

    def list_keys(self, bucket: str, prefix: str = "", page: int = 1000) -> list[str]:
        """All keys, fetched in pages of `page`."""
        keys = []
        while True:
            r = self.list(bucket, prefix=prefix, start_after=keys[-1] if keys else "", max_keys=page)
            keys += [o["key"] for o in r["objects"]]
            if not r["truncated"]:
                return keys

    # sessions

    def open_session(self, bucket: str, key: str, metadata: dict | None = None) -> str:
        return self.request("POST", self._obj(bucket, key),
                            headers=self._meta_headers(metadata)).json()["session"]

    def put_part(self, bucket: str, key: str, session: str, number: int, data: bytes) -> dict:
        return self.request("PUT", self._obj(bucket, key), params={"session": session, "part": number},
                            data=data).json()

    def copy_part(self, bucket: str, key: str, session: str, number: int, src_key: str,
                  range: str | None = None, src_generation_match: int | None = None) -> dict:
        params = {"session": session, "part": number, **self._copy_params(src_key, range, src_generation_match)}
        return self.request("PUT", self._obj(bucket, key), params=params).json()

    def describe_session(self, bucket: str, key: str, session: str) -> dict:
        return self.request("GET", self._obj(bucket, key), params={"session": session}).json()

    def commit(self, bucket: str, key: str, session: str, parts: list[int] | None = None,
               generation_match: int | None = None) -> dict:
        """Commits `parts`, or all sealed parts in number order if None."""
        params = {"session": session}
        if parts is not None:
            params["parts"] = ",".join(str(n) for n in parts)
        headers = {}
        if generation_match is not None:
            headers["X-Labaz-If-Generation-Match"] = str(generation_match)
        return self.request("PATCH", self._obj(bucket, key), params=params, headers=headers).json()

    def abort(self, bucket: str, key: str, session: str):
        self.request("DELETE", self._obj(bucket, key), params={"session": session})
