# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0


class FacadeError(Exception):
    """An error reply of a facade, with the facade's own error code."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
