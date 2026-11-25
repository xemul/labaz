/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "storage.hh"

namespace labaz {

/// A listing as S3 and GCS do it, where the keys that have the delimiter
/// after the prefix are rolled up into their common prefixes.
struct list_request {
    sstring bucket;
    sstring prefix;
    // None if empty.
    sstring delimiter;
    // List what follows this key, or with `inclusive`, this key on.
    sstring after;
    bool inclusive = false;
    unsigned max_entries = 1000;
};

/// Keys and common prefixes in byte order, up to max_entries of them
/// together.
struct list_result {
    std::vector<object_info> objects;
    std::vector<sstring> prefixes;
    bool truncated = false;
    // The last key or prefix listed.
    sstring last;
};

future<list_result> list_delimited(storage_service& storage, const list_request& lr);

} // namespace labaz
