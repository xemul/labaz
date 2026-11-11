/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage.hh"

#include <algorithm>

namespace labaz {

std::string_view to_string(errc e) {
    switch (e) {
    case errc::no_such_bucket: return "no_such_bucket";
    case errc::no_such_key: return "no_such_key";
    case errc::no_such_session: return "no_such_session";
    case errc::no_such_part: return "no_such_part";
    case errc::bucket_exists: return "bucket_exists";
    case errc::bucket_not_empty: return "bucket_not_empty";
    case errc::precondition_failed: return "precondition_failed";
    case errc::invalid_argument: return "invalid_argument";
    case errc::invalid_range: return "invalid_range";
    }
    return "unknown";
}

resolved_range resolve_range(const byte_range& r, uint64_t size) {
    if (r.first) {
        if (*r.first >= size || (r.last && *r.last < *r.first)) {
            throw storage_error(errc::invalid_range, "range not satisfiable");
        }
        uint64_t last = r.last ? std::min(*r.last, size - 1) : size - 1;
        return {*r.first, last - *r.first + 1};
    }
    if (!r.last || *r.last == 0 || size == 0) {
        throw storage_error(errc::invalid_range, "range not satisfiable");
    }
    uint64_t len = std::min(*r.last, size);
    return {size - len, len};
}

} // namespace labaz
