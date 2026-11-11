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

checksums concatenated_checksums(const std::vector<part_info>& parts, const std::set<checksum_algorithm>& algorithms) {
    if (parts.empty()) {
        return checksummer(algorithms).finish();
    }
    checksums ret;
    for (auto alg : algorithms) {
        if (parts.size() > 1 && !is_crc(alg)) {
            continue;
        }
        if (!std::ranges::all_of(parts, [alg] (const part_info& p) { return p.checksums.contains(alg); })) {
            continue;
        }
        auto value = parts[0].checksums.at(alg);
        for (size_t i = 1; i < parts.size(); i++) {
            value = crc_combine(alg, value, parts[i].checksums.at(alg), parts[i].size);
        }
        ret.emplace(alg, std::move(value));
    }
    return ret;
}

checksums object_checksums(const object_info& info) {
    return concatenated_checksums(info.parts, info.attrs.checksums);
}

} // namespace labaz
