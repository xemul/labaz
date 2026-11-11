/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sharder.hh"

#include <seastar/core/smp.hh>

#include "checksum.hh"

namespace labaz {

unsigned sharder::shard_of(std::string_view bucket, std::string_view key) const {
    auto crc = crc32c(key, crc32c(std::string_view("\0", 1), crc32c(bucket)));
    return crc % seastar::this_smp_shard_count();
}

} // namespace labaz
