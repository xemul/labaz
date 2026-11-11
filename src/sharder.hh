/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <string_view>

namespace labaz {

/// Decides which shard owns an object. Every call about an object, and
/// every upload session of it, is to run on its owner shard; facades route
/// them there.
class sharder {
public:
    /// A fixed hash of the bucket and the key modulo the number of shards,
    /// so it is stable across restarts with the same number of shards.
    unsigned shard_of(std::string_view bucket, std::string_view key) const;
};

} // namespace labaz
