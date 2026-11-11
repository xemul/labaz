/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <seastar/core/deleter.hh>
#include <seastar/core/iostream.hh>

namespace labaz {

/// A stream of the current shard reading `in`, which belongs to shard
/// `owner` and stays there. Every buffer is read on that shard and handed
/// over as is: its memory is released on that shard once the buffer is
/// destroyed. Closing the stream closes `in` on its shard. `keep` is
/// destroyed with the stream, so it can keep `in` alive.
seastar::input_stream<char> make_foreign_stream(unsigned owner, seastar::input_stream<char>& in, seastar::deleter keep);

} // namespace labaz
