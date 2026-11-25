/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <optional>
#include <string_view>

#include <seastar/core/sstring.hh>

#include "storage.hh"

namespace labaz {

/// Percent-encodes everything but the unreserved characters and, unless
/// told otherwise, '/'.
seastar::sstring url_encode(std::string_view s, bool keep_slash = true);

/// "Wed, 12 Oct 2009 17:50:00 GMT", as HTTP dates are.
seastar::sstring http_date(clock_type::time_point t);

/// "2009-10-12T17:50:30.000Z": ISO 8601 (RFC 3339) in UTC, with
/// milliseconds.
seastar::sstring iso8601_date(clock_type::time_point t);

/// Parses a single "bytes=first-last", "bytes=first-" or "bytes=-length"
/// range. Returns nullopt for anything else, which HTTP lets a server
/// ignore.
std::optional<byte_range> parse_range_header(std::string_view header);

} // namespace labaz
