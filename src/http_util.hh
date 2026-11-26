/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

#include <fmt/format.h>

#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

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

/// Milliseconds since `start`, for the trace logs.
inline double ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

/// A request's id, unique within the server; "<shard>-<counter>" in hex.
struct request_id {
    unsigned shard;
    uint64_t counter;
};

request_id next_request_id();

using handler_func = seastar::noncopyable_function<seastar::future<std::unique_ptr<seastar::http::reply>>(
        std::unique_ptr<seastar::http::request>&, std::unique_ptr<seastar::http::reply>)>;

/// Runs a facade's handler on the request, tracing the request and the
/// reply on `log`.
seastar::future<std::unique_ptr<seastar::http::reply>> run_traced(seastar::logger& log, request_id id,
        std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep, handler_func run);

/// Sends an object's data into a reply's body, tracing on `log` how that
/// went; it goes on after the handler has returned the reply.
seastar::future<> send_data(seastar::logger& log, seastar::input_stream<char> in, seastar::output_stream<char>& out,
        seastar::sstring bucket, seastar::sstring key);

} // namespace labaz

template <>
struct fmt::formatter<labaz::request_id> : fmt::formatter<std::string_view> {
    auto format(const labaz::request_id& id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{:x}-{:x}", id.shard, id.counter);
    }
};
