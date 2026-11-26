/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "http_util.hh"

#include <cctype>
#include <charconv>
#include <ctime>
#include <string>

#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace labaz {

using namespace seastar;

sstring url_encode(std::string_view s, bool keep_slash) {
    std::string ret;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || (c == '/' && keep_slash)) {
            ret += char(c);
        } else {
            ret += fmt::format("%{:02X}", c);
        }
    }
    return sstring(ret);
}

sstring http_date(clock_type::time_point t) {
    auto tt = clock_type::to_time_t(t);
    struct tm tm;
    gmtime_r(&tt, &tm);
    char buf[64];
    auto n = strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    return sstring(buf, n);
}

sstring iso8601_date(clock_type::time_point t) {
    auto tt = clock_type::to_time_t(t);
    struct tm tm;
    gmtime_r(&tt, &tm);
    char buf[64];
    auto n = strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count() % 1000;
    return sstring(fmt::format("{}.{:03}Z", std::string_view(buf, n), ms));
}

static std::optional<uint64_t> parse_offset(std::string_view s) {
    uint64_t v;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || p != s.data() + s.size()) {
        return std::nullopt;
    }
    return v;
}

std::optional<byte_range> parse_range_header(std::string_view s) {
    if (!s.starts_with("bytes=") || s.find(',') != std::string_view::npos) {
        return std::nullopt;
    }
    s.remove_prefix(6);
    auto dash = s.find('-');
    if (dash == std::string_view::npos || (dash == 0 && dash + 1 == s.size())) {
        return std::nullopt;
    }
    byte_range r;
    if (dash > 0) {
        r.first = parse_offset(s.substr(0, dash));
        if (!r.first) {
            return std::nullopt;
        }
    }
    if (dash + 1 < s.size()) {
        r.last = parse_offset(s.substr(dash + 1));
        if (!r.last) {
            return std::nullopt;
        }
    }
    return r;
}

request_id next_request_id() {
    static thread_local uint64_t counter = 0;
    return request_id{this_shard_id(), ++counter};
}

future<std::unique_ptr<http::reply>> run_traced(logger& log, request_id id,
        std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep, handler_func run) {
    auto start = std::chrono::steady_clock::now();
    log.trace("{} {} {}", id, req->_method, req->_url);
    auto ret = co_await run(req, std::move(rep));
    log.trace("{} replied {} in {:.3f}ms", id, static_cast<int>(ret->_status), ms_since(start));
    co_return ret;
}

future<> send_data(logger& log, input_stream<char> in, output_stream<char>& out, sstring bucket, sstring key) {
    auto start = std::chrono::steady_clock::now();
    std::exception_ptr ex;
    try {
        co_await copy(in, out);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        log.trace("{}/{}: sending data failed in {:.3f}ms: {}", bucket, key, ms_since(start), ex);
        std::rethrow_exception(ex);
    }
    log.trace("{}/{}: data sent in {:.3f}ms", bucket, key, ms_since(start));
}

} // namespace labaz
