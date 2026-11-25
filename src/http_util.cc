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

} // namespace labaz
