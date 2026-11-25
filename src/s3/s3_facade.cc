/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// What of the S3 API is supported is described in docs/s3-protocol.md.

#include "s3/s3_facade.hh"

#include <charconv>
#include <ctime>
#include <ranges>
#include <string>

#include <fmt/format.h>

#include <pugixml.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/matchrules.hh>
#include <seastar/http/url.hh>
#include <seastar/util/log.hh>
#include <seastar/util/short_streams.hh>

namespace labaz::s3 {

using namespace seastar;
using reply = http::reply;
using request = http::request;
using status = http::reply::status_type;
namespace bpo = boost::program_options;

static logger s3log("s3");

static constexpr std::string_view xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
static constexpr std::string_view meta_header_prefix = "x-amz-meta-";
static constexpr std::string_view checksum_header_prefix = "x-amz-checksum-";
static constexpr std::string_view xml_type = "application/xml";
// What S3 reports for objects uploaded without a Content-Type.
static constexpr std::string_view default_object_type = "binary/octet-stream";
static constexpr unsigned max_list_keys = 1000;
static constexpr unsigned max_list_parts = 1000;
static constexpr unsigned max_delete_keys = 1000;
// How many keys of a DeleteObjects request are deleted at once.
static constexpr size_t delete_concurrency = 16;
static constexpr size_t max_key_length = 1024;
static constexpr unsigned max_part_number = 10000;
// Every part of a multipart upload but the last must have at least this.
static constexpr uint64_t min_part_size = 5 << 20;
// The only owner there is.
static constexpr std::string_view owner_id = "labaz";

// Every object gets an MD5, for its ETag, and a CRC64NVME, which S3
// computes by default for objects uploaded without a checksum.
static const std::set<checksum_algorithm> default_checksums = {checksum_algorithm::md5, checksum_algorithm::crc64nvme};

//
// Errors
//

// An error reply: <Error><Code>...</Code><Message>...</Message></Error>.
class s3_error : public std::runtime_error {
    status _status;
    sstring _code;
public:
    s3_error(status st, sstring code, const std::string& msg) : std::runtime_error(msg), _status(st), _code(std::move(code)) {}
    s3_error(int st, sstring code, const std::string& msg) : s3_error(static_cast<status>(st), std::move(code), msg) {}
    status get_status() const noexcept { return _status; }
    const sstring& code() const noexcept { return _code; }
};

static s3_error not_implemented(std::string_view what) {
    return s3_error(status::not_implemented, "NotImplemented",
                    fmt::format("{} is not implemented", what));
}

static s3_error invalid_argument(const std::string& msg) {
    return s3_error(status::bad_request, "InvalidArgument", msg);
}

static s3_error invalid_request(const std::string& msg) {
    return s3_error(status::bad_request, "InvalidRequest", msg);
}

static s3_error malformed_xml() {
    return s3_error(status::bad_request, "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.");
}

static s3_error invalid_part() {
    return s3_error(status::bad_request, "InvalidPart",
                    "One or more of the specified parts could not be found. The part may not have been uploaded, "
                    "or the specified entity tag may not match the part's entity tag.");
}

// The S3 error for a storage error, with S3's own message where it has one.
static s3_error to_s3_error(const storage_error& e) {
    switch (e.code()) {
    case errc::no_such_bucket:
        return s3_error(status::not_found, "NoSuchBucket", "The specified bucket does not exist.");
    case errc::no_such_key:
        return s3_error(status::not_found, "NoSuchKey", "The specified key does not exist.");
    case errc::no_such_session:
        return s3_error(status::not_found, "NoSuchUpload", "The specified upload does not exist.");
    case errc::no_such_part:
        return s3_error(status::bad_request, "InvalidPart", "One or more of the specified parts could not be found.");
    case errc::bucket_exists:
        return s3_error(status::conflict, "BucketAlreadyOwnedByYou",
                        "Your previous request to create the named bucket succeeded and you already own it.");
    case errc::bucket_not_empty:
        return s3_error(status::conflict, "BucketNotEmpty", "The bucket you tried to delete is not empty.");
    case errc::precondition_failed:
        return s3_error(412, "PreconditionFailed", "At least one of the preconditions you specified did not hold.");
    case errc::invalid_argument:
        return invalid_argument(e.what());
    case errc::invalid_range:
        return s3_error(416, "InvalidRange", "The requested range is not satisfiable.");
    }
    return s3_error(status::internal_server_error, "InternalError", e.what());
}

//
// Encodings
//

static constexpr std::string_view base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static sstring to_base64(std::string_view bytes) {
    std::string ret;
    size_t i = 0;
    for (; i + 2 < bytes.size(); i += 3) {
        uint32_t v = uint8_t(bytes[i]) << 16 | uint8_t(bytes[i + 1]) << 8 | uint8_t(bytes[i + 2]);
        ret += base64_chars[v >> 18];
        ret += base64_chars[(v >> 12) & 63];
        ret += base64_chars[(v >> 6) & 63];
        ret += base64_chars[v & 63];
    }
    if (i + 1 == bytes.size()) {
        uint32_t v = uint8_t(bytes[i]) << 16;
        ret += base64_chars[v >> 18];
        ret += base64_chars[(v >> 12) & 63];
        ret += "==";
    } else if (i + 2 == bytes.size()) {
        uint32_t v = uint8_t(bytes[i]) << 16 | uint8_t(bytes[i + 1]) << 8;
        ret += base64_chars[v >> 18];
        ret += base64_chars[(v >> 12) & 63];
        ret += base64_chars[(v >> 6) & 63];
        ret += '=';
    }
    return sstring(ret);
}

// Padded base64 only.
static std::optional<sstring> from_base64(std::string_view s) {
    if (s.size() % 4) {
        return std::nullopt;
    }
    std::string ret;
    uint32_t v = 0;
    int bits = 0;
    for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '=') {
            // Padding: at most two, and only at the end.
            if (i + 2 < s.size() || (i + 1 < s.size() && s[i + 1] != '=')) {
                return std::nullopt;
            }
            break;
        }
        auto p = base64_chars.find(s[i]);
        if (p == std::string_view::npos) {
            return std::nullopt;
        }
        v = v << 6 | p;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            ret += char((v >> bits) & 0xff);
        }
    }
    return sstring(ret);
}

// Percent-encodes everything but the unreserved characters and '/', as S3
// does for encoding-type=url.
static sstring url_encode(std::string_view s) {
    std::string ret;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/') {
            ret += char(c);
        } else {
            ret += fmt::format("%{:02X}", c);
        }
    }
    return sstring(ret);
}

// "Wed, 12 Oct 2009 17:50:00 GMT"
static sstring http_date(clock_type::time_point t) {
    auto tt = clock_type::to_time_t(t);
    struct tm tm;
    gmtime_r(&tt, &tm);
    char buf[64];
    auto n = strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    return sstring(buf, n);
}

// Parses an HTTP date in any of the formats HTTP allows: IMF-fixdate (as
// http_date() writes it), RFC 850 and asctime.
static std::optional<clock_type::time_point> parse_http_date(const sstring& s) {
    for (auto format : {"%a, %d %b %Y %H:%M:%S GMT", "%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"}) {
        struct tm tm = {};
        auto end = strptime(s.c_str(), format, &tm);
        if (end && *end == '\0') {
            return clock_type::from_time_t(timegm(&tm));
        }
    }
    return std::nullopt;
}

// "2009-10-12T17:50:30.000Z"
static sstring iso8601_date(clock_type::time_point t) {
    auto tt = clock_type::to_time_t(t);
    struct tm tm;
    gmtime_r(&tt, &tm);
    char buf[64];
    auto n = strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count() % 1000;
    return sstring(fmt::format("{}.{:03}Z", std::string_view(buf, n), ms));
}

//
// XML
//

class xml_writer {
    std::string _out = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    void escaped(std::string_view s) {
        for (char c : s) {
            switch (c) {
            case '&': _out += "&amp;"; break;
            case '<': _out += "&lt;"; break;
            case '>': _out += "&gt;"; break;
            case '"': _out += "&quot;"; break;
            case '\'': _out += "&apos;"; break;
            default: _out += c;
            }
        }
    }

public:
    // The document's root element, in the S3 namespace.
    xml_writer& root(std::string_view tag) {
        _out += fmt::format("<{} xmlns=\"{}\">", tag, xmlns);
        return *this;
    }
    xml_writer& open(std::string_view tag) {
        _out += fmt::format("<{}>", tag);
        return *this;
    }
    xml_writer& close(std::string_view tag) {
        _out += fmt::format("</{}>", tag);
        return *this;
    }
    xml_writer& element(std::string_view tag, std::string_view value) {
        open(tag);
        escaped(value);
        return close(tag);
    }
    template <typename T>
    requires std::is_arithmetic_v<T>
    xml_writer& element(std::string_view tag, T value) {
        return element(tag, std::string_view(fmt::to_string(value)));
    }
    sstring str() const {
        return sstring(_out);
    }
};

static std::unique_ptr<reply> xml_reply(std::unique_ptr<reply> rep, const xml_writer& w, status st = status::ok) {
    rep->set_status(st);
    rep->write_body(xml_type, w.str());
    return rep;
}

static std::unique_ptr<reply> empty_reply(std::unique_ptr<reply> rep, status st) {
    rep->set_status(st);
    return rep;
}

//
// Requests
//

template <typename T>
static std::optional<T> parse_number(std::string_view s) {
    T v;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || p != s.data() + s.size()) {
        return std::nullopt;
    }
    return v;
}

static bool equals_ignoring_case(std::string_view a, std::string_view b) {
    return std::ranges::equal(a, b, [] (char x, char y) { return std::tolower(x) == std::tolower(y); });
}

// The part of the URL before the query.
static std::string_view url_path(const request& req) {
    std::string_view url = req._url;
    return url.substr(0, url.find('?'));
}

static sstring bucket_param(const request& req) {
    return req.get_path_param("bucket");
}

static void check_key_length(std::string_view key) {
    if (key.size() > max_key_length) {
        throw s3_error(status::bad_request, "KeyTooLongError", "Your key is too long");
    }
}

// Checks a key given other than as the request's path.
static void check_key(std::string_view key) {
    if (key.ends_with('/')) {
        throw invalid_argument("Keys ending with '/' are not supported");
    }
    check_key_length(key);
}

static sstring key_param(const request& req) {
    // The routes drop a trailing '/' from the path, but keys ending with
    // '/' are not supported, so refuse them rather than use another key.
    if (url_path(req).ends_with('/')) {
        throw invalid_argument("Keys ending with '/' are not supported");
    }
    auto key = req.get_path_param("key");
    check_key_length(key);
    return key;
}

struct object_name {
    sstring bucket;
    sstring key;
};

// The source of a copy, from x-amz-copy-source: "[/]bucket/key", URL-encoded,
// which may be followed by "?versionId=...".
static object_name copy_source(const request& req) {
    auto header = req.get_header("x-amz-copy-source");
    std::string_view s = header;
    if (auto q = s.find('?'); q != std::string_view::npos) {
        if (s.substr(q + 1).starts_with("versionId=")) {
            throw not_implemented("Copying a version of an object");
        }
        s = s.substr(0, q);
    }
    sstring decoded;
    if (!http::internal::path_decode(s, decoded)) {
        throw invalid_argument("Invalid copy source encoding");
    }
    std::string_view path = decoded;
    if (path.starts_with('/')) {
        path.remove_prefix(1);
    }
    auto slash = path.find('/');
    if (slash == 0 || slash == std::string_view::npos || slash + 1 == path.size()) {
        throw invalid_argument("Copy Source must mention the source bucket and key: sourcebucket/sourcekey");
    }
    check_key(path.substr(slash + 1));
    return object_name{sstring(path.substr(0, slash)), sstring(path.substr(slash + 1))};
}

// The range of x-amz-copy-source-range, which must be "bytes=first-last".
static std::optional<byte_range> copy_source_range(const request& req) {
    if (!req._headers.contains("x-amz-copy-source-range")) {
        return std::nullopt;
    }
    auto header = req.get_header("x-amz-copy-source-range");
    std::string_view s = header;
    auto invalid = [] {
        return invalid_argument("The x-amz-copy-source-range value must be of the form bytes=first-last "
                                "where first and last are the zero-based offsets of the first and last bytes to copy");
    };
    if (!s.starts_with("bytes=")) {
        throw invalid();
    }
    s.remove_prefix(6);
    auto dash = s.find('-');
    if (dash == std::string_view::npos) {
        throw invalid();
    }
    auto first = parse_number<uint64_t>(s.substr(0, dash));
    auto last = parse_number<uint64_t>(s.substr(dash + 1));
    if (!first || !last || *first > *last) {
        throw invalid();
    }
    return byte_range{first, last};
}

// S3's rules for names of new buckets.
static void validate_bucket_name(std::string_view b) {
    auto invalid = [] {
        return s3_error(status::bad_request, "InvalidBucketName", "The specified bucket is not valid.");
    };
    auto is_alnum = [] (char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    if (b.size() < 3 || b.size() > 63 || !is_alnum(b.front()) || !is_alnum(b.back())
            || b.find("..") != std::string_view::npos
            || !std::ranges::all_of(b, [&] (char c) { return is_alnum(c) || c == '.' || c == '-'; })) {
        throw invalid();
    }
    // Not formatted as an IP address.
    unsigned dots = std::ranges::count(b, '.');
    if (dots == 3 && std::ranges::all_of(b, [] (char c) { return (c >= '0' && c <= '9') || c == '.'; })) {
        throw invalid();
    }
}

// Fails requests for any of the query parameters (sub-resources), which
// select operations that are not implemented.
static void reject_query_params(const request& req, std::initializer_list<std::string_view> params) {
    for (auto p : params) {
        if (req.has_query_param(p)) {
            throw not_implemented(fmt::format("The '{}' query parameter", p));
        }
    }
}

// Fails requests with any of the headers, which select features that are
// not implemented.
static void reject_headers(const request& req, std::initializer_list<std::string_view> headers) {
    for (auto h : headers) {
        if (req._headers.contains(sstring(h))) {
            throw not_implemented(fmt::format("The '{}' header", h));
        }
    }
}

// Calls func(suffix, value) for every header named `prefix` (lower case)
// followed by a non-empty suffix, with the suffix lower-cased.
template <typename Func>
static void for_each_prefixed_header(const request& req, std::string_view prefix, Func func) {
    for (auto& [name, value] : req._headers) {
        if (name.size() > prefix.size() && equals_ignoring_case(std::string_view(name).substr(0, prefix.size()), prefix)) {
            sstring suffix = name.substr(prefix.size());
            std::ranges::transform(suffix, suffix.begin(), [] (char c) { return std::tolower(c); });
            func(suffix, value);
        }
    }
}

static user_metadata metadata_from_headers(const request& req) {
    user_metadata ret;
    for_each_prefixed_header(req, meta_header_prefix, [&] (const sstring& k, const sstring& value) {
        ret[k] = value;
    });
    return ret;
}

// The precondition of a write: only If-None-Match: * (the key must not
// exist) is supported.
static precondition write_precondition(const request& req) {
    reject_headers(req, {"If-Match"});
    precondition pre;
    if (req._headers.contains("If-None-Match")) {
        if (req.get_header("If-None-Match") != "*") {
            throw not_implemented("An If-None-Match header other than '*'");
        }
        pre.generation_match = 0;
    }
    return pre;
}

// An algorithm of the x-amz-checksum-* headers, by its name in them
// (in any case). MD5 is not one: it comes as Content-MD5.
static std::optional<checksum_algorithm> s3_checksum_algorithm(std::string_view name) {
    sstring lower(name);
    std::ranges::transform(lower, lower.begin(), [] (char c) { return std::tolower(c); });
    auto alg = checksum_algorithm_from_string(lower);
    if (alg == checksum_algorithm::md5) {
        return std::nullopt;
    }
    return alg;
}

// The checksums an upload's body must have, and the algorithms to keep
// checksums of.
struct upload_checksums {
    checksums expected;
    std::set<checksum_algorithm> algorithms;
};

// The algorithm x-amz-checksum-algorithm asks for, if any.
static std::optional<checksum_algorithm> requested_algorithm(const request& req) {
    auto name = req.get_header("x-amz-checksum-algorithm");
    if (name.empty()) {
        return std::nullopt;
    }
    auto alg = s3_checksum_algorithm(name);
    if (!alg) {
        throw invalid_request(fmt::format("Unsupported checksum algorithm '{}'", name));
    }
    return alg;
}

static upload_checksums upload_checksums_from_headers(const request& req) {
    upload_checksums ret;
    std::optional<checksum_algorithm> given;
    for_each_prefixed_header(req, checksum_header_prefix, [&] (const sstring& name, const sstring& value) {
        // x-amz-checksum-type, -algorithm and -mode are other headers.
        if (name == "type" || name == "algorithm" || name == "mode") {
            return;
        }
        auto alg = s3_checksum_algorithm(name);
        if (!alg) {
            throw invalid_request(fmt::format("Unsupported checksum algorithm '{}'", name));
        }
        if (given) {
            throw invalid_request("Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.");
        }
        auto bytes = from_base64(value);
        if (!bytes) {
            throw invalid_request(fmt::format("Value for x-amz-checksum-{} header is invalid.", name));
        }
        given = *alg;
        ret.expected.emplace(*alg, std::move(*bytes));
        ret.algorithms.insert(*alg);
    });
    auto sdk = req.get_header("x-amz-sdk-checksum-algorithm");
    if (!sdk.empty()) {
        auto alg = s3_checksum_algorithm(sdk);
        if (!alg) {
            throw invalid_request(fmt::format("Unsupported checksum algorithm '{}'", sdk));
        }
        if (given && *given != *alg) {
            throw invalid_request("Value for x-amz-sdk-checksum-algorithm header is invalid.");
        }
        ret.algorithms.insert(*alg);
    }
    auto md5 = req.get_header("Content-MD5");
    if (!md5.empty()) {
        auto bytes = from_base64(md5);
        if (!bytes || bytes->size() != 16) {
            throw s3_error(status::bad_request, "InvalidDigest", "The Content-MD5 you specified was not valid.");
        }
        ret.expected.emplace(checksum_algorithm::md5, std::move(*bytes));
    }
    return ret;
}

static bool is_storage_error(const std::exception_ptr& ep, errc code) {
    try {
        std::rethrow_exception(ep);
    } catch (const storage_error& e) {
        return e.code() == code;
    } catch (...) {
        return false;
    }
}

// Request bodies must be plain: aws-chunked ones carry chunk signatures
// and trailers.
static void check_plain_body(const request& req) {
    auto sha = req.get_header("x-amz-content-sha256");
    if (sha.starts_with("STREAMING-") || req.get_header("Content-Encoding").find("aws-chunked") != sstring::npos
            || req._headers.contains("x-amz-decoded-content-length")) {
        throw not_implemented("An aws-chunked request body");
    }
}

// Parses a "bytes=first-last" range. S3 ignores a Range header it cannot
// parse, as HTTP allows, so this returns nullopt for such.
static std::optional<byte_range> range_from_headers(const request& req) {
    auto header = req.get_header("Range");
    std::string_view s = header;
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
        r.first = parse_number<uint64_t>(s.substr(0, dash));
        if (!r.first) {
            return std::nullopt;
        }
    }
    if (dash + 1 < s.size()) {
        r.last = parse_number<uint64_t>(s.substr(dash + 1));
        if (!r.last) {
            return std::nullopt;
        }
    }
    return r;
}

//
// Objects
//

// S3's ETag of an object: the MD5 of its data for a single upload, and the
// MD5 of its parts' MD5s followed by the number of parts for a multipart
// one. Objects without MD5s (written through other facades) get one made
// of their generation.
static sstring etag(const object_info& info) {
    if (info.upload == upload_kind::multipart) {
        if (std::ranges::all_of(info.parts, [] (const part_info& p) { return p.checksums.contains(checksum_algorithm::md5); })) {
            checksummer md5({checksum_algorithm::md5});
            for (auto& p : info.parts) {
                md5.update(p.checksums.at(checksum_algorithm::md5));
            }
            return sstring(fmt::format("\"{}-{}\"", to_hex(md5.finish().at(checksum_algorithm::md5)), info.parts.size()));
        }
    } else if (info.parts.size() <= 1) {
        auto sums = object_checksums(info);
        if (auto it = sums.find(checksum_algorithm::md5); it != sums.end()) {
            return sstring(fmt::format("\"{}\"", to_hex(it->second)));
        }
    }
    return sstring(fmt::format("\"{:016x}\"", info.generation));
}

// A part's ETag: the MD5 of its data.
static sstring etag(const part_info& p) {
    auto it = p.checksums.find(checksum_algorithm::md5);
    return sstring(fmt::format("\"{}\"", it != p.checksums.end() ? to_hex(it->second) : ""));
}

// The checksum algorithm of an upload, as S3 has one: the one the client
// asked for or, by default, CRC64NVME. The storage keeps it along with the
// MD5 and the CRC64NVME every upload keeps.
static checksum_algorithm upload_algorithm(const std::set<checksum_algorithm>& algorithms) {
    for (auto a : algorithms) {
        if (a != checksum_algorithm::md5 && a != checksum_algorithm::crc64nvme) {
            return a;
        }
    }
    return checksum_algorithm::crc64nvme;
}

// Whether a multipart upload's checksum of the algorithm is COMPOSITE (a
// checksum of its parts' ones) rather than FULL_OBJECT (of all its data).
// Only S3's default type of each algorithm is supported: COMPOSITE, but
// for CRC64NVME, which can only be FULL_OBJECT.
static bool is_composite(checksum_algorithm alg) {
    return alg != checksum_algorithm::crc64nvme;
}

// The name of an algorithm in x-amz-checksum-algorithm and XML elements.
static sstring algorithm_name(checksum_algorithm alg) {
    sstring ret(to_string(alg));
    std::ranges::transform(ret, ret.begin(), [] (char c) { return std::toupper(c); });
    return ret;
}

// S3's checksum of an object or a part.
struct s3_checksum {
    checksum_algorithm algorithm;
    // Base64; for a COMPOSITE one, followed by -<number of parts>.
    sstring value;
    bool composite;
};

static std::optional<s3_checksum> object_checksum(const object_info& info) {
    auto alg = upload_algorithm(info.attrs.checksums);
    if (info.upload == upload_kind::multipart && is_composite(alg)) {
        checksummer sum({alg});
        for (auto& p : info.parts) {
            auto it = p.checksums.find(alg);
            if (it == p.checksums.end()) {
                return std::nullopt;
            }
            sum.update(it->second);
        }
        return s3_checksum{alg, sstring(fmt::format("{}-{}", to_base64(sum.finish().at(alg)), info.parts.size())), true};
    }
    auto sums = object_checksums(info);
    auto it = sums.find(alg);
    if (it == sums.end()) {
        return std::nullopt;
    }
    return s3_checksum{alg, to_base64(it->second), false};
}

static void add_checksum_headers(reply& rep, const s3_checksum& c) {
    rep.add_header(fmt::format("x-amz-checksum-{}", to_string(c.algorithm)), c.value);
    rep.add_header("x-amz-checksum-type", c.composite ? "COMPOSITE" : "FULL_OBJECT");
}

static void write_checksum(xml_writer& w, const s3_checksum& c) {
    w.element(fmt::format("Checksum{}", algorithm_name(c.algorithm)), c.value);
    w.element("ChecksumType", c.composite ? "COMPOSITE" : "FULL_OBJECT");
}

static unsigned part_number(std::string_view s) {
    auto n = parse_number<unsigned>(s);
    if (!n || *n < 1 || *n > max_part_number) {
        throw invalid_argument("Part number must be an integer between 1 and 10000, inclusive");
    }
    return *n;
}

// A part listed in a CompleteMultipartUpload request.
struct completed_part {
    unsigned number;
    sstring etag;
    checksums sums;
};

// Parses a CompleteMultipartUpload request body.
static std::vector<completed_part> parse_completed_parts(std::string_view body) {
    pugi::xml_document doc;
    if (!doc.load_buffer(body.data(), body.size())) {
        throw malformed_xml();
    }
    auto root = doc.child("CompleteMultipartUpload");
    if (!root) {
        throw malformed_xml();
    }
    std::vector<completed_part> ret;
    for (auto p : root.children("Part")) {
        auto n = parse_number<unsigned>(p.child_value("PartNumber"));
        if (!n) {
            throw malformed_xml();
        }
        std::string_view tag = p.child_value("ETag");
        if (tag.size() >= 2 && tag.front() == '"' && tag.back() == '"') {
            tag = tag.substr(1, tag.size() - 2);
        }
        completed_part cp{*n, sstring(fmt::format("\"{}\"", tag)), {}};
        for (auto alg : {checksum_algorithm::crc32, checksum_algorithm::crc32c, checksum_algorithm::crc64nvme,
                         checksum_algorithm::sha1, checksum_algorithm::sha256}) {
            auto e = p.child(fmt::format("Checksum{}", algorithm_name(alg)).c_str());
            if (e) {
                auto v = from_base64(e.child_value());
                if (!v) {
                    throw invalid_request(fmt::format("Value for Checksum{} is invalid.", algorithm_name(alg)));
                }
                cp.sums.emplace(alg, std::move(*v));
            }
        }
        ret.push_back(std::move(cp));
    }
    if (ret.empty()) {
        throw malformed_xml();
    }
    return ret;
}

// The objects listed in a DeleteObjects request.
struct objects_to_delete {
    bool quiet;
    std::vector<sstring> keys;
};

// Parses a DeleteObjects request body.
static objects_to_delete parse_objects_to_delete(std::string_view body) {
    pugi::xml_document doc;
    // Keep a key that is all whitespace.
    if (!doc.load_buffer(body.data(), body.size(), pugi::parse_default | pugi::parse_ws_pcdata_single)) {
        throw malformed_xml();
    }
    auto root = doc.child("Delete");
    if (!root) {
        throw malformed_xml();
    }
    objects_to_delete ret{root.child("Quiet").text().as_bool(), {}};
    for (auto o : root.children("Object")) {
        auto key = o.child("Key");
        if (!key) {
            throw malformed_xml();
        }
        for (auto name : {"VersionId", "ETag", "LastModifiedTime", "Size"}) {
            if (o.child(name)) {
                throw not_implemented(fmt::format("The {} of an object to delete", name));
            }
        }
        ret.keys.emplace_back(key.child_value());
    }
    if (ret.keys.empty() || ret.keys.size() > max_delete_keys) {
        throw malformed_xml();
    }
    return ret;
}

// The headers of GetObject and HeadObject replies. The checksums of the
// object only go with the whole object, and when asked for.
static void add_object_headers(reply& rep, const request& req, const object_info& info, bool whole) {
    rep.add_header("ETag", etag(info));
    rep.add_header("Last-Modified", http_date(info.mtime));
    rep.add_header("Accept-Ranges", "bytes");
    for (auto& [k, v] : info.attrs.metadata) {
        rep.add_header(fmt::format("x-amz-meta-{}", k), v);
    }
    if (whole && equals_ignoring_case(req.get_header("x-amz-checksum-mode"), "ENABLED")) {
        if (auto c = object_checksum(info)) {
            add_checksum_headers(rep, *c);
        }
    }
}

// Sets up a GetObject or HeadObject reply for the range, if any, and
// returns the length of the body.
static uint64_t set_range(reply& rep, const std::optional<byte_range>& range, uint64_t size) {
    if (!range) {
        rep.set_status(status::ok);
        return size;
    }
    auto rr = resolve_range(*range, size);
    rep.add_header("Content-Range", fmt::format("bytes {}-{}/{}", rr.offset, rr.offset + rr.length - 1, size));
    rep.set_status(status::partial_content);
    return rr.length;
}

// Whether the comma-separated entity tags of an If-Match or If-None-Match
// header include the ETag, comparing weakly or strongly (where weak tags,
// W/"...", match nothing); "*" includes any. Tags may come without quotes.
static bool etag_listed(std::string_view header, std::string_view tag, bool weak) {
    auto unquoted = [] (std::string_view t) {
        return t.size() >= 2 && t.front() == '"' && t.back() == '"' ? t.substr(1, t.size() - 2) : t;
    };
    tag = unquoted(tag);
    for (auto part : header | std::views::split(',')) {
        std::string_view t(part.begin(), part.end());
        while (!t.empty() && t.front() == ' ') {
            t.remove_prefix(1);
        }
        while (!t.empty() && t.back() == ' ') {
            t.remove_suffix(1);
        }
        if (t == "*") {
            return true;
        }
        if (t.starts_with("W/")) {
            if (!weak) {
                continue;
            }
            t.remove_prefix(2);
        }
        if (unquoted(t) == tag) {
            return true;
        }
    }
    return false;
}

// Evaluates the conditional headers of GetObject and HeadObject against
// the version of the object read, in the order HTTP does, and returns the
// status to reply with instead of the object, if any: 412 or 304. A date
// that does not parse makes its header ignored. CopyObject's conditions on
// its source are the same headers with the x-amz-copy-source- prefix.
static std::optional<status> check_conditions(const request& req, const object_info& info,
                                              std::string_view prefix = "") {
    auto header = [&] (std::string_view name) { return sstring(fmt::format("{}{}", prefix, name)); };
    auto tag = etag(info);
    // As Last-Modified reports it.
    auto modified = std::chrono::time_point_cast<std::chrono::seconds>(info.mtime);
    if (req._headers.contains(header("If-Match"))) {
        if (!etag_listed(req.get_header(header("If-Match")), tag, false)) {
            return static_cast<status>(412);
        }
    } else if (auto date = parse_http_date(req.get_header(header("If-Unmodified-Since")))) {
        if (modified > *date) {
            return static_cast<status>(412);
        }
    }
    if (req._headers.contains(header("If-None-Match"))) {
        if (etag_listed(req.get_header(header("If-None-Match")), tag, true)) {
            return status::not_modified;
        }
    } else if (auto date = parse_http_date(req.get_header(header("If-Modified-Since")))) {
        if (modified <= *date) {
            return status::not_modified;
        }
    }
    return std::nullopt;
}

// The reply to a read whose conditions failed with `st`: a 412 error, or a
// 304 with the object's ETag and Last-Modified and no body.
static std::unique_ptr<reply> condition_reply(std::unique_ptr<reply> rep, const object_info& info, status st) {
    if (st != status::not_modified) {
        throw storage_error(errc::precondition_failed, "");
    }
    rep->add_header("ETag", etag(info));
    rep->add_header("Last-Modified", http_date(info.mtime));
    rep->set_status(st);
    rep->skip_body();
    // The length a 200 would have; skip_body() keeps the writer from being called.
    rep->write_body(std::nullopt, info.size, [] (output_stream<char>&) { return make_ready_future(); });
    return rep;
}

static void check_read_headers(const request& req) {
    reject_query_params(req, {"acl", "attributes", "legal-hold", "partNumber", "restore", "retention", "select",
                              "tagging", "torrent", "uploads", "versionId",
                              "response-cache-control", "response-content-disposition", "response-content-encoding",
                              "response-content-language", "response-content-type", "response-expires"});
}

static future<> send_data(input_stream<char> in, output_stream<char>& out) {
    std::exception_ptr ex;
    try {
        co_await copy(in, out);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

//
// Listing
//

// What a ListObjects(V2) request asks for.
struct list_request {
    sstring bucket;
    sstring prefix;
    sstring delimiter;
    // List what follows this key.
    sstring after;
    unsigned max_keys = max_list_keys;
    bool url_encoded = false;
};

// Keys and common prefixes in byte order, up to max_keys of them together.
struct list_result {
    std::vector<object_info> objects;
    std::vector<sstring> prefixes;
    bool truncated = false;
    // The last key or prefix listed.
    sstring last;
};

//
// Handlers
//

// Runs a request and renders errors as S3 error replies.
class s3_handler final : public httpd::handler_base {
public:
    using func_type = std::function<future<std::unique_ptr<reply>>(request&, std::unique_ptr<reply>)>;

private:
    func_type _func;

    static sstring next_request_id() {
        static thread_local uint64_t counter = 0;
        return sstring(fmt::format("{:x}-{:x}", this_shard_id(), ++counter));
    }

    static std::unique_ptr<reply> error_reply(const request& req, const s3_error& e, const sstring& id) {
        xml_writer w;
        w.open("Error")
            .element("Code", e.code())
            .element("Message", e.what())
            .element("Resource", url_path(req))
            .element("RequestId", id)
            .close("Error");
        auto rep = xml_reply(std::make_unique<reply>(), w, e.get_status());
        rep->add_header("x-amz-request-id", id);
        // This reply replaces the one the server set up for the request.
        if (req._method == "HEAD") {
            rep->skip_body();
        }
        return rep;
    }

public:
    explicit s3_handler(func_type func) : _func(std::move(func)) {}

    future<std::unique_ptr<reply>> handle(const sstring&, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        auto id = next_request_id();
        std::exception_ptr ex;
        try {
            auto ret = co_await _func(*req, std::move(rep));
            ret->add_header("x-amz-request-id", id);
            co_return ret;
        } catch (...) {
            ex = std::current_exception();
        }
        try {
            std::rethrow_exception(ex);
        } catch (const s3_error& e) {
            co_return error_reply(*req, e, id);
        } catch (const storage_error& e) {
            co_return error_reply(*req, to_s3_error(e), id);
        } catch (const checksum_mismatch& e) {
            co_return error_reply(*req, s3_error(status::bad_request, "BadDigest",
                                                 "The checksum you specified did not match the calculated checksum."), id);
        } catch (const std::exception& e) {
            s3log.warn("{} {}: {}", req->_method, req->_url, e.what());
            co_return error_reply(*req, s3_error(status::internal_server_error, "InternalError",
                                                 "We encountered an internal error. Please try again."), id);
        }
    }
};

class s3_facade final : public facade {
    storage_service& _storage;

public:
    explicit s3_facade(storage_service& s) : _storage(s) {}

    void set_routes(httpd::routes& r) override {
        using enum httpd::operation_type;

        r.put(GET, "/", handler(&s3_facade::list_buckets));

        // Bucket rules go first: an object rule would match a bare bucket
        // path too, with an empty key.
        r.add(bucket_rule(&s3_facade::list_objects), GET);
        r.add(bucket_rule(&s3_facade::head_bucket), HEAD);
        r.add(bucket_rule(&s3_facade::create_bucket), PUT);
        r.add(bucket_rule(&s3_facade::delete_bucket), DELETE);
        r.add(bucket_rule(&s3_facade::post_bucket), POST);

        r.add(object_rule(&s3_facade::get_object), GET);
        r.add(object_rule(&s3_facade::head_object), HEAD);
        r.add(object_rule(&s3_facade::put_object), PUT);
        r.add(object_rule(&s3_facade::delete_object), DELETE);
        r.add(object_rule(&s3_facade::post_object), POST);
    }

private:
    using method = future<std::unique_ptr<reply>> (s3_facade::*)(request&, std::unique_ptr<reply>);

    s3_handler* handler(method m) {
        return new s3_handler([this, m] (request& req, std::unique_ptr<reply> rep) {
            return (this->*m)(req, std::move(rep));
        });
    }

    httpd::match_rule* bucket_rule(method m) {
        auto rule = new httpd::match_rule(handler(m));
        rule->add_param("bucket", false);
        return rule;
    }

    httpd::match_rule* object_rule(method m) {
        auto rule = new httpd::match_rule(handler(m));
        rule->add_param("bucket", false);
        rule->add_param("key", true);
        return rule;
    }

    // Buckets

    future<std::unique_ptr<reply>> list_buckets(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"bucket-region", "continuation-token", "max-buckets", "prefix"});
        auto buckets = co_await _storage.list_buckets();
        xml_writer w;
        w.root("ListAllMyBucketsResult");
        w.open("Owner").element("ID", owner_id).element("DisplayName", owner_id).close("Owner");
        w.open("Buckets");
        for (auto& b : buckets) {
            // The storage does not keep the creation time.
            w.open("Bucket").element("Name", b).element("CreationDate", iso8601_date(clock_type::time_point())).close("Bucket");
        }
        w.close("Buckets");
        w.close("ListAllMyBucketsResult");
        co_return xml_reply(std::move(rep), w);
    }

    future<std::unique_ptr<reply>> create_bucket(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"accelerate", "acl", "analytics", "cors", "encryption", "intelligent-tiering",
                                  "inventory", "lifecycle", "logging", "metrics", "notification", "object-lock",
                                  "ownershipControls", "policy", "publicAccessBlock", "replication",
                                  "requestPayment", "tagging", "versioning", "website"});
        auto bucket = bucket_param(req);
        validate_bucket_name(bucket);
        // The body, if any, is a CreateBucketConfiguration with the
        // bucket's location, which does not matter here.
        co_await util::skip_entire_stream(*req.content_stream);
        co_await _storage.create_bucket(bucket);
        rep->add_header("Location", fmt::format("/{}", bucket));
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> delete_bucket(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"analytics", "cors", "encryption", "intelligent-tiering", "inventory", "lifecycle",
                                  "metrics", "ownershipControls", "policy", "publicAccessBlock", "replication",
                                  "tagging", "website"});
        co_await _storage.delete_bucket(bucket_param(req));
        co_return empty_reply(std::move(rep), status::no_content);
    }

    future<> check_bucket(const sstring& bucket) {
        auto buckets = co_await _storage.list_buckets();
        if (std::ranges::find(buckets, bucket) == buckets.end()) {
            throw storage_error(errc::no_such_bucket, "");
        }
    }

    future<std::unique_ptr<reply>> head_bucket(request& req, std::unique_ptr<reply> rep) {
        co_await check_bucket(bucket_param(req));
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> post_bucket(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("delete")) {
            co_return co_await delete_objects(req, std::move(rep));
        }
        throw not_implemented("POST on a bucket");
    }

    // Deletes the keys one by one, as DeleteObject does, so a key that
    // does not exist is deleted too. A key that fails to be deleted gets an
    // error of its own, and the others are deleted still.
    future<std::unique_ptr<reply>> delete_objects(request& req, std::unique_ptr<reply> rep) {
        check_plain_body(req);
        auto sums = upload_checksums_from_headers(req);
        if (sums.expected.empty()) {
            throw invalid_request("Missing required header for this request: Content-MD5");
        }
        auto in = make_verifying_stream(*req.content_stream, std::move(sums.expected));
        auto objects = parse_objects_to_delete(co_await util::read_entire_stream_contiguous(in));
        auto bucket = bucket_param(req);
        co_await check_bucket(bucket);

        std::vector<std::optional<s3_error>> errors(objects.keys.size());
        co_await max_concurrent_for_each(std::views::iota(size_t(0), objects.keys.size()), delete_concurrency,
                [&] (size_t i) -> future<> {
            errors[i] = co_await delete_key(bucket, objects.keys[i]);
        });

        xml_writer w;
        w.root("DeleteResult");
        for (size_t i = 0; i < objects.keys.size(); i++) {
            if (errors[i]) {
                w.open("Error")
                    .element("Key", objects.keys[i])
                    .element("Code", errors[i]->code())
                    .element("Message", errors[i]->what())
                    .close("Error");
            } else if (!objects.quiet) {
                w.open("Deleted").element("Key", objects.keys[i]).close("Deleted");
            }
        }
        w.close("DeleteResult");
        co_return xml_reply(std::move(rep), w);
    }

    future<std::optional<s3_error>> delete_key(const sstring& bucket, const sstring& key) {
        try {
            check_key(key);
            co_await _storage.delete_object(bucket, key);
        } catch (const s3_error& e) {
            co_return e;
        } catch (const storage_error& e) {
            if (e.code() != errc::no_such_key) {
                co_return to_s3_error(e);
            }
        }
        co_return std::nullopt;
    }

    future<std::unique_ptr<reply>> list_objects(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"accelerate", "acl", "analytics", "cors", "encryption", "intelligent-tiering",
                                  "inventory", "lifecycle", "location", "logging", "metrics", "notification",
                                  "object-lock", "ownershipControls", "policy", "policyStatus", "publicAccessBlock",
                                  "replication", "requestPayment", "tagging", "uploads", "versioning", "versions",
                                  "website"});
        bool v2 = false;
        if (req.has_query_param("list-type")) {
            if (req.get_query_param("list-type") != "2") {
                throw invalid_argument("Invalid List Type specified in Request");
            }
            v2 = true;
        }

        list_request lr;
        lr.bucket = bucket_param(req);
        lr.prefix = req.get_query_param("prefix");
        lr.delimiter = req.get_query_param("delimiter");
        if (req.has_query_param("encoding-type")) {
            if (req.get_query_param("encoding-type") != "url") {
                throw invalid_argument("Invalid Encoding Method specified in Request");
            }
            lr.url_encoded = true;
        }
        if (req.has_query_param("max-keys")) {
            auto n = parse_number<unsigned>(req.get_query_param("max-keys"));
            if (!n) {
                throw invalid_argument("Provided max-keys not an integer or within integer range");
            }
            lr.max_keys = std::min(*n, max_list_keys);
        }
        sstring token;
        if (v2 && req.has_query_param("continuation-token")) {
            auto t = from_base64(req.get_query_param("continuation-token"));
            if (!t) {
                throw invalid_argument("The continuation token provided is incorrect");
            }
            token = *t;
            lr.after = token;
        } else if (v2) {
            lr.after = req.get_query_param("start-after");
        } else {
            lr.after = req.get_query_param("marker");
        }

        auto res = co_await list(lr);

        auto encoded = [&] (std::string_view s) { return lr.url_encoded ? url_encode(s) : sstring(s); };
        xml_writer w;
        w.root("ListBucketResult");
        w.element("Name", lr.bucket);
        w.element("Prefix", encoded(lr.prefix));
        if (!lr.delimiter.empty()) {
            w.element("Delimiter", encoded(lr.delimiter));
        }
        w.element("MaxKeys", lr.max_keys);
        if (lr.url_encoded) {
            w.element("EncodingType", "url");
        }
        w.element("IsTruncated", res.truncated ? "true" : "false");
        if (v2) {
            w.element("KeyCount", res.objects.size() + res.prefixes.size());
            if (req.has_query_param("continuation-token")) {
                w.element("ContinuationToken", req.get_query_param("continuation-token"));
            }
            if (res.truncated) {
                w.element("NextContinuationToken", to_base64(res.last));
            }
            if (req.has_query_param("start-after")) {
                w.element("StartAfter", encoded(req.get_query_param("start-after")));
            }
        } else {
            w.element("Marker", encoded(lr.after));
            // Without a delimiter, the last key listed is the next marker.
            if (res.truncated && !lr.delimiter.empty()) {
                w.element("NextMarker", encoded(res.last));
            }
        }
        bool owner = !v2 || req.get_query_param("fetch-owner") == "true";
        for (auto& o : res.objects) {
            w.open("Contents");
            w.element("Key", encoded(o.key));
            w.element("LastModified", iso8601_date(o.mtime));
            w.element("ETag", etag(o));
            w.element("Size", o.size);
            w.element("StorageClass", "STANDARD");
            if (owner) {
                w.open("Owner").element("ID", owner_id).element("DisplayName", owner_id).close("Owner");
            }
            w.close("Contents");
        }
        for (auto& p : res.prefixes) {
            w.open("CommonPrefixes").element("Prefix", encoded(p)).close("CommonPrefixes");
        }
        w.close("ListBucketResult");
        co_return xml_reply(std::move(rep), w);
    }

    // Lists the keys after lr.after, rolling those that have the delimiter
    // after the prefix into common prefixes (up to and including it). A
    // common prefix counts once, and only if it sorts after lr.after, so
    // that listing after a prefix skips its keys.
    future<list_result> list(const list_request& lr) {
        list_result ret;
        sstring cursor = lr.after;
        unsigned count = 0;
        while (true) {
            auto l = co_await _storage.list_objects(lr.bucket, lr.prefix, cursor, max_list_keys);
            for (auto& o : l.objects) {
                cursor = o.key;
                std::optional<sstring> common;
                if (!lr.delimiter.empty()) {
                    auto pos = o.key.find(lr.delimiter, lr.prefix.size());
                    if (pos != sstring::npos) {
                        common = o.key.substr(0, pos + lr.delimiter.size());
                    }
                }
                if (common && (!(lr.after < *common) || (!ret.prefixes.empty() && ret.prefixes.back() == *common))) {
                    continue;
                }
                if (count == lr.max_keys) {
                    ret.truncated = true;
                    co_return ret;
                }
                count++;
                if (common) {
                    ret.last = *common;
                    ret.prefixes.push_back(std::move(*common));
                } else {
                    ret.last = o.key;
                    ret.objects.push_back(std::move(o));
                }
            }
            if (!l.truncated) {
                co_return ret;
            }
        }
    }

    // Objects

    // Objects, or their multipart uploads with ?uploadId= (and ?uploads)

    future<std::unique_ptr<reply>> get_object(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("uploadId")) {
            return list_parts(req, std::move(rep));
        }
        return get_whole_object(req, std::move(rep));
    }

    future<std::unique_ptr<reply>> put_object(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("uploadId") || req.has_query_param("partNumber")) {
            return upload_part(req, std::move(rep));
        }
        if (req._headers.contains("x-amz-copy-source")) {
            return copy_object(req, std::move(rep));
        }
        return put_whole_object(req, std::move(rep));
    }

    future<std::unique_ptr<reply>> delete_object(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("uploadId")) {
            return abort_multipart_upload(req, std::move(rep));
        }
        return delete_whole_object(req, std::move(rep));
    }

    future<std::unique_ptr<reply>> post_object(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("uploads")) {
            return create_multipart_upload(req, std::move(rep));
        }
        if (req.has_query_param("uploadId")) {
            return complete_multipart_upload(req, std::move(rep));
        }
        reject_query_params(req, {"restore", "select"});
        throw not_implemented("POST on an object");
    }

    // Objects

    future<std::unique_ptr<reply>> get_whole_object(request& req, std::unique_ptr<reply> rep) {
        check_read_headers(req);
        auto range = range_from_headers(req);
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto f = co_await coroutine::as_future(_storage.read_object(bucket, key, range));
        if (f.failed()) {
            // The conditions go before the range, so a range the object
            // does not have is only reported if they hold.
            auto ex = f.get_exception();
            if (!is_storage_error(ex, errc::invalid_range)) {
                std::rethrow_exception(ex);
            }
            auto info = co_await _storage.head_object(bucket, key);
            if (auto st = check_conditions(req, info)) {
                co_return condition_reply(std::move(rep), info, *st);
            }
            std::rethrow_exception(ex);
        }
        auto r = f.get();
        if (auto st = check_conditions(req, r.info)) {
            co_await r.data.close();
            co_return condition_reply(std::move(rep), r.info, *st);
        }
        add_object_headers(*rep, req, r.info, !range);
        // The storage resolved the range already, so this does not throw.
        auto length = set_range(*rep, range, r.info.size);
        rep->write_body(default_object_type, length, [data = std::move(r.data)] (output_stream<char>& out) mutable {
            return send_data(std::move(data), out);
        });
        co_return std::move(rep);
    }

    future<std::unique_ptr<reply>> head_object(request& req, std::unique_ptr<reply> rep) {
        check_read_headers(req);
        auto range = range_from_headers(req);
        auto info = co_await _storage.head_object(bucket_param(req), key_param(req));
        if (auto st = check_conditions(req, info)) {
            co_return condition_reply(std::move(rep), info, *st);
        }
        add_object_headers(*rep, req, info, !range);
        auto length = set_range(*rep, range, info.size);
        // A reply to HEAD reports the length without writing the body.
        rep->write_body(default_object_type, length, [] (output_stream<char>&) { return make_ready_future(); });
        co_return std::move(rep);
    }

    future<std::unique_ptr<reply>> put_whole_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"acl", "legal-hold", "retention", "tagging"});
        reject_headers(req, {"x-amz-tagging"});
        check_plain_body(req);
        auto pre = write_precondition(req);
        auto sums = upload_checksums_from_headers(req);
        object_attrs attrs;
        attrs.metadata = metadata_from_headers(req);
        attrs.checksums = default_checksums;
        attrs.checksums.insert(sums.algorithms.begin(), sums.algorithms.end());

        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = co_await _storage.open_session(bucket, key, std::move(attrs));
        object_info info;
        std::exception_ptr ex;
        try {
            if (sums.expected.empty()) {
                co_await _storage.write_part(bucket, key, id, 1, *req.content_stream);
            } else {
                auto in = make_verifying_stream(*req.content_stream, std::move(sums.expected));
                co_await _storage.write_part(bucket, key, id, 1, in);
            }
            info = co_await _storage.commit_session(bucket, key, id, {1}, pre, upload_kind::single);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            // Report the original error, not a failure to clean up after it.
            auto f = co_await coroutine::as_future(_storage.abort_session(bucket, key, id));
            f.ignore_ready_future();
            std::rethrow_exception(ex);
        }

        rep->add_header("ETag", etag(info));
        if (auto c = object_checksum(info)) {
            add_checksum_headers(*rep, *c);
        }
        co_return empty_reply(std::move(rep), status::ok);
    }

    // The version of a copy's source to copy, which the
    // x-amz-copy-source-if-* conditions hold for.
    future<object_info> copy_source_info(const request& req, const sstring& bucket, const sstring& key) {
        auto info = co_await _storage.head_object(bucket, key);
        // Unlike a read's, all the conditions fail the copy alike.
        if (check_conditions(req, info, "x-amz-copy-source-")) {
            throw storage_error(errc::precondition_failed, "");
        }
        co_return info;
    }

    // A copy is a new single upload of the source's data, whose checksums
    // the storage derives from the source's parts where it can, and
    // computes from the data otherwise. So its ETag is the MD5 of its data,
    // even when the source's is a multipart one.
    future<std::unique_ptr<reply>> copy_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"acl", "legal-hold", "retention", "tagging"});
        reject_headers(req, {"x-amz-tagging"});
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto src = copy_source(req);
        if (src.bucket != bucket) {
            throw not_implemented("Copying from another bucket");
        }
        auto pre = write_precondition(req);
        auto directive = req.get_header("x-amz-metadata-directive");
        if (!directive.empty() && directive != "COPY" && directive != "REPLACE") {
            throw invalid_argument("Unknown metadata directive.");
        }
        bool replace = directive == "REPLACE";
        if (src.key == key && !replace) {
            throw invalid_request("This copy request is illegal because it is trying to copy an object to itself "
                                  "without changing the object's metadata, storage class, website redirect location "
                                  "or encryption attributes.");
        }
        auto alg = requested_algorithm(req);

        object_info info;
        for (;;) {
            auto src_info = co_await copy_source_info(req, bucket, src.key);
            object_attrs attrs;
            attrs.metadata = replace ? metadata_from_headers(req) : src_info.attrs.metadata;
            attrs.checksums = default_checksums;
            attrs.checksums.insert(alg.value_or(upload_algorithm(src_info.attrs.checksums)));

            auto id = co_await _storage.open_session(bucket, key, std::move(attrs));
            bool copied = false;
            std::exception_ptr ex;
            try {
                co_await _storage.copy_part(bucket, key, id, 1, src.key, std::nullopt,
                                            precondition{src_info.generation});
                copied = true;
                info = co_await _storage.commit_session(bucket, key, id, {1}, pre, upload_kind::single);
            } catch (...) {
                ex = std::current_exception();
            }
            if (!ex) {
                break;
            }
            // Report the original error, not a failure to clean up after it.
            auto f = co_await coroutine::as_future(_storage.abort_session(bucket, key, id));
            f.ignore_ready_future();
            // The source changed after its conditions were checked: check
            // them against its new version.
            if (copied || !is_storage_error(ex, errc::precondition_failed)) {
                std::rethrow_exception(ex);
            }
        }

        xml_writer w;
        w.root("CopyObjectResult");
        w.element("ETag", etag(info)).element("LastModified", iso8601_date(info.mtime));
        if (auto c = object_checksum(info)) {
            write_checksum(w, *c);
        }
        w.close("CopyObjectResult");
        co_return xml_reply(std::move(rep), w);
    }

    future<std::unique_ptr<reply>> delete_whole_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"tagging", "versionId"});
        try {
            co_await _storage.delete_object(bucket_param(req), key_param(req));
        } catch (const storage_error& e) {
            // Deleting a key that does not exist succeeds.
            if (e.code() != errc::no_such_key) {
                throw;
            }
        }
        co_return empty_reply(std::move(rep), status::no_content);
    }

    // Multipart uploads, which are the storage's sessions

    future<std::unique_ptr<reply>> create_multipart_upload(request& req, std::unique_ptr<reply> rep) {
        reject_headers(req, {"x-amz-tagging"});
        object_attrs attrs;
        attrs.metadata = metadata_from_headers(req);
        attrs.checksums = default_checksums;
        auto alg = requested_algorithm(req);
        if (alg) {
            attrs.checksums.insert(*alg);
        }
        if (auto type = req.get_header("x-amz-checksum-type"); !type.empty()) {
            if (!alg) {
                throw invalid_request("The x-amz-checksum-type header requires the x-amz-checksum-algorithm header.");
            }
            if (type != "COMPOSITE" && type != "FULL_OBJECT") {
                throw invalid_request("Value for x-amz-checksum-type header is invalid.");
            }
            if ((type == "COMPOSITE") != is_composite(*alg)) {
                if (*alg == checksum_algorithm::crc32 || *alg == checksum_algorithm::crc32c) {
                    throw not_implemented(fmt::format("The {} checksum type of {}", type, algorithm_name(*alg)));
                }
                throw invalid_request(fmt::format("The {} checksum type cannot be used with the {} checksum algorithm.",
                                                  type, algorithm_name(*alg)));
            }
        }

        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = co_await _storage.open_session(bucket, key, std::move(attrs));
        if (alg) {
            rep->add_header("x-amz-checksum-algorithm", algorithm_name(*alg));
            rep->add_header("x-amz-checksum-type", is_composite(*alg) ? "COMPOSITE" : "FULL_OBJECT");
        }
        xml_writer w;
        w.root("InitiateMultipartUploadResult");
        w.element("Bucket", bucket).element("Key", key).element("UploadId", id);
        w.close("InitiateMultipartUploadResult");
        co_return xml_reply(std::move(rep), w);
    }

    future<std::unique_ptr<reply>> upload_part(request& req, std::unique_ptr<reply> rep) {
        if (!req.has_query_param("uploadId") || !req.has_query_param("partNumber")) {
            throw invalid_request("Uploading a part requires both partNumber and uploadId");
        }
        if (req._headers.contains("x-amz-copy-source")) {
            co_return co_await upload_part_copy(req, std::move(rep));
        }
        check_plain_body(req);
        auto number = part_number(req.get_query_param("partNumber"));
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = req.get_query_param("uploadId");
        auto sums = upload_checksums_from_headers(req);

        // A part may carry a checksum of another algorithm than its upload
        // only if the upload has the default one; it is verified, not kept.
        auto session = co_await _storage.describe_session(bucket, key, id);
        auto alg = upload_algorithm(session.attrs.checksums);
        if (alg != checksum_algorithm::crc64nvme) {
            for (auto a : sums.algorithms) {
                if (a != alg) {
                    throw invalid_request(fmt::format("Checksum Type mismatch occurred, expected checksum Type: {}, "
                                                      "actual checksum Type: {}", to_string(alg), to_string(a)));
                }
            }
        }

        part_info p;
        if (sums.expected.empty()) {
            p = co_await _storage.write_part(bucket, key, id, number, *req.content_stream);
        } else {
            auto in = make_verifying_stream(*req.content_stream, sums.expected);
            p = co_await _storage.write_part(bucket, key, id, number, in);
        }

        rep->add_header("ETag", etag(p));
        if (auto it = p.checksums.find(alg); it != p.checksums.end()) {
            rep->add_header(fmt::format("x-amz-checksum-{}", to_string(alg)), to_base64(it->second));
        }
        for (auto& [a, value] : sums.expected) {
            if (a != checksum_algorithm::md5 && a != alg) {
                rep->add_header(fmt::format("x-amz-checksum-{}", to_string(a)), to_base64(value));
            }
        }
        co_return empty_reply(std::move(rep), status::ok);
    }

    // UploadPartCopy: the source's data (or the range of it) becomes the
    // part, with the checksums of its upload, as for CopyObject.
    future<std::unique_ptr<reply>> upload_part_copy(request& req, std::unique_ptr<reply> rep) {
        auto number = part_number(req.get_query_param("partNumber"));
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = req.get_query_param("uploadId");
        auto src = copy_source(req);
        if (src.bucket != bucket) {
            throw not_implemented("Copying from another bucket");
        }
        auto range = copy_source_range(req);
        auto session = co_await _storage.describe_session(bucket, key, id);
        auto alg = upload_algorithm(session.attrs.checksums);

        part_info p;
        for (;;) {
            auto src_info = co_await copy_source_info(req, bucket, src.key);
            if (range && *range->last >= src_info.size) {
                throw invalid_argument(fmt::format("Range specified is not valid for source object of size: {}",
                                                   src_info.size));
            }
            auto f = co_await coroutine::as_future(_storage.copy_part(bucket, key, id, number, src.key, range,
                                                                      precondition{src_info.generation}));
            if (!f.failed()) {
                p = f.get();
                break;
            }
            // The source changed after its conditions were checked: check
            // them against its new version.
            auto ex = f.get_exception();
            if (!is_storage_error(ex, errc::precondition_failed)) {
                std::rethrow_exception(ex);
            }
        }

        xml_writer w;
        w.root("CopyPartResult");
        w.element("ETag", etag(p)).element("LastModified", iso8601_date(clock_type::now()));
        if (auto it = p.checksums.find(alg); it != p.checksums.end()) {
            w.element(fmt::format("Checksum{}", algorithm_name(alg)), to_base64(it->second));
        }
        w.close("CopyPartResult");
        co_return xml_reply(std::move(rep), w);
    }

    future<std::unique_ptr<reply>> complete_multipart_upload(request& req, std::unique_ptr<reply> rep) {
        auto pre = write_precondition(req);
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = req.get_query_param("uploadId");
        auto parts = parse_completed_parts(co_await util::read_entire_stream_contiguous(*req.content_stream));
        std::vector<unsigned> numbers;
        for (auto& p : parts) {
            if (!numbers.empty() && p.number <= numbers.back()) {
                throw s3_error(status::bad_request, "InvalidPartOrder",
                               "The list of parts was not in ascending order. Parts must be ordered by part number.");
            }
            numbers.push_back(p.number);
        }

        // A completed upload is no session any more, and completing it
        // again gets the same reply from the storage.
        auto f = co_await coroutine::as_future(_storage.describe_session(bucket, key, id));
        if (f.failed()) {
            auto ex = f.get_exception();
            try {
                std::rethrow_exception(ex);
            } catch (const storage_error& e) {
                if (e.code() != errc::no_such_session) {
                    throw;
                }
            }
        } else {
            check_completed_parts(req, f.get(), parts);
        }

        auto info = co_await _storage.commit_session(bucket, key, id, std::move(numbers), pre, upload_kind::multipart);
        xml_writer w;
        w.root("CompleteMultipartUploadResult");
        w.element("Location", url_path(req)).element("Bucket", bucket).element("Key", key).element("ETag", etag(info));
        if (auto c = object_checksum(info)) {
            write_checksum(w, *c);
        }
        w.close("CompleteMultipartUploadResult");
        co_return xml_reply(std::move(rep), w);
    }

    // Checks the parts listed to complete an upload against the uploaded
    // ones, and the object they make against the request's headers.
    static void check_completed_parts(const request& req, const session_info& session, const std::vector<completed_part>& parts) {
        std::map<unsigned, const part_info*> uploaded;
        for (auto& p : session.parts) {
            uploaded.emplace(p.number, &p);
        }
        object_info whole;
        whole.attrs = session.attrs;
        whole.upload = upload_kind::multipart;
        for (size_t i = 0; i < parts.size(); i++) {
            auto it = uploaded.find(parts[i].number);
            if (it == uploaded.end() || etag(*it->second) != parts[i].etag) {
                throw invalid_part();
            }
            auto& p = *it->second;
            for (auto& [alg, value] : parts[i].sums) {
                if (auto s = p.checksums.find(alg); s != p.checksums.end() && s->second != value) {
                    throw invalid_part();
                }
            }
            if (i + 1 < parts.size() && p.size < min_part_size) {
                throw s3_error(status::bad_request, "EntityTooSmall",
                               "Your proposed upload is smaller than the minimum allowed object size.");
            }
            whole.parts.push_back(p);
            whole.size += p.size;
        }

        if (auto size = req.get_header("x-amz-mp-object-size"); !size.empty()) {
            if (parse_number<uint64_t>(size) != whole.size) {
                throw invalid_request("The provided x-amz-mp-object-size does not match the size of the object.");
            }
        }
        // The checksum of the whole object, if given, must be the object's.
        auto c = object_checksum(whole);
        for_each_prefixed_header(req, checksum_header_prefix, [&] (const sstring& name, const sstring& value) {
            if (name == "type" || name == "algorithm" || name == "mode") {
                return;
            }
            if (!c || name != to_string(c->algorithm)) {
                throw invalid_request(fmt::format("The upload has no {} checksum.", name));
            }
            if (value != c->value) {
                throw s3_error(status::bad_request, "BadDigest",
                               "The checksum you specified did not match the calculated checksum.");
            }
        });
    }

    future<std::unique_ptr<reply>> abort_multipart_upload(request& req, std::unique_ptr<reply> rep) {
        co_await _storage.abort_session(bucket_param(req), key_param(req), req.get_query_param("uploadId"));
        co_return empty_reply(std::move(rep), status::no_content);
    }

    future<std::unique_ptr<reply>> list_parts(request& req, std::unique_ptr<reply> rep) {
        unsigned max_parts = max_list_parts;
        if (req.has_query_param("max-parts")) {
            auto n = parse_number<unsigned>(req.get_query_param("max-parts"));
            if (!n) {
                throw invalid_argument("Provided max-parts not an integer or within integer range");
            }
            max_parts = std::min(*n, max_list_parts);
        }
        unsigned marker = 0;
        if (req.has_query_param("part-number-marker")) {
            auto n = parse_number<unsigned>(req.get_query_param("part-number-marker"));
            if (!n) {
                throw invalid_argument("Provided part-number-marker not an integer or within integer range");
            }
            marker = *n;
        }
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto session = co_await _storage.describe_session(bucket, key, req.get_query_param("uploadId"));
        auto alg = upload_algorithm(session.attrs.checksums);

        std::vector<const part_info*> listed;
        bool truncated = false;
        for (auto& p : session.parts) {
            if (p.number <= marker) {
                continue;
            }
            if (listed.size() == max_parts) {
                truncated = true;
                break;
            }
            listed.push_back(&p);
        }

        xml_writer w;
        w.root("ListPartsResult");
        w.element("Bucket", bucket).element("Key", key).element("UploadId", session.id);
        w.element("PartNumberMarker", marker);
        w.element("NextPartNumberMarker", listed.empty() ? marker : listed.back()->number);
        w.element("MaxParts", max_parts);
        w.element("IsTruncated", truncated ? "true" : "false");
        for (auto* p : listed) {
            w.open("Part");
            w.element("PartNumber", p->number);
            // The storage does not keep when parts were uploaded.
            w.element("LastModified", iso8601_date(clock_type::time_point()));
            w.element("ETag", etag(*p));
            w.element("Size", p->size);
            if (auto it = p->checksums.find(alg); it != p->checksums.end()) {
                w.element(fmt::format("Checksum{}", algorithm_name(alg)), to_base64(it->second));
            }
            w.close("Part");
        }
        w.open("Initiator").element("ID", owner_id).element("DisplayName", owner_id).close("Initiator");
        w.open("Owner").element("ID", owner_id).element("DisplayName", owner_id).close("Owner");
        w.element("StorageClass", "STANDARD");
        w.element("ChecksumAlgorithm", algorithm_name(alg));
        w.element("ChecksumType", is_composite(alg) ? "COMPOSITE" : "FULL_OBJECT");
        w.close("ListPartsResult");
        co_return xml_reply(std::move(rep), w);
    }
};

facade_type make_facade_type() {
    return facade_type{
        .name = "s3",
        .description = "The Amazon S3 REST API, path-style; see docs/s3-protocol.md for what is supported",
        .add_options = [] (bpo::options_description&) {},
        .make = [] (const bpo::variables_map&, storage_service& s) -> std::unique_ptr<facade> {
            return std::make_unique<s3_facade>(s);
        },
    };
}

} // namespace labaz::s3
