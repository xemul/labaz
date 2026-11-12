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

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/matchrules.hh>
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
static constexpr size_t max_key_length = 1024;
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

static sstring key_param(const request& req) {
    // The routes drop a trailing '/' from the path, but keys ending with
    // '/' are not supported, so refuse them rather than use another key.
    if (url_path(req).ends_with('/')) {
        throw invalid_argument("Keys ending with '/' are not supported");
    }
    auto key = req.get_path_param("key");
    if (key.size() > max_key_length) {
        throw s3_error(status::bad_request, "KeyTooLongError", "Your key is too long");
    }
    return key;
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

// S3's ETag of an object: the MD5 of its data for a single part. For
// several parts, each with an MD5, the MD5 of their MD5s followed by the
// number of parts. Objects without MD5s get one made of their generation.
static sstring etag(const object_info& info) {
    if (info.parts.size() <= 1) {
        auto sums = object_checksums(info);
        if (auto it = sums.find(checksum_algorithm::md5); it != sums.end()) {
            return sstring(fmt::format("\"{}\"", to_hex(it->second)));
        }
    } else if (std::ranges::all_of(info.parts, [] (const part_info& p) { return p.checksums.contains(checksum_algorithm::md5); })) {
        checksummer md5({checksum_algorithm::md5});
        for (auto& p : info.parts) {
            md5.update(p.checksums.at(checksum_algorithm::md5));
        }
        return sstring(fmt::format("\"{}-{}\"", to_hex(md5.finish().at(checksum_algorithm::md5)), info.parts.size()));
    }
    return sstring(fmt::format("\"{:016x}\"", info.generation));
}

static void add_checksum_headers(reply& rep, const checksums& sums) {
    for (auto& [alg, value] : sums) {
        if (alg != checksum_algorithm::md5) {
            rep.add_header(fmt::format("x-amz-checksum-{}", to_string(alg)), to_base64(value));
        }
    }
    rep.add_header("x-amz-checksum-type", "FULL_OBJECT");
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
        add_checksum_headers(rep, object_checksums(info));
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

// Conditional reads are not implemented.
static void check_read_headers(const request& req) {
    reject_headers(req, {"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"});
    reject_query_params(req, {"acl", "attributes", "legal-hold", "partNumber", "restore", "retention", "select",
                              "tagging", "torrent", "uploadId", "uploads", "versionId",
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

    future<std::unique_ptr<reply>> head_bucket(request& req, std::unique_ptr<reply> rep) {
        auto bucket = bucket_param(req);
        auto buckets = co_await _storage.list_buckets();
        if (std::ranges::find(buckets, bucket) == buckets.end()) {
            throw storage_error(errc::no_such_bucket, "");
        }
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> post_bucket(request& req, std::unique_ptr<reply>) {
        reject_query_params(req, {"delete"});
        throw not_implemented("POST on a bucket");
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

    future<std::unique_ptr<reply>> get_object(request& req, std::unique_ptr<reply> rep) {
        check_read_headers(req);
        auto range = range_from_headers(req);
        auto r = co_await _storage.read_object(bucket_param(req), key_param(req), range);
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
        add_object_headers(*rep, req, info, !range);
        auto length = set_range(*rep, range, info.size);
        // A reply to HEAD reports the length without writing the body.
        rep->write_body(default_object_type, length, [] (output_stream<char>&) { return make_ready_future(); });
        co_return std::move(rep);
    }

    future<std::unique_ptr<reply>> put_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"acl", "legal-hold", "partNumber", "retention", "tagging", "uploadId"});
        reject_headers(req, {"x-amz-copy-source", "x-amz-tagging", "If-Match"});
        check_plain_body(req);
        precondition pre;
        if (req._headers.contains("If-None-Match")) {
            if (req.get_header("If-None-Match") != "*") {
                throw not_implemented("An If-None-Match header other than '*'");
            }
            pre.generation_match = 0;
        }
        auto sums = upload_checksums_from_headers(req);
        object_attrs attrs;
        for_each_prefixed_header(req, meta_header_prefix, [&] (const sstring& k, const sstring& value) {
            attrs.metadata[k] = value;
        });
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
            info = co_await _storage.commit_session(bucket, key, id, {1}, pre);
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
        // The checksums of the algorithms asked for, or the default one.
        auto all = object_checksums(info);
        checksums reported;
        for (auto alg : sums.algorithms.empty() ? std::set{checksum_algorithm::crc64nvme} : sums.algorithms) {
            reported.emplace(alg, all.at(alg));
        }
        add_checksum_headers(*rep, reported);
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> delete_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"tagging", "uploadId", "versionId"});
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

    future<std::unique_ptr<reply>> post_object(request& req, std::unique_ptr<reply>) {
        reject_query_params(req, {"restore", "select", "uploadId", "uploads"});
        throw not_implemented("POST on an object");
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
