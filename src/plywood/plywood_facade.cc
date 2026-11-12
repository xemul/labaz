/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// The protocol is described in docs/plywood-protocol.md.

#include "plywood/plywood_facade.hh"

#include <charconv>
#include <ranges>

#include <fmt/format.h>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/matchrules.hh>

namespace labaz::plywood {

using namespace seastar;
using reply = http::reply;
using request = http::request;
using status = http::reply::status_type;
namespace bpo = boost::program_options;

static constexpr std::string_view meta_header_prefix = "x-labaz-meta-";
static constexpr std::string_view checksum_header_prefix = "x-labaz-checksum-";
static constexpr unsigned default_max_keys = 1000;

class bad_request : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

template <typename T>
static T parse_number(std::string_view s, std::string_view what) {
    T v;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || p != s.data() + s.size()) {
        throw bad_request(fmt::format("bad {} '{}'", what, s));
    }
    return v;
}

static status error_status(errc e) {
    switch (e) {
    case errc::no_such_bucket:
    case errc::no_such_key:
    case errc::no_such_session:
    case errc::no_such_part:
        return status::not_found;
    case errc::bucket_exists:
    case errc::bucket_not_empty:
        return status::conflict;
    case errc::precondition_failed:
        return static_cast<status>(412);  // not in seastar's list
    case errc::invalid_argument:
        return status::bad_request;
    case errc::invalid_range:
        return static_cast<status>(416);  // not in seastar's list
    }
    return status::internal_server_error;
}

//
// JSON
//

using json_writer = rapidjson::Writer<rapidjson::StringBuffer>;

static void write_string(json_writer& w, std::string_view s) {
    w.String(s.data(), s.size());
}

static void write_metadata(json_writer& w, const user_metadata& md) {
    w.StartObject();
    for (auto& [k, v] : md) {
        write_string(w, k);
        write_string(w, v);
    }
    w.EndObject();
}

static void write_checksums(json_writer& w, const checksums& sums) {
    w.StartObject();
    for (auto& [alg, value] : sums) {
        write_string(w, to_string(alg));
        write_string(w, to_hex(value));
    }
    w.EndObject();
}

static void write_part_info(json_writer& w, const part_info& p) {
    w.StartObject();
    w.Key("number"); w.Uint(p.number);
    w.Key("size"); w.Uint64(p.size);
    w.Key("checksums"); write_checksums(w, p.checksums);
    w.EndObject();
}

static void write_object_info(json_writer& w, const object_info& info) {
    w.StartObject();
    w.Key("key"); write_string(w, info.key);
    w.Key("size"); w.Uint64(info.size);
    w.Key("generation"); w.Uint64(info.generation);
    w.Key("multipart"); w.Bool(info.upload == upload_kind::multipart);
    w.Key("mtime_ns"); w.Int64(std::chrono::duration_cast<std::chrono::nanoseconds>(info.mtime.time_since_epoch()).count());
    w.Key("metadata"); write_metadata(w, info.attrs.metadata);
    w.Key("checksums"); write_checksums(w, object_checksums(info));
    w.Key("parts");
    w.StartArray();
    for (auto& p : info.parts) {
        write_part_info(w, p);
    }
    w.EndArray();
    w.EndObject();
}

template <typename Func>
static std::unique_ptr<reply> json_reply(std::unique_ptr<reply> rep, Func func) {
    rapidjson::StringBuffer sb;
    json_writer w(sb);
    func(w);
    rep->set_status(status::ok);
    rep->write_body("json", sstring(sb.GetString(), sb.GetSize()));
    return rep;
}

static std::unique_ptr<reply> error_reply(status st, std::string_view code, std::string_view msg) {
    auto rep = json_reply(std::make_unique<reply>(), [&] (json_writer& w) {
        w.StartObject();
        w.Key("error"); write_string(w, code);
        w.Key("message"); write_string(w, msg);
        w.EndObject();
    });
    rep->set_status(st);
    return rep;
}

static std::unique_ptr<reply> empty_reply(std::unique_ptr<reply> rep, status st = status::no_content) {
    rep->set_status(st);
    return rep;
}

//
// Request helpers
//

static sstring bucket_param(const request& req) {
    auto b = req.get_path_param("bucket");
    if (b.empty()) {
        throw bad_request("bad bucket name in the path");
    }
    return b;
}

static sstring key_param(const request& req) {
    // The routes drop a trailing '/' from the path, but such keys are not
    // supported, so refuse them rather than store a different key.
    std::string_view path = req._url;
    if (path.substr(0, path.find('?')).ends_with('/')) {
        throw storage_error(errc::invalid_argument, "a key must not end with '/'");
    }
    return req.get_path_param("key");
}

// Calls func(suffix, value) for every header named `prefix` (lower case)
// followed by a non-empty suffix, with the suffix lower-cased.
template <typename Func>
static void for_each_prefixed_header(const request& req, std::string_view prefix, Func func) {
    for (auto& [name, value] : req._headers) {
        if (name.size() > prefix.size()
                && std::ranges::equal(std::string_view(name).substr(0, prefix.size()), prefix,
                                      [] (char a, char b) { return std::tolower(a) == b; })) {
            sstring suffix = name.substr(prefix.size());
            std::ranges::transform(suffix, suffix.begin(), [] (char c) { return std::tolower(c); });
            func(suffix, value);
        }
    }
}

static checksum_algorithm parse_checksum_algorithm(std::string_view name) {
    auto alg = checksum_algorithm_from_string(name);
    if (!alg) {
        throw bad_request(fmt::format("unknown checksum algorithm '{}'", name));
    }
    return *alg;
}

static object_attrs attrs_from_headers(const request& req) {
    object_attrs attrs;
    for_each_prefixed_header(req, meta_header_prefix, [&] (const sstring& k, const sstring& value) {
        attrs.metadata[k] = value;
    });
    auto algs = req.get_header("X-Labaz-Checksums");
    for (auto name : std::string_view(algs) | std::views::split(',')) {
        if (!algs.empty()) {
            attrs.checksums.insert(parse_checksum_algorithm(std::string_view(name)));
        }
    }
    return attrs;
}

// The checksums the body must have, from X-Labaz-Checksum-<algorithm>.
static checksums expected_checksums(const request& req) {
    checksums ret;
    for_each_prefixed_header(req, checksum_header_prefix, [&] (const sstring& name, const sstring& value) {
        auto bytes = from_hex(value);
        if (!bytes) {
            throw bad_request(fmt::format("bad {} checksum '{}'", name, value));
        }
        ret.emplace(parse_checksum_algorithm(name), std::move(*bytes));
    });
    return ret;
}

static precondition precondition_from_headers(const request& req) {
    precondition pre;
    auto v = req.get_header("X-Labaz-If-Generation-Match");
    if (!v.empty()) {
        pre.generation_match = parse_number<uint64_t>(v, "generation");
    }
    return pre;
}

// Parses an HTTP-style "bytes=first-last" range.
static byte_range parse_range(std::string_view v) {
    std::string_view s = v;
    if (!s.starts_with("bytes=") || s.find(',') != std::string_view::npos) {
        throw bad_request(fmt::format("unsupported range '{}'", s));
    }
    s.remove_prefix(6);
    auto dash = s.find('-');
    if (dash == std::string_view::npos || (dash == 0 && dash + 1 == s.size())) {
        throw bad_request(fmt::format("bad range '{}'", v));
    }
    byte_range r;
    if (dash > 0) {
        r.first = parse_number<uint64_t>(s.substr(0, dash), "range");
    }
    if (dash + 1 < s.size()) {
        r.last = parse_number<uint64_t>(s.substr(dash + 1), "range");
    }
    return r;
}

static std::optional<byte_range> range_from_headers(const request& req) {
    auto v = req.get_header("Range");
    if (v.empty()) {
        return std::nullopt;
    }
    return parse_range(v);
}

// The copy source's range, from ?copy-range=.
static std::optional<byte_range> copy_range(const request& req) {
    if (!req.has_query_param("copy-range")) {
        return std::nullopt;
    }
    return parse_range(req.get_query_param("copy-range"));
}

// The copy source's precondition, from ?copy-if-generation-match=.
static precondition copy_precondition(const request& req) {
    precondition pre;
    if (req.has_query_param("copy-if-generation-match")) {
        pre.generation_match = parse_number<uint64_t>(req.get_query_param("copy-if-generation-match"), "generation");
    }
    return pre;
}

// Whether the request addresses a session (?session=) rather than the object.
// A session request takes ?part= if and only if `takes_part`; an object
// request never does.
static bool is_session_request(const request& req, bool takes_part) {
    bool session = req.has_query_param("session");
    bool part = req.has_query_param("part");
    if (part && !session) {
        throw bad_request("'part' requires 'session'");
    }
    if (session && part != takes_part) {
        throw bad_request(takes_part ? "'session' requires 'part'" : "unexpected 'part'");
    }
    return session;
}

// Whether the request copies from another object of the bucket (?copy-key=)
// rather than carrying the data. Only allowed if `allowed`.
static bool is_copy_request(const request& req, bool allowed) {
    bool copy = req.has_query_param("copy-key");
    if (!copy && (req.has_query_param("copy-range") || req.has_query_param("copy-if-generation-match"))) {
        throw bad_request("'copy-range' and 'copy-if-generation-match' require 'copy-key'");
    }
    if (copy && !allowed) {
        throw bad_request("unexpected 'copy-key'");
    }
    if (copy && !expected_checksums(req).empty()) {
        throw bad_request("a copy has no body to verify");
    }
    return copy;
}

static void add_object_headers(reply& rep, const object_info& info) {
    rep.add_header("X-Labaz-Size", fmt::to_string(info.size));
    rep.add_header("X-Labaz-Generation", fmt::to_string(info.generation));
    for (auto& [alg, value] : object_checksums(info)) {
        rep.add_header(fmt::format("X-Labaz-Checksum-{}", to_string(alg)), to_hex(value));
    }
    for (auto& [k, v] : info.attrs.metadata) {
        rep.add_header(fmt::format("X-Labaz-Meta-{}", k), v);
    }
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
// Handlers
//

// Runs a request and renders storage and request errors as JSON replies.
class plywood_handler final : public httpd::handler_base {
public:
    using func_type = std::function<future<std::unique_ptr<reply>>(request&, std::unique_ptr<reply>)>;

private:
    func_type _func;

public:
    explicit plywood_handler(func_type func) : _func(std::move(func)) {}

    future<std::unique_ptr<reply>> handle(const sstring&, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        std::unique_ptr<reply> err;
        try {
            co_return co_await _func(*req, std::move(rep));
        } catch (const storage_error& e) {
            err = error_reply(error_status(e.code()), to_string(e.code()), e.what());
        } catch (const bad_request& e) {
            err = error_reply(status::bad_request, "bad_request", e.what());
        } catch (const checksum_mismatch& e) {
            err = error_reply(status::bad_request, "bad_digest", e.what());
        }
        // The error reply replaces the one the server set up for the request.
        if (req->_method == "HEAD") {
            err->skip_body();
        }
        co_return std::move(err);
    }
};

class plywood_facade final : public facade {
    storage_service& _storage;

public:
    explicit plywood_facade(storage_service& s) : _storage(s) {}

    void set_routes(httpd::routes& r) override {
        using enum httpd::operation_type;

        r.put(GET, "/", handler(&plywood_facade::list_buckets));

        // Bucket rules go first: an object rule would match a bare bucket
        // path too, with an empty key.
        r.add(bucket_rule(&plywood_facade::list_objects), GET);
        r.add(bucket_rule(&plywood_facade::create_bucket), PUT);
        r.add(bucket_rule(&plywood_facade::delete_bucket), DELETE);

        r.add(object_rule(&plywood_facade::get_object_or_session), GET);
        r.add(object_rule(&plywood_facade::head_object), HEAD);
        r.add(object_rule(&plywood_facade::put_object_or_session), PUT);
        r.add(object_rule(&plywood_facade::delete_object_or_session), DELETE);
        r.add(object_rule(&plywood_facade::open_session), POST);
        r.add(object_rule(&plywood_facade::commit_session), PATCH);
    }

private:
    using method = future<std::unique_ptr<reply>> (plywood_facade::*)(request&, std::unique_ptr<reply>);

    plywood_handler* handler(method m) {
        return new plywood_handler([this, m] (request& req, std::unique_ptr<reply> rep) {
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

    future<std::unique_ptr<reply>> list_buckets(request&, std::unique_ptr<reply> rep) {
        auto buckets = co_await _storage.list_buckets();
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            w.Key("buckets");
            w.StartArray();
            for (auto& b : buckets) {
                write_string(w, b);
            }
            w.EndArray();
            w.EndObject();
        });
    }

    future<std::unique_ptr<reply>> create_bucket(request& req, std::unique_ptr<reply> rep) {
        co_await _storage.create_bucket(bucket_param(req));
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> delete_bucket(request& req, std::unique_ptr<reply> rep) {
        co_await _storage.delete_bucket(bucket_param(req));
        co_return empty_reply(std::move(rep));
    }

    future<std::unique_ptr<reply>> list_objects(request& req, std::unique_ptr<reply> rep) {
        unsigned max_keys = default_max_keys;
        if (req.has_query_param("max-keys")) {
            max_keys = parse_number<unsigned>(req.get_query_param("max-keys"), "max-keys");
        }
        auto l = co_await _storage.list_objects(bucket_param(req), req.get_query_param("prefix"),
                                                req.get_query_param("start-after"), max_keys);
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            w.Key("objects");
            w.StartArray();
            for (auto& o : l.objects) {
                write_object_info(w, o);
            }
            w.EndArray();
            w.Key("truncated"); w.Bool(l.truncated);
            w.EndObject();
        });
    }

    // Objects, or their sessions with ?session=

    future<std::unique_ptr<reply>> get_object_or_session(request& req, std::unique_ptr<reply> rep) {
        is_copy_request(req, false);
        if (is_session_request(req, false)) {
            return get_session(req, std::move(rep));
        }
        return get_object(req, std::move(rep));
    }

    future<std::unique_ptr<reply>> put_object_or_session(request& req, std::unique_ptr<reply> rep) {
        bool copy = is_copy_request(req, true);
        if (is_session_request(req, true)) {
            return copy ? copy_session(req, std::move(rep)) : put_session(req, std::move(rep));
        }
        return copy ? copy_object(req, std::move(rep)) : put_object(req, std::move(rep));
    }

    future<std::unique_ptr<reply>> delete_object_or_session(request& req, std::unique_ptr<reply> rep) {
        is_copy_request(req, false);
        if (is_session_request(req, false)) {
            return delete_session(req, std::move(rep));
        }
        return delete_object(req, std::move(rep));
    }

    // Objects

    future<std::unique_ptr<reply>> get_object(request& req, std::unique_ptr<reply> rep) {
        auto range = range_from_headers(req);
        auto r = co_await _storage.read_object(bucket_param(req), key_param(req), range);
        add_object_headers(*rep, r.info);
        if (range) {
            auto rr = resolve_range(*range, r.info.size);
            rep->add_header("Content-Range", fmt::format("bytes {}-{}/{}", rr.offset, rr.offset + rr.length - 1, r.info.size));
            rep->set_status(status::partial_content);
        } else {
            rep->set_status(status::ok);
        }
        rep->write_body("bin", [data = std::move(r.data)] (output_stream<char>& out) mutable {
            return send_data(std::move(data), out);
        });
        co_return std::move(rep);
    }

    future<std::unique_ptr<reply>> head_object(request& req, std::unique_ptr<reply> rep) {
        auto info = co_await _storage.head_object(bucket_param(req), key_param(req));
        add_object_headers(*rep, info);
        co_return empty_reply(std::move(rep), status::ok);
    }

    future<std::unique_ptr<reply>> put_object(request& req, std::unique_ptr<reply> rep) {
        return single_part_upload(req, std::move(rep), [this, &req] (sstring bucket, sstring key, session_id id) {
            return write_body(std::move(bucket), std::move(key), std::move(id), 1, req);
        });
    }

    // Copies another object of the bucket, or a range of it, as the object.
    future<std::unique_ptr<reply>> copy_object(request& req, std::unique_ptr<reply> rep) {
        return single_part_upload(req, std::move(rep), [this, &req] (sstring bucket, sstring key, session_id id) {
            return _storage.copy_part(std::move(bucket), std::move(key), std::move(id), 1,
                                      req.get_query_param("copy-key"), copy_range(req), copy_precondition(req));
        });
    }

    // Makes the object of a session whose only part `make_part` makes.
    template <typename Func>
    future<std::unique_ptr<reply>> single_part_upload(request& req, std::unique_ptr<reply> rep, Func make_part) {
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto pre = precondition_from_headers(req);
        auto id = co_await _storage.open_session(bucket, key, attrs_from_headers(req));
        object_info info;
        std::exception_ptr ex;
        try {
            co_await make_part(bucket, key, id);
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
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object_info(w, info); });
    }

    future<std::unique_ptr<reply>> delete_object(request& req, std::unique_ptr<reply> rep) {
        co_await _storage.delete_object(bucket_param(req), key_param(req));
        co_return empty_reply(std::move(rep));
    }

    // Sessions

    future<std::unique_ptr<reply>> open_session(request& req, std::unique_ptr<reply> rep) {
        auto id = co_await _storage.open_session(bucket_param(req), key_param(req), attrs_from_headers(req));
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            w.Key("session"); write_string(w, id);
            w.EndObject();
        });
    }

    future<std::unique_ptr<reply>> get_session(request& req, std::unique_ptr<reply> rep) {
        auto info = co_await _storage.describe_session(bucket_param(req), key_param(req), req.get_query_param("session"));
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            w.Key("session"); write_string(w, info.id);
            w.Key("bucket"); write_string(w, info.bucket);
            w.Key("key"); write_string(w, info.key);
            w.Key("metadata"); write_metadata(w, info.attrs.metadata);
            w.Key("checksums");
            w.StartArray();
            for (auto alg : info.attrs.checksums) {
                write_string(w, to_string(alg));
            }
            w.EndArray();
            w.Key("parts");
            w.StartArray();
            for (auto& p : info.parts) {
                write_part_info(w, p);
            }
            w.EndArray();
            w.EndObject();
        });
    }

    // Uploads a part.
    future<std::unique_ptr<reply>> put_session(request& req, std::unique_ptr<reply> rep) {
        auto number = parse_number<unsigned>(req.get_query_param("part"), "part number");
        auto p = co_await write_body(bucket_param(req), key_param(req), req.get_query_param("session"), number, req);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_part_info(w, p); });
    }

    // Writes the request body as a part, verified against the
    // X-Labaz-Checksum-<algorithm> headers, if any.
    future<part_info> write_body(sstring bucket, sstring key, session_id id, unsigned number, request& req) {
        auto expected = expected_checksums(req);
        if (expected.empty()) {
            co_return co_await _storage.write_part(std::move(bucket), std::move(key), std::move(id), number, *req.content_stream);
        }
        auto in = make_verifying_stream(*req.content_stream, std::move(expected));
        co_return co_await _storage.write_part(std::move(bucket), std::move(key), std::move(id), number, in);
    }

    // Aborts the session.
    future<std::unique_ptr<reply>> delete_session(request& req, std::unique_ptr<reply> rep) {
        co_await _storage.abort_session(bucket_param(req), key_param(req), req.get_query_param("session"));
        co_return empty_reply(std::move(rep));
    }

    // Copies another object of the bucket, or a range of it, as a part.
    future<std::unique_ptr<reply>> copy_session(request& req, std::unique_ptr<reply> rep) {
        auto number = parse_number<unsigned>(req.get_query_param("part"), "part number");
        auto p = co_await _storage.copy_part(bucket_param(req), key_param(req), req.get_query_param("session"), number,
                                             req.get_query_param("copy-key"), copy_range(req), copy_precondition(req));
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_part_info(w, p); });
    }

    future<std::unique_ptr<reply>> commit_session(request& req, std::unique_ptr<reply> rep) {
        if (!req.has_query_param("session")) {
            throw bad_request("missing 'session'");
        }
        auto bucket = bucket_param(req);
        auto key = key_param(req);
        auto id = req.get_query_param("session");

        std::vector<unsigned> parts;
        if (req.has_query_param("parts")) {
            // An empty list commits no parts: an empty object.
            auto list = req.get_query_param("parts");
            for (auto n : std::string_view(list) | std::views::split(',')) {
                if (!list.empty()) {
                    parts.push_back(parse_number<unsigned>(std::string_view(n), "part number"));
                }
            }
        } else {
            // All sealed parts, in number order.
            auto info = co_await _storage.describe_session(bucket, key, id);
            for (auto& p : info.parts) {
                parts.push_back(p.number);
            }
        }

        auto info = co_await _storage.commit_session(bucket, key, id, std::move(parts), precondition_from_headers(req),
                                                     upload_kind::multipart);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object_info(w, info); });
    }
};

facade_type make_facade_type() {
    return facade_type{
        .name = "plywood",
        .description = "Testing-only HTTP/JSON protocol mapping 1:1 to the storage API",
        .add_options = [] (bpo::options_description&) {},
        .make = [] (const bpo::variables_map&, storage_service& s) -> std::unique_ptr<facade> {
            return std::make_unique<plywood_facade>(s);
        },
    };
}

} // namespace labaz::plywood
