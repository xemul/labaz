/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// The protocol is described in docs/gcs-protocol.md.

#include "gcs/gcs_facade.hh"

#include <charconv>
#include <ranges>

#include <fmt/format.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/matchrules.hh>
#include <seastar/util/log.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/util/short_streams.hh>

#include "http_util.hh"
#include "listing.hh"
#include "streams.hh"

namespace labaz::gcs {

using namespace seastar;
using reply = http::reply;
using request = http::request;
using status = http::reply::status_type;
namespace bpo = boost::program_options;

static logger gcslog("gcs");

static constexpr unsigned max_list_results = 1000;
static constexpr unsigned max_compose_sources = 32;
static constexpr size_t max_name_length = 1024;
// Every chunk of a resumable upload but the last is a multiple of it.
static constexpr uint64_t chunk_granularity = 256 << 10;
static constexpr std::string_view object_content_type = "application/octet-stream";
// What GCS answers a cancelled resumable upload with.
static constexpr int client_closed_request = 499;

// The checksums GCS keeps of an object.
static const std::set<checksum_algorithm> object_checksums_kept = {checksum_algorithm::md5, checksum_algorithm::crc32c};

//
// Errors
//

// An error reply: {"error": {"code": ..., "message": ..., "errors": [{..., "reason": ...}]}}.
class gcs_error : public std::runtime_error {
    status _status;
    sstring _reason;
public:
    gcs_error(int st, sstring reason, const std::string& msg)
        : std::runtime_error(msg), _status(static_cast<status>(st)), _reason(std::move(reason)) {}
    status get_status() const noexcept { return _status; }
    const sstring& reason() const noexcept { return _reason; }
};

static gcs_error invalid(const std::string& msg) {
    return gcs_error(400, "invalid", msg);
}

static gcs_error required(std::string_view what) {
    return gcs_error(400, "required", fmt::format("Required {}", what));
}

static gcs_error not_implemented(std::string_view what) {
    return gcs_error(501, "notImplemented", fmt::format("{} is not implemented", what));
}

// What a read whose ifGenerationNotMatch or ifMetagenerationNotMatch does
// not hold gets.
static gcs_error not_modified() {
    return gcs_error(304, "notModified", "");
}

// The GCS error for a storage error, with GCS's own message where it has one.
static gcs_error to_gcs_error(const storage_error& e, std::string_view bucket, std::string_view name) {
    switch (e.code()) {
    case errc::no_such_bucket:
        return gcs_error(404, "notFound", "The specified bucket does not exist.");
    case errc::no_such_key:
        return gcs_error(404, "notFound", fmt::format("No such object: {}/{}", bucket, name));
    case errc::no_such_session:
        return gcs_error(404, "notFound", "No such upload session.");
    case errc::no_such_part:
        return invalid(e.what());
    case errc::bucket_exists:
        return gcs_error(409, "conflict", "Your previous request to create the named bucket succeeded and you already own it.");
    case errc::bucket_not_empty:
        return gcs_error(409, "conflict", "The bucket you tried to delete is not empty.");
    case errc::precondition_failed:
        return gcs_error(412, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.");
    case errc::invalid_argument:
        return invalid(e.what());
    case errc::invalid_range:
        return gcs_error(416, "requestedRangeNotSatisfiable", "The requested range cannot be satisfied.");
    }
    return gcs_error(500, "backendError", e.what());
}

//
// JSON
//

using json_writer = rapidjson::Writer<rapidjson::StringBuffer>;

static void write_string(json_writer& w, std::string_view s) {
    w.String(s.data(), s.size());
}

static void field(json_writer& w, std::string_view name, std::string_view value) {
    w.Key(name.data(), name.size());
    write_string(w, value);
}

template <typename Func>
static std::unique_ptr<reply> json_reply(std::unique_ptr<reply> rep, Func func, status st = status::ok) {
    rapidjson::StringBuffer sb;
    json_writer w(sb);
    func(w);
    rep->set_status(st);
    rep->write_body("json", sstring(sb.GetString(), sb.GetSize()));
    return rep;
}

static std::unique_ptr<reply> empty_reply(std::unique_ptr<reply> rep, status st) {
    rep->set_status(st);
    return rep;
}

static std::unique_ptr<reply> error_reply(const gcs_error& e) {
    if (e.get_status() == static_cast<status>(304)) {
        return empty_reply(std::make_unique<reply>(), e.get_status());
    }
    return json_reply(std::make_unique<reply>(), [&] (json_writer& w) {
        w.StartObject();
        w.Key("error");
        w.StartObject();
        w.Key("code"); w.Int(int(e.get_status()));
        field(w, "message", e.what());
        w.Key("errors");
        w.StartArray();
        w.StartObject();
        field(w, "message", e.what());
        field(w, "domain", "global");
        field(w, "reason", e.reason());
        w.EndObject();
        w.EndArray();
        w.EndObject();
        w.EndObject();
    }, e.get_status());
}

//
// Requests
//

template <typename T>
static T parse_number(std::string_view s, std::string_view what) {
    T v;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || p != s.data() + s.size()) {
        throw invalid(fmt::format("Invalid value for {}: {}", what, s));
    }
    return v;
}

static std::optional<uint64_t> query_number(const request& req, const sstring& name) {
    if (!req.has_query_param(name)) {
        return std::nullopt;
    }
    return parse_number<uint64_t>(req.get_query_param(name), name);
}

static void reject_query_params(const request& req, std::initializer_list<std::string_view> params) {
    for (auto p : params) {
        if (req.has_query_param(sstring(p))) {
            throw not_implemented(fmt::format("The '{}' parameter", p));
        }
    }
}

// The scheme and host the client reached the server at, for links back.
static sstring base_url(const request& req) {
    return sstring(fmt::format("{}://{}", req.get_protocol_name(), req.get_header("Host")));
}

static sstring bucket_param(const request& req) {
    return req.get_path_param("bucket");
}

// GCS's object naming rules, beyond the storage's.
static void check_object_name(std::string_view name) {
    if (name.empty()) {
        throw required("parameter: name");
    }
    if (name.size() > max_name_length) {
        throw invalid(fmt::format("The maximum object length is {} characters, but got a name with {} characters",
                                  max_name_length, name.size()));
    }
    if (name == "." || name == "..") {
        throw invalid("Object name cannot be . or ..");
    }
    if (name.find_first_of("\r\n") != std::string_view::npos) {
        throw invalid("Object name contains invalid characters");
    }
}

static sstring object_param(const request& req) {
    auto name = req.get_path_param("object");
    check_object_name(name);
    return name;
}

// GCS's bucket naming rules: 3 to 63 lower case letters, digits, '-', '_'
// and '.', beginning and ending with a letter or a digit.
static void check_bucket_name(std::string_view b) {
    auto alnum = [] (char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    if (b.size() < 3 || b.size() > 63 || !alnum(b.front()) || !alnum(b.back())
            || !std::ranges::all_of(b, [&] (char c) { return alnum(c) || c == '-' || c == '_' || c == '.'; })) {
        throw invalid(fmt::format("Invalid bucket name: '{}'", b));
    }
}

// The precondition of a write. The storage only checks the generation.
static precondition write_precondition(const request& req) {
    reject_query_params(req, {"ifGenerationNotMatch", "ifMetagenerationMatch", "ifMetagenerationNotMatch"});
    precondition pre;
    pre.generation_match = query_number(req, "ifGenerationMatch");
    return pre;
}

// Checks the conditions of a read against the version read. Objects have
// no other version, and are never changed in place: their metageneration
// is always 1.
static void check_read_conditions(const request& req, const object_info& info) {
    if (auto g = query_number(req, "generation"); g && *g != info.generation) {
        throw storage_error(errc::no_such_key, "");
    }
    auto gen_match = query_number(req, "ifGenerationMatch");
    auto meta_match = query_number(req, "ifMetagenerationMatch");
    if ((gen_match && *gen_match != info.generation) || (meta_match && *meta_match != 1)) {
        throw storage_error(errc::precondition_failed, "");
    }
    auto gen_not_match = query_number(req, "ifGenerationNotMatch");
    auto meta_not_match = query_number(req, "ifMetagenerationNotMatch");
    if ((gen_not_match && *gen_not_match == info.generation) || (meta_not_match && *meta_not_match == 1)) {
        throw not_modified();
    }
}

// What an upload request's JSON object resource says, of what is kept.
struct object_resource {
    sstring name;
    // Whether the resource has metadata, which a copy takes from its
    // source otherwise.
    bool has_metadata = false;
    user_metadata metadata;
    // The data's checksums, which it must have.
    checksums expected;
};

static rapidjson::Document parse_json(std::string_view body) {
    rapidjson::Document doc;
    doc.Parse(body.data(), body.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        throw invalid("Invalid JSON payload received.");
    }
    return doc;
}

static object_resource to_object_resource(const rapidjson::Value& doc) {
    object_resource ret;
    if (!doc.IsObject()) {
        throw invalid("Invalid value for an object resource");
    }
    if (auto it = doc.FindMember("name"); it != doc.MemberEnd() && !it->value.IsNull()) {
        if (!it->value.IsString()) {
            throw invalid("Invalid value for name");
        }
        ret.name = sstring(it->value.GetString(), it->value.GetStringLength());
    }
    if (auto it = doc.FindMember("metadata"); it != doc.MemberEnd() && !it->value.IsNull()) {
        ret.has_metadata = true;
        if (!it->value.IsObject()) {
            throw invalid("Invalid value for metadata");
        }
        for (auto& m : it->value.GetObject()) {
            if (m.value.IsNull()) {
                continue;
            }
            if (!m.value.IsString()) {
                throw invalid(fmt::format("Invalid value for metadata.{}", m.name.GetString()));
            }
            ret.metadata[sstring(m.name.GetString(), m.name.GetStringLength())] =
                    sstring(m.value.GetString(), m.value.GetStringLength());
        }
    }
    for (auto [member, alg] : {std::pair{"md5Hash", checksum_algorithm::md5}, std::pair{"crc32c", checksum_algorithm::crc32c}}) {
        if (auto it = doc.FindMember(member); it != doc.MemberEnd() && !it->value.IsNull()) {
            auto value = it->value.IsString() ? from_base64(it->value.GetString()) : std::nullopt;
            if (!value) {
                throw invalid(fmt::format("Invalid value for {}", member));
            }
            ret.expected.emplace(alg, std::move(*value));
        }
    }
    return ret;
}

static object_resource parse_object_resource(std::string_view body) {
    if (body.empty()) {
        return {};
    }
    return to_object_resource(parse_json(body));
}

// The name of the object an upload request is for: the name parameter, or
// that of its resource.
static sstring upload_name(const request& req, const object_resource& res) {
    auto name = req.get_query_param("name");
    if (name.empty()) {
        name = res.name;
    } else if (!res.name.empty() && res.name != name) {
        throw invalid("The name parameter and the name of the object resource differ");
    }
    check_object_name(name);
    return name;
}

static object_attrs upload_attrs(user_metadata metadata) {
    object_attrs attrs;
    attrs.metadata = std::move(metadata);
    attrs.checksums = object_checksums_kept;
    return attrs;
}

// The object resource and the data of a multipart/related upload body.
static std::pair<std::string_view, std::string_view> parse_multipart(std::string_view content_type, std::string_view body) {
    auto b = content_type.find("boundary=");
    if (!content_type.starts_with("multipart/related") || b == std::string_view::npos) {
        throw invalid("A multipart upload needs a multipart/related Content-Type with a boundary");
    }
    auto boundary = content_type.substr(b + 9);
    boundary = boundary.substr(0, boundary.find(';'));
    if (boundary.size() >= 2 && boundary.front() == '"' && boundary.back() == '"') {
        boundary = boundary.substr(1, boundary.size() - 2);
    }
    auto delimiter = fmt::format("\r\n--{}", boundary);
    auto malformed = [] { return invalid("Malformed multipart body"); };

    // The first delimiter may begin the body, without the line break.
    auto pos = body.starts_with(std::string_view(delimiter).substr(2)) ? 0 : body.find(delimiter);
    if (pos == std::string_view::npos) {
        throw malformed();
    }
    // A part follows a delimiter line, and its headers.
    auto part_start = [&] (size_t delim_pos, size_t delim_len) {
        auto headers = body.find("\r\n", delim_pos + delim_len);
        auto end = headers == std::string_view::npos ? headers : body.find("\r\n\r\n", headers);
        if (end == std::string_view::npos) {
            throw malformed();
        }
        return end + 4;
    };
    auto first = part_start(pos, pos == 0 ? delimiter.size() - 2 : delimiter.size());
    auto first_end = body.find(delimiter, first);
    if (first_end == std::string_view::npos) {
        throw malformed();
    }
    auto second = part_start(first_end, delimiter.size());
    auto second_end = body.rfind(fmt::format("{}--", delimiter));
    if (second_end == std::string_view::npos || second_end < second) {
        throw malformed();
    }
    return {body.substr(first, first_end - first), body.substr(second, second_end - second)};
}

// A resumable upload's Content-Range: "bytes <first>-<last>/<total>",
// where the total is '*' while unknown, or "bytes */<total>" (or "*/*")
// with no data.
struct chunk_range {
    std::optional<uint64_t> first;
    std::optional<uint64_t> last;
    std::optional<uint64_t> total;
};

static chunk_range parse_content_range(std::string_view s) {
    auto malformed = [s] { return invalid(fmt::format("Invalid Content-Range: '{}'", s)); };
    if (!s.starts_with("bytes ")) {
        throw malformed();
    }
    s.remove_prefix(6);
    auto slash = s.find('/');
    if (slash == std::string_view::npos) {
        throw malformed();
    }
    auto number = [&] (std::string_view n) {
        uint64_t v;
        auto [p, ec] = std::from_chars(n.data(), n.data() + n.size(), v);
        if (n.empty() || ec != std::errc() || p != n.data() + n.size()) {
            throw malformed();
        }
        return v;
    };
    chunk_range ret;
    auto bytes = s.substr(0, slash);
    if (bytes != "*") {
        auto dash = bytes.find('-');
        if (dash == std::string_view::npos) {
            throw malformed();
        }
        ret.first = number(bytes.substr(0, dash));
        ret.last = number(bytes.substr(dash + 1));
        if (*ret.last < *ret.first) {
            throw malformed();
        }
    }
    if (auto total = s.substr(slash + 1); total != "*") {
        ret.total = number(total);
    }
    return ret;
}

static void write_object(json_writer& w, const request& req, std::string_view bucket, const object_info& info) {
    auto base = base_url(req);
    auto name = url_encode(info.key, false);
    auto generation = fmt::to_string(info.generation);
    w.StartObject();
    field(w, "kind", "storage#object");
    field(w, "id", fmt::format("{}/{}/{}", bucket, info.key, generation));
    field(w, "selfLink", fmt::format("{}/storage/v1/b/{}/o/{}", base, bucket, name));
    field(w, "mediaLink", fmt::format("{}/download/storage/v1/b/{}/o/{}?generation={}&alt=media", base, bucket, name, generation));
    field(w, "name", info.key);
    field(w, "bucket", bucket);
    field(w, "generation", generation);
    field(w, "metageneration", "1");
    field(w, "contentType", object_content_type);
    field(w, "storageClass", "STANDARD");
    field(w, "size", fmt::to_string(info.size));
    auto sums = object_checksums(info);
    // An object of several parts (chunks, or composed) is a composite one.
    if (auto it = sums.find(checksum_algorithm::md5); it != sums.end() && info.upload == upload_kind::single) {
        field(w, "md5Hash", to_base64(it->second));
    }
    if (auto it = sums.find(checksum_algorithm::crc32c); it != sums.end()) {
        field(w, "crc32c", to_base64(it->second));
    }
    field(w, "etag", to_base64(fmt::format("{}/1", generation)));
    field(w, "timeCreated", iso8601_date(info.mtime));
    field(w, "updated", iso8601_date(info.mtime));
    if (!info.attrs.metadata.empty()) {
        w.Key("metadata");
        w.StartObject();
        for (auto& [k, v] : info.attrs.metadata) {
            field(w, k, v);
        }
        w.EndObject();
    }
    w.EndObject();
}

// The storage does not keep when buckets were created.
static void write_bucket(json_writer& w, const request& req, std::string_view bucket) {
    auto epoch = iso8601_date(clock_type::time_point());
    w.StartObject();
    field(w, "kind", "storage#bucket");
    field(w, "id", bucket);
    field(w, "selfLink", fmt::format("{}/storage/v1/b/{}", base_url(req), bucket));
    field(w, "name", bucket);
    field(w, "projectNumber", "0");
    field(w, "metageneration", "1");
    field(w, "location", "US");
    field(w, "storageClass", "STANDARD");
    field(w, "etag", "CAE=");
    field(w, "timeCreated", epoch);
    field(w, "updated", epoch);
    w.EndObject();
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

//
// Handlers
//

// Runs a request and renders errors as GCS error replies.
class gcs_handler final : public httpd::handler_base {
public:
    using func_type = std::function<future<std::unique_ptr<reply>>(request&, std::unique_ptr<reply>)>;

private:
    func_type _func;

    future<std::unique_ptr<reply>> run(std::unique_ptr<request>& req, std::unique_ptr<reply> rep) {
        std::exception_ptr ex;
        try {
            co_return co_await _func(*req, std::move(rep));
        } catch (...) {
            ex = std::current_exception();
        }
        try {
            std::rethrow_exception(ex);
        } catch (const gcs_error& e) {
            co_return error_reply(e);
        } catch (const storage_error& e) {
            auto name = req->get_path_param("object");
            co_return error_reply(to_gcs_error(e, bucket_param(*req), name.empty() ? req->get_query_param("name") : name));
        } catch (const checksum_mismatch& e) {
            co_return error_reply(invalid(fmt::format("The checksum of the data does not match: {}", e.what())));
        } catch (const std::exception& e) {
            gcslog.warn("{} {}: {}", req->_method, req->_url, e.what());
            co_return error_reply(gcs_error(500, "backendError", "We encountered an internal error. Please try again."));
        }
    }

public:
    explicit gcs_handler(func_type func) : _func(std::move(func)) {}

    future<std::unique_ptr<reply>> handle(const sstring&, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        return run_traced(gcslog, next_request_id(), std::move(req), std::move(rep),
                [this] (std::unique_ptr<request>& req, std::unique_ptr<reply> rep) {
            return run(req, std::move(rep));
        });
    }
};

class gcs_facade final : public facade {
    storage_service& _storage;

public:
    explicit gcs_facade(storage_service& s) : _storage(s) {}

    void set_routes(httpd::routes& r) override {
        using enum httpd::operation_type;

        r.put(GET, "/storage/v1/b", handler(&gcs_facade::list_buckets));
        r.put(POST, "/storage/v1/b", handler(&gcs_facade::insert_bucket));

        r.add(bucket_rule("/storage/v1/b", &gcs_facade::get_bucket), GET);
        r.add(bucket_rule("/storage/v1/b", &gcs_facade::delete_bucket), DELETE);

        r.add(&bucket_rule("/storage/v1/b", &gcs_facade::list_objects)->add_str("/o"), GET);
        r.add(object_rule("/storage/v1/b", &gcs_facade::get_object), GET);
        r.add(object_rule("/storage/v1/b", &gcs_facade::delete_object), DELETE);
        r.add(object_rule("/download/storage/v1/b", &gcs_facade::download_object), GET);

        r.add(&object_rule("/storage/v1/b", &gcs_facade::rewrite_object)->add_str("/rewriteTo/b").add_param("dbucket", false)
              .add_str("/o").add_param("dobject", false), POST);
        r.add(&object_rule("/storage/v1/b", &gcs_facade::copy_object)->add_str("/copyTo/b").add_param("dbucket", false)
              .add_str("/o").add_param("dobject", false), POST);
        r.add(&object_rule("/storage/v1/b", &gcs_facade::compose_object)->add_str("/compose"), POST);

        r.add(&bucket_rule("/upload/storage/v1/b", &gcs_facade::insert_object)->add_str("/o"), POST);
        r.add(&bucket_rule("/upload/storage/v1/b", &gcs_facade::upload_chunk)->add_str("/o"), PUT);
        r.add(&bucket_rule("/upload/storage/v1/b", &gcs_facade::cancel_upload)->add_str("/o"), DELETE);
    }

private:
    using method = future<std::unique_ptr<reply>> (gcs_facade::*)(request&, std::unique_ptr<reply>);

    gcs_handler* handler(method m) {
        return new gcs_handler([this, m] (request& req, std::unique_ptr<reply> rep) {
            return (this->*m)(req, std::move(rep));
        });
    }

    // <prefix>/{bucket}
    httpd::match_rule* bucket_rule(std::string_view prefix, method m) {
        auto rule = new httpd::match_rule(handler(m));
        rule->add_str(sstring(prefix));
        rule->add_param("bucket", false);
        return rule;
    }

    // <prefix>/{bucket}/o/{object}, where the object's name is a single,
    // URL-encoded, path segment.
    httpd::match_rule* object_rule(std::string_view prefix, method m) {
        auto rule = bucket_rule(prefix, m);
        rule->add_str("/o");
        rule->add_param("object", false);
        return rule;
    }

    // Buckets

    future<std::unique_ptr<reply>> list_buckets(request& req, std::unique_ptr<reply> rep) {
        if (req.get_query_param("project").empty()) {
            throw required("parameter: project");
        }
        auto prefix = req.get_query_param("prefix");
        unsigned max_results = max_list_results;
        if (auto n = query_number(req, "maxResults"); n && *n > 0) {
            max_results = std::min<uint64_t>(*n, max_list_results);
        }
        sstring after;
        if (req.has_query_param("pageToken")) {
            auto t = from_base64(req.get_query_param("pageToken"));
            if (!t) {
                throw invalid("Invalid pageToken");
            }
            after = *t;
        }
        auto buckets = co_await _storage.list_buckets();
        std::ranges::sort(buckets);
        std::vector<sstring> listed;
        bool truncated = false;
        for (auto& b : buckets) {
            if (b <= after || !b.starts_with(prefix)) {
                continue;
            }
            if (listed.size() == max_results) {
                truncated = true;
                break;
            }
            listed.push_back(b);
        }
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            field(w, "kind", "storage#buckets");
            if (truncated) {
                field(w, "nextPageToken", to_base64(listed.back()));
            }
            if (!listed.empty()) {
                w.Key("items");
                w.StartArray();
                for (auto& b : listed) {
                    write_bucket(w, req, b);
                }
                w.EndArray();
            }
            w.EndObject();
        });
    }

    // The bucket resource's location, storage class and the rest are ignored.
    future<std::unique_ptr<reply>> insert_bucket(request& req, std::unique_ptr<reply> rep) {
        if (req.get_query_param("project").empty()) {
            throw required("parameter: project");
        }
        auto doc = parse_json(co_await util::read_entire_stream_contiguous(*req.content_stream));
        auto it = doc.FindMember("name");
        if (it == doc.MemberEnd() || !it->value.IsString()) {
            throw required("field: name");
        }
        sstring bucket(it->value.GetString(), it->value.GetStringLength());
        check_bucket_name(bucket);
        co_await _storage.create_bucket(bucket);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_bucket(w, req, bucket); });
    }

    future<std::unique_ptr<reply>> get_bucket(request& req, std::unique_ptr<reply> rep) {
        auto bucket = bucket_param(req);
        auto buckets = co_await _storage.list_buckets();
        if (std::ranges::find(buckets, bucket) == buckets.end()) {
            throw storage_error(errc::no_such_bucket, "");
        }
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_bucket(w, req, bucket); });
    }

    future<std::unique_ptr<reply>> delete_bucket(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"ifMetagenerationMatch", "ifMetagenerationNotMatch"});
        co_await _storage.delete_bucket(bucket_param(req));
        co_return empty_reply(std::move(rep), status::no_content);
    }

    // Objects

    future<std::unique_ptr<reply>> list_objects(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"endOffset", "matchGlob"});
        for (auto flag : {"versions", "includeTrailingDelimiter", "includeFoldersAsPrefixes", "softDeleted"}) {
            if (req.get_query_param(flag) == "true") {
                throw not_implemented(fmt::format("The '{}' parameter", flag));
            }
        }
        list_request lr;
        lr.bucket = bucket_param(req);
        lr.prefix = req.get_query_param("prefix");
        lr.delimiter = req.get_query_param("delimiter");
        lr.max_entries = max_list_results;
        if (auto n = query_number(req, "maxResults")) {
            if (*n == 0) {
                throw invalid("Invalid value for maxResults: 0");
            }
            lr.max_entries = std::min<uint64_t>(*n, max_list_results);
        }
        if (req.has_query_param("pageToken")) {
            auto t = from_base64(req.get_query_param("pageToken"));
            if (!t) {
                throw invalid("Invalid pageToken");
            }
            lr.after = *t;
        } else if (req.has_query_param("startOffset")) {
            lr.after = req.get_query_param("startOffset");
            lr.inclusive = true;
        }

        auto res = co_await list_delimited(_storage, lr);
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            field(w, "kind", "storage#objects");
            if (res.truncated) {
                field(w, "nextPageToken", to_base64(res.last));
            }
            if (!res.prefixes.empty()) {
                w.Key("prefixes");
                w.StartArray();
                for (auto& p : res.prefixes) {
                    write_string(w, p);
                }
                w.EndArray();
            }
            if (!res.objects.empty()) {
                w.Key("items");
                w.StartArray();
                for (auto& o : res.objects) {
                    write_object(w, req, lr.bucket, o);
                }
                w.EndArray();
            }
            w.EndObject();
        });
    }

    future<std::unique_ptr<reply>> get_object(request& req, std::unique_ptr<reply> rep) {
        auto alt = req.get_query_param("alt");
        if (alt == "media") {
            co_return co_await read_media(req, std::move(rep));
        }
        if (!alt.empty() && alt != "json") {
            throw invalid(fmt::format("Invalid value for alt: {}", alt));
        }
        auto bucket = bucket_param(req);
        auto info = co_await _storage.head_object(bucket, object_param(req));
        check_read_conditions(req, info);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    future<std::unique_ptr<reply>> download_object(request& req, std::unique_ptr<reply> rep) {
        return read_media(req, std::move(rep));
    }

    // The object's data, or the range of it. The checksums go with the
    // whole object only.
    future<std::unique_ptr<reply>> read_media(request& req, std::unique_ptr<reply> rep) {
        auto range = parse_range_header(req.get_header("Range"));
        auto bucket = bucket_param(req);
        auto name = object_param(req);
        auto r = co_await _storage.read_object(bucket, name, range);
        std::exception_ptr ex;
        try {
            check_read_conditions(req, r.info);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await r.data.close();
            std::rethrow_exception(ex);
        }
        auto& info = r.info;
        rep->add_header("x-goog-generation", fmt::to_string(info.generation));
        rep->add_header("x-goog-metageneration", "1");
        rep->add_header("x-goog-stored-content-length", fmt::to_string(info.size));
        rep->add_header("x-goog-storage-class", "STANDARD");
        rep->add_header("ETag", fmt::format("\"{}\"", to_base64(fmt::format("{}/1", info.generation))));
        rep->add_header("Last-Modified", http_date(info.mtime));
        uint64_t length = info.size;
        if (range) {
            auto rr = resolve_range(*range, info.size);
            length = rr.length;
            rep->add_header("Content-Range", fmt::format("bytes {}-{}/{}", rr.offset, rr.offset + rr.length - 1, info.size));
            rep->set_status(status::partial_content);
        } else {
            std::vector<sstring> hashes;
            auto sums = object_checksums(info);
            for (auto [alg, name] : {std::pair{checksum_algorithm::crc32c, "crc32c"}, std::pair{checksum_algorithm::md5, "md5"}}) {
                if (auto it = sums.find(alg); it != sums.end()) {
                    hashes.push_back(sstring(fmt::format("{}={}", name, to_base64(it->second))));
                }
            }
            if (!hashes.empty()) {
                rep->add_header("x-goog-hash", sstring(fmt::format("{}", fmt::join(hashes, ","))));
            }
            rep->set_status(status::ok);
        }
        rep->write_body(sstring(object_content_type), length, [data = std::move(r.data), bucket = std::move(bucket), name = std::move(name)] (output_stream<char>& out) mutable {
            return send_data(gcslog, std::move(data), out, std::move(bucket), std::move(name));
        });
        co_return std::move(rep);
    }

    future<std::unique_ptr<reply>> delete_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"generation", "ifGenerationMatch", "ifGenerationNotMatch", "ifMetagenerationMatch",
                                  "ifMetagenerationNotMatch"});
        co_await _storage.delete_object(bucket_param(req), object_param(req));
        co_return empty_reply(std::move(rep), status::no_content);
    }

    // Copies

    // rewriteTo copies all at once, so it is always done, and gives no
    // rewriteToken to continue with.
    future<std::unique_ptr<reply>> rewrite_object(request& req, std::unique_ptr<reply> rep) {
        if (req.has_query_param("rewriteToken")) {
            throw invalid("Invalid rewriteToken");
        }
        auto [bucket, info] = co_await copy(req);
        co_return json_reply(std::move(rep), [&] (json_writer& w) {
            w.StartObject();
            field(w, "kind", "storage#rewriteResponse");
            field(w, "totalBytesRewritten", fmt::to_string(info.size));
            field(w, "objectSize", fmt::to_string(info.size));
            w.Key("done"); w.Bool(true);
            w.Key("resource"); write_object(w, req, bucket, info);
            w.EndObject();
        });
    }

    future<std::unique_ptr<reply>> copy_object(request& req, std::unique_ptr<reply> rep) {
        auto [bucket, info] = co_await copy(req);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    // Copies {bucket}/o/{object} to {dbucket}/o/{dobject}, a new object of
    // the source's data and, unless the request's resource has some, its
    // metadata. The copy is of the source's version that the conditions
    // hold for, and is made again if the source changes meanwhile.
    future<std::pair<sstring, object_info>> copy(request& req) {
        reject_query_params(req, {"destinationKmsKeyName", "destinationPredefinedAcl"});
        auto bucket = bucket_param(req);
        auto src = object_param(req);
        auto dst = req.get_path_param("dobject");
        check_object_name(dst);
        if (req.get_path_param("dbucket") != bucket) {
            throw not_implemented("Copying to another bucket");
        }
        auto pre = write_precondition(req);
        auto res = parse_object_resource(co_await util::read_entire_stream_contiguous(*req.content_stream));
        if (!res.expected.empty()) {
            throw not_implemented("Checksums of a copy");
        }
        for (;;) {
            auto src_info = co_await _storage.head_object(bucket, src);
            check_source_conditions(req, src_info);
            auto attrs = upload_attrs(res.has_metadata ? res.metadata : src_info.attrs.metadata);
            auto id = co_await _storage.open_session(bucket, dst, std::move(attrs));
            bool copied = false;
            std::exception_ptr ex;
            try {
                co_await _storage.copy_part(bucket, dst, id, 1, src, std::nullopt, precondition{src_info.generation});
                copied = true;
                co_return std::pair(bucket, co_await _storage.commit_session(bucket, dst, id, {1}, pre, upload_kind::single));
            } catch (...) {
                ex = std::current_exception();
            }
            // Report the original error, not a failure to clean up after it.
            auto f = co_await coroutine::as_future(_storage.abort_session(bucket, dst, id));
            f.ignore_ready_future();
            // The source changed after its conditions were checked: check
            // them against its new version.
            if (copied || !is_storage_error(ex, errc::precondition_failed)) {
                std::rethrow_exception(ex);
            }
        }
    }

    // Any of a copy's conditions on its source failing fails it with 412.
    // There are no other versions of the source to copy.
    static void check_source_conditions(const request& req, const object_info& info) {
        if (auto g = query_number(req, "sourceGeneration"); g && *g != info.generation) {
            throw storage_error(errc::no_such_key, "");
        }
        auto gen_match = query_number(req, "ifSourceGenerationMatch");
        auto gen_not_match = query_number(req, "ifSourceGenerationNotMatch");
        auto meta_match = query_number(req, "ifSourceMetagenerationMatch");
        auto meta_not_match = query_number(req, "ifSourceMetagenerationNotMatch");
        if ((gen_match && *gen_match != info.generation) || (gen_not_match && *gen_not_match == info.generation)
                || (meta_match && *meta_match != 1) || (meta_not_match && *meta_not_match == 1)) {
            throw storage_error(errc::precondition_failed, "");
        }
    }

    // A composite object of up to 32 objects of the bucket, in order.
    future<std::unique_ptr<reply>> compose_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"kmsKeyName", "destinationPredefinedAcl"});
        auto bucket = bucket_param(req);
        auto dst = object_param(req);
        auto pre = write_precondition(req);
        auto doc = parse_json(co_await util::read_entire_stream_contiguous(*req.content_stream));

        // Of the destination's resource only the metadata is used: clients
        // send what they know of the object, its old checksums included.
        object_resource res;
        if (auto it = doc.FindMember("destination"); it != doc.MemberEnd() && !it->value.IsNull()) {
            res = to_object_resource(it->value);
        }
        // A source's generation, given or required, is that it must have.
        struct source {
            sstring name;
            std::optional<uint64_t> generation;
            std::optional<uint64_t> generation_match;
        };
        std::vector<source> sources;
        auto it = doc.FindMember("sourceObjects");
        if (it == doc.MemberEnd() || !it->value.IsArray() || it->value.Empty()) {
            throw required("field: sourceObjects");
        }
        if (it->value.Size() > max_compose_sources) {
            throw invalid(fmt::format("The number of source components provided ({}) exceeds the maximum ({})",
                                      it->value.Size(), max_compose_sources));
        }
        auto generation_of = [] (const rapidjson::Value& v, const char* member) -> std::optional<uint64_t> {
            auto m = v.FindMember(member);
            if (m == v.MemberEnd() || m->value.IsNull()) {
                return std::nullopt;
            }
            if (m->value.IsUint64()) {
                return m->value.GetUint64();
            }
            if (m->value.IsString()) {
                return parse_number<uint64_t>(m->value.GetString(), member);
            }
            throw invalid(fmt::format("Invalid value for {}", member));
        };
        for (auto& s : it->value.GetArray()) {
            auto name = s.IsObject() ? s.FindMember("name") : s.MemberEnd();
            if (!s.IsObject() || name == s.MemberEnd() || !name->value.IsString()) {
                throw required("field: sourceObjects.name");
            }
            source src{sstring(name->value.GetString(), name->value.GetStringLength()), generation_of(s, "generation"), {}};
            check_object_name(src.name);
            if (auto p = s.FindMember("objectPreconditions"); p != s.MemberEnd()) {
                if (!p->value.IsObject()) {
                    throw invalid("Invalid value for objectPreconditions");
                }
                src.generation_match = generation_of(p->value, "ifGenerationMatch");
            }
            sources.push_back(std::move(src));
        }

        auto id = co_await _storage.open_session(bucket, dst, upload_attrs(std::move(res.metadata)));
        object_info info;
        std::exception_ptr ex;
        try {
            std::vector<unsigned> numbers;
            for (auto& src : sources) {
                numbers.push_back(numbers.size() + 1);
                if (src.generation && src.generation_match && *src.generation != *src.generation_match) {
                    throw storage_error(errc::precondition_failed, "");
                }
                auto f = co_await coroutine::as_future(_storage.copy_part(bucket, dst, id, numbers.back(), src.name, std::nullopt,
                                                                          precondition{src.generation_match ? src.generation_match : src.generation}));
                if (f.failed()) {
                    auto e = f.get_exception();
                    // There is no other version of the source to read.
                    if (!src.generation_match && is_storage_error(e, errc::precondition_failed)) {
                        throw storage_error(errc::no_such_key, "");
                    }
                    std::rethrow_exception(e);
                }
            }
            info = co_await _storage.commit_session(bucket, dst, id, std::move(numbers), pre, upload_kind::multipart);
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            // Report the original error, not a failure to clean up after it.
            auto f = co_await coroutine::as_future(_storage.abort_session(bucket, dst, id));
            f.ignore_ready_future();
            std::rethrow_exception(ex);
        }
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    // Uploads

    future<std::unique_ptr<reply>> insert_object(request& req, std::unique_ptr<reply> rep) {
        reject_query_params(req, {"contentEncoding", "kmsKeyName", "predefinedAcl"});
        auto type = req.get_query_param("uploadType");
        if (type == "media") {
            co_return co_await upload_media(req, std::move(rep));
        } else if (type == "multipart") {
            co_return co_await upload_multipart(req, std::move(rep));
        } else if (type == "resumable") {
            co_return co_await start_resumable_upload(req, std::move(rep));
        }
        throw invalid(fmt::format("Invalid value for uploadType: '{}'", type));
    }

    // The body is the data.
    future<std::unique_ptr<reply>> upload_media(request& req, std::unique_ptr<reply> rep) {
        auto bucket = bucket_param(req);
        auto name = upload_name(req, {});
        auto info = co_await upload_single(bucket, name, upload_attrs({}), write_precondition(req), *req.content_stream);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    // The body has the object resource and the data, which is read into
    // memory.
    future<std::unique_ptr<reply>> upload_multipart(request& req, std::unique_ptr<reply> rep) {
        auto pre = write_precondition(req);
        auto body = co_await util::read_entire_stream_contiguous(*req.content_stream);
        auto [resource, data] = parse_multipart(req.get_header("Content-Type"), body);
        auto res = parse_object_resource(resource);
        auto bucket = bucket_param(req);
        auto name = upload_name(req, res);
        auto buffered = util::as_input_stream(temporary_buffer<char>(data.data(), data.size()));
        object_info info;
        if (res.expected.empty()) {
            info = co_await upload_single(bucket, name, upload_attrs(std::move(res.metadata)), pre, buffered);
        } else {
            auto verified = make_verifying_stream(buffered, std::move(res.expected));
            info = co_await upload_single(bucket, name, upload_attrs(std::move(res.metadata)), pre, verified);
        }
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    future<object_info> upload_single(sstring bucket, sstring name, object_attrs attrs, precondition pre,
                                      input_stream<char>& in) {
        auto id = co_await _storage.open_session(bucket, name, std::move(attrs));
        std::exception_ptr ex;
        try {
            co_await _storage.write_part(bucket, name, id, 1, in);
            co_return co_await _storage.commit_session(bucket, name, id, {1}, pre, upload_kind::single);
        } catch (...) {
            ex = std::current_exception();
        }
        // Report the original error, not a failure to clean up after it.
        auto f = co_await coroutine::as_future(_storage.abort_session(bucket, name, id));
        f.ignore_ready_future();
        std::rethrow_exception(ex);
    }

    // A resumable upload is a storage session, whose chunks are its parts.
    // The session URI names the object, and carries the precondition to
    // commit it with, so that the session holds all that is needed.
    future<std::unique_ptr<reply>> start_resumable_upload(request& req, std::unique_ptr<reply> rep) {
        auto pre = write_precondition(req);
        auto res = parse_object_resource(co_await util::read_entire_stream_contiguous(*req.content_stream));
        if (!res.expected.empty()) {
            throw not_implemented("Checksums of a resumable upload");
        }
        auto bucket = bucket_param(req);
        auto name = upload_name(req, res);
        auto id = co_await _storage.open_session(bucket, name, upload_attrs(std::move(res.metadata)));
        auto location = fmt::format("{}/upload/storage/v1/b/{}/o?uploadType=resumable&name={}&upload_id={}",
                                    base_url(req), bucket, url_encode(name, false), url_encode(id, false));
        if (pre.generation_match) {
            location += fmt::format("&ifGenerationMatch={}", *pre.generation_match);
        }
        rep->add_header("Location", sstring(location));
        co_return empty_reply(std::move(rep), status::ok);
    }

    // A chunk of a resumable upload, or a query of where it stands. The
    // chunks come in order, each but the last a multiple of 256 KiB, and
    // the one that reaches the declared total size, or a "*/<total>" with
    // all of it there, completes the upload.
    future<std::unique_ptr<reply>> upload_chunk(request& req, std::unique_ptr<reply> rep) {
        auto bucket = bucket_param(req);
        auto name = upload_name(req, {});
        auto id = req.get_query_param("upload_id");
        if (id.empty()) {
            throw required("parameter: upload_id");
        }
        auto pre = write_precondition(req);
        if (!req._headers.contains("Content-Length") && !req._headers.contains("Content-Range")) {
            throw gcs_error(411, "required", "Length required");
        }
        uint64_t length = req.content_length;
        chunk_range cr;
        if (req._headers.contains("Content-Range")) {
            cr = parse_content_range(req.get_header("Content-Range"));
        } else if (length) {
            // The whole of the data at once.
            cr = chunk_range{0, length - 1, length};
        } else {
            cr.total = 0;
        }
        if (!cr.first && length) {
            throw invalid("A Content-Range without bytes goes with no data");
        }
        if (cr.first && *cr.last - *cr.first + 1 != length) {
            throw invalid(fmt::format("The Content-Range declares {} bytes, but the body has {}",
                                      *cr.last - *cr.first + 1, length));
        }
        if (cr.total && cr.last && *cr.last >= *cr.total) {
            throw invalid(fmt::format("The Content-Range goes past the declared size of {} bytes", *cr.total));
        }

        auto f = co_await coroutine::as_future(_storage.describe_session(bucket, name, id));
        if (f.failed()) {
            auto ex = f.get_exception();
            if (!is_storage_error(ex, errc::no_such_session)) {
                std::rethrow_exception(ex);
            }
            // Done already: the storage gives the same object again, for a
            // query, or a resent final request.
            auto info = co_await _storage.commit_session(bucket, name, id, {}, pre, upload_kind::single);
            bool query = !cr.first && !cr.total;
            if (!query && (!cr.total || *cr.total != info.size || (cr.last && *cr.last + 1 != info.size))) {
                throw invalid("The upload is complete");
            }
            co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
        }
        auto session = f.get();
        uint64_t held = 0;
        for (auto& p : session.parts) {
            held += p.size;
        }
        unsigned parts = session.parts.size();

        if (cr.first) {
            if (*cr.first != held) {
                throw invalid(fmt::format("The chunk starts at {}, but the upload has {} bytes", *cr.first, held));
            }
            bool last = cr.total && *cr.last + 1 == *cr.total;
            if (!last && length % chunk_granularity) {
                throw invalid(fmt::format("A chunk but the last must be a multiple of {} bytes", chunk_granularity));
            }
            co_await _storage.write_part(bucket, name, id, ++parts, *req.content_stream);
            held += length;
        }
        if (!cr.total || *cr.total != held) {
            if (cr.total && !cr.first) {
                throw invalid(fmt::format("The upload is {} bytes, not {}", held, *cr.total));
            }
            // Resume Incomplete
            if (held) {
                rep->add_header("Range", sstring(fmt::format("bytes=0-{}", held - 1)));
            }
            co_return empty_reply(std::move(rep), status::permanent_redirect);
        }
        std::vector<unsigned> numbers;
        for (unsigned n = 1; n <= parts; n++) {
            numbers.push_back(n);
        }
        auto info = co_await _storage.commit_session(bucket, name, id, std::move(numbers), pre,
                                                     parts > 1 ? upload_kind::multipart : upload_kind::single);
        co_return json_reply(std::move(rep), [&] (json_writer& w) { write_object(w, req, bucket, info); });
    }

    future<std::unique_ptr<reply>> cancel_upload(request& req, std::unique_ptr<reply> rep) {
        auto id = req.get_query_param("upload_id");
        if (id.empty()) {
            throw required("parameter: upload_id");
        }
        co_await _storage.abort_session(bucket_param(req), upload_name(req, {}), id);
        co_return empty_reply(std::move(rep), static_cast<status>(client_closed_request));
    }
};

facade_type make_facade_type() {
    return facade_type{
        .name = "gcs",
        .description = "The Google Cloud Storage JSON API; see docs/gcs-protocol.md for what is supported",
        .add_options = [] (bpo::options_description&) {},
        .make = [] (const bpo::variables_map&, storage_service& s) -> std::unique_ptr<facade> {
            return std::make_unique<gcs_facade>(s);
        },
    };
}

} // namespace labaz::gcs
