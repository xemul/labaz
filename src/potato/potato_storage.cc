/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// On-disk layout: an object is a directory named by the SHA-1 of its key, so
// keys need no encoding and need not map to paths.
//
//   <data-dir>/<bucket>/<sha1(key)>/meta.json      object meta: the key, the
//                                                  attributes and the parts
//   <data-dir>/<bucket>/<sha1(key)>/data.<gen>/    the object's parts
//   <data-dir>/<bucket>/<sha1(key)>/session.<id>/  an upload session's parts
//
// A commit renames the session directory into a data directory, writes the
// meta to a temporary file and renames it over the current one, so an
// overwrite is atomic, and a failure before the meta rename leaves the
// current version intact.
//
// Every call is served on the shard it is made on. Sessions live in the
// memory of the shard that opened them: in-progress uploads do not survive a
// restart, committed objects do.

#include "potato/potato_storage.hh"

#include <algorithm>
#include <filesystem>
#include <set>
#include <system_error>
#include <unordered_map>

#include <fmt/format.h>

#include <gnutls/crypto.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/file.hh>
#include <seastar/util/log.hh>

namespace labaz::potato {

using namespace seastar;
namespace fs = std::filesystem;
namespace bpo = boost::program_options;

static logger plog("potato");

static constexpr std::string_view meta_name = "meta.json";
static constexpr std::string_view data_prefix = "data.";
static constexpr std::string_view session_prefix = "session.";
static constexpr std::string_view part_prefix = "part.";

[[noreturn]] static void fail(errc code, std::string msg) {
    throw storage_error(code, std::move(msg));
}

static bool is_enoent(const std::exception_ptr& ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const std::system_error& e) {
        return e.code().value() == ENOENT;
    } catch (...) {
        return false;
    }
}

//
// Names
//

static bool is_plain_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
        || c == '.' || c == '_' || c == '-';
}

static void validate_key(std::string_view key) {
    if (key.empty()) {
        fail(errc::invalid_argument, "empty key");
    }
    if (key.ends_with('/')) {
        fail(errc::invalid_argument, "a key must not end with '/'");
    }
}

// The name of the key's object directory: the key's SHA-1 in hex.
static std::string object_dir_name(std::string_view key) {
    unsigned char digest[20];
    if (gnutls_hash_fast(GNUTLS_DIG_SHA1, key.data(), key.size(), digest) < 0) {
        throw std::runtime_error("cannot hash a key");
    }
    std::string ret;
    for (auto b : digest) {
        ret += fmt::format("{:02x}", b);
    }
    return ret;
}

static void validate_bucket_name(std::string_view b) {
    if (b.empty() || b.size() > 63 || b == "." || b == ".."
            || !std::ranges::all_of(b, [] (char c) { return is_plain_char(c); })) {
        fail(errc::invalid_argument, fmt::format("invalid bucket name '{}'", b));
    }
}

//
// Small files
//

static future<> write_small_file(fs::path path, sstring content) {
    auto f = co_await open_file_dma(path.native(), open_flags::wo | open_flags::create | open_flags::truncate);
    std::exception_ptr ex;
    try {
        auto out = co_await make_file_output_stream(f);
        co_await out.write(content);
        co_await out.flush();
        co_await out.close();
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        co_await f.close();
        std::rethrow_exception(ex);
    }
}

//
// Object meta
//

struct part_file {
    sstring name;   // within the data directory
    uint64_t size;
};

struct object_meta {
    object_info info;
    sstring data_dir;  // within the object directory
    std::vector<part_file> parts;
};

static sstring serialize(const object_meta& m) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> w(sb);
    auto str = [&w] (std::string_view s) { w.String(s.data(), s.size()); };
    w.StartObject();
    w.Key("key"); str(m.info.key);
    w.Key("size"); w.Uint64(m.info.size);
    w.Key("mtime_ns"); w.Int64(std::chrono::duration_cast<std::chrono::nanoseconds>(m.info.mtime.time_since_epoch()).count());
    w.Key("generation"); w.Uint64(m.info.generation);
    w.Key("metadata");
    w.StartObject();
    for (auto& [k, v] : m.info.metadata) {
        str(k); str(v);
    }
    w.EndObject();
    w.Key("data"); str(m.data_dir);
    w.Key("parts");
    w.StartArray();
    for (auto& p : m.parts) {
        w.StartObject();
        w.Key("name"); str(p.name);
        w.Key("size"); w.Uint64(p.size);
        w.EndObject();
    }
    w.EndArray();
    w.EndObject();
    return sstring(sb.GetString(), sb.GetSize());
}

static object_meta deserialize(std::string_view s) {
    rapidjson::Document d;
    d.Parse(s.data(), s.size());
    if (d.HasParseError() || !d.IsObject()) {
        throw std::runtime_error("corrupt object meta");
    }
    auto str = [] (const rapidjson::Value& v) { return sstring(v.GetString(), v.GetStringLength()); };
    object_meta m;
    m.info.key = str(d["key"]);
    m.info.size = d["size"].GetUint64();
    m.info.mtime = clock_type::time_point(std::chrono::duration_cast<clock_type::duration>(
            std::chrono::nanoseconds(d["mtime_ns"].GetInt64())));
    m.info.generation = d["generation"].GetUint64();
    for (auto& kv : d["metadata"].GetObject()) {
        m.info.metadata.emplace(str(kv.name), str(kv.value));
    }
    m.data_dir = str(d["data"]);
    for (auto& p : d["parts"].GetArray()) {
        m.parts.push_back(part_file{str(p["name"]), p["size"].GetUint64()});
    }
    return m;
}

//
// Reading an object across its part files
//

class parts_source final : public data_source_impl {
    std::vector<file> _files;
    std::vector<uint64_t> _sizes;
    size_t _idx = 0;
    uint64_t _offset;      // within the current part
    uint64_t _remaining;
    std::optional<input_stream<char>> _cur;

public:
    parts_source(std::vector<file> files, std::vector<uint64_t> sizes, uint64_t offset, uint64_t length)
        : _files(std::move(files)), _sizes(std::move(sizes)), _offset(offset), _remaining(length)
    {
        while (_idx < _sizes.size() && _offset >= _sizes[_idx]) {
            _offset -= _sizes[_idx++];
        }
    }

    future<temporary_buffer<char>> get() override {
        while (_remaining > 0) {
            if (!_cur) {
                if (_idx == _sizes.size()) {
                    throw std::runtime_error("object is shorter than its meta says");
                }
                auto len = std::min(_sizes[_idx] - _offset, _remaining);
                _cur.emplace(make_file_input_stream(_files[_idx], _offset, len));
            }
            auto buf = co_await _cur->read();
            if (buf.empty()) {
                co_await _cur->close();
                _cur.reset();
                _idx++;
                _offset = 0;
                continue;
            }
            _remaining -= std::min<uint64_t>(_remaining, buf.size());
            co_return buf;
        }
        co_return temporary_buffer<char>();
    }

    future<> close() override {
        if (_cur) {
            co_await _cur->close();
            _cur.reset();
        }
        for (auto& f : _files) {
            co_await f.close();
        }
        _files.clear();
    }
};

//
// The storage
//

struct sealed_part {
    sstring name;
    uint64_t size;
};

struct session_state {
    sstring bucket;
    sstring key;
    object_attrs attrs;
    std::map<unsigned, sealed_part> parts;
    bool committing = false;
    // Kept after the commit, so that a retried commit gets the same reply.
    std::optional<object_info> committed;
};

class potato_storage final : public storage {
    fs::path _root;
    std::unordered_map<session_id, session_state> _sessions;
    uint64_t _last_unique = 0;
    // Taken for write to publish, delete or abort, which remove directories;
    // and for read to open an object's parts, so that they are not removed
    // under the reader, and to create a session directory, so that its
    // object directory is not removed meanwhile. Objects of a shard are
    // served by that shard only, so a shard-wide lock covers all of them.
    rwlock _lock;

public:
    explicit potato_storage(fs::path root)
        : _root(std::move(root))
    {}

    future<> start() override {
        if (this_shard_id() == 0) {
            co_await recursive_touch_directory(_root.native());
            plog.info("Storing data in {}", _root.native());
        }
    }

    future<> stop() override {
        co_return;
    }

    future<> create_bucket(sstring bucket) override {
        validate_bucket_name(bucket);
        auto dir = bucket_dir(bucket);
        if (co_await file_exists(dir.native())) {
            fail(errc::bucket_exists, fmt::format("bucket '{}' exists", bucket));
        }
        co_await make_directory(dir.native());
    }

    future<> delete_bucket(sstring bucket) override {
        validate_bucket_name(bucket);
        co_await check_bucket(bucket);
        if (co_await has_objects(bucket_dir(bucket))) {
            fail(errc::bucket_not_empty, fmt::format("bucket '{}' is not empty", bucket));
        }
        co_await recursive_remove_directory(bucket_dir(bucket));
    }

    future<std::vector<sstring>> list_buckets() override {
        std::vector<sstring> ret;
        auto dir = co_await open_directory(_root.native());
        co_await dir.list_directory([&ret] (directory_entry de) {
            if (de.type == directory_entry_type::directory) {
                ret.push_back(de.name);
            }
            return make_ready_future();
        }).done();
        co_await dir.close();
        std::ranges::sort(ret);
        co_return ret;
    }

    future<session_id> open_session(sstring bucket, sstring key, object_attrs attrs) override {
        validate_bucket_name(bucket);
        auto dir = object_dir(bucket, key);
        co_await check_bucket(bucket);
        // The shard keeps ids unique among the shards' sessions.
        auto id = session_id(fmt::format("{}.{:x}", this_shard_id(), next_unique()));
        auto lock = co_await _lock.hold_read_lock();
        co_await touch_directory(dir.native());
        co_await make_directory(session_dir(bucket, key, id).native());
        _sessions.emplace(id, session_state{std::move(bucket), std::move(key), std::move(attrs), {}, false, std::nullopt});
        co_return id;
    }

    future<part_info> write_part(sstring bucket, sstring key, session_id id, unsigned number, input_stream<char>& in) override {
        active_session(bucket, key, id);
        auto final = session_dir(bucket, key, id) / fmt::format("{}{}.{:x}", part_prefix, number, next_unique());
        auto tmp = fs::path(final.native() + ".tmp");
        auto f = co_await open_file_dma(tmp.native(), open_flags::wo | open_flags::create | open_flags::exclusive);
        auto out = co_await make_file_output_stream(std::move(f));
        std::exception_ptr ex;
        try {
            co_await copy(in, out);
        } catch (...) {
            ex = std::current_exception();
        }
        auto closed = co_await coroutine::as_future(out.close());
        if (closed.failed()) {
            auto e = closed.get_exception();
            if (!ex) {
                ex = std::move(e);
            }
        }
        if (ex) {
            // Report the original error, not a failure to clean up after it.
            auto removed = co_await coroutine::as_future(remove_file(tmp.native()));
            removed.ignore_ready_future();
            std::rethrow_exception(ex);
        }
        co_await rename_file(tmp.native(), final.native());
        auto size = co_await file_size(final.native());
        co_return co_await add_part(bucket, key, id, number, sealed_part{final.filename().native(), size});
    }

    future<session_info> describe_session(sstring bucket, sstring key, session_id id) override {
        auto& st = active_session(bucket, key, id);
        session_info ret{id, st.bucket, st.key, st.attrs, {}};
        for (auto& [n, p] : st.parts) {
            ret.parts.push_back(part_info{n, p.size});
        }
        co_return ret;
    }

    future<object_info> commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> numbers,
                                       precondition pre) override {
        auto it = _sessions.find(id);
        if (it != _sessions.end() && it->second.committed && it->second.bucket == bucket && it->second.key == key) {
            co_return *it->second.committed;
        }
        auto& st = active_session(bucket, key, id);

        object_meta m;
        std::set<unsigned> listed;
        for (auto n : numbers) {
            auto p = st.parts.find(n);
            if (p == st.parts.end()) {
                fail(errc::no_such_part, fmt::format("session '{}' has no part {}", id, n));
            }
            if (!listed.insert(n).second) {
                fail(errc::invalid_argument, fmt::format("part {} listed twice", n));
            }
            m.parts.push_back(part_file{p->second.name, p->second.size});
            m.info.size += p->second.size;
        }

        st.committing = true;
        auto lock = co_await _lock.hold_write_lock();
        auto f = co_await coroutine::as_future(publish(id, st, std::move(m), listed, pre));
        lock.return_all();
        st.committing = false;
        if (f.failed()) {
            co_return coroutine::exception(f.get_exception());
        }
        st.parts.clear();
        st.attrs = {};
        st.committed = f.get();
        co_return *st.committed;
    }

    future<> abort_session(sstring bucket, sstring key, session_id id) override {
        active_session(bucket, key, id);
        _sessions.erase(id);
        auto lock = co_await _lock.hold_write_lock();
        co_await recursive_remove_directory(session_dir(bucket, key, id));
        co_await remove_if_empty(object_dir(bucket, key));
    }

    future<object_info> head_object(sstring bucket, sstring key) override {
        auto m = co_await load_meta(bucket, key);
        co_return std::move(m.info);
    }

    future<object_reader> read_object(sstring bucket, sstring key, std::optional<byte_range> range) override {
        // Once the parts are open, removing them does not affect the reader.
        auto lock = co_await _lock.hold_read_lock();
        auto m = co_await load_meta(bucket, key);
        auto [offset, length] = range ? resolve_range(*range, m.info.size) : resolved_range{0, m.info.size};

        std::vector<file> files;
        std::vector<uint64_t> sizes;
        auto dir = object_dir(bucket, key) / m.data_dir.c_str();
        std::exception_ptr ex;
        try {
            for (auto& p : m.parts) {
                files.push_back(co_await open_file_dma((dir / p.name.c_str()).native(), open_flags::ro));
                sizes.push_back(p.size);
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await close_all(files);
            std::rethrow_exception(ex);
        }

        auto src = std::make_unique<parts_source>(std::move(files), std::move(sizes), offset, length);
        co_return object_reader{std::move(m.info), input_stream<char>(data_source(std::move(src)))};
    }

    future<> delete_object(sstring bucket, sstring key) override {
        auto lock = co_await _lock.hold_write_lock();
        auto m = co_await load_meta(bucket, key);
        auto dir = object_dir(bucket, key);
        co_await remove_file((dir / meta_name).native());
        co_await recursive_remove_directory(dir / m.data_dir.c_str());
        co_await remove_if_empty(dir);
    }

    future<listing> list_objects(sstring bucket, sstring prefix, sstring start_after, unsigned max_keys) override {
        validate_bucket_name(bucket);
        co_await check_bucket(bucket);
        // Object directories are named by hashes, so every object's meta is
        // read to learn its key.
        std::vector<sstring> dirs;
        auto d = co_await open_directory(bucket_dir(bucket).native());
        co_await d.list_directory([&dirs] (directory_entry de) {
            if (de.type == directory_entry_type::directory) {
                dirs.push_back(de.name);
            }
            return make_ready_future();
        }).done();
        co_await d.close();

        listing ret;
        for (auto& name : dirs) {
            auto path = bucket_dir(bucket) / name.c_str() / meta_name;
            auto f = co_await coroutine::as_future(util::read_entire_file_contiguous(path));
            if (f.failed()) {
                auto ep = f.get_exception();
                if (is_enoent(ep)) {
                    continue; // no committed version, or deleted meanwhile
                }
                std::rethrow_exception(ep);
            }
            auto info = deserialize(f.get()).info;
            if (info.key > start_after && info.key.starts_with(prefix)) {
                ret.objects.push_back(std::move(info));
            }
        }
        std::ranges::sort(ret.objects, {}, &object_info::key);
        ret.truncated = ret.objects.size() > max_keys;
        ret.objects.resize(std::min<size_t>(ret.objects.size(), max_keys));
        co_return ret;
    }

private:
    // Unique among the values this shard produced, also across restarts as
    // long as the clock does not go back. Used for generations and names.
    uint64_t next_unique() {
        auto now = std::chrono::duration_cast<std::chrono::microseconds>(clock_type::now().time_since_epoch()).count();
        _last_unique = std::max<uint64_t>(_last_unique + 1, now);
        return _last_unique;
    }

    fs::path bucket_dir(std::string_view bucket) const {
        return _root / std::string(bucket);
    }

    fs::path object_dir(std::string_view bucket, std::string_view key) const {
        validate_key(key);
        return bucket_dir(bucket) / object_dir_name(key);
    }

    fs::path session_dir(std::string_view bucket, std::string_view key, std::string_view id) const {
        return object_dir(bucket, key) / fmt::format("{}{}", session_prefix, id);
    }

    // Removes an object directory that has neither an object nor sessions
    // left. Runs under the write lock.
    static future<> remove_if_empty(fs::path dir) {
        auto f = co_await coroutine::as_future(remove_file(dir.native()));
        if (f.failed()) {
            try {
                std::rethrow_exception(f.get_exception());
            } catch (const std::system_error& e) {
                if (e.code().value() != ENOTEMPTY && e.code().value() != EEXIST) {
                    throw;
                }
            }
        }
    }

    future<> check_bucket(std::string_view bucket) {
        if (!co_await file_exists(bucket_dir(bucket).native())) {
            fail(errc::no_such_bucket, fmt::format("no such bucket '{}'", bucket));
        }
    }

    session_state& active_session(std::string_view bucket, std::string_view key, const session_id& id) {
        auto it = _sessions.find(id);
        if (it == _sessions.end() || it->second.bucket != bucket || it->second.key != key
                || it->second.committed || it->second.committing) {
            fail(errc::no_such_session, fmt::format("no such session '{}' for {}/{}", id, bucket, key));
        }
        return it->second;
    }

    static future<> close_all(std::vector<file>& files) {
        for (auto& f : files) {
            co_await f.close();
        }
        files.clear();
    }

    future<std::optional<object_meta>> try_load_meta(std::string_view bucket, std::string_view key) {
        auto f = co_await coroutine::as_future(util::read_entire_file_contiguous(object_dir(bucket, key) / meta_name));
        if (f.failed()) {
            auto ep = f.get_exception();
            if (is_enoent(ep)) {
                co_return std::nullopt;
            }
            std::rethrow_exception(ep);
        }
        auto m = deserialize(f.get());
        if (m.info.key != key) {
            throw std::runtime_error(fmt::format("keys '{}' and '{}' have the same hash", key, m.info.key));
        }
        co_return m;
    }

    future<object_meta> load_meta(sstring bucket, sstring key) {
        validate_bucket_name(bucket);
        auto m = co_await try_load_meta(bucket, key);
        if (!m) {
            co_await check_bucket(bucket);
            fail(errc::no_such_key, fmt::format("no such key '{}'", key));
        }
        co_return std::move(*m);
    }

    // Whether the bucket has any object or session.
    future<bool> has_objects(fs::path dir) {
        bool found = false;
        auto d = co_await open_directory(dir.native());
        co_await d.list_directory([&found] (directory_entry) {
            found = true;
            return make_ready_future();
        }).done();
        co_await d.close();
        co_return found;
    }

    // Runs under the write lock.
    future<object_info> publish(const session_id& id, session_state& st, object_meta m, const std::set<unsigned>& listed,
                                precondition pre) {
        auto old = co_await try_load_meta(st.bucket, st.key);
        if (pre.generation_match) {
            auto current = old ? old->info.generation : 0;
            if (current != *pre.generation_match) {
                fail(errc::precondition_failed, fmt::format("generation is {}, not {}", current, *pre.generation_match));
            }
        }

        // The session's directory becomes the object's data directory.
        auto dir = object_dir(st.bucket, st.key);
        m.info.generation = next_unique();
        m.data_dir = fmt::format("{}{:x}", data_prefix, m.info.generation);
        auto data_dir = dir / m.data_dir.c_str();
        co_await rename_file(session_dir(st.bucket, st.key, id).native(), data_dir.native());
        for (auto& [n, p] : st.parts) {
            if (!listed.contains(n)) {
                co_await remove_file((data_dir / p.name.c_str()).native());
            }
        }

        m.info.key = st.key;
        m.info.mtime = clock_type::now();
        m.info.metadata = st.attrs.metadata;

        auto path = dir / meta_name;
        auto tmp = fs::path(fmt::format("{}.{:x}.tmp", path.native(), m.info.generation));
        co_await write_small_file(tmp, serialize(m));
        co_await rename_file(tmp.native(), path.native());

        if (old) {
            co_await recursive_remove_directory(dir / old->data_dir.c_str());
        }
        co_return m.info;
    }

    // Makes a written part a member of its session, unless the session went
    // away while the part was being written.
    future<part_info> add_part(const sstring& bucket, const sstring& key, const session_id& id, unsigned number, sealed_part part) {
        auto it = _sessions.find(id);
        if (it == _sessions.end() || it->second.bucket != bucket || it->second.key != key
                || it->second.committed || it->second.committing) {
            if (it != _sessions.end() && !it->second.committed) {
                co_await remove_file((session_dir(bucket, key, id) / part.name.c_str()).native());
            }
            fail(errc::no_such_session, fmt::format("no such session '{}' for {}/{}", id, bucket, key));
        }
        auto& st = it->second;
        part_info ret{number, part.size};
        std::optional<sstring> replaced;
        if (auto old = st.parts.find(number); old != st.parts.end()) {
            replaced = old->second.name;
        }
        st.parts.insert_or_assign(number, std::move(part));
        if (replaced) {
            co_await remove_file((session_dir(bucket, key, id) / replaced->c_str()).native());
        }
        co_return ret;
    }
};

backend_type make_backend_type() {
    return backend_type{
        .name = "potato",
        .description = "Objects in plain files, in the simplest way possible; not for production use",
        .add_options = [] (bpo::options_description& opts) {
            opts.add_options()
                ("data-dir", bpo::value<std::string>(), "Directory to keep the data in (potato backend)");
        },
        .make = [] (const bpo::variables_map& opts) -> std::unique_ptr<storage> {
            if (!opts.contains("data-dir")) {
                throw std::invalid_argument("the potato backend requires --data-dir");
            }
            return std::make_unique<potato_storage>(fs::path(opts["data-dir"].as<std::string>()));
        },
    };
}

} // namespace labaz::potato
