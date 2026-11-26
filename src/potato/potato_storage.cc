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
//   <data-dir>/.staging/                           links of copies on their
//                                                  way between shards
//
// Sealed part files are immutable. An object or a session part is a list
// of slices (byte ranges) of them, and a copy hardlinks the source's files
// instead of copying bytes. Every slice has a file (a link) of its own.
//
// A commit renames the session directory into a data directory, writes the
// meta to a temporary file and renames it over the current one, so an
// overwrite is atomic, and a failure before the meta rename leaves the
// current version intact.
//
// Every call is served on the shard it is made on, except that a copy links
// its source's files on the source's shard. Sessions live in the memory of
// the shard that opened them: in-progress uploads do not survive a restart,
// committed objects do.

#include "potato/potato_storage.hh"

#include <algorithm>
#include <chrono>
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
#include <seastar/util/short_streams.hh>
#include <seastar/util/log.hh>

namespace labaz::potato {

using namespace seastar;
namespace fs = std::filesystem;
namespace bpo = boost::program_options;

static logger plog("potato");

static double ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

struct object_lock;

// Objects' locks, by "<bucket>/<key>".
using object_locks = std::unordered_map<sstring, object_lock*>;

// An object's lock. It is kept while held or waited for, and leaves the
// map when the last user drops it.
struct object_lock : enable_lw_shared_from_this<object_lock> {
    rwlock lock;
    object_locks& locks;
    sstring name;

    object_lock(object_locks& locks, sstring name) : locks(locks), name(std::move(name)) {}
    ~object_lock() { locks.erase(name); }
};

// A hold of an object's lock. With tracing on, it names what it is for and
// traces how long it was held.
class lock_hold {
    lw_shared_ptr<object_lock> _lock;
    rwlock::holder _units;
    sstring _what;
    std::chrono::steady_clock::time_point _taken = std::chrono::steady_clock::now();

public:
    lock_hold(lw_shared_ptr<object_lock> lock, rwlock::holder units, sstring what)
        : _lock(std::move(lock)), _units(std::move(units)), _what(std::move(what)) {}
    lock_hold(lock_hold&&) noexcept = default;
    ~lock_hold() { release(); }

    void release() noexcept {
        if (_units.count() == 0) {
            return;
        }
        _units.return_all();
        _lock = {};
        if (!_what.empty()) {
            plog.trace("{}: lock held for {:.3f}ms", _what, ms_since(_taken));
        }
    }
};

static constexpr std::string_view meta_name = "meta.json";
static constexpr std::string_view data_prefix = "data.";
static constexpr std::string_view session_prefix = "session.";
static constexpr std::string_view part_prefix = "part.";
// Bucket names do not start with a dot.
static constexpr std::string_view staging_name = ".staging";

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
    if (b.empty() || b.size() > 63 || b.starts_with('.')
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

// A byte range of a part file. Sealed part files are immutable, so a copy
// hardlinks them and an object or a part refers to ranges of them.
struct slice {
    sstring file;   // within the data or session directory
    uint64_t offset;
    uint64_t size;
};

struct object_meta {
    object_info info;
    sstring data_dir;  // within the object directory
    std::vector<slice> slices;
};

// The [offset, offset + length) range of the bytes that `slices` make up.
static std::vector<slice> cut(const std::vector<slice>& slices, uint64_t offset, uint64_t length) {
    std::vector<slice> ret;
    for (auto& s : slices) {
        if (length == 0) {
            break;
        }
        if (offset >= s.size) {
            offset -= s.size;
            continue;
        }
        auto n = std::min(s.size - offset, length);
        ret.push_back(slice{s.file, s.offset + offset, n});
        offset = 0;
        length -= n;
    }
    if (length > 0) {
        throw std::runtime_error("object is shorter than its meta says");
    }
    return ret;
}

static sstring serialize(const object_meta& m) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> w(sb);
    auto str = [&w] (std::string_view s) { w.String(s.data(), s.size()); };
    w.StartObject();
    w.Key("key"); str(m.info.key);
    w.Key("size"); w.Uint64(m.info.size);
    w.Key("mtime_ns"); w.Int64(std::chrono::duration_cast<std::chrono::nanoseconds>(m.info.mtime.time_since_epoch()).count());
    w.Key("generation"); w.Uint64(m.info.generation);
    w.Key("multipart"); w.Bool(m.info.upload == upload_kind::multipart);
    w.Key("metadata");
    w.StartObject();
    for (auto& [k, v] : m.info.attrs.metadata) {
        str(k); str(v);
    }
    w.EndObject();
    w.Key("tags");
    w.StartObject();
    for (auto& [k, v] : m.info.attrs.tags) {
        str(k); str(v);
    }
    w.EndObject();
    w.Key("checksums");
    w.StartArray();
    for (auto alg : m.info.attrs.checksums) {
        str(to_string(alg));
    }
    w.EndArray();
    w.Key("parts");
    w.StartArray();
    for (auto& p : m.info.parts) {
        w.StartObject();
        w.Key("number"); w.Uint(p.number);
        w.Key("size"); w.Uint64(p.size);
        w.Key("checksums");
        w.StartObject();
        for (auto& [alg, value] : p.checksums) {
            str(to_string(alg)); str(to_hex(value));
        }
        w.EndObject();
        w.EndObject();
    }
    w.EndArray();
    w.Key("data"); str(m.data_dir);
    w.Key("slices");
    w.StartArray();
    for (auto& s : m.slices) {
        w.StartObject();
        w.Key("file"); str(s.file);
        w.Key("offset"); w.Uint64(s.offset);
        w.Key("size"); w.Uint64(s.size);
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
    m.info.upload = d["multipart"].GetBool() ? upload_kind::multipart : upload_kind::single;
    auto algorithm = [] (std::string_view name) {
        auto alg = checksum_algorithm_from_string(name);
        if (!alg) {
            throw std::runtime_error(fmt::format("unknown checksum algorithm '{}' in object meta", name));
        }
        return *alg;
    };
    for (auto& kv : d["metadata"].GetObject()) {
        m.info.attrs.metadata.emplace(str(kv.name), str(kv.value));
    }
    for (auto& kv : d["tags"].GetObject()) {
        m.info.attrs.tags.emplace(str(kv.name), str(kv.value));
    }
    for (auto& a : d["checksums"].GetArray()) {
        m.info.attrs.checksums.insert(algorithm(str(a)));
    }
    for (auto& p : d["parts"].GetArray()) {
        part_info part{p["number"].GetUint(), p["size"].GetUint64(), {}};
        for (auto& kv : p["checksums"].GetObject()) {
            auto value = from_hex(str(kv.value));
            if (!value) {
                throw std::runtime_error("corrupt checksum in object meta");
            }
            part.checksums.emplace(algorithm(str(kv.name)), std::move(*value));
        }
        m.info.parts.push_back(std::move(part));
    }
    m.data_dir = str(d["data"]);
    for (auto& s : d["slices"].GetArray()) {
        m.slices.push_back(slice{str(s["file"]), s["offset"].GetUint64(), s["size"].GetUint64()});
    }
    return m;
}

//
// Reading slices of part files
//

class slices_source final : public data_source_impl {
    std::vector<file> _files;
    std::vector<slice> _slices;
    size_t _idx = 0;
    uint64_t _left = 0;    // in the current slice
    std::optional<input_stream<char>> _cur;

public:
    // files[i] is the file of slices[i].
    slices_source(std::vector<file> files, std::vector<slice> slices)
        : _files(std::move(files)), _slices(std::move(slices))
    {}

    future<temporary_buffer<char>> get() override {
        while (_idx < _slices.size()) {
            if (!_cur) {
                auto& s = _slices[_idx];
                _cur.emplace(make_file_input_stream(_files[_idx], s.offset, s.size));
                _left = s.size;
            }
            auto buf = co_await _cur->read();
            if (buf.empty()) {
                if (_left > 0) {
                    throw std::runtime_error("part file is shorter than its slice");
                }
                co_await _cur->close();
                _cur.reset();
                _idx++;
                continue;
            }
            _left -= std::min<uint64_t>(_left, buf.size());
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
    std::vector<slice> slices;
    uint64_t size;
    checksums sums;
};

// The bytes a copy links: slices of files in `dir`, and the checksums of
// them known so far.
struct copy_source {
    fs::path dir;
    std::vector<slice> slices;
    checksums sums;
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
    // An object's lock is taken for write to publish, delete or abort, which
    // remove its directories; and for read to open or link its parts, so that
    // they are not removed under the reader or the copy, and to create a
    // session directory or add files to it, so that it is not removed
    // meanwhile. Objects of a shard are served by that shard only, so its
    // locks cover them.
    object_locks _locks;

    future<lock_hold> hold_lock(bool write, std::string_view op, const sstring& bucket, const sstring& key) {
        sstring what;
        if (plog.is_enabled(log_level::trace)) {
            what = fmt::format("{} {}/{} ({})", op, bucket, key, write ? "write" : "read");
        }
        auto start = std::chrono::steady_clock::now();
        auto name = sstring(fmt::format("{}/{}", bucket, key));
        lw_shared_ptr<object_lock> lock;
        if (auto it = _locks.find(name); it != _locks.end()) {
            lock = it->second->shared_from_this();
        } else {
            lock = make_lw_shared<object_lock>(_locks, name);
            _locks.emplace(std::move(name), lock.get());
        }
        auto units = co_await (write ? lock->lock.hold_write_lock() : lock->lock.hold_read_lock());
        if (!what.empty()) {
            plog.trace("{}: lock waited for {:.3f}ms", what, ms_since(start));
        }
        co_return lock_hold(std::move(lock), std::move(units), std::move(what));
    }

    future<lock_hold> read_lock(std::string_view op, const sstring& bucket, const sstring& key) {
        return hold_lock(false, op, bucket, key);
    }

    future<lock_hold> write_lock(std::string_view op, const sstring& bucket, const sstring& key) {
        return hold_lock(true, op, bucket, key);
    }

public:
    explicit potato_storage(fs::path root)
        : _root(std::move(root))
    {}

    future<> start() override {
        if (this_shard_id() == 0) {
            co_await recursive_touch_directory(_root.native());
            // Drop the links of copies a restart interrupted.
            if (co_await file_exists(staging_dir().native())) {
                co_await recursive_remove_directory(staging_dir());
            }
            co_await make_directory(staging_dir().native());
            plog.info("Storing data in {}", _root.native());
        }
    }

    future<> stop() override {
        co_return;
    }

    future<> create_bucket(sstring bucket) override {
        validate_bucket_name(bucket);
        auto dir = bucket_dir(bucket);
        // FIXME: two shards creating the bucket at once can both pass the
        // check, and the one that loses fails with EEXIST, not bucket_exists.
        if (co_await file_exists(dir.native())) {
            fail(errc::bucket_exists, fmt::format("bucket '{}' exists", bucket));
        }
        co_await make_directory(dir.native());
    }

    future<> delete_bucket(sstring bucket) override {
        validate_bucket_name(bucket);
        // FIXME: nothing excludes object calls on other shards, whose locks
        // are their own. A session opened there between the emptiness check
        // and the removal either loses its directory or fails the removal,
        // and listing a bucket being removed can fail; such races end with
        // errors, not with lost objects.
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
            if (de.type == directory_entry_type::directory && !de.name.starts_with('.')) {
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
        auto lock = co_await read_lock("open session", bucket, key);
        co_await touch_directory(dir.native());
        co_await make_directory(session_dir(bucket, key, id).native());
        _sessions.emplace(id, session_state{std::move(bucket), std::move(key), std::move(attrs), {}, false, std::nullopt});
        co_return id;
    }

    future<part_info> write_part(sstring bucket, sstring key, session_id id, unsigned number, input_stream<char>& in) override {
        auto algorithms = active_session(bucket, key, id).attrs.checksums;
        auto final = session_dir(bucket, key, id) / fmt::format("{}{}.{:x}", part_prefix, number, next_unique());
        auto tmp = fs::path(final.native() + ".tmp");
        auto f = co_await open_file_dma(tmp.native(), open_flags::wo | open_flags::create | open_flags::exclusive);
        auto out = co_await make_file_output_stream(std::move(f));
        checksums sums;
        std::exception_ptr ex;
        try {
            if (algorithms.empty()) {
                co_await copy(in, out);
            } else {
                auto summed = make_checksumming_stream(in, std::move(algorithms), [&sums] (checksums c) { sums = std::move(c); });
                co_await copy(summed, out);
            }
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
        co_return co_await add_part(bucket, key, id, number,
                                    sealed_part{{slice{final.filename().native(), 0, size}}, size, std::move(sums)});
    }

    future<part_info> copy_part(sstring bucket, sstring key, session_id id, unsigned number,
                                sstring src_key, std::optional<byte_range> range, precondition src_pre) override {
        auto algorithms = active_session(bucket, key, id).attrs.checksums;
        auto dir = session_dir(bucket, key, id);
        // The source is linked into the staging directory by its shard, under
        // its lock, and linked from there under the session's, so no one waits
        // for a lock while holding another.
        auto staged = co_await invoke_on(owner_of(bucket, src_key), [bucket, src_key, range, src_pre, algorithms] (storage& s) {
            return static_cast<potato_storage&>(s).stage_source(bucket, src_key, range, src_pre, algorithms);
        });
        sealed_part part{{}, 0, {}};
        std::exception_ptr ex;
        try {
            // Keeps the session directory in place; the links then live on
            // their own.
            auto lock = co_await read_lock("copy part", bucket, key);
            active_session(bucket, key, id);
            part = co_await link_slices(staged, dir, fmt::format("{}{}.", part_prefix, number));
        } catch (...) {
            ex = std::current_exception();
        }
        auto removed = co_await coroutine::as_future(remove_files(staged.dir, staged.slices));
        removed.ignore_ready_future();
        if (ex) {
            std::rethrow_exception(ex);
        }
        co_await complete_checksums(dir, part, algorithms);
        co_return co_await add_part(bucket, key, id, number, std::move(part));
    }

    future<session_info> describe_session(sstring bucket, sstring key, session_id id) override {
        auto& st = active_session(bucket, key, id);
        session_info ret{id, st.bucket, st.key, st.attrs, {}};
        for (auto& [n, p] : st.parts) {
            ret.parts.push_back(part_info{n, p.size, p.sums});
        }
        co_return ret;
    }

    future<object_info> commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> numbers,
                                       precondition pre, upload_kind kind) override {
        auto it = _sessions.find(id);
        if (it != _sessions.end() && it->second.committed && it->second.bucket == bucket && it->second.key == key) {
            co_return *it->second.committed;
        }
        auto& st = active_session(bucket, key, id);

        object_meta m;
        m.info.upload = kind;
        std::set<unsigned> listed;
        for (auto n : numbers) {
            auto p = st.parts.find(n);
            if (p == st.parts.end()) {
                fail(errc::no_such_part, fmt::format("session '{}' has no part {}", id, n));
            }
            if (!listed.insert(n).second) {
                fail(errc::invalid_argument, fmt::format("part {} listed twice", n));
            }
            m.slices.insert(m.slices.end(), p->second.slices.begin(), p->second.slices.end());
            m.info.size += p->second.size;
            m.info.parts.push_back(part_info{n, p->second.size, p->second.sums});
        }

        st.committing = true;
        auto lock = co_await write_lock("commit session", bucket, key);
        auto f = co_await coroutine::as_future(publish(id, st, std::move(m), listed, pre));
        lock.release();
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
        auto lock = co_await write_lock("abort session", bucket, key);
        co_await recursive_remove_directory(session_dir(bucket, key, id));
        co_await remove_if_empty(object_dir(bucket, key));
    }

    future<object_info> head_object(sstring bucket, sstring key) override {
        auto m = co_await load_meta(bucket, key);
        co_return std::move(m.info);
    }

    future<object_reader> read_object(sstring bucket, sstring key, std::optional<byte_range> range) override {
        // Once the parts are open, removing them does not affect the reader.
        auto lock = co_await read_lock("read", bucket, key);
        auto m = co_await load_meta(bucket, key);
        auto [offset, length] = range ? resolve_range(*range, m.info.size) : resolved_range{0, m.info.size};
        auto src = co_await open_slices(object_dir(bucket, key) / m.data_dir.c_str(), cut(m.slices, offset, length));
        co_return object_reader{std::move(m.info), input_stream<char>(std::move(src))};
    }

    future<> delete_object(sstring bucket, sstring key) override {
        auto lock = co_await write_lock("delete", bucket, key);
        auto m = co_await load_meta(bucket, key);
        auto dir = object_dir(bucket, key);
        co_await remove_file((dir / meta_name).native());
        co_await recursive_remove_directory(dir / m.data_dir.c_str());
        co_await remove_if_empty(dir);
    }

    future<object_info> update_object(sstring bucket, sstring key, attrs_update update, precondition pre) override {
        auto lock = co_await write_lock("update", bucket, key);
        auto m = co_await load_meta(bucket, key);
        if (pre.generation_match && *pre.generation_match != m.info.generation) {
            fail(errc::precondition_failed, fmt::format("generation is {}, not {}", m.info.generation, *pre.generation_match));
        }
        if (update.metadata) {
            m.info.attrs.metadata = std::move(*update.metadata);
        }
        if (update.tags) {
            m.info.attrs.tags = std::move(*update.tags);
        }
        co_await write_meta(object_dir(bucket, key), m);
        co_return m.info;
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

    fs::path staging_dir() const {
        return _root / staging_name;
    }

    // The source object's bytes (all of them, or the range), and what
    // checksums of them can be derived from its parts'. The source's files
    // stay as long as the lock is held.
    future<copy_source> find_source(const sstring& bucket, const sstring& src_key, std::optional<byte_range> range,
                                    precondition src_pre, const std::set<checksum_algorithm>& algorithms) {
        auto m = co_await load_meta(bucket, src_key);
        if (src_pre.generation_match && *src_pre.generation_match != m.info.generation) {
            fail(errc::precondition_failed, fmt::format("source generation is {}, not {}",
                                                        m.info.generation, *src_pre.generation_match));
        }
        auto [offset, length] = range ? resolve_range(*range, m.info.size) : resolved_range{0, m.info.size};
        copy_source ret{object_dir(bucket, src_key) / m.data_dir.c_str(), cut(m.slices, offset, length), {}};
        if (offset == 0 && length == m.info.size) {
            ret.sums = concatenated_checksums(m.info.parts, algorithms);
        }
        co_return ret;
    }

    // Links the source's slices into `to`, in files named <prefix><unique>;
    // on failure, removes the links made.
    future<sealed_part> link_slices(const copy_source& src, fs::path to, sstring prefix) {
        sealed_part part{{}, 0, src.sums};
        std::exception_ptr ex;
        try {
            for (auto& s : src.slices) {
                auto name = sstring(fmt::format("{}{:x}", prefix, next_unique()));
                co_await link_file((src.dir / s.file.c_str()).native(), (to / name.c_str()).native());
                part.slices.push_back(slice{std::move(name), s.offset, s.size});
                part.size += s.size;
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            auto removed = co_await coroutine::as_future(remove_files(to, part.slices));
            removed.ignore_ready_future();
            std::rethrow_exception(ex);
        }
        co_return part;
    }

    // Run on the source's shard for a copy: links the source's files into
    // the staging directory, where they stay after the
    // lock is released.
    future<copy_source> stage_source(sstring bucket, sstring src_key, std::optional<byte_range> range,
                                     precondition src_pre, std::set<checksum_algorithm> algorithms) {
        auto lock = co_await read_lock("stage copy source", bucket, src_key);
        auto src = co_await find_source(bucket, src_key, range, src_pre, algorithms);
        auto part = co_await link_slices(src, staging_dir(), fmt::format("{}.", this_shard_id()));
        co_return copy_source{staging_dir(), std::move(part.slices), std::move(part.sums)};
    }

    // Computes the checksums the part lacks from its files in `dir`; on
    // failure, removes them. The files are links of the copy's own, so
    // they are read without the lock.
    static future<> complete_checksums(fs::path dir, sealed_part& part, const std::set<checksum_algorithm>& algorithms) {
        std::set<checksum_algorithm> missing;
        std::ranges::copy_if(algorithms, std::inserter(missing, missing.end()),
                             [&part] (checksum_algorithm alg) { return !part.sums.contains(alg); });
        if (missing.empty()) {
            co_return;
        }
        auto f = co_await coroutine::as_future(checksum_slices(dir, part.slices, std::move(missing)));
        if (f.failed()) {
            auto ex = f.get_exception();
            auto removed = co_await coroutine::as_future(remove_files(dir, part.slices));
            removed.ignore_ready_future();
            std::rethrow_exception(ex);
        }
        part.sums.merge(f.get());
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

    // Every slice has a file of its own.
    static future<> remove_files(fs::path dir, const std::vector<slice>& slices) {
        for (auto& s : slices) {
            co_await remove_file((dir / s.file.c_str()).native());
        }
    }

    static future<> close_all(std::vector<file>& files) {
        for (auto& f : files) {
            co_await f.close();
        }
        files.clear();
    }

    // A source of the slices' data; their files are open once it is returned.
    static future<data_source> open_slices(fs::path dir, std::vector<slice> slices) {
        std::vector<file> files;
        std::exception_ptr ex;
        try {
            for (auto& s : slices) {
                files.push_back(co_await open_file_dma((dir / s.file.c_str()).native(), open_flags::ro));
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await close_all(files);
            std::rethrow_exception(ex);
        }
        co_return data_source(std::make_unique<slices_source>(std::move(files), std::move(slices)));
    }

    // Reads the slices' data to compute its checksums.
    static future<checksums> checksum_slices(fs::path dir, std::vector<slice> slices, std::set<checksum_algorithm> algorithms) {
        auto in = input_stream<char>(co_await open_slices(std::move(dir), std::move(slices)));
        checksums ret;
        auto summed = make_checksumming_stream(in, std::move(algorithms), [&ret] (checksums c) { ret = std::move(c); });
        auto f = co_await coroutine::as_future(util::skip_entire_stream(summed));
        co_await in.close();
        if (f.failed()) {
            std::rethrow_exception(f.get_exception());
        }
        co_return ret;
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

    // Replaces the object's meta atomically. Runs under the write lock.
    future<> write_meta(const fs::path& dir, const object_meta& m) {
        auto path = dir / meta_name;
        auto tmp = fs::path(fmt::format("{}.{:x}.tmp", path.native(), next_unique()));
        co_await write_small_file(tmp, serialize(m));
        co_await rename_file(tmp.native(), path.native());
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
                co_await remove_files(data_dir, p.slices);
            }
        }

        m.info.key = st.key;
        m.info.mtime = clock_type::now();
        m.info.attrs = st.attrs;
        co_await write_meta(dir, m);

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
                co_await remove_files(session_dir(bucket, key, id), part.slices);
            }
            fail(errc::no_such_session, fmt::format("no such session '{}' for {}/{}", id, bucket, key));
        }
        auto& st = it->second;
        part_info ret{number, part.size, part.sums};
        std::vector<slice> replaced;
        if (auto old = st.parts.find(number); old != st.parts.end()) {
            replaced = std::move(old->second.slices);
        }
        st.parts.insert_or_assign(number, std::move(part));
        co_await remove_files(session_dir(bucket, key, id), replaced);
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
