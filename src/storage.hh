/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include "checksum.hh"
#include "sharder.hh"

namespace labaz {

using seastar::future;
using seastar::sstring;

/// Protocol-neutral error codes. Facades render them in their own protocol.
enum class errc {
    no_such_bucket,
    no_such_key,
    no_such_session,
    no_such_part,
    bucket_exists,
    bucket_not_empty,
    precondition_failed,
    invalid_argument,
    invalid_range,
};

std::string_view to_string(errc);

class storage_error : public std::runtime_error {
    errc _code;
public:
    storage_error(errc code, const std::string& msg) : std::runtime_error(msg), _code(code) {}
    errc code() const noexcept { return _code; }
};

using user_metadata = std::map<sstring, sstring>;
using object_tags = std::map<sstring, sstring>;
using clock_type = std::chrono::system_clock;

/// Attributes given when an upload starts and stored with the object.
struct object_attrs {
    user_metadata metadata;
    object_tags tags;
    // Computed for every part.
    std::set<checksum_algorithm> checksums;
};

struct part_info {
    unsigned number = 0;
    uint64_t size = 0;
    // Of the part's data, for the algorithms of the object's attributes.
    labaz::checksums checksums;
};

/// How an object was uploaded, as the facade that committed it says. Some
/// protocols tell the two apart (S3's ETags do), the storage only keeps it.
enum class upload_kind {
    single,     // in one request
    multipart,  // part by part
};

struct object_info {
    sstring key;
    uint64_t size = 0;
    clock_type::time_point mtime;
    // Unique among all versions the key ever had, and grows with every
    // write of the key.
    uint64_t generation = 0;
    object_attrs attrs;
    // The parts the object was committed from, in their order.
    std::vector<part_info> parts;
    upload_kind upload = upload_kind::single;
};

/// The checksums of the parts' data concatenated, for those of the
/// algorithms that can be derived from the parts' checksums: all the
/// checksums of a single part, those of no data for no parts, and otherwise
/// the CRCs that every part has, combined. Never reads data.
labaz::checksums concatenated_checksums(const std::vector<part_info>& parts,
                                        const std::set<checksum_algorithm>& algorithms);

/// The checksums of the object's data as a whole, as far as they can be
/// derived from its parts' (see concatenated_checksums()).
labaz::checksums object_checksums(const object_info& info);

using session_id = sstring;

struct session_info {
    session_id id;
    sstring bucket;
    sstring key;
    object_attrs attrs;
    // Sealed parts, ordered by number.
    std::vector<part_info> parts;
};

/// A condition checked against the object's current version when a commit
/// publishes a new one.
struct precondition {
    // The generation the current version must have; 0 means the key must
    // not exist.
    std::optional<uint64_t> generation_match;
};

/// An HTTP-style byte range: [first, last] inclusive, to the end of the
/// object when last is missing, the final `last` bytes when first is missing.
struct byte_range {
    std::optional<uint64_t> first;
    std::optional<uint64_t> last;
};

/// A byte_range resolved against an object's size.
struct resolved_range {
    uint64_t offset = 0;
    uint64_t length = 0;
};

/// Throws storage_error(invalid_range) if no byte of the object is in the
/// range.
resolved_range resolve_range(const byte_range& r, uint64_t size);

/// The attributes update_object() replaces: those given.
struct attrs_update {
    std::optional<user_metadata> metadata;
    std::optional<object_tags> tags;
};

struct object_reader {
    // The version being read; resolve_range(range, info.size) gives the
    // bytes `data` yields.
    object_info info;
    // The caller must close it.
    seastar::input_stream<char> data;
};

struct listing {
    std::vector<object_info> objects;
    // More keys follow the last one in `objects`.
    bool truncated = false;
};

class storage_service;

/// The interface of a storage backend, used through storage_service. There
/// is one instance per shard, and every object belongs to one shard (see
/// sharder): the calls about an object, and about its upload sessions, are
/// made only on that shard, as storage_service routes them there. Bucket
/// calls and listing are made on the shard they were called on, and the
/// backend answers them for all shards: a backend whose shards do not share
/// their data spreads them over the shards itself (with owner_of() and
/// invoke_on()), and merges the listings. A backend that needs another
/// object's shard (as a copy does its source's) reaches it with owner_of()
/// and invoke_on() as well.
///
/// The calls are those of storage_service, which documents them.
class storage {
    friend class storage_service;

    storage_service* _service = nullptr;

public:
    virtual ~storage() = default;

    virtual future<> start() = 0;
    virtual future<> stop() = 0;

protected:
    virtual future<> create_bucket(sstring bucket) = 0;
    virtual future<> delete_bucket(sstring bucket) = 0;
    virtual future<std::vector<sstring>> list_buckets() = 0;
    virtual future<session_id> open_session(sstring bucket, sstring key, object_attrs attrs) = 0;
    virtual future<part_info> write_part(sstring bucket, sstring key, session_id id, unsigned number,
                                         seastar::input_stream<char>& in) = 0;
    virtual future<part_info> copy_part(sstring bucket, sstring key, session_id id, unsigned number,
                                        sstring src_key, std::optional<byte_range> range, precondition src_pre) = 0;
    virtual future<session_info> describe_session(sstring bucket, sstring key, session_id id) = 0;
    virtual future<object_info> commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> parts,
                                               precondition pre, upload_kind kind) = 0;
    virtual future<> abort_session(sstring bucket, sstring key, session_id id) = 0;
    virtual future<object_info> head_object(sstring bucket, sstring key) = 0;
    virtual future<object_reader> read_object(sstring bucket, sstring key, std::optional<byte_range> range) = 0;
    virtual future<> delete_object(sstring bucket, sstring key) = 0;
    virtual future<object_info> update_object(sstring bucket, sstring key, attrs_update update, precondition pre) = 0;
    virtual future<listing> list_objects(sstring bucket, sstring prefix, sstring start_after, unsigned max_keys) = 0;

    inline unsigned owner_of(std::string_view bucket, std::string_view key) const;

    // Runs func(storage&) with the storage of the given shard; its result
    // moves to this shard.
    template <typename Func>
    auto invoke_on(unsigned shard, Func func);
};

/// A backend type, selected with --backend.
struct backend_type {
    sstring name;
    sstring description;
    // Adds the backend's own command line options.
    std::function<void(boost::program_options::options_description&)> add_options;
    // Called on every shard.
    std::function<std::unique_ptr<storage>(const boost::program_options::variables_map&)> make;
};

const std::vector<backend_type>& backend_types();
const backend_type* find_backend_type(std::string_view name);

/// The storage: the shard's instance of the backend, with the calls routed
/// to the shard owning their object. There is one instance per shard.
///
/// Keys are non-empty and must not end with '/'.
class storage_service : public seastar::peering_sharded_service<storage_service> {
    friend class storage;

    std::unique_ptr<storage> _impl;
    sharder _sharder;

    unsigned owner_of(std::string_view bucket, std::string_view key) const {
        return _sharder.shard_of(bucket, key);
    }

    template <typename Func>
    auto invoke_on(unsigned shard, Func func) {
        return container().invoke_on(shard, [func = std::move(func)] (storage_service& s) mutable {
            return func(*s._impl);
        });
    }

public:
    storage_service(const backend_type& type, const boost::program_options::variables_map& opts);

    future<> start();
    future<> stop();

    future<> create_bucket(sstring bucket);
    /// Fails with bucket_not_empty if the bucket has objects or sessions.
    future<> delete_bucket(sstring bucket);
    future<std::vector<sstring>> list_buckets();

    // A session is addressed by its object's bucket and key along with its
    // id; a session of another object is reported as no_such_session.

    future<session_id> open_session(sstring bucket, sstring key, object_attrs attrs);
    /// Reads `in` to its end as part `number` of the session, replacing an
    /// earlier part with the same number. On failure the part is discarded.
    /// The stream stays the caller's: it is not closed.
    future<part_info> write_part(sstring bucket, sstring key, session_id id, unsigned number,
                                 seastar::input_stream<char>& in);
    /// Makes the bytes of the bucket's object `src_key` (all of them, or the
    /// range) part `number` of the session, replacing an earlier part with
    /// the same number. `src_pre` is checked against the source object.
    future<part_info> copy_part(sstring bucket, sstring key, session_id id, unsigned number,
                                sstring src_key, std::optional<byte_range> range, precondition src_pre);
    future<session_info> describe_session(sstring bucket, sstring key, session_id id);
    /// Publishes the listed parts, in the given order, as the session's
    /// object, replacing the current version atomically. Parts not listed
    /// are dropped. The object's info records `kind`. Committing an already
    /// committed session returns the same result again.
    future<object_info> commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> parts,
                                       precondition pre, upload_kind kind);
    future<> abort_session(sstring bucket, sstring key, session_id id);

    future<object_info> head_object(sstring bucket, sstring key);
    future<object_reader> read_object(sstring bucket, sstring key, std::optional<byte_range> range);
    future<> delete_object(sstring bucket, sstring key);
    /// Replaces the attributes of the object's current version that the
    /// update has, in place: the data, the generation and the modification
    /// time stay. `pre` is checked against that version.
    future<object_info> update_object(sstring bucket, sstring key, attrs_update update, precondition pre);
    /// Keys in byte order, starting after `start_after`.
    future<listing> list_objects(sstring bucket, sstring prefix, sstring start_after, unsigned max_keys);
};

unsigned storage::owner_of(std::string_view bucket, std::string_view key) const {
    return _service->owner_of(bucket, key);
}

template <typename Func>
auto storage::invoke_on(unsigned shard, Func func) {
    return _service->invoke_on(shard, std::move(func));
}

} // namespace labaz
