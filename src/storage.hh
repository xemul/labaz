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
#include <stdexcept>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

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
using clock_type = std::chrono::system_clock;

/// Attributes given when an upload starts and stored with the object.
struct object_attrs {
    user_metadata metadata;
};

struct object_info {
    sstring key;
    uint64_t size = 0;
    clock_type::time_point mtime;
    // Unique among all versions the key ever had, and grows with every
    // write of the key.
    uint64_t generation = 0;
    user_metadata metadata;
};

struct part_info {
    unsigned number = 0;
    uint64_t size = 0;
};

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

/// The storage backend. There is one instance per shard, and every call is
/// served by the shard it is made on: the storage does no routing between
/// shards. Running related calls (e.g. the calls of one upload session) on
/// the same shard is up to the caller.
///
/// Keys are non-empty and must not end with '/'.
class storage {
public:
    virtual ~storage() = default;

    virtual future<> start() = 0;
    virtual future<> stop() = 0;

    virtual future<> create_bucket(sstring bucket) = 0;
    /// Fails with bucket_not_empty if the bucket has objects or sessions.
    virtual future<> delete_bucket(sstring bucket) = 0;
    virtual future<std::vector<sstring>> list_buckets() = 0;

    // A session is addressed by its object's bucket and key along with its
    // id; a session of another object is reported as no_such_session.

    virtual future<session_id> open_session(sstring bucket, sstring key, object_attrs attrs) = 0;
    /// Reads `in` to its end as part `number` of the session, replacing an
    /// earlier part with the same number. On failure the part is discarded.
    /// The stream stays the caller's: it is not closed.
    virtual future<part_info> write_part(sstring bucket, sstring key, session_id id, unsigned number,
                                         seastar::input_stream<char>& in) = 0;
    virtual future<session_info> describe_session(sstring bucket, sstring key, session_id id) = 0;
    /// Publishes the listed parts, in the given order, as the session's
    /// object, replacing the current version atomically. Parts not listed
    /// are dropped. Committing an already committed session returns the
    /// same result again.
    virtual future<object_info> commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> parts,
                                               precondition pre) = 0;
    virtual future<> abort_session(sstring bucket, sstring key, session_id id) = 0;

    virtual future<object_info> head_object(sstring bucket, sstring key) = 0;
    virtual future<object_reader> read_object(sstring bucket, sstring key, std::optional<byte_range> range) = 0;
    virtual future<> delete_object(sstring bucket, sstring key) = 0;
    /// Keys in byte order, starting after `start_after`.
    virtual future<listing> list_objects(sstring bucket, sstring prefix, sstring start_after, unsigned max_keys) = 0;
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

/// Holds the shard's backend instance.
class storage_service {
    std::unique_ptr<storage> _impl;
public:
    storage_service(const backend_type& type, const boost::program_options::variables_map& opts);
    storage& get() noexcept { return *_impl; }
    future<> stop();
};

} // namespace labaz
