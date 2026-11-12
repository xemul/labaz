/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage.hh"

#include <algorithm>

#include <seastar/core/smp.hh>

#include "streams.hh"

namespace labaz {

std::string_view to_string(errc e) {
    switch (e) {
    case errc::no_such_bucket: return "no_such_bucket";
    case errc::no_such_key: return "no_such_key";
    case errc::no_such_session: return "no_such_session";
    case errc::no_such_part: return "no_such_part";
    case errc::bucket_exists: return "bucket_exists";
    case errc::bucket_not_empty: return "bucket_not_empty";
    case errc::precondition_failed: return "precondition_failed";
    case errc::invalid_argument: return "invalid_argument";
    case errc::invalid_range: return "invalid_range";
    }
    return "unknown";
}

resolved_range resolve_range(const byte_range& r, uint64_t size) {
    if (r.first) {
        if (*r.first >= size || (r.last && *r.last < *r.first)) {
            throw storage_error(errc::invalid_range, "range not satisfiable");
        }
        uint64_t last = r.last ? std::min(*r.last, size - 1) : size - 1;
        return {*r.first, last - *r.first + 1};
    }
    if (!r.last || *r.last == 0 || size == 0) {
        throw storage_error(errc::invalid_range, "range not satisfiable");
    }
    uint64_t len = std::min(*r.last, size);
    return {size - len, len};
}

checksums concatenated_checksums(const std::vector<part_info>& parts, const std::set<checksum_algorithm>& algorithms) {
    if (parts.empty()) {
        return checksummer(algorithms).finish();
    }
    checksums ret;
    for (auto alg : algorithms) {
        if (parts.size() > 1 && !is_crc(alg)) {
            continue;
        }
        if (!std::ranges::all_of(parts, [alg] (const part_info& p) { return p.checksums.contains(alg); })) {
            continue;
        }
        auto value = parts[0].checksums.at(alg);
        for (size_t i = 1; i < parts.size(); i++) {
            value = crc_combine(alg, value, parts[i].checksums.at(alg), parts[i].size);
        }
        ret.emplace(alg, std::move(value));
    }
    return ret;
}

checksums object_checksums(const object_info& info) {
    return concatenated_checksums(info.parts, info.attrs.checksums);
}

//
// Routing calls to the shard owning their object
//

using namespace seastar;

future<> storage_service::start() {
    return _impl->start();
}

future<> storage_service::stop() {
    return _impl->stop();
}

future<> storage_service::create_bucket(sstring bucket) {
    return _impl->create_bucket(std::move(bucket));
}

future<> storage_service::delete_bucket(sstring bucket) {
    return _impl->delete_bucket(std::move(bucket));
}

future<std::vector<sstring>> storage_service::list_buckets() {
    return _impl->list_buckets();
}

future<listing> storage_service::list_objects(sstring bucket, sstring prefix, sstring start_after, unsigned max_keys) {
    return _impl->list_objects(std::move(bucket), std::move(prefix), std::move(start_after), max_keys);
}

future<session_id> storage_service::open_session(sstring bucket, sstring key, object_attrs attrs) {
    return invoke_on(owner_of(bucket, key), [bucket, key, attrs = std::move(attrs)] (storage& s) {
        return s.open_session(bucket, key, attrs);
    });
}

future<part_info> storage_service::write_part(sstring bucket, sstring key, session_id id, unsigned number, input_stream<char>& in) {
    auto owner = owner_of(bucket, key);
    if (owner == this_shard_id()) {
        return _impl->write_part(std::move(bucket), std::move(key), std::move(id), number, in);
    }
    // The owner reads `in` from this shard, which waits for it to finish.
    return invoke_on(owner, [bucket, key, id, number, &in, from = this_shard_id()] (storage& s) {
        return do_with(make_foreign_stream(from, in, deleter()), [&s, bucket, key, id, number] (input_stream<char>& foreign) {
            return s.write_part(bucket, key, id, number, foreign);
        });
    });
}

future<part_info> storage_service::copy_part(sstring bucket, sstring key, session_id id, unsigned number,
                                             sstring src_key, std::optional<byte_range> range, precondition src_pre) {
    return invoke_on(owner_of(bucket, key), [=] (storage& s) {
        return s.copy_part(bucket, key, id, number, src_key, range, src_pre);
    });
}

future<session_info> storage_service::describe_session(sstring bucket, sstring key, session_id id) {
    return invoke_on(owner_of(bucket, key), [bucket, key, id] (storage& s) {
        return s.describe_session(bucket, key, id);
    });
}

future<object_info> storage_service::commit_session(sstring bucket, sstring key, session_id id, std::vector<unsigned> parts,
                                                    precondition pre) {
    return invoke_on(owner_of(bucket, key), [bucket, key, id, parts = std::move(parts), pre] (storage& s) {
        return s.commit_session(bucket, key, id, parts, pre);
    });
}

future<> storage_service::abort_session(sstring bucket, sstring key, session_id id) {
    return invoke_on(owner_of(bucket, key), [bucket, key, id] (storage& s) {
        return s.abort_session(bucket, key, id);
    });
}

future<object_info> storage_service::head_object(sstring bucket, sstring key) {
    return invoke_on(owner_of(bucket, key), [bucket, key] (storage& s) {
        return s.head_object(bucket, key);
    });
}

future<> storage_service::delete_object(sstring bucket, sstring key) {
    return invoke_on(owner_of(bucket, key), [bucket, key] (storage& s) {
        return s.delete_object(bucket, key);
    });
}

namespace {

// A reader whose stream stays on the shard that made it.
struct parked_reader {
    object_info info;
    foreign_ptr<std::unique_ptr<input_stream<char>>> data;
};

}

future<object_reader> storage_service::read_object(sstring bucket, sstring key, std::optional<byte_range> range) {
    auto owner = owner_of(bucket, key);
    if (owner == this_shard_id()) {
        return _impl->read_object(std::move(bucket), std::move(key), range);
    }
    return invoke_on(owner, [bucket, key, range] (storage& s) {
        return s.read_object(bucket, key, range).then([] (object_reader r) {
            return parked_reader{std::move(r.info), make_foreign(std::make_unique<input_stream<char>>(std::move(r.data)))};
        });
    }).then([owner] (parked_reader r) {
        auto& in = *r.data;
        return object_reader{std::move(r.info), make_foreign_stream(owner, in, make_object_deleter(std::move(r.data)))};
    });
}

} // namespace labaz
