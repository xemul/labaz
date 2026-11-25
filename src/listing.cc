/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "listing.hh"

#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

namespace labaz {

using namespace seastar;

// How many keys a listing asks the storage for at once.
static constexpr unsigned list_batch = 1000;

namespace {

class lister {
    const list_request& _lr;
    list_result _ret;
    unsigned _count = 0;

public:
    explicit lister(const list_request& lr) : _lr(lr) {}

    // Adds the object, or its common prefix. Returns false when the
    // listing is full.
    bool add(object_info o) {
        std::optional<sstring> common;
        if (!_lr.delimiter.empty()) {
            auto pos = o.key.find(_lr.delimiter, _lr.prefix.size());
            if (pos != sstring::npos) {
                common = o.key.substr(0, pos + _lr.delimiter.size());
            }
        }
        if (common && (!(_lr.after < *common) || (!_ret.prefixes.empty() && _ret.prefixes.back() == *common))) {
            return true;
        }
        if (_count == _lr.max_entries) {
            _ret.truncated = true;
            return false;
        }
        _count++;
        if (common) {
            _ret.last = *common;
            _ret.prefixes.push_back(std::move(*common));
        } else {
            _ret.last = o.key;
            _ret.objects.push_back(std::move(o));
        }
        return true;
    }

    list_result result() && {
        return std::move(_ret);
    }
};

}

future<list_result> list_delimited(storage_service& storage, const list_request& lr) {
    lister l(lr);
    if (lr.inclusive && !lr.after.empty() && lr.after.starts_with(lr.prefix)) {
        // The storage lists what follows a key, so look the key itself up.
        auto f = co_await coroutine::as_future(storage.head_object(lr.bucket, lr.after));
        if (!f.failed()) {
            if (!l.add(f.get())) {
                co_return std::move(l).result();
            }
        } else {
            auto ex = f.get_exception();
            try {
                std::rethrow_exception(ex);
            } catch (const storage_error& e) {
                // Not a key that is there, or could be.
                if (e.code() != errc::no_such_key && e.code() != errc::invalid_argument) {
                    throw;
                }
            }
        }
    }
    sstring cursor = lr.after;
    while (true) {
        auto batch = co_await storage.list_objects(lr.bucket, lr.prefix, cursor, list_batch);
        for (auto& o : batch.objects) {
            cursor = o.key;
            if (!l.add(std::move(o))) {
                co_return std::move(l).result();
            }
        }
        if (!batch.truncated) {
            co_return std::move(l).result();
        }
    }
}

} // namespace labaz
