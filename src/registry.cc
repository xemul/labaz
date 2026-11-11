/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

// The list of built-in backends and facades, and the services that hold
// their per-shard instances.

#include "facade.hh"
#include "plywood/plywood_facade.hh"
#include "potato/potato_storage.hh"
#include "storage.hh"

namespace labaz {

const std::vector<backend_type>& backend_types() {
    static const std::vector<backend_type> types = {
        potato::make_backend_type(),
    };
    return types;
}

const backend_type* find_backend_type(std::string_view name) {
    for (auto& t : backend_types()) {
        if (t.name == name) {
            return &t;
        }
    }
    return nullptr;
}

const std::vector<facade_type>& facade_types() {
    static const std::vector<facade_type> types = {
        plywood::make_facade_type(),
    };
    return types;
}

const facade_type* find_facade_type(std::string_view name) {
    for (auto& t : facade_types()) {
        if (t.name == name) {
            return &t;
        }
    }
    return nullptr;
}

storage_service::storage_service(const backend_type& type, const boost::program_options::variables_map& opts)
    : _impl(type.make(opts))
{
    _impl->_service = this;
}

future<> storage_service::stop() {
    return _impl->stop();
}

facade_service::facade_service(const facade_type& type, const boost::program_options::variables_map& opts,
                               storage_service& storage)
    : _impl(type.make(opts, storage.get()))
{}

future<> facade_service::stop() {
    return _impl->stop();
}

} // namespace labaz
