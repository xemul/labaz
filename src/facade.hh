/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include <boost/program_options.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/routes.hh>

#include "storage.hh"

namespace labaz {

/// A wire protocol on top of the storage. There is one instance per shard
/// for every facade given with --facade.
class facade {
public:
    virtual ~facade() = default;

    /// Installs the facade's handlers into the shard's HTTP server routes,
    /// which take ownership of them. The facade must outlive the server.
    virtual void set_routes(seastar::httpd::routes& r) = 0;

    virtual future<> stop() { return seastar::make_ready_future(); }
};

/// A facade type, selected with --facade.
struct facade_type {
    sstring name;
    sstring description;
    std::function<void(boost::program_options::options_description&)> add_options;
    // Called on every shard with that shard's storage.
    std::function<std::unique_ptr<facade>(const boost::program_options::variables_map&, storage_service&)> make;
};

const std::vector<facade_type>& facade_types();
const facade_type* find_facade_type(std::string_view name);

/// Holds the shard's instance of one facade.
class facade_service {
    std::unique_ptr<facade> _impl;
public:
    facade_service(const facade_type& type, const boost::program_options::variables_map& opts,
                   storage_service& storage);
    facade& get() noexcept { return *_impl; }
    future<> stop();
};

} // namespace labaz
