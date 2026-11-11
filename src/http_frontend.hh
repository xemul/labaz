/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>

namespace labaz {

struct http_frontend_config {
    seastar::net::inet_address address;
    std::optional<uint16_t> http_port;
    std::optional<uint16_t> https_port;
    // Required when https_port is set. Copied to every shard, which builds
    // its own credentials from it.
    std::optional<seastar::tls::credentials_builder> tls;
};

/// The HTTP(S) server, one instance per shard, all listening on the same
/// addresses. Serves no routes yet.
class http_frontend {
    seastar::sharded<seastar::httpd::http_server> _servers;
    std::vector<seastar::socket_address> _http_addresses;
    std::vector<seastar::socket_address> _https_addresses;

public:
    /// Starts the servers on all shards and listens on the configured
    /// ports. On failure the servers are stopped before the error is
    /// returned.
    seastar::future<> start(http_frontend_config cfg);

    /// Stops listening and waits for in-flight connections to finish.
    seastar::future<> stop();

    const std::vector<seastar::socket_address>& http_addresses() const noexcept { return _http_addresses; }
    const std::vector<seastar::socket_address>& https_addresses() const noexcept { return _https_addresses; }
};

} // namespace labaz
