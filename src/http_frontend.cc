/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "http_frontend.hh"

#include <seastar/core/coroutine.hh>

namespace labaz {

using namespace seastar;

future<> http_frontend::start(http_frontend_config cfg) {
    co_await _servers.start(sstring("labaz"));

    std::exception_ptr ex;
    try {
        co_await _servers.invoke_on_all([] (httpd::http_server& s) {
            // Request bodies are streamed rather than buffered, as objects
            // can be arbitrarily large.
            s.set_content_streaming(true);
            s.set_server_header(sstring("Labaz"));
        });

        if (cfg.http_port) {
            socket_address addr(cfg.address, *cfg.http_port);
            co_await _servers.invoke_on_all([addr] (httpd::http_server& s) {
                return s.listen(addr);
            });
            _http_addresses.push_back(addr);
        }

        if (cfg.https_port) {
            socket_address addr(cfg.address, *cfg.https_port);
            co_await _servers.invoke_on_all([addr, &builder = *cfg.tls] (httpd::http_server& s) {
                return s.listen(addr, builder.build_server_credentials());
            });
            _https_addresses.push_back(addr);
        }
    } catch (...) {
        ex = std::current_exception();
    }

    if (ex) {
        co_await _servers.stop();
        std::rethrow_exception(ex);
    }
}

future<> http_frontend::stop() {
    return _servers.stop();
}

} // namespace labaz
