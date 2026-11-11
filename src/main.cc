/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <iostream>
#include <stdexcept>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include "http_frontend.hh"
#include "stop_signal.hh"

namespace bpo = boost::program_options;

using namespace seastar;

static logger lblog("labaz");

static labaz::http_frontend_config make_frontend_config(const bpo::variables_map& opts) {
    labaz::http_frontend_config cfg;

    cfg.address = net::inet_address(opts["listen-address"].as<std::string>());
    if (opts.contains("http-port")) {
        cfg.http_port = opts["http-port"].as<uint16_t>();
    }
    if (opts.contains("https-port")) {
        cfg.https_port = opts["https-port"].as<uint16_t>();
    }
    if (!cfg.http_port && !cfg.https_port) {
        throw std::invalid_argument("at least one of --http-port and --https-port is required");
    }

    bool have_cert = opts.contains("tls-cert");
    bool have_key = opts.contains("tls-key");
    if (have_cert != have_key) {
        throw std::invalid_argument("--tls-cert and --tls-key must be given together");
    }
    if (cfg.https_port && !have_cert) {
        throw std::invalid_argument("--https-port requires --tls-cert and --tls-key");
    }
    if (have_cert) {
        tls::credentials_builder builder;
        builder.set_x509_key_file(opts["tls-cert"].as<std::string>(),
                                  opts["tls-key"].as<std::string>(),
                                  tls::x509_crt_format::PEM).get();
        cfg.tls = std::move(builder);
    }

    return cfg;
}

int main(int ac, char** av) {
    app_template::config app_cfg;
    app_cfg.name = "labaz";
    app_cfg.description = "S3- and GCS-compatible object storage";
    app_template app(std::move(app_cfg));

    app.add_options()
        ("listen-address", bpo::value<std::string>()->default_value("127.0.0.1"), "Address to listen on")
        ("http-port", bpo::value<uint16_t>(), "Port for plain HTTP")
        ("https-port", bpo::value<uint16_t>(), "Port for HTTPS; needs --tls-cert and --tls-key")
        ("tls-cert", bpo::value<std::string>(), "PEM certificate (chain) file for HTTPS")
        ("tls-key", bpo::value<std::string>(), "PEM private key file for HTTPS")
        ;

    return app.run(ac, av, [&app] {
        return seastar::async([&app] {
            labaz::stop_signal stop_signal;

            labaz::http_frontend_config cfg;
            try {
                cfg = make_frontend_config(app.configuration());
            } catch (const std::exception& e) {
                lblog.error("Invalid configuration: {}", e.what());
                return 2;
            }

            labaz::http_frontend frontend;
            frontend.start(std::move(cfg)).get();
            auto stop_frontend = defer([&frontend] () noexcept {
                lblog.info("Stopping");
                frontend.stop().get();
            });

            // Tests wait for this line before talking to the server.
            std::cout << fmt::format("labaz: listening http={} https={}",
                                     frontend.http_addresses(), frontend.https_addresses()) << std::endl;

            stop_signal.wait().get();
            return 0;
        });
    });
}
