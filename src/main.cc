/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <iostream>
#include <list>
#include <stdexcept>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include "facade.hh"
#include "http_frontend.hh"
#include "stop_signal.hh"
#include "storage.hh"

namespace bpo = boost::program_options;

using namespace seastar;

static logger lblog("labaz");

struct facade_spec {
    const labaz::facade_type* type;
    labaz::http_frontend_config frontend;
};

// Parses "<name>[:http=<port>][,https=<port>][,address=<addr>]".
static facade_spec parse_facade_spec(std::string_view spec, const bpo::variables_map& opts) {
    auto colon = spec.find(':');
    auto name = spec.substr(0, colon);
    auto type = labaz::find_facade_type(name);
    if (!type) {
        throw std::invalid_argument(fmt::format("unknown facade '{}'", name));
    }

    facade_spec ret{type, {}};
    auto& cfg = ret.frontend;
    cfg.facade = sstring(name);
    cfg.address = net::inet_address(opts["listen-address"].as<std::string>());

    auto parse_port = [spec] (std::string_view v) {
        unsigned port;
        auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), port);
        if (ec != std::errc() || p != v.data() + v.size() || port > 65535) {
            throw std::invalid_argument(fmt::format("bad port '{}' in --facade {}", v, spec));
        }
        return static_cast<uint16_t>(port);
    };

    std::string_view params = colon == std::string_view::npos ? std::string_view() : spec.substr(colon + 1);
    while (!params.empty()) {
        auto comma = params.find(',');
        auto param = params.substr(0, comma);
        params = comma == std::string_view::npos ? std::string_view() : params.substr(comma + 1);

        auto eq = param.find('=');
        if (eq == std::string_view::npos) {
            throw std::invalid_argument(fmt::format("expected key=value, got '{}' in --facade {}", param, spec));
        }
        auto key = param.substr(0, eq);
        auto value = param.substr(eq + 1);
        if (key == "http") {
            cfg.http_port = parse_port(value);
        } else if (key == "https") {
            cfg.https_port = parse_port(value);
        } else if (key == "address") {
            cfg.address = net::inet_address(std::string(value));
        } else {
            throw std::invalid_argument(fmt::format("unknown parameter '{}' in --facade {}", key, spec));
        }
    }

    if (!cfg.http_port && !cfg.https_port) {
        throw std::invalid_argument(fmt::format("--facade {} has neither an http nor an https port", spec));
    }
    return ret;
}

static std::vector<facade_spec> make_facade_specs(const bpo::variables_map& opts) {
    if (!opts.contains("facade")) {
        throw std::invalid_argument("at least one --facade is required");
    }

    bool have_cert = opts.contains("tls-cert");
    bool have_key = opts.contains("tls-key");
    if (have_cert != have_key) {
        throw std::invalid_argument("--tls-cert and --tls-key must be given together");
    }
    std::optional<tls::credentials_builder> tls;
    if (have_cert) {
        tls.emplace();
        tls->set_x509_key_file(opts["tls-cert"].as<std::string>(),
                               opts["tls-key"].as<std::string>(),
                               tls::x509_crt_format::PEM).get();
    }

    std::vector<facade_spec> ret;
    for (auto& s : opts["facade"].as<std::vector<std::string>>()) {
        auto spec = parse_facade_spec(s, opts);
        spec.frontend.name = fmt::format("{}-{}", spec.frontend.facade, ret.size());
        if (spec.frontend.https_port) {
            if (!tls) {
                throw std::invalid_argument(fmt::format("--facade {} has an https port, which requires --tls-cert and --tls-key", s));
            }
            spec.frontend.tls = *tls;
        }
        ret.push_back(std::move(spec));
    }
    return ret;
}

static std::string names_of(const auto& types) {
    std::vector<std::string> names;
    for (auto& t : types) {
        names.push_back(fmt::format("{} ({})", t.name, t.description));
    }
    return fmt::format("{}", fmt::join(names, ", "));
}

int main(int ac, char** av) {
    app_template::config app_cfg;
    app_cfg.name = "labaz";
    app_cfg.description = "S3- and GCS-compatible object storage";
    app_template app(std::move(app_cfg));

    app.add_options()
        ("facade", bpo::value<std::vector<std::string>>(),
            fmt::format("A protocol to serve, as <name>:http=<port>,https=<port>,address=<addr>; "
                        "at least one port is required. Can be repeated. Facades: {}", names_of(labaz::facade_types())).c_str())
        ("backend", bpo::value<std::string>()->default_value("potato"),
            fmt::format("Storage backend. Backends: {}", names_of(labaz::backend_types())).c_str())
        ("listen-address", bpo::value<std::string>()->default_value("127.0.0.1"), "Default address for facades to listen on")
        ("tls-cert", bpo::value<std::string>(), "PEM certificate (chain) file for HTTPS")
        ("tls-key", bpo::value<std::string>(), "PEM private key file for HTTPS")
        ;
    for (auto& t : labaz::backend_types()) {
        bpo::options_description group(fmt::format("Backend '{}' options", t.name));
        t.add_options(group);
        if (!group.options().empty()) {
            app.get_options_description().add(group);
        }
    }
    for (auto& t : labaz::facade_types()) {
        bpo::options_description group(fmt::format("Facade '{}' options", t.name));
        t.add_options(group);
        if (!group.options().empty()) {
            app.get_options_description().add(group);
        }
    }

    return app.run(ac, av, [&app] {
        return seastar::async([&app] {
            labaz::stop_signal stop_signal;
            auto& opts = app.configuration();

            const labaz::backend_type* backend;
            std::vector<facade_spec> specs;
            try {
                backend = labaz::find_backend_type(opts["backend"].as<std::string>());
                if (!backend) {
                    throw std::invalid_argument(fmt::format("unknown backend '{}'", opts["backend"].as<std::string>()));
                }
                specs = make_facade_specs(opts);
            } catch (const std::invalid_argument& e) {
                lblog.error("Invalid configuration: {}", e.what());
                return 2;
            }

            sharded<labaz::storage_service> storage;
            try {
                storage.start(std::cref(*backend), std::cref(opts)).get();
            } catch (const std::invalid_argument& e) {
                lblog.error("Invalid configuration: {}", e.what());
                return 2;
            }
            auto stop_storage = defer([&storage] () noexcept {
                storage.stop().get();
            });
            storage.invoke_on_all([] (labaz::storage_service& s) { return s.start(); }).get();

            std::list<sharded<labaz::facade_service>> facades;
            std::list<labaz::http_frontend> frontends;
            auto stop_facades = defer([&] () noexcept {
                lblog.info("Stopping");
                for (auto& f : frontends) {
                    f.stop().get();
                }
                for (auto& f : facades) {
                    f.stop().get();
                }
            });

            for (auto& spec : specs) {
                auto& facade = facades.emplace_back();
                try {
                    facade.start(std::cref(*spec.type), std::cref(opts),
                                 sharded_parameter([&storage] { return std::ref(storage.local()); })).get();
                } catch (...) {
                    facades.pop_back();
                    throw;
                }
                auto& frontend = frontends.emplace_back();
                try {
                    frontend.start(spec.frontend, facade).get();
                } catch (...) {
                    frontends.pop_back();
                    throw;
                }
                std::cout << fmt::format("labaz: facade {} http={} https={}", spec.frontend.facade,
                                         frontend.http_addresses(), frontend.https_addresses()) << std::endl;
            }

            // Tests wait for this line before talking to the server.
            std::cout << "labaz: ready" << std::endl;

            stop_signal.wait().get();
            return 0;
        });
    });
}
