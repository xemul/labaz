/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <csignal>

#include <seastar/core/condition-variable.hh>
#include <seastar/core/signal.hh>

namespace labaz {

/// Resolves wait() once SIGINT or SIGTERM is received.
class stop_signal {
    bool _caught = false;
    seastar::condition_variable _cond;

    void signaled() {
        if (!_caught) {
            _caught = true;
            _cond.broadcast();
        }
    }

public:
    stop_signal() {
        seastar::handle_signal(SIGINT, [this] { signaled(); });
        seastar::handle_signal(SIGTERM, [this] { signaled(); });
    }

    ~stop_signal() {
        // A handler cannot be unregistered, so replace it with a no-op.
        seastar::handle_signal(SIGINT, [] {});
        seastar::handle_signal(SIGTERM, [] {});
    }

    seastar::future<> wait() {
        return _cond.wait([this] { return _caught; });
    }
};

} // namespace labaz
