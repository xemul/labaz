/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "streams.hh"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace labaz {

using namespace seastar;

namespace {

class foreign_source final : public data_source_impl {
    using foreign_buffer = foreign_ptr<std::unique_ptr<temporary_buffer<char>>>;

    deleter _keep;
    unsigned _owner;
    input_stream<char>& _in;

public:
    foreign_source(unsigned owner, input_stream<char>& in, deleter keep)
        : _keep(std::move(keep)), _owner(owner), _in(in)
    {}

    future<temporary_buffer<char>> get() override {
        return smp::submit_to(_owner, [&in = _in] {
            return in.read().then([] (temporary_buffer<char> buf) {
                return buf.empty() ? foreign_buffer() : make_foreign(std::make_unique<temporary_buffer<char>>(std::move(buf)));
            });
        }).then([] (foreign_buffer buf) {
            if (!buf) {
                return temporary_buffer<char>();
            }
            auto data = buf->get_write();
            auto size = buf->size();
            return temporary_buffer<char>(data, size, make_object_deleter(std::move(buf)));
        });
    }

    future<> close() override {
        return smp::submit_to(_owner, [&in = _in] {
            return in.close();
        });
    }
};

}

input_stream<char> make_foreign_stream(unsigned owner, input_stream<char>& in, deleter keep) {
    return input_stream<char>(data_source(std::make_unique<foreign_source>(owner, in, std::move(keep))));
}

} // namespace labaz
