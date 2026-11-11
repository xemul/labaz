/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

namespace labaz {

enum class checksum_algorithm {
    crc32,      // the zlib/IEEE one
    crc32c,
    crc64nvme,
    md5,
    sha1,
    sha256,
};

std::string_view to_string(checksum_algorithm);
std::optional<checksum_algorithm> checksum_algorithm_from_string(std::string_view);
/// CRCs of two pieces of data combine into the CRC of their concatenation.
bool is_crc(checksum_algorithm);

/// Digests by algorithm, as bytes; CRCs are big-endian.
using checksums = std::map<checksum_algorithm, seastar::sstring>;

/// Computes a set of checksums over data given piece by piece.
class checksummer {
public:
    struct state;
private:
    std::vector<std::unique_ptr<state>> _states;
public:
    explicit checksummer(const std::set<checksum_algorithm>& algorithms);
    checksummer(checksummer&&) noexcept;
    ~checksummer();
    void update(std::string_view data);
    /// The checksums of all the data given. Call once.
    checksums finish();
};

/// The CRC-32C of the data, continuing from `crc` (that of the preceding
/// data, or 0).
uint32_t crc32c(std::string_view data, uint32_t crc = 0);

/// The CRC of A || B, given CRC(A), CRC(B) and the length of B.
seastar::sstring crc_combine(checksum_algorithm, std::string_view crc_a, std::string_view crc_b, uint64_t length_b);

seastar::sstring to_hex(std::string_view bytes);
std::optional<seastar::sstring> from_hex(std::string_view hex);

class checksum_mismatch : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// Passes `in` through, computing the checksums of the data, and calls
/// at_end() with them when it reaches its end; an exception from at_end()
/// fails that final read. `in` must outlive the returned stream, which does
/// not close it.
seastar::input_stream<char> make_checksumming_stream(seastar::input_stream<char>& in,
                                                     std::set<checksum_algorithm> algorithms,
                                                     seastar::noncopyable_function<void(checksums)> at_end);

/// Passes `in` through, failing its final read with checksum_mismatch if
/// the data does not have the `expected` checksums. As for
/// make_checksumming_stream(), `in` must outlive the returned stream.
seastar::input_stream<char> make_verifying_stream(seastar::input_stream<char>& in, checksums expected);

} // namespace labaz
