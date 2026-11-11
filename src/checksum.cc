/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "checksum.hh"

#include <array>

#include <fmt/format.h>
#include <gnutls/crypto.h>
#include <zlib.h>

#include <seastar/core/coroutine.hh>

namespace labaz {

using namespace seastar;

static constexpr std::pair<checksum_algorithm, std::string_view> names[] = {
    {checksum_algorithm::crc32, "crc32"},
    {checksum_algorithm::crc32c, "crc32c"},
    {checksum_algorithm::crc64nvme, "crc64nvme"},
    {checksum_algorithm::md5, "md5"},
    {checksum_algorithm::sha1, "sha1"},
    {checksum_algorithm::sha256, "sha256"},
};

std::string_view to_string(checksum_algorithm a) {
    for (auto& [alg, name] : names) {
        if (alg == a) {
            return name;
        }
    }
    return "unknown";
}

std::optional<checksum_algorithm> checksum_algorithm_from_string(std::string_view s) {
    for (auto& [alg, name] : names) {
        if (name == s) {
            return alg;
        }
    }
    return std::nullopt;
}

bool is_crc(checksum_algorithm a) {
    return a == checksum_algorithm::crc32 || a == checksum_algorithm::crc32c || a == checksum_algorithm::crc64nvme;
}

//
// CRCs
//
// All three are reflected, with all-ones initial value and final xor, so
// they differ only in width and polynomial. CRC-32 itself comes from zlib.
//

template <typename T>
static sstring to_big_endian(T v) {
    sstring ret(sstring::initialized_later(), sizeof(T));
    for (size_t i = 0; i < sizeof(T); i++) {
        ret[i] = static_cast<char>(v >> (8 * (sizeof(T) - 1 - i)));
    }
    return ret;
}

template <typename T>
static T from_big_endian(std::string_view s) {
    if (s.size() != sizeof(T)) {
        throw std::invalid_argument(fmt::format("a {}-byte CRC expected, got {} bytes", sizeof(T), s.size()));
    }
    T v = 0;
    for (unsigned char c : s) {
        v = (v << 8) | c;
    }
    return v;
}

// A reflected CRC with the given (reflected) polynomial.
template <typename T, T Poly>
struct crc {
    static constexpr std::array<T, 256> table = [] {
        std::array<T, 256> t{};
        for (unsigned i = 0; i < 256; i++) {
            T c = i;
            for (int k = 0; k < 8; k++) {
                c = c & 1 ? (c >> 1) ^ Poly : c >> 1;
            }
            t[i] = c;
        }
        return t;
    }();

    static T update(T v, std::string_view data) {
        v = ~v;
        for (unsigned char b : data) {
            v = table[(v ^ b) & 0xff] ^ (v >> 8);
        }
        return ~v;
    }

    // a * b modulo the polynomial, in the reflected representation; `a`
    // must not be zero.
    static T multiply(T a, T b) {
        T m = T(1) << (sizeof(T) * 8 - 1);
        T p = 0;
        while (true) {
            if (a & m) {
                p ^= b;
                if ((a & (m - 1)) == 0) {
                    return p;
                }
            }
            m >>= 1;
            b = b & 1 ? (b >> 1) ^ Poly : b >> 1;
        }
    }

    // CRC(A || B) = CRC(A) * x^(8 * len(B)) + CRC(B), modulo the polynomial.
    static T combine(T crc_a, T crc_b, uint64_t length_b) {
        T p = T(1) << (sizeof(T) * 8 - 1);  // x^0
        T x = p >> 1;                       // x^1
        for (int i = 0; i < 3; i++) {
            x = multiply(x, x);             // x^8 in the end
        }
        for (; length_b; length_b >>= 1) {
            if (length_b & 1) {
                p = multiply(x, p);
            }
            x = multiply(x, x);
        }
        return multiply(p, crc_a) ^ crc_b;
    }
};

using crc32_ieee = crc<uint32_t, 0xedb88320>;
using crc32c_crc = crc<uint32_t, 0x82f63b78>;
using crc64nvme = crc<uint64_t, 0x9a6c9329ac4bc9b5>;

uint32_t crc32c(std::string_view data, uint32_t crc) {
    return crc32c_crc::update(crc, data);
}

//
// The checksummer
//

struct checksummer::state {
    checksum_algorithm algorithm;
    explicit state(checksum_algorithm a) : algorithm(a) {}
    virtual ~state() = default;
    virtual void update(std::string_view data) = 0;
    virtual sstring finish() = 0;
};

namespace {

class zlib_crc32_state final : public checksummer::state {
    uLong _crc = ::crc32_z(0, nullptr, 0);
public:
    using state::state;
    void update(std::string_view data) override {
        _crc = ::crc32_z(_crc, reinterpret_cast<const Bytef*>(data.data()), data.size());
    }
    sstring finish() override {
        return to_big_endian(static_cast<uint32_t>(_crc));
    }
};

template <typename Crc, typename T>
class crc_state final : public checksummer::state {
    T _crc = 0;
public:
    using state::state;
    void update(std::string_view data) override {
        _crc = Crc::update(_crc, data);
    }
    sstring finish() override {
        return to_big_endian(_crc);
    }
};

class gnutls_state final : public checksummer::state {
    gnutls_digest_algorithm_t _alg;
    gnutls_hash_hd_t _hd = nullptr;
public:
    gnutls_state(checksum_algorithm a, gnutls_digest_algorithm_t alg) : state(a), _alg(alg) {
        if (gnutls_hash_init(&_hd, alg) < 0) {
            throw std::runtime_error(fmt::format("cannot compute {}", to_string(a)));
        }
    }
    ~gnutls_state() {
        if (_hd) {
            gnutls_hash_deinit(_hd, nullptr);
        }
    }
    void update(std::string_view data) override {
        gnutls_hash(_hd, data.data(), data.size());
    }
    sstring finish() override {
        sstring ret(sstring::initialized_later(), gnutls_hash_get_len(_alg));
        gnutls_hash_deinit(std::exchange(_hd, nullptr), ret.data());
        return ret;
    }
};

}

checksummer::checksummer(const std::set<checksum_algorithm>& algorithms) {
    for (auto a : algorithms) {
        switch (a) {
        case checksum_algorithm::crc32:
            _states.push_back(std::make_unique<zlib_crc32_state>(a));
            break;
        case checksum_algorithm::crc32c:
            _states.push_back(std::make_unique<crc_state<crc32c_crc, uint32_t>>(a));
            break;
        case checksum_algorithm::crc64nvme:
            _states.push_back(std::make_unique<crc_state<crc64nvme, uint64_t>>(a));
            break;
        case checksum_algorithm::md5:
            _states.push_back(std::make_unique<gnutls_state>(a, GNUTLS_DIG_MD5));
            break;
        case checksum_algorithm::sha1:
            _states.push_back(std::make_unique<gnutls_state>(a, GNUTLS_DIG_SHA1));
            break;
        case checksum_algorithm::sha256:
            _states.push_back(std::make_unique<gnutls_state>(a, GNUTLS_DIG_SHA256));
            break;
        }
    }
}

checksummer::checksummer(checksummer&&) noexcept = default;
checksummer::~checksummer() = default;

void checksummer::update(std::string_view data) {
    for (auto& s : _states) {
        s->update(data);
    }
}

checksums checksummer::finish() {
    checksums ret;
    for (auto& s : _states) {
        ret.emplace(s->algorithm, s->finish());
    }
    return ret;
}

sstring crc_combine(checksum_algorithm a, std::string_view crc_a, std::string_view crc_b, uint64_t length_b) {
    switch (a) {
    case checksum_algorithm::crc32:
        return to_big_endian(crc32_ieee::combine(from_big_endian<uint32_t>(crc_a), from_big_endian<uint32_t>(crc_b), length_b));
    case checksum_algorithm::crc32c:
        return to_big_endian(crc32c_crc::combine(from_big_endian<uint32_t>(crc_a), from_big_endian<uint32_t>(crc_b), length_b));
    case checksum_algorithm::crc64nvme:
        return to_big_endian(crc64nvme::combine(from_big_endian<uint64_t>(crc_a), from_big_endian<uint64_t>(crc_b), length_b));
    default:
        throw std::invalid_argument(fmt::format("{} values cannot be combined", to_string(a)));
    }
}

//
// Hex
//

sstring to_hex(std::string_view bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    sstring ret(sstring::initialized_later(), bytes.size() * 2);
    for (size_t i = 0; i < bytes.size(); i++) {
        auto b = static_cast<unsigned char>(bytes[i]);
        ret[2 * i] = digits[b >> 4];
        ret[2 * i + 1] = digits[b & 0xf];
    }
    return ret;
}

static int hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::optional<sstring> from_hex(std::string_view hex) {
    if (hex.size() % 2) {
        return std::nullopt;
    }
    sstring ret(sstring::initialized_later(), hex.size() / 2);
    for (size_t i = 0; i < ret.size(); i++) {
        int hi = hex_digit(hex[2 * i]);
        int lo = hex_digit(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            return std::nullopt;
        }
        ret[i] = static_cast<char>(hi * 16 + lo);
    }
    return ret;
}

//
// Streams
//

namespace {

class checksumming_source final : public data_source_impl {
    input_stream<char>& _in;
    checksummer _sum;
    noncopyable_function<void(checksums)> _at_end;
    bool _ended = false;

public:
    checksumming_source(input_stream<char>& in, const std::set<checksum_algorithm>& algorithms,
                        noncopyable_function<void(checksums)> at_end)
        : _in(in), _sum(algorithms), _at_end(std::move(at_end))
    {}

    future<temporary_buffer<char>> get() override {
        auto buf = co_await _in.read();
        if (!buf.empty()) {
            _sum.update(std::string_view(buf.get(), buf.size()));
        } else if (!std::exchange(_ended, true)) {
            _at_end(_sum.finish());
        }
        co_return buf;
    }

    future<> close() override {
        return make_ready_future();
    }
};

}

input_stream<char> make_checksumming_stream(input_stream<char>& in, std::set<checksum_algorithm> algorithms,
                                            noncopyable_function<void(checksums)> at_end) {
    return input_stream<char>(data_source(std::make_unique<checksumming_source>(in, algorithms, std::move(at_end))));
}

input_stream<char> make_verifying_stream(input_stream<char>& in, checksums expected) {
    std::set<checksum_algorithm> algorithms;
    for (auto& [alg, value] : expected) {
        algorithms.insert(alg);
    }
    return make_checksumming_stream(in, std::move(algorithms), [expected = std::move(expected)] (checksums got) {
        for (auto& [alg, value] : expected) {
            if (got[alg] != value) {
                throw checksum_mismatch(fmt::format("{} of the data is {}, not {}",
                                                    to_string(alg), to_hex(got[alg]), to_hex(value)));
            }
        }
    });
}

} // namespace labaz
