/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "facade.hh"

namespace labaz::plywood {

/// The "plywood" facade: a testing-only HTTP/JSON protocol that maps 1:1 to
/// the storage API; see docs/plywood-protocol.md.
facade_type make_facade_type();

} // namespace labaz::plywood
