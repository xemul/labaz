/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "facade.hh"

namespace labaz::gcs {

/// The "gcs" facade: the Google Cloud Storage JSON API; see
/// docs/gcs-protocol.md for what of it is supported.
facade_type make_facade_type();

} // namespace labaz::gcs
