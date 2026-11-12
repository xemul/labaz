/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "facade.hh"

namespace labaz::s3 {

/// The "s3" facade: the Amazon S3 REST API, path-style; see
/// docs/s3-protocol.md for what of it is supported.
facade_type make_facade_type();

} // namespace labaz::s3
