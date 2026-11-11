/*
 * Copyright (C) 2026 The Labaz Authors
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage.hh"

namespace labaz::potato {

/// The "potato" backend: objects in plain files, in the simplest way that
/// works. Not meant for production use; see docs/backends.md.
backend_type make_backend_type();

} // namespace labaz::potato
