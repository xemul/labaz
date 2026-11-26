#!/bin/sh
# Copyright (C) 2026 The Labaz Authors
# SPDX-License-Identifier: Apache-2.0
#
# Runs Labaz with the S3 and GCS facades on all addresses, keeping the data
# in /var/lib/labaz. The environment configures it:
#
#   LABAZ_S3_PORT       the S3 facade's port (7070); empty for none
#   LABAZ_GCS_PORT      the GCS facade's port (4443); empty for none
#   LABAZ_S3_KEY        <access-key>:<secret> requests to S3 must be signed with
#   LABAZ_S3_REGION     the region they must be signed for
#   LABAZ_SMP           the number of shards (1)
#   LABAZ_MEMORY        the memory to use (1G)
#   LABAZ_LOG_LEVEL     loggers' levels, as --logger-log-level takes them,
#                       e.g. s3=trace:gcs=trace:potato=trace
#
# Arguments are passed on to labaz, after the ones above. It prints
# "labaz: ready" once it serves.

set -e

s3_port=${LABAZ_S3_PORT-7070}
gcs_port=${LABAZ_GCS_PORT-4443}

set -- --smp "${LABAZ_SMP:-1}" -m "${LABAZ_MEMORY:-1G}" --overprovisioned \
    --data-dir /var/lib/labaz --listen-address 0.0.0.0 "$@"
if [ -n "$s3_port" ]; then
    set -- "$@" --facade "s3:http=$s3_port"
fi
if [ -n "$gcs_port" ]; then
    set -- "$@" --facade "gcs:http=$gcs_port"
fi
if [ -n "$LABAZ_S3_KEY" ]; then
    set -- "$@" --s3-key "$LABAZ_S3_KEY"
fi
if [ -n "$LABAZ_S3_REGION" ]; then
    set -- "$@" --s3-region "$LABAZ_S3_REGION"
fi
if [ -n "$LABAZ_LOG_LEVEL" ]; then
    set -- "$@" --logger-log-level "$LABAZ_LOG_LEVEL"
fi
exec /usr/bin/labaz "$@"
