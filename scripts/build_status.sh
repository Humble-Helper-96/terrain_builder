#!/bin/bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Humble-Helper-96
#
# Part of the terrain_builder pipeline — see HOW_TO_USE.md for full documentation.

# =============================================================================
# build_status.sh
# One-screen progress summary for a running build_all_states.sh
#
# PURPOSE:
#   Reads the build log and the working directories (read-only) and prints
#   the current state, last pipeline step, tile count, states completed and
#   free disk space — without scrolling through the full log.
#
# USAGE:
#   ./scripts/build_status.sh                       # log: ~/conus_build.log
#   ./scripts/build_status.sh /path/to/build.log    # or LOG=/path/... env var
#   watch -n 60 ./scripts/build_status.sh           # refresh every minute
#
# EXAMPLE OUTPUT:
#   Current state:  WA
#   Last step:      [OK] All VRT tiles created successfully!
#   Pipeline stage: 2. Generate Contours from VRT Tiles
#     Sub-step:     Stage 2/2: Merging contours into single GeoPackage...
#   Tiles on disk:  44 / 44
#   States done:    0 / 48
#   Disk free:      1.2T
#
# NOTES:
#   - "Tiles on disk" includes a tile that is still downloading, and stays
#     at its maximum during processing until process_dem.py clears raw_dem/
#   - The output directory is read from the log's "Output dir:" header line
# =============================================================================

set -uo pipefail

# Every grep on the log uses -a: an unclean shutdown (e.g. a power cut) can
# leave NUL bytes in the log, and grep then treats it as binary and stops
# printing matching lines, which froze the status on a stale state.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAIN_BUILDER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOG="${1:-${LOG:-${HOME}/conus_build.log}}"

if [ ! -f "${LOG}" ]; then
    echo "[ERROR] Build log not found: ${LOG}"
    echo "        Pass the log path: ./scripts/build_status.sh /path/to/build.log"
    exit 1
fi

OUTPUT_DIR="$(grep -a -m1 -oP 'Output dir:\s+\K.*' "${LOG}" || true)"
OUTPUT_DIR="${OUTPUT_DIR:-${TERRAIN_BUILDER_DIR}/output}"
TOTAL_STATES="$(grep -a -m1 -oP '^\s*States:\s+\K[0-9]+' "${LOG}" || echo '?')"

STATE="$(grep -a -oP 'Processing: \K[A-Z]{2}' "${LOG}" | tail -n 1)"
LAST_STEP="$(grep -a -E '^\[(STEP|OK|WARN|ERROR)\]' "${LOG}" | tail -n 1)"

# Pipeline stage (process_dem.py "STAGE: ..." banner) and the latest
# "Phase n/m" / "Stage n/m" line printed by that stage's sub-script, both
# scoped to the current state. tr splits \r-separated progress updates.
read -r -d '' STAGE_AWK <<'AWK'
/Processing: [A-Z][A-Z]/        { stage = ""; sub_step = "" }
/^STAGE: /                      { stage = substr($0, 8); sub_step = "" }
/^ *(Phase|Stage) [0-9]+\/[0-9]+:/ { sub_step = $0; sub(/^ +/, "", sub_step) }
END                             { print stage; print sub_step }
AWK
{ read -r PIPELINE_STAGE; read -r SUB_STEP; } < <(tr -d '\000' < "${LOG}" | tr '\r' '\n' | awk "${STAGE_AWK}")
# Unique states finished in this log, whether built ("[OK] XX complete —")
# or skipped on a resumed run ("[SKIP] XX already complete")
DONE="$(grep -a -oP '^\[(OK|SKIP)\] +\K[A-Z]{2}(?= (complete —|already complete))' "${LOG}" | sort -u | wc -l)"

TILES="$(find "${TERRAIN_BUILDER_DIR}/raw_dem" -maxdepth 1 -name '*.tif' 2>/dev/null | wc -l)"
DL_LIST="${TERRAIN_BUILDER_DIR}/USGS_DL_Lists/${STATE}_data.txt"
if [ -n "${STATE}" ] && [ -f "${DL_LIST}" ]; then
    # Unique URLs: a duplicated list entry is downloaded only once
    LISTED="$(grep '^http' "${DL_LIST}" | tr -d '\r' | sort -u | wc -l)"
else
    LISTED='?'
fi

echo "Current state:  ${STATE:-(not started)}"
echo "Last step:      ${LAST_STEP:-(none yet)}"
if [ -n "${PIPELINE_STAGE}" ]; then
    echo "Pipeline stage: ${PIPELINE_STAGE}"
    echo "  Sub-step:     ${SUB_STEP:-(starting)}"
fi
echo "Tiles on disk:  ${TILES} / ${LISTED}"
echo "States done:    ${DONE} / ${TOTAL_STATES}"
echo "Disk free:      $(df -h "${OUTPUT_DIR}" 2>/dev/null | awk 'NR==2 {print $4}')  (${OUTPUT_DIR})"

# Only report errors from the latest run: a resumed build appended to the
# same log would otherwise keep showing the error that stopped the last one
RUN_START="$(grep -a -n 'Full CONUS Build' "${LOG}" | tail -n 1 | cut -d: -f1)"
if tail -n +"${RUN_START:-1}" "${LOG}" | grep -a -q '^\[ERROR\]'; then
    echo ""
    echo "!! ERROR in log:"
    tail -n +"${RUN_START:-1}" "${LOG}" | grep -a -A3 '^\[ERROR\]' | tail -n 5
fi

if grep -a -q '\[SUCCESS\] All' "${LOG}"; then
    echo ""
    echo "Build finished — next: python3 scripts/export_mbtiles.py --output-dir ${OUTPUT_DIR} --dest-dir <tileserver data dir>"
fi
