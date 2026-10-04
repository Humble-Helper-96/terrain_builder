#!/bin/bash
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Humble-Helper-96
#
# Part of the terrain_builder pipeline — see HOW_TO_USE.md for full documentation.

# =============================================================================
# build_all_states.sh
# Full CONUS DEM processing pipeline for terrain_builder
#
# PURPOSE:
#   Loops over all 48 contiguous US states (CONUS), downloads USGS DEM tiles,
#   and processes each into clipped contour (.gpkg) and hillshade (.tif)
#   outputs ready for MBTiles export.
#
# WORKFLOW (per state):
#   1. Verify USGS_DL_Lists/<STATE>_data.txt exists
#   2. Clear raw_dem/ to ensure no leftover tiles from a prior state
#   3. wget downloads DEM tiles listed in USGS_DL_Lists/<STATE>_data.txt
#      into raw_dem/  (-c flag resumes partial downloads within a state),
#      logging one OK/SKIP/FAIL line per tile instead of progress bars
#   4. process_dem.py reprojects, builds VRT, generates contours and
#      hillshade, clips to state boundary, saves to OUTPUT_DIR
#   5. --skip-export defers MBTiles generation until all states are done
#   6. Loop aborts immediately if wget or process_dem.py fails
#
# BEFORE A LARGE BATCH:
#   Refresh the download lists so stale URLs don't abort the run (a single
#   404 fails that tile and stops this script once the state's list is done):
#
#     python3 scripts/update_dem_lists.py --verify
#
# AFTER COMPLETION:
#   Run export_mbtiles.py to merge all state outputs and build MBTiles:
#
#     python3 scripts/export_mbtiles.py \
#         --output-dir ./output \
#         --dest-dir /path/to/tileserver/data
#
# OUTPUT:
#   output/contours_<STATE>.gpkg   — Clipped contour lines per state
#   output/hillshade_<STATE>.tif   — Clipped hillshade raster per state
#
# CONFIGURATION (edit the variables block below):
#   WORKERS      — Parallel CPU workers passed to process_dem.py
#                  Default: 75% of available CPUs (auto-detected via nproc)
#                  Override example: WORKERS=4 ./scripts/build_all_states.sh
#   OUTPUT_DIR   — Destination for clipped state outputs
#                  Default: terrain_builder/output/
#   STATES       — Ordered list of CONUS state abbreviations to process
#                  Edit to run a subset, e.g. STATES=(CT MA RI VT NH ME)
#
# DISK SPACE:
#   Raw DEM tiles per state:   1–20 GB  (varies by state size and resolution)
#   Output per state:          0.5–5 GB (contours + hillshade)
#   Full CONUS estimated:      ~200–400 GB output (raw tiles are deleted per state)
#   Ensure at least 50 GB free before starting; 100+ GB recommended for large
#   western states (CA, MT, WY, CO, NM).
#
# RESUMABILITY:
#   - wget -c resumes partial downloads if interrupted mid-state
#   - raw_dem/ is cleared at the START of each state, not the end
#   - If process_dem.py fails mid-state, re-running the script will
#     re-download and re-process that state from scratch
#   - Already-completed states (output/contours_XX.gpkg exists) are NOT
#     automatically skipped — comment out completed states in STATES if
#     you want to resume a partial CONUS run without reprocessing them
#
# NOTES:
#   - Estimated runtime: 1–6 hours per state depending on hardware
#   - Full CONUS run: 2–5 days on a typical workstation
#   - See HOW_TO_USE.md for dependency installation instructions
# =============================================================================

set -euo pipefail

# =============================================================================
# Resolve script and project directories
# All paths are relative to terrain_builder/ regardless of where this script
# is invoked from.
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAIN_BUILDER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${TERRAIN_BUILDER_DIR}"

# shellcheck source=download_tiles.sh
source "${SCRIPT_DIR}/download_tiles.sh"

# =============================================================================
# Configuration — override via environment variables if desired
# Example: WORKERS=4 OUTPUT_DIR=/mnt/usb2/output ./scripts/build_all_states.sh
# =============================================================================

# Auto-detect 75% of available CPUs; minimum 1
_TOTAL_CPUS=$(nproc 2>/dev/null || echo 4)
_DEFAULT_WORKERS=$(( (_TOTAL_CPUS * 3) / 4 ))
_DEFAULT_WORKERS=$(( _DEFAULT_WORKERS < 1 ? 1 : _DEFAULT_WORKERS ))

WORKERS="${WORKERS:-${_DEFAULT_WORKERS}}"
OUTPUT_DIR="${OUTPUT_DIR:-${TERRAIN_BUILDER_DIR}/output}"

# =============================================================================
# CONUS state list — 48 contiguous US states
# Edit this list to run a subset, e.g.: STATES=(CT MA RI VT NH ME)
# =============================================================================
STATES=(
    WA OR CA ID NV AZ UT CO WY MT
    ND SD NE KS NM MN IA MO WI MI
    IL IN OH TX OK AR LA MS AL TN
    KY WV VA NC SC GA FL ME NH VT
    MA RI CT NY NJ PA MD DE
)

# =============================================================================
# Pre-flight checks
# =============================================================================
echo "============================================================"
echo "  terrain_builder — Full CONUS Build"
echo "============================================================"
echo "  Project dir:   ${TERRAIN_BUILDER_DIR}"
echo "  Output dir:    ${OUTPUT_DIR}"
echo "  Workers:       ${WORKERS}  (of ${_TOTAL_CPUS} CPUs)"
echo "  States:        ${#STATES[@]}"
echo "  Started:       $(date)"
echo "============================================================"
echo ""

# Verify required tools are available
for tool in wget python3; do
    if ! command -v "${tool}" &>/dev/null; then
        echo "[ERROR] Required tool not found: ${tool}"
        echo "        Install wget:   sudo apt install wget"
        echo "        Install python: sudo apt install python3"
        exit 1
    fi
done

# Verify process_dem.py exists
if [ ! -f "${TERRAIN_BUILDER_DIR}/process_dem.py" ]; then
    echo "[ERROR] process_dem.py not found in: ${TERRAIN_BUILDER_DIR}"
    echo "        Ensure you are running this script from within a complete"
    echo "        terrain_builder clone."
    exit 1
fi

# Verify USGS download lists exist for all states before starting
echo "[CHECK] Verifying USGS download lists..."
MISSING_LISTS=()
for STATE in "${STATES[@]}"; do
    DL_LIST="${TERRAIN_BUILDER_DIR}/USGS_DL_Lists/${STATE}_data.txt"
    if [ ! -f "${DL_LIST}" ]; then
        MISSING_LISTS+=("${DL_LIST}")
    fi
done

if [ ${#MISSING_LISTS[@]} -gt 0 ]; then
    echo "[ERROR] Missing USGS download list file(s):"
    for f in "${MISSING_LISTS[@]}"; do
        echo "        - ${f}"
    done
    echo ""
    echo "        Check USGS_DL_Lists/ — filenames must be <STATE>_data.txt"
    echo "        Example: MD_data.txt  (not MD_.txt)"
    exit 1
fi
echo "[OK]    All ${#STATES[@]} download lists found"
echo ""

# Ensure output directory exists
mkdir -p "${OUTPUT_DIR}"

# =============================================================================
# Per-state processing loop
# =============================================================================
OVERALL_START=${SECONDS}
COMPLETED=0
FAILED_STATE=""

for STATE in "${STATES[@]}"; do
    STATE_START=${SECONDS}
    DL_LIST="${TERRAIN_BUILDER_DIR}/USGS_DL_Lists/${STATE}_data.txt"
    RAW_DEM_DIR="${TERRAIN_BUILDER_DIR}/raw_dem"

    echo ""
    echo "========================================================"
    echo "  Processing: ${STATE}  (${COMPLETED}/${#STATES[@]} complete)"
    echo "  Started:    $(date)"
    echo "========================================================"
    echo ""

    # ------------------------------------------------------------------
    # Clear raw_dem/ before each state to prevent leftover tiles from a
    # prior state being included in this state's reprojection pass.
    # This is safe because process_dem.py also clears raw_dem/ on
    # completion — this guard handles interrupted prior runs.
    # ------------------------------------------------------------------
    if ls "${RAW_DEM_DIR}"/*.tif 1>/dev/null 2>&1; then
        echo "[WARN]  raw_dem/ contains leftover tiles from a prior run — clearing"
        rm -f "${RAW_DEM_DIR:?}"/*.tif
        echo "[OK]    raw_dem/ cleared"
        echo ""
    fi

    # ------------------------------------------------------------------
    # Download USGS DEM tiles — one log line per tile (see download_tiles.sh)
    # wget -c resumes partial downloads; failed tiles are listed at the end
    # ------------------------------------------------------------------
    echo "[STEP]  Downloading DEM tiles for ${STATE}..."
    if ! download_tile_list "${DL_LIST}" "${RAW_DEM_DIR}"; then
        echo ""
        echo "[ERROR] Tile download failed for ${STATE}"
        echo "        Check network connectivity and the download list:"
        echo "        ${DL_LIST}"
        echo "        A 404 usually means USGS replaced a tile — refresh the list:"
        echo "          python3 scripts/update_dem_lists.py --state ${STATE}"
        FAILED_STATE="${STATE}"
        exit 1
    fi
    echo "[OK]    Download complete for ${STATE}"
    echo ""

    # ------------------------------------------------------------------
    # Run the DEM processing pipeline for this state
    # ------------------------------------------------------------------
    echo "[STEP]  Running process_dem.py for ${STATE}..."
    if ! python3 "${TERRAIN_BUILDER_DIR}/process_dem.py" \
            --state "${STATE}" \
            --workers "${WORKERS}" \
            --yes \
            --skip-export \
            --output-dir "${OUTPUT_DIR}"; then
        EXIT_CODE=$?
        echo ""
        echo "[ERROR] process_dem.py failed for ${STATE} (exit code ${EXIT_CODE})"
        echo "        Check the log in: ${TERRAIN_BUILDER_DIR}/logs/"
        echo "        To retry ${STATE}: re-run this script or run process_dem.py manually:"
        echo "          python3 process_dem.py --state ${STATE} --workers ${WORKERS} --skip-export --output-dir ${OUTPUT_DIR}"
        FAILED_STATE="${STATE}"
        exit ${EXIT_CODE}
    fi

    STATE_ELAPSED=$(( SECONDS - STATE_START ))
    STATE_MINS=$(( STATE_ELAPSED / 60 ))
    STATE_SECS=$(( STATE_ELAPSED % 60 ))
    COMPLETED=$(( COMPLETED + 1 ))

    echo ""
    echo "[OK]    ${STATE} complete — ${STATE_MINS}m ${STATE_SECS}s  (${COMPLETED}/${#STATES[@]} states done)"
    echo ""
done

# =============================================================================
# Completion summary
# =============================================================================
OVERALL_ELAPSED=$(( SECONDS - OVERALL_START ))
OVERALL_HOURS=$(( OVERALL_ELAPSED / 3600 ))
OVERALL_MINS=$(( (OVERALL_ELAPSED % 3600) / 60 ))
OVERALL_SECS=$(( OVERALL_ELAPSED % 60 ))

echo ""
echo "============================================================"
echo "  [SUCCESS] All ${#STATES[@]} states complete!"
echo "  Finished:  $(date)"
echo "  Total time: ${OVERALL_HOURS}h ${OVERALL_MINS}m ${OVERALL_SECS}s"
echo "============================================================"
echo ""
echo "Clipped outputs are in: ${OUTPUT_DIR}"
echo ""
echo "Next step — export MBTiles:"
echo ""
echo "  python3 scripts/export_mbtiles.py \\"
echo "      --output-dir ${OUTPUT_DIR} \\"
echo "      --dest-dir /path/to/tileserver/data"
echo ""
