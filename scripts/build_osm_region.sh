#!/usr/bin/env bash
#
# build_osm_region.sh — generate contours + hillshade for a Geofabrik
# US OSM extract region, for tileserver-starter style deployments.
#
# Place in ~/terrain_builder/scripts/ and run from ~/terrain_builder/
#
#   cd ~/terrain_builder
#   ./scripts/build_osm_region.sh us-south
#   ./scripts/build_osm_region.sh us-northeast
#   ./scripts/build_osm_region.sh us-midwest
#   ./scripts/build_osm_region.sh us-west
#   ./scripts/build_osm_region.sh us-pacific     # see ALASKA note below
#
#   ./scripts/build_osm_region.sh --list         # show regions and state counts
#
# Region membership verified against Geofabrik extract sizes (Aug 2026):
# the per-state .osm.pbf sizes sum exactly to each region's extract size.
#
# Resumable: states with existing outputs are skipped. Each region gets its
# own output dir so regions export to independent mbtiles pairs.
#
# Before a large batch, refresh the download lists so a stale URL doesn't
# abort a state mid-download:
#   python3 scripts/update_dem_lists.py --verify
#
# Env overrides:
#   WORKERS=4          parallel workers per stage (default 4)
#   MIN_FREE_GB=80     abort before a state if free space drops below this
#   OUTPUT_ROOT=path   parent for per-region output dirs
#                      (default terrain_builder/output)
#
# ---------------------------------------------------------------------------
# ALASKA
# ---------------------------------------------------------------------------
# There is no USGS_DL_Lists/AK_data.txt, and this is deliberate: USGS 1/3
# arc-second seamless coverage does not exist for most of Alaska. The state is
# IFSAR 5m in places, 2 arc-second elsewhere, with genuine gaps. Alaska also
# requires --target-srs EPSG:3338 (Alaska Albers).
#
# Recommendation: build Alaska terrain from Copernicus GLO-30 instead, using
# the tileserver-starter scripts, which have uniform global coverage:
#   ./scripts/build-contours.sh  -180 51 -129 72 ft
#   ./scripts/build-hillshade.sh -180 51 -129 72
#
# If you do assemble an AK_data.txt yourself, drop it in USGS_DL_Lists/ and
# this script will pick it up automatically and apply EPSG:3338.
# ---------------------------------------------------------------------------

set -uo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR" || exit 1

WORKERS="${WORKERS:-4}"
MIN_FREE_GB="${MIN_FREE_GB:-80}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$BASE_DIR/output}"
RAW_DEM="$BASE_DIR/raw_dem"
DL_LISTS="$BASE_DIR/USGS_DL_Lists"

# --- Region definitions -----------------------------------------------------
# Geofabrik "special sub regions" for the United States.
# us-south follows the US Census South region (16 states + DC).
# us-pacific is Alaska and Hawaii ONLY -- not the West Coast.
# CA, OR, WA live in us-west alongside the Mountain states.

REGION_us_south="AL AR DE FL GA KY LA MS NC OK SC TN TX VA WV MD DC"
REGION_us_northeast="CT ME MA NH RI VT NJ NY PA"
REGION_us_midwest="IL IN MI OH WI IA KS MN MO NE ND SD"
REGION_us_west="AZ CO ID MT NV NM UT WY OR WA CA"
REGION_us_pacific="HI AK"

ALL_REGIONS="us-south us-northeast us-midwest us-west us-pacific"

# --- States with no download list of their own ------------------------------
# Value is the state whose DEM tiles cover them. Processed immediately after
# that state, reusing its intermediates via --skip-cleanup.
PIGGYBACK_DC="MD"

# --- Per-state projection overrides -----------------------------------------
srs_for_state() {
    case "$1" in
        AK) echo "EPSG:3338" ;;   # Alaska Albers
        *)  echo "EPSG:3857" ;;   # Web Mercator
    esac
}

piggyback_host() {
    case "$1" in
        DC) echo "$PIGGYBACK_DC" ;;
        *)  echo "" ;;
    esac
}

# --- Arg handling -----------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") <region>

Regions:
EOF
    for R in $ALL_REGIONS; do
        VAR="REGION_${R//-/_}"
        STATES="${!VAR}"
        printf "  %-14s %2d regions:  %s\n" "$R" "$(wc -w <<<"$STATES")" "$STATES"
    done
    cat <<EOF

Env: WORKERS (default $WORKERS), MIN_FREE_GB (default $MIN_FREE_GB), OUTPUT_ROOT
EOF
}

[[ $# -eq 1 ]] || { usage; exit 1; }
case "$1" in
    --list|-l|--help|-h) usage; exit 0 ;;
esac

REGION="$1"
VAR="REGION_${REGION//-/_}"
STATES="${!VAR:-}"
if [[ -z "$STATES" ]]; then
    echo "[ERROR] Unknown region: $REGION"
    usage
    exit 1
fi

OUTPUT_DIR="$OUTPUT_ROOT/$REGION"
mkdir -p "$OUTPUT_DIR" "$RAW_DEM" "$BASE_DIR/logs"
RUN_LOG="$BASE_DIR/logs/${REGION}_batch_$(date +%Y%m%d_%H%M%S).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$RUN_LOG"; }
free_gb() { df -BG --output=avail "$BASE_DIR" | tail -1 | tr -dc '0-9'; }

# Order states so any piggyback host runs immediately before its dependent.
ORDERED=""
for S in $STATES; do
    [[ -n "$(piggyback_host "$S")" ]] && continue
    ORDERED="$ORDERED $S"
done
for S in $STATES; do
    HOST="$(piggyback_host "$S")"
    [[ -z "$HOST" ]] && continue
    # insert S directly after HOST
    NEW=""
    for T in $ORDERED; do
        NEW="$NEW $T"
        [[ "$T" == "$HOST" ]] && NEW="$NEW $S"
    done
    ORDERED="$NEW"
done

log "=== $REGION terrain build starting ==="
log "Base dir:   $BASE_DIR"
log "Output dir: $OUTPUT_DIR"
log "Workers:    $WORKERS"
log "Order:      $ORDERED"
log "Free space: $(free_gb) GB"
log ""

FAILED=(); SKIPPED=(); COMPLETED=(); NOLIST=()

for STATE in $ORDERED; do

    if [[ -f "$OUTPUT_DIR/contours_${STATE}.gpkg" && -f "$OUTPUT_DIR/hillshade_${STATE}.tif" ]]; then
        log "[SKIP] $STATE — outputs already present"
        SKIPPED+=("$STATE"); continue
    fi

    AVAIL=$(free_gb)
    if (( AVAIL < MIN_FREE_GB )); then
        log "[ABORT] Only ${AVAIL}GB free, need ${MIN_FREE_GB}GB. Stopping before $STATE."
        log "        Move finished output off the NVMe, then re-run to resume."
        break
    fi

    HOST="$(piggyback_host "$STATE")"
    SRS="$(srs_for_state "$STATE")"

    # --- Piggyback states: clip from the host's retained intermediates ------
    if [[ -n "$HOST" ]]; then
        if [[ ! -d "$BASE_DIR/tiles_vrt" ]] || ! compgen -G "$BASE_DIR/tiles_vrt/*.vrt" >/dev/null; then
            log "[FAIL] $STATE — needs $HOST intermediates, none present."
            log "       Re-run this script; $HOST must complete in the same pass."
            FAILED+=("$STATE"); continue
        fi
        log "--- $STATE: clipping from $HOST intermediates (no DEM list of its own) ---"
        if python3 process_dem.py --state "$STATE" --workers "$WORKERS" \
                --output-dir "$OUTPUT_DIR" --target-srs "$SRS" \
                --skip-cleanup --skip-export --yes; then
            log "[OK] $STATE complete"; COMPLETED+=("$STATE")
        else
            log "[FAIL] $STATE — check $HOST DEM coverage includes it"
            FAILED+=("$STATE")
        fi
        # Host held intermediates for this dependent; release them now.
        rm -f "${RAW_DEM:?}"/*.tif
        rm -rf "${BASE_DIR:?}/reprojected" "${BASE_DIR:?}/tiles_vrt"
        mkdir -p "$BASE_DIR/reprojected" "$BASE_DIR/tiles_vrt"
        log "[OK] $HOST/$STATE intermediates cleared"
        log "Free space now: $(free_gb) GB"; log ""
        continue
    fi

    # --- Normal states ------------------------------------------------------
    LIST="$DL_LISTS/${STATE}_data.txt"
    if [[ ! -f "$LIST" ]]; then
        log "[SKIP] $STATE — no download list at $LIST"
        [[ "$STATE" == "AK" ]] && log "       Alaska: see the ALASKA note at the top of this script."
        NOLIST+=("$STATE"); continue
    fi

    log "--- $STATE: downloading DEM tiles (${AVAIL}GB free, SRS $SRS) ---"
    wget -c -q --show-progress -i "$LIST" -P "$RAW_DEM/" 2>&1 | tee -a "$RUN_LOG"

    TILE_COUNT=$(find "$RAW_DEM" -maxdepth 1 -name '*.tif' | wc -l)
    if (( TILE_COUNT == 0 )); then
        log "[FAIL] $STATE — no .tif tiles downloaded"
        FAILED+=("$STATE"); continue
    fi
    log "--- $STATE: $TILE_COUNT tiles downloaded, processing ---"

    # Retain intermediates only if a piggyback state depends on this one.
    EXTRA=()
    for DEP in $STATES; do
        [[ "$(piggyback_host "$DEP")" == "$STATE" ]] && EXTRA+=(--skip-cleanup) && break
    done

    if python3 process_dem.py --state "$STATE" --workers "$WORKERS" \
            --output-dir "$OUTPUT_DIR" --target-srs "$SRS" \
            --skip-export --yes "${EXTRA[@]}"; then
        log "[OK] $STATE complete"; COMPLETED+=("$STATE")
    else
        log "[FAIL] $STATE — process_dem.py returned non-zero; see logs/"
        FAILED+=("$STATE")
        [[ ${#EXTRA[@]} -eq 0 ]] && rm -f "${RAW_DEM:?}"/*.tif
    fi

    log "Free space now: $(free_gb) GB"; log ""
done

log "=== $REGION batch finished ==="
log "Completed (${#COMPLETED[@]}): ${COMPLETED[*]:-none}"
log "Skipped   (${#SKIPPED[@]}): ${SKIPPED[*]:-none}"
log "No list   (${#NOLIST[@]}): ${NOLIST[*]:-none}"
log "Failed    (${#FAILED[@]}): ${FAILED[*]:-none}"
log ""
log "$(ls -1 "$OUTPUT_DIR"/contours_*.gpkg 2>/dev/null | wc -l) contour files, \
$(ls -1 "$OUTPUT_DIR"/hillshade_*.tif 2>/dev/null | wc -l) hillshade files in $OUTPUT_DIR"
log ""
log "NEXT — export this region to MBTiles in a single pass."
log "Export destination flag is --dest-dir:"
log ""
log "  python3 scripts/export_mbtiles.py \\"
log "      --output-dir $OUTPUT_DIR \\"
log "      --dest-dir /mnt/storage2/us/$REGION \\"
log "      --contour-interval 40 --major-multiplier 5"

(( ${#FAILED[@]} == 0 )) || exit 1
