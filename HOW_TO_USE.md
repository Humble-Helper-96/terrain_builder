# terrain_builder

**Generate hillshade and contour MBTiles from USGS DEM data for TileServer GL.**

> **United States only.** This pipeline is built around USGS 1/3 arc-second DEM
> tiles and per-state download lists, so it covers US states and territories only.

This pipeline downloads USGS 1/3 arc-second DEM tiles, reprojects them, generates
contour lines and hillshade rasters, clips them to state or region boundaries, and
exports two MBTiles files ready to serve with TileServer GL or any MBTiles-compatible
map server.

---

## Table of Contents

1. [What This Produces](#what-this-produces)
2. [System Requirements](#system-requirements)
3. [Installation](#installation)
4. [Directory Structure](#directory-structure)
5. [Quick Start — Single State](#quick-start--single-state)
6. [Incremental Builds — Multiple States](#incremental-builds--multiple-states)
7. [Full CONUS Batch Build](#full-conus-batch-build)
8. [Keeping Download Lists Current](#keeping-download-lists-current)
9. [Running Individual Scripts](#running-individual-scripts)
10. [Output Reference](#output-reference)
11. [Viewing in QGIS](#viewing-in-qgis)
12. [Tips and Troubleshooting](#tips-and-troubleshooting)

---

## What This Produces

| File | Type | Description |
|------|------|-------------|
| `output/contours_XX.gpkg` | GeoPackage (vector) | Clipped contour lines per state |
| `output/hillshade_XX.tif` | GeoTIFF (raster) | Clipped hillshade per state |
| `hillshade.mbtiles` | Raster MBTiles | Merged hillshade, zoom 6–12 |
| `contours.mbtiles` | Vector MBTiles | Merged contours, zoom 8–13 |

Contour attributes:
- `elev_m` — Elevation in metres (from GDAL)
- `elev_ft` — Elevation in feet, rounded to the contour interval

Vector tile layers in `contours.mbtiles`:
- `contours_major` — Every 200 ft (default: every 5th contour)
- `contours_minor` — Every 40 ft (all other contours)

---

## System Requirements

### Operating System

- Ubuntu 22.04 LTS or Ubuntu 24.04 LTS (recommended)
- Other Debian-based Linux distributions should work

### Hardware (recommended minimums)

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8 GB | 32 GB |
| CPU cores | 4 | 8+ |
| Disk (per state) | 10 GB free | 50 GB free |
| Disk (full CONUS) | 200 GB free | 400+ GB free |

> **Note:** Contour and hillshade generation is memory-intensive. The default
> worker count (2) is conservative and safe on 8 GB machines. See
> [Memory and Workers](#memory-and-workers) to tune for your hardware.

---

## Installation

### 1. Install system dependencies

```bash
# GDAL tools (gdalwarp, gdalbuildvrt, gdal_contour, ogr2ogr, etc.)
sudo apt update
sudo apt install gdal-bin python3-gdal

# Verify GDAL version (3.4+ required; 3.8+ recommended)
gdalinfo --version
```

> **Ubuntu 22.04 note:** The default apt GDAL is 3.4. For GDAL 3.8, add the
> UbuntuGIS unstable PPA before installing:
> ```bash
> sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
> sudo apt update && sudo apt install gdal-bin python3-gdal
> ```

### 2. Install tippecanoe

tippecanoe is not available via apt on Ubuntu 22.04. Build from source:

```bash
sudo apt install build-essential libsqlite3-dev zlib1g-dev
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe
make -j$(nproc)
sudo make install
cd ..
tippecanoe --version
```

On Ubuntu 24.04 you can install directly:

```bash
sudo apt install tippecanoe
```

### 3. Install wget

```bash
sudo apt install wget
```

### 4. Install optional Python packages

`psutil` is optional. If installed, it enables CPU and RAM usage display
in the startup header and summary of each script. The pipeline runs
normally without it.

```bash
pip install psutil --break-system-packages
```

### 5. Clone terrain_builder

```bash
git clone https://github.com/Humble-Helper-96/terrain_builder.git
cd terrain_builder
```

### 6. Verify everything is working

```bash
python3 process_dem.py --version
gdalinfo --version
tippecanoe --version
wget --version
```

---

## Directory Structure

```
terrain_builder/
├── process_dem.py              Main orchestration script
├── HOW_TO_USE.md               This file
│
├── scripts/
│   ├── reproject_dem_tiles.py  Stage 1: Reproject and tile raw DEMs
│   ├── generate_contours.py    Stage 2: Generate contour lines
│   ├── generate_hillshade.py   Stage 3: Generate hillshade raster
│   ├── clip_to_state.py        Stage 4: Clip outputs to region boundary
│   ├── export_mbtiles.py       Stage 5: Merge and export to MBTiles
│   ├── update_dem_lists.py     Refresh USGS_DL_Lists/ against the USGS S3 bucket
│   ├── build_all_states.sh     Batch script: full CONUS build
│   ├── build_osm_region.sh     Batch script: build per Geofabrik US region
│   ├── build_status.sh         One-screen progress summary for a running build
│   ├── download_tiles.sh       Per-tile DEM downloader used by the batch scripts
│   └── resource_monitor.py     Optional: CPU/RAM usage tracking
│
├── shape_files/                Region boundary GeoPackages
│   ├── CT.gpkg                 Connecticut boundary
│   ├── NY.gpkg                 New York boundary
│   ├── ConUS.gpkg              Contiguous US boundary
│   └── ...                     All 50 states + territories + special regions
│
├── USGS_DL_Lists/              Per-state wget download lists
│   ├── CT_data.txt
│   ├── NY_data.txt
│   ├── backup/                 Pre-update copies written by update_dem_lists.py
│   └── ...
│
├── raw_dem/                    Place downloaded DEM .tif files here
│                               (auto-cleared after each successful run)
│
├── reprojected/                Reprojected intermediate tiles
│                               (auto-cleared after each successful run)
│
├── tiles_vrt/                  Non-overlapping VRT tiles
│                               (auto-cleared after each successful run)
│
├── tmp/                        Unclipped intermediate contours and hillshade
│                               (auto-cleared after each run)
│
├── output/                     Clipped per-state outputs (PERSISTENT)
│   ├── contours_CT.gpkg
│   ├── hillshade_CT.tif
│   └── ...
│
└── logs/                       Per-run log files
    ├── process_dem_CT_20260101_120000.log
    └── ...
```

---

## Quick Start — Single State

### Step 1: Download DEM tiles

Each state has a pre-built wget download list in `USGS_DL_Lists/`.

```bash
# Download Connecticut DEM tiles into raw_dem/
wget -c -i USGS_DL_Lists/CT_data.txt -P raw_dem/
```

The `-c` flag resumes partial downloads if interrupted.

> **If wget reports a 404:** USGS may have replaced a tile since the list was
> written. Refresh the list first — see
> [Keeping Download Lists Current](#keeping-download-lists-current).

### Step 2: Process the state

```bash
python3 process_dem.py --state CT --tileserver-dir /path/to/tileserver/data
```

This single command runs all five pipeline stages and produces:
- `output/contours_CT.gpkg` — Clipped contour lines
- `output/hillshade_CT.tif` — Clipped hillshade raster
- `/path/to/tileserver/data/contours.mbtiles`
- `/path/to/tileserver/data/hillshade.mbtiles`

### Key options for `process_dem.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--state XX` | *(required)* | Region code, e.g. `CT`, `NY`, `ConUS` |
| `--tileserver-dir PATH` | *(required)* | Destination for MBTiles output |
| `--workers N` | 2 | Parallel workers (see [Memory and Workers](#memory-and-workers)) |
| `--target-srs EPSG:XXXX` | `EPSG:3857` | Output projection (Web Mercator) |
| `--tile-size KM` | 100 | VRT tile edge length in kilometres |
| `--skip-export` | off | Skip MBTiles export (export manually later) |
| `--skip-cleanup` | off | Keep intermediate files after run |
| `--input-dir PATH` | `raw_dem/` | Custom directory for input DEM tiles |
| `--output-dir PATH` | `output/` | Custom directory for clipped outputs |
| `--no-log` | off | Disable log file creation |

```bash
# Full help
python3 process_dem.py --help
```

---

## Incremental Builds — Multiple States

Process states one at a time and export all regions together in a single
MBTiles pass. This is the recommended approach for multi-state coverage.

```bash
# Process each state with --skip-export
# (downloads and processes DEM tiles; saves clipped outputs to output/)
wget -c -i USGS_DL_Lists/CT_data.txt -P raw_dem/
python3 process_dem.py --state CT --skip-export

wget -c -i USGS_DL_Lists/NY_data.txt -P raw_dem/
python3 process_dem.py --state NY --skip-export

wget -c -i USGS_DL_Lists/MA_data.txt -P raw_dem/
python3 process_dem.py --state MA --skip-export

# After all states are processed, export everything to MBTiles in one pass
python3 scripts/export_mbtiles.py \
    --output-dir output \
    --dest-dir /path/to/tileserver/data
```

`export_mbtiles.py` automatically finds and merges all
`output/contours_*.gpkg` and `output/hillshade_*.tif` files.

---

## Full CONUS Batch Build

`scripts/build_all_states.sh` automates the full 48-state CONUS pipeline.
It downloads and processes each state sequentially, skipping MBTiles export
until all states are complete.

> **Time estimate:** 1–6 hours per state depending on hardware.
> Full CONUS: approximately 2–5 days.

> **Disk space:** Ensure at least 50 GB free before starting.
> 100+ GB recommended for large western states (CA, MT, WY, CO, NM).
> Raw DEM tiles are deleted after each state; only clipped outputs accumulate.

> **Before a large batch, refresh the download lists:**
> `python3 scripts/update_dem_lists.py --verify`
> A single stale URL makes `wget` exit non-zero, and the batch scripts stop
> at that state — after the rest of its tiles have already downloaded. See
> [Keeping Download Lists Current](#keeping-download-lists-current).

```bash
# Default run — auto-detects 75% of available CPUs
./scripts/build_all_states.sh

# Override worker count (e.g. limit to 4 on a shared machine)
WORKERS=4 ./scripts/build_all_states.sh

# Override output directory
OUTPUT_DIR=/mnt/external/output ./scripts/build_all_states.sh
```

After all states complete, run the export manually:

```bash
python3 scripts/export_mbtiles.py \
    --output-dir output \
    --dest-dir /path/to/tileserver/data
```

### Monitoring a running build

The batch script prints to the terminal; run it in `tmux` (or `screen`) with
the output saved to a log so you can detach and check in later:

```bash
./scripts/build_all_states.sh > ~/conus_build.log 2>&1
```

`scripts/build_status.sh` reads that log (read-only) and prints a short
summary — current state, last pipeline step, tiles downloaded, states done
and free disk space — plus any `[ERROR]` lines:

```bash
./scripts/build_status.sh                     # reads ~/conus_build.log
./scripts/build_status.sh /path/to/build.log  # or another log
watch -n 60 ./scripts/build_status.sh         # refresh every minute
```

To follow just the milestones live:

```bash
tail -f ~/conus_build.log | grep --line-buffered -E 'Processing:|^\[(STEP|OK|WARN|ERROR)\]'
```

> **Resuming a partial CONUS run (crash, power cut, failed state):**
> Each finished state gets a marker, `OUTPUT_DIR/.complete_<STATE>`, written
> after its outputs are flushed to disk. Just re-run the script: marked states
> are skipped and the interrupted state is redone from scratch. Append to the
> same log so `build_status.sh` keeps counting across runs:
>
> ```bash
> ./scripts/build_all_states.sh 2>&1 | tee -a ~/conus_build.log
> ```
>
> Runs from before markers were added have none — resume those at the state
> that was interrupted with `START_AT=<STATE>` (e.g. `START_AT=IL`).
> `FORCE=1` rebuilds states even if they are marked complete.

### Per-region builds (Geofabrik US extracts)

`scripts/build_osm_region.sh` builds one Geofabrik-style US region at a time
(`us-south`, `us-northeast`, `us-midwest`, `us-west`, `us-pacific`) into its
own output directory, skipping states that already have outputs.

```bash
./scripts/build_osm_region.sh --list
./scripts/build_osm_region.sh us-south
```

---

## Keeping Download Lists Current

USGS stores each 1/3 arc-second cell under two sibling prefixes:

```
.../Elevation/13/TIFF/current/<CELL>/USGS_13_<CELL>.tif              (undated, live)
.../Elevation/13/TIFF/historical/<CELL>/USGS_13_<CELL>_<DATE>.tif    (dated, superseded)
```

When USGS publishes a new version of a cell, the old file moves into
`historical/` and the new one takes its place in `current/`. A dated
`historical/` URL in `USGS_DL_Lists/` can therefore start returning 404 at
any time.

`scripts/update_dem_lists.py` checks each list against the USGS S3 bucket
(`current/` first, then `historical/`) and rewrites entries that have a newer
or live replacement. It never replaces a tile with an older one.

```bash
# See what would change for one state (writes nothing)
python3 scripts/update_dem_lists.py --state LA --dry-run

# Update every list; also HEAD-check every URL that was not changed
python3 scripts/update_dem_lists.py --verify
```

- Changed lists are backed up to `USGS_DL_Lists/backup/<STATE>_data.txt.bkup`.
- `--verify` reports URLs that no longer resolve in either prefix — these are
  the ones that would 404 during `wget`.
- Each tile costs up to two S3 requests; a full run over all states takes on
  the order of 40–50 minutes at the default delay.

> **If you ran v1.0.0 of this script:** it only looked in `historical/` and
> treated any difference as an update, so it could rewrite a list to an
> *older* tile. Compare your lists with `USGS_DL_Lists/backup/` and re-run the
> v1.1.0 updater.

---

## Running Individual Scripts

Each pipeline stage can be run independently for debugging or custom workflows.
All scripts resolve paths relative to their own location — run them from
anywhere.

### Stage 1: Reproject DEM tiles

```bash
python3 scripts/reproject_dem_tiles.py \
    --input-dir raw_dem \
    --output-dir reprojected \
    --tiles-dir tiles_vrt \
    --target-srs EPSG:3857 \
    --tile-size 100 \
    --workers 4
```

Produces:
- `reprojected/*.tif` — Reprojected tiles
- `reprojected/dem_mosaic.vrt` — Master VRT
- `tiles_vrt/tile_XXXX_YYYY.vrt` — Non-overlapping smart tiles

### Stage 2: Generate contours

```bash
python3 scripts/generate_contours.py \
    tiles_vrt/ \
    tmp/contours.gpkg \
    --interval 40 \
    --simplify 2 \
    --workers 2
```

### Stage 3: Generate hillshade

```bash
python3 scripts/generate_hillshade.py \
    tiles_vrt/ \
    tmp/hillshade.tif \
    --contrast 0.59 \
    --shadow-base 225 \
    --workers 2
```

### Stage 4: Clip to region boundary

```bash
python3 scripts/clip_to_state.py \
    --state CT \
    --gpkg-dir shape_files \
    --contours-in tmp/contours.gpkg \
    --contours-out output/contours_CT.gpkg \
    --hillshade-in tmp/hillshade.tif \
    --hillshade-out output/hillshade_CT.tif
```

Optional buffer beyond the boundary:

```bash
python3 scripts/clip_to_state.py --state CT --buffer 5000
```

### Stage 5: Export to MBTiles

```bash
python3 scripts/export_mbtiles.py \
    --output-dir output \
    --dest-dir /path/to/tileserver/data
```

Non-standard contour interval (e.g. 100 ft minor / 500 ft major):

```bash
python3 scripts/export_mbtiles.py \
    --output-dir output \
    --dest-dir /path/to/tileserver/data \
    --contour-interval 100 \
    --major-multiplier 5
```

---

## Output Reference

### Contours — `output/contours_XX.gpkg`

| Property | Value |
|----------|-------|
| Format | GeoPackage |
| Layer name | `contours` |
| Geometry | MultiLineString |
| CRS | EPSG:3857 (Web Mercator) |
| Spatial index | Yes |

Attributes:

| Column | Type | Description |
|--------|------|-------------|
| `elev_m` | REAL | Elevation in metres |
| `elev_ft` | INTEGER | Elevation in feet, rounded to contour interval |

Filtering by contour class (default 40 ft interval):

```sql
-- Major contours (every 200 ft)
WHERE elev_ft % 200 = 0

-- Minor contours (every 40 ft)
WHERE elev_ft % 200 != 0
```

### Hillshade — `output/hillshade_XX.tif`

| Property | Value |
|----------|-------|
| Format | GeoTIFF |
| CRS | EPSG:3857 (Web Mercator) |
| Band 1 | Grayscale hillshade (0–255) |
| Band 2 | Alpha channel (transparency) |
| Compression | DEFLATE, tiled |

Alpha formula: `alpha = 225 - (gray × 0.59)`
Adjust with `--contrast` and `--shadow-base` flags on `generate_hillshade.py`.

### MBTiles

| File | Type | Zoom | Layers |
|------|------|------|--------|
| `hillshade.mbtiles` | Raster | 6–12 | — |
| `contours.mbtiles` | Vector | 8–13 | `contours_major`, `contours_minor` |

> **Zoom cap note:** Contours are capped at z13. z14 causes exponential tile
> count growth on dense contour datasets and produces files too large for
> practical TileServer GL use.

---

## Viewing in QGIS

### Load hillshade

1. **Layer → Add Layer → Add Raster Layer**
2. Select `output/hillshade_CT.tif`
3. In Layer Properties → Symbology:
   - Render type: **Singleband gray**
   - Blending mode: **Multiply** or **Overlay**
4. Place the hillshade layer below contours and above base imagery

### Load contours

1. **Layer → Add Layer → Add Vector Layer**
2. Select `output/contours_CT.gpkg`, layer `contours`
3. In Layer Properties → Symbology → **Rule-based**:
   - Rule 1 — Major contours:
     - Filter: `"elev_ft" % 200 = 0`
     - Symbol: thick line (0.4mm), labelled with `elev_ft`
   - Rule 2 — Minor contours:
     - Filter: `"elev_ft" % 200 != 0`
     - Symbol: thin line (0.1mm), no label

---

## Tips and Troubleshooting

### Memory and Workers

Each `generate_contours.py` and `generate_hillshade.py` worker processes
one 100 km × 100 km VRT tile at a time. These operations are memory-intensive.

| Available RAM | `--workers` |
|---------------|-------------|
| 8 GB | 2 (default) |
| 16 GB | 4 |
| 32 GB | 6–8 |

Pass `--workers N` to `process_dem.py` and it is forwarded to all stages.

### Skipping empty tiles

The pipeline automatically skips tiles with no valid elevation data. In the
log you will see:

```
[SKIP] tile_0012_0005 (no data)
```

This is normal for coastal areas, ocean tiles, and sparse datasets such as
Alaska IFSAR coverage. The SMART tiling system in `reproject_dem_tiles.py`
also pre-filters empty grid cells before generating VRT tiles.

### State boundary files

All included `shape_files/*.gpkg` boundaries use EPSG:4326 (WGS84).
`clip_to_state.py` and `generate_contours.py` reproject on the fly as needed.

To add a custom region:
1. Create a GeoPackage with a single polygon layer
2. Name the file `<CODE>.gpkg` and place it in `shape_files/`
3. Use `--state <CODE>` when running `process_dem.py`

Valid region codes are detected automatically from whatever `.gpkg` files
are present in `shape_files/` at runtime.

### Alaska and non-CONUS regions

Alaska uses a different projection. Pass `--target-srs EPSG:3338`
(Alaska Albers) for better accuracy:

```bash
python3 process_dem.py --state AK --target-srs EPSG:3338
```

### Re-running a failed state

If `process_dem.py` fails mid-run, intermediate files in `reprojected/`
and `tiles_vrt/` may be present. By default the next run clears and
rebuilds these. To reuse them and skip to a later stage, use
`--skip-cleanup`:

```bash
python3 process_dem.py --state CT --skip-cleanup --tileserver-dir /path/to/data
```

### wget fails with a 404 on one tile

The download list points at a tile USGS has since replaced. Most of the
state's tiles will already be in `raw_dem/`, but `wget` exits non-zero and the
batch scripts abort. Run `python3 scripts/update_dem_lists.py --state XX`
(see [Keeping Download Lists Current](#keeping-download-lists-current)), then
re-run `wget -c -i USGS_DL_Lists/XX_data.txt -P raw_dem/` — `-c` skips the
files you already have.

### Disk space

| Stage | Approximate size |
|-------|-----------------|
| Raw DEM tiles (per state) | 1–20 GB |
| Reprojected tiles (per state) | 1–25 GB (cleared after run) |
| Clipped contours (per state) | 50–500 MB |
| Clipped hillshade (per state) | 200 MB–3 GB |
| `contours.mbtiles` (full CONUS) | 5–15 GB |
| `hillshade.mbtiles` (full CONUS) | 10–30 GB |

Intermediate directories (`raw_dem/`, `reprojected/`, `tiles_vrt/`, `tmp/`)
are cleared automatically after each successful run. Only `output/` and
`logs/` persist across runs.

### Log files

Each `process_dem.py` run writes a timestamped log to `logs/`:

```
logs/process_dem_CT_20260101_120000.log
```

If a run fails, check the log for the exact error before re-running.
Pass `--no-log` to disable log file creation.

### ogrmerge.py not found

On some systems `ogrmerge.py` is installed but not on `PATH`. The scripts
probe known fallback locations automatically. If the probe fails:

```bash
# Find it manually
find /usr -name "ogrmerge.py" 2>/dev/null

# Add its directory to PATH
export PATH="/usr/share/gdal:$PATH"
```

### gdal_calc.py / gdal_merge.py not found

These tools are in the `python3-gdal` package, separate from `gdal-bin`:

```bash
sudo apt install python3-gdal
```

---

## Getting Help

Every script has a `--help` flag and a `--version` flag:

```bash
python3 process_dem.py --help
python3 scripts/reproject_dem_tiles.py --help
python3 scripts/generate_contours.py --help
python3 scripts/generate_hillshade.py --help
python3 scripts/clip_to_state.py --help
python3 scripts/export_mbtiles.py --help
python3 scripts/update_dem_lists.py --help
```

Each script's module docstring also contains detailed documentation on
inputs, outputs, dependencies, and processing notes.

---

*Licensed under the [MIT License](LICENSE).*
