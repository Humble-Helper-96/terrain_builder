# Terrain Builder

A self-contained toolkit for generating contour lines, hillshade, and web-ready MBTiles from USGS Digital Elevation Model (DEM) data.

All paths are relative to the project directory. You can run the script from anywhere — it resolves its own location automatically.

## What This Does

This toolkit processes DEM data to create:

- **Contour lines** — elevation lines at 40ft intervals (major every 200ft)
- **Hillshade** — 3D terrain shading with transparency
- **MBTiles** — web-ready map tiles for TileServer GL, Martin, or similar

One command processes a full state:

```bash
python3 process_dem.py --state CT
```

This will reproject your raw DEM tiles, generate contours and hillshade, clip to the state boundary, and export MBTiles — all automatically.

## Prerequisites

This toolkit requires **Python 3.8+** and several GIS command-line tools. All tools are free and open source.

### GDAL (core tools + Python scripts)

**Ubuntu/Debian:**

```bash
sudo apt install gdal-bin python3-gdal
```

**macOS (Homebrew):**

```bash
brew install gdal
```

This provides all of the following (used by the pipeline):

`gdal_contour` `gdaldem` `gdalwarp` `gdal_translate` `gdalbuildvrt` `gdaladdo` `gdalinfo` `ogr2ogr` `ogrinfo` `gdal_calc.py` `gdal_merge.py` `gdal_edit.py` `ogrmerge.py`

Verify with:

```bash
gdalinfo --version
gdal_calc.py --help
```

### Tippecanoe (vector tile creation)

**Ubuntu/Debian (build from source):**

```bash
sudo apt install build-essential libsqlite3-dev zlib1g-dev
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe && make -j && sudo make install
```

**macOS (Homebrew):**

```bash
brew install tippecanoe
```

Verify with:

```bash
tippecanoe --version
```

### Optional — psutil (RAM monitoring)

```bash
pip install psutil --break-system-packages
```

The pipeline works without it but cannot report detailed memory usage.

## Directory Structure

```
terrain_builder/
├── process_dem.py           # Main orchestration script
├── scripts/
│   ├── reproject_dem_tiles.py
│   ├── generate_contours.py
│   ├── generate_hillshade.py
│   ├── clip_to_state.py
│   ├── export_mbtiles.py
│   └── resource_monitor.py
├── raw_dem/                 # Input: place your DEM .tif files here
├── shape_files/             # Input: state boundary files (e.g., CT.gpkg)
├── reprojected/             # Intermediate: reprojected tiles
├── tiles_vrt/               # Intermediate: non-overlapping VRT tiles
├── tmp/                     # Intermediate: auto-cleaned each run
├── output/                  # Final clipped outputs (persistent)
│   └── mbtiles/             # Final MBTiles files
├── logs/                    # Processing logs
└── USGS_DL_Lists/           # USGS download lists (reference)
```

## Quick Start

### 1. Get DEM data

Download DEM tiles from the [USGS National Map](https://apps.nationalmap.gov/downloader/) and place the `.tif` files in `raw_dem/`.

### 2. Add a state boundary

Place a GeoPackage in `shape_files/` named by state code (e.g., `CT.gpkg`). The GPKG should have a layer named with the state code.

### 3. Run it

```bash
python3 process_dem.py --state CT
```

You can run the script from any directory:

```bash
python3 /path/to/terrain_builder/process_dem.py --state CT
```

### 4. Check outputs

```
output/
├── contours_CT.gpkg
├── hillshade_CT.tif
└── mbtiles/
    ├── contours.mbtiles
    └── hillshade.mbtiles
```

## Common Options

```bash
# Use more CPU cores for faster processing
python3 process_dem.py --state CT --workers 4

# Use a custom input directory instead of raw_dem/
python3 process_dem.py --state CT --input-dir /path/to/my/dem/files

# Skip cleanup to keep intermediate files (reprojected/, tiles_vrt/)
python3 process_dem.py --state CT --skip-cleanup

# Skip MBTiles export (clip only)
python3 process_dem.py --state CT --skip-export

# Change VRT tile size (default 100km)
python3 process_dem.py --state AK --tile-size 50

# Use a different projection (default: EPSG:3857 Web Mercator)
python3 process_dem.py --state AK --target-srs EPSG:3338
```

### Full Command Reference

```bash
python3 process_dem.py --help
```

| Flag | Description |
|------|-------------|
| `--state XX` | State code — required (e.g., CT, NY, MA, AK) |
| `--workers N` | Number of parallel workers (default: 2) |
| `--target-srs CODE` | Target projection (default: EPSG:3857) |
| `--tile-size KM` | VRT tile size in km (default: 100) |
| `--input-dir PATH` | Custom raw DEM directory instead of `raw_dem/` |
| `--skip-cleanup` | Keep intermediate files (`reprojected/`, `tiles_vrt/`) |
| `--skip-export` | Skip MBTiles export stage |
| `--no-log` | Disable logging to file |

## Incremental Processing

Process multiple states, then merge into unified MBTiles:

```bash
python3 process_dem.py --state CT
python3 process_dem.py --state NY
python3 process_dem.py --state MA

# Export merges all output/contours_*.gpkg and output/hillshade_*.tif
python3 scripts/export_mbtiles.py \
    --output-dir output/ \
    --mbtiles-dir output/mbtiles/
```

The `output/` directory accumulates clipped regions across runs.

## Understanding the Output

### Contours (`output/contours_XX.gpkg`)

| Property | Value |
|----------|-------|
| Format | GeoPackage (vector) |
| Layer | `contours` |
| Attributes | `elev_m` (meters), `elev_ft` (feet, rounded to 20ft) |
| Major contours | Every 200ft — `WHERE elev_ft % 200 = 0` |
| Minor contours | Every 40ft (all others) |

### Hillshade (`output/hillshade_XX.tif`)

| Property | Value |
|----------|-------|
| Format | GeoTIFF (raster) |
| Band 1 | Grayscale hillshade (0–255) |
| Band 2 | Alpha channel (transparency) |
| Compression | DEFLATE |

### MBTiles (`output/mbtiles/`)

| File | Type | Contents |
|------|------|----------|
| `hillshade.mbtiles` | Raster | Hillshade tiles with overviews |
| `contours.mbtiles` | Vector (zoom 8–14) | `contours_major` (200ft) + `contours_minor` (40ft) |

## Deploying MBTiles

After processing, copy the MBTiles to your tile server's data directory:

```bash
cp output/mbtiles/*.mbtiles /path/to/tileserver/data/
```

Then reload your tile server (TileServer GL, Martin, etc.) to pick up the new tiles.

## Advanced Usage — Individual Scripts

You can run each step independently for more control. When running scripts individually, `cd` into the `terrain_builder` directory first — the sub-scripts use relative paths for their defaults.

```bash
cd /path/to/terrain_builder
```

**Step 1: Reproject DEM Tiles**

```bash
python3 scripts/reproject_dem_tiles.py \
    --input-dir raw_dem/ \
    --output-dir reprojected/ \
    --target-srs EPSG:3857 \
    --tile-size 100 \
    --workers 2
```

**Step 2: Generate Contours**

```bash
python3 scripts/generate_contours.py \
    tiles_vrt/ tmp/contours.gpkg \
    --workers 2
```

**Step 3: Generate Hillshade**

```bash
python3 scripts/generate_hillshade.py \
    tiles_vrt/ tmp/hillshade.tif \
    --workers 2
```

**Step 4: Clip to State Boundary**

```bash
python3 scripts/clip_to_state.py \
    --state CT \
    --gpkg-dir shape_files/ \
    --contours-in tmp/contours.gpkg \
    --contours-out output/contours_CT.gpkg \
    --hillshade-in tmp/hillshade.tif \
    --hillshade-out output/hillshade_CT.tif
```

**Step 5: Export to MBTiles**

```bash
python3 scripts/export_mbtiles.py \
    --output-dir output/ \
    --mbtiles-dir output/mbtiles/
```

## Tips & Troubleshooting

### Memory Usage

The scripts default to 2 workers to stay safe on lower-RAM systems.

| RAM | Recommended `--workers` |
|-----|------------------------|
| 8GB | 2 (default) |
| 16GB | 4–6 |
| 32GB+ | 8–12 |

### Processing Time

Typical times for one state (e.g., Connecticut):

| Stage | Time |
|-------|------|
| Reprojection | ~2 minutes |
| Contours | ~5 minutes |
| Hillshade | ~5 minutes |
| Clipping | ~1–5 minutes |
| **Total** | **~10–15 minutes** |

### Empty/No Data Tiles

The scripts automatically skip tiles with no valid elevation data. You'll see `[SKIP] tile_name (no data)` — this is normal for coastal areas or regions outside coverage.

### Disk Space

Budget approximately 2–5x input size for temporary files and 1–2x for final outputs. The `tmp/` directory is auto-cleaned after each run.

## Styling in QGIS

**Contours:**

1. Add Vector Layer → `output/contours_CT.gpkg`
2. Style by expression: `"elev_ft" % 200 = 0`
   - Major contours (200ft): thick line, with labels
   - Minor contours (40ft): thin line, no labels

**Hillshade:**

1. Add Raster Layer → `output/hillshade_CT.tif`
2. Set blending mode to "Multiply" or "Overlay"
3. Place below contours and above base imagery

## Getting Help

Each script has detailed help:

```bash
python3 process_dem.py --help
python3 scripts/generate_contours.py --help
python3 scripts/generate_hillshade.py --help
python3 scripts/clip_to_state.py --help
python3 scripts/export_mbtiles.py --help
```

## License

MIT — see [LICENSE](LICENSE) for details.
