#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Humble-Helper-96
#
# Part of the terrain_builder pipeline — see HOW_TO_USE.md for full documentation.
"""
DEM Processing Pipeline for GIS Tool Stack

Processes raw DEM tiles into contours and hillshade MBTiles for TileServer GL.

Workflow:
  1) Reproject raw DEM tiles        --> reprojected/  (may overlap)
  2) Build master VRT + create non-overlapping 100km×100km VRT tiles --> tiles_vrt/
  3) Generate contours from VRT tiles  --> tmp/contours.gpkg
  4) Generate hillshade from VRT tiles --> tmp/hillshade.tif
  5) Clip to state/region boundary  --> output/contours_XX.gpkg, hillshade_XX.tif
  6) Merge and export MBTiles       --> <tileserver-dir>/*.mbtiles

Directory structure (all paths relative to terrain_builder/):
  terrain_builder/
    process_dem.py            - This script (main entry point)
    raw_dem/                  - Place your raw DEM .tif files here before running
    reprojected/              - Reprojected tiles (intermediate, auto-cleaned)
      └── dem_mosaic.vrt      - Master VRT of all reprojected tiles
    tiles_vrt/                - Non-overlapping VRT tiles (intermediate, auto-cleaned)
    tmp/                      - Unclipped contours and hillshade (intermediate, auto-cleaned)
    output/                   - Clipped outputs ready for merging (PERSISTENT across runs)
    shape_files/              - State/region boundary GeoPackages (<REGION>.gpkg)
    scripts/                  - Processing sub-scripts called by this pipeline
    logs/                     - Per-run log files
    USGS_DL_Lists/            - Per-state wget download lists (<STATE>_data.txt)

The output/ folder accumulates clipped regions across runs. When you process
multiple states, their outputs are added to output/ and merged together into
larger regional MBTiles by export_mbtiles.py.

NOTE: reprojected/ and tiles_vrt/ are cleaned at the START of each run to
ensure only the current batch of raw DEM tiles is processed. Use --skip-cleanup
to reuse existing intermediate files (e.g. to re-run only the export stage).

Valid region codes are determined automatically from whatever .gpkg files are
present in shape_files/ at runtime.

System dependencies (must be installed separately — see HOW_TO_USE.md):
  - GDAL/OGR tools: gdalwarp, gdalbuildvrt, ogr2ogr, ogrmerge.py
  - tippecanoe
  - wget

Python dependencies:
  - psutil (optional) -- pip install psutil
      Enables RAM/CPU display in the startup header. The pipeline runs
      normally without it; system info is simply omitted.
"""

import subprocess
import sys
import shutil
from pathlib import Path
import argparse
import time
import platform
from datetime import datetime

# Optional dependency — graceful fallback if not installed
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Resolve BASE_DIR from the script's own location, not the caller's cwd.
# This ensures all relative paths work correctly regardless of where the
# script is invoked from (e.g. from a shell script in another directory).
BASE_DIR = Path(__file__).resolve().parent

# Required system tools — pipeline cannot run without these
REQUIRED_TOOLS = ["gdalwarp", "gdalbuildvrt", "ogr2ogr", "tippecanoe"]

# Optional tools — warn if missing, but do not abort
OPTIONAL_TOOLS = ["wget"]


# ---------------------------------------------------------------------------
# Dependency checking
# ---------------------------------------------------------------------------

def check_dependencies():
    """
    Check that required system tools are available on PATH.
    Warns about optional tools. Aborts if any required tool is missing.
    Returns True if all required tools are present, False otherwise.
    """
    missing_required = []
    missing_optional = []

    for tool in REQUIRED_TOOLS:
        if not shutil.which(tool):
            missing_required.append(tool)

    for tool in OPTIONAL_TOOLS:
        if not shutil.which(tool):
            missing_optional.append(tool)

    if missing_optional:
        print("[WARN] Optional tools not found on PATH:")
        for tool in missing_optional:
            print(f"       - {tool}  (needed for USGS tile downloads)")
        print()

    if missing_required:
        print("[ERROR] Required tools not found on PATH:")
        for tool in missing_required:
            print(f"        - {tool}")
        print()
        print("        Install GDAL:       sudo apt install gdal-bin")
        print("        Install tippecanoe: see HOW_TO_USE.md for build instructions")
        print()
        return False

    return True


# ---------------------------------------------------------------------------
# TeeOutput — duplicate stdout/stderr to terminal and log file simultaneously
# ---------------------------------------------------------------------------

class TeeOutput:
    """
    Wraps sys.stdout so that all print() output goes to both the terminal
    and a log file at the same time. Assigned to sys.stdout after log path
    is known. Restored in the finally block of __main__.
    """
    def __init__(self, log_file_path):
        self.terminal = sys.stdout
        self.log = open(log_file_path, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def fileno(self):
        # Needed by some subprocesses and C extensions that call fileno() on stderr
        return self.terminal.fileno()

    def close(self):
        self.log.close()


# ---------------------------------------------------------------------------
# System info display
# ---------------------------------------------------------------------------

def get_system_info():
    """
    Collect OS, CPU, RAM, and disk info for the startup header.
    RAM/CPU fields are omitted if psutil is not installed.
    """
    info = {}

    # OS detection
    if platform.system() == "Linux":
        try:
            os_info = {}
            with open("/etc/os-release", "r") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line:
                        key, value = line.split("=", 1)
                        os_info[key] = value.strip('"')
            info['os'] = os_info.get("PRETTY_NAME") or os_info.get("NAME") or f"Linux {platform.release()}"
        except FileNotFoundError:
            info['os'] = f"Linux {platform.release()}"
    else:
        info['os'] = f"{platform.system()} {platform.release()}"

    # Disk usage — always available via stdlib
    disk_usage = shutil.disk_usage(BASE_DIR)
    free_gb = disk_usage.free / (1024 ** 3)
    total_gb = disk_usage.total / (1024 ** 3)
    info['disk_free'] = f"{free_gb:.1f} GB free of {total_gb:.1f} GB"

    # CPU and RAM — only if psutil is available
    if HAS_PSUTIL:
        mem = psutil.virtual_memory()
        info['ram_total'] = f"{mem.total / (1024**3):.1f} GB"
        info['ram_available'] = f"{mem.available / (1024**3):.1f} GB ({mem.percent:.1f}% used)"
        cpu_cores = psutil.cpu_count(logical=False) or 0
        cpu_threads = psutil.cpu_count(logical=True) or 0
        info['cpu_cores'] = cpu_cores
        info['cpu_threads'] = cpu_threads
    else:
        info['ram_available'] = "unknown (install psutil: pip install psutil)"
        info['cpu_cores'] = "unknown"
        info['cpu_threads'] = "unknown"

    return info


def print_header():
    """Print script header with system info"""
    print()
    print("=" * 70)
    print("DEM Processing Pipeline for GIS Tool Stack")
    print("=" * 70)
    print()

    info = get_system_info()
    print(f"Operating System:     {info['os']}")
    print(f"CPU Cores/Threads:    {info['cpu_cores']} / {info['cpu_threads']}")
    print(f"Available RAM:        {info['ram_available']}")
    print(f"Available Disk:       {info['disk_free']}")
    print()


# ---------------------------------------------------------------------------
# Valid region detection
# ---------------------------------------------------------------------------

def get_valid_regions(shape_files_dir: Path) -> set:
    """
    Return the set of valid region codes by scanning shape_files/ for .gpkg files.
    This automatically includes all states, territories, and special regions
    (e.g. ConUS) without requiring a hardcoded list.
    """
    return {p.stem.upper() for p in shape_files_dir.glob("*.gpkg")}


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

def run_stage(script_path: Path, args: list, stage_name: str) -> bool:
    """
    Run a pipeline sub-script as a subprocess, streaming its output to stdout
    (which may be a TeeOutput, so output goes to both terminal and log).
    Returns True on success, False on failure. Always prints elapsed time.
    """
    print()
    print("=" * 70)
    print(f"STAGE: {stage_name}")
    print("=" * 70)
    print()

    start_time = time.time()

    # -u: unbuffered, so sub-script output reaches the log as it happens
    # (stdout is a pipe here, which Python would otherwise block-buffer)
    cmd = [sys.executable, '-u', str(script_path)] + args

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    for line in process.stdout:
        print(line, end='')
        sys.stdout.flush()

    return_code = process.wait()
    elapsed = time.time() - start_time

    print()
    if return_code == 0:
        print(f"[SUCCESS] {stage_name} completed ({elapsed / 60:.1f} minutes)")
    else:
        print(f"[FAIL] {stage_name} failed ({elapsed / 60:.1f} minutes elapsed)")
    print()

    return return_code == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Main entry point"""

    shape_files_dir = BASE_DIR / 'shape_files'
    valid_regions = get_valid_regions(shape_files_dir)

    parser = argparse.ArgumentParser(
        description="Process raw DEM tiles into MBTiles for TileServer GL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 process_dem.py --state CT
  python3 process_dem.py --state AK --target-srs EPSG:3338
  python3 process_dem.py --state CT --workers 4
  python3 process_dem.py --state AK --skip-cleanup
  python3 process_dem.py --state AK --tile-size 50
  python3 process_dem.py --state NV --workers 6 --yes --skip-export
  python3 process_dem.py --state TX --skip-export --tileserver-dir /srv/tileserver/data
        """
    )

    parser.add_argument(
        '--state',
        required=True,
        metavar='CODE',
        help=(
            'Region code for clipping (e.g. CT, NY, AK, ConUS). '
            'Must match a .gpkg file in shape_files/. '
            f'Available: {", ".join(sorted(valid_regions)) if valid_regions else "none found"}'
        )
    )

    parser.add_argument(
        '--target-srs',
        default='EPSG:3857',
        help='Target projection code (default: EPSG:3857 — Web Mercator)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=2,
        help='Number of parallel workers (default: 2)'
    )

    parser.add_argument(
        '--yes',
        action='store_true',
        help='Skip confirmation prompts'
    )

    parser.add_argument(
        '--no-log',
        action='store_true',
        help='Disable logging to file (logs/ directory)'
    )

    parser.add_argument(
        '--input-dir',
        help=(
            'Custom directory containing raw DEM .tif files. '
            'Defaults to raw_dem/ inside terrain_builder/.'
        )
    )

    parser.add_argument(
        '--skip-cleanup',
        action='store_true',
        help=(
            'Skip pre-run cleanup of reprojected/ and tiles_vrt/ '
            'and skip post-run cleanup of raw_dem/. '
            'Useful for re-running only the export stage without re-downloading tiles.'
        )
    )

    parser.add_argument(
        '--tile-size',
        type=int,
        default=100,
        help='VRT tile size in kilometers (default: 100)'
    )

    parser.add_argument(
        '--skip-export',
        action='store_true',
        help=(
            'Skip MBTiles export stage. '
            'Run manually later with: '
            'python3 scripts/export_mbtiles.py --output-dir output'
        )
    )

    parser.add_argument(
        '--output-dir',
        help=(
            'Custom directory for clipped output files. '
            'Defaults to output/ inside terrain_builder/. '
            'Example: --output-dir /mnt/usb2'
        )
    )

    parser.add_argument(
        '--tileserver-dir',
        help=(
            'Path to TileServer GL data directory where MBTiles will be written. '
            'Required when --skip-export is not set. '
            'Example: --tileserver-dir /srv/tileserver/data'
        )
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Validate --state before doing anything else (before log is opened,
    # before any directory is created, before any stage runs)
    # ------------------------------------------------------------------
    state_upper = args.state.upper()
    if valid_regions and state_upper not in valid_regions:
        print(f"[ERROR] Unknown region code: {state_upper}")
        print(f"        Available regions: {', '.join(sorted(valid_regions))}")
        print(f"        To add a new region, place <CODE>.gpkg in: {shape_files_dir}")
        return 1

    # ------------------------------------------------------------------
    # Validate --tileserver-dir is provided when export is not skipped
    # ------------------------------------------------------------------
    if not args.skip_export and not args.tileserver_dir:
        print("[ERROR] --tileserver-dir is required unless --skip-export is set.")
        print("        Example: --tileserver-dir /srv/tileserver/data")
        print("        To skip MBTiles export entirely: --skip-export")
        return 1

    tileserver_dir = Path(args.tileserver_dir).resolve() if args.tileserver_dir else None

    # ------------------------------------------------------------------
    # Validate --workers against logical CPU count
    # ------------------------------------------------------------------
    if HAS_PSUTIL:
        max_workers = psutil.cpu_count(logical=True) or 1
        if args.workers > max_workers:
            print(f"[WARN] --workers {args.workers} exceeds logical CPU count ({max_workers}).")
            print(f"       Clamping to {max_workers}.")
            args.workers = max_workers

    # ------------------------------------------------------------------
    # Check system tool dependencies before opening log or creating dirs
    # ------------------------------------------------------------------
    if not check_dependencies():
        return 1

    # ------------------------------------------------------------------
    # Set up logging
    # ------------------------------------------------------------------
    logs_dir = BASE_DIR / 'logs'
    logs_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f'process_dem_{state_upper}_{timestamp}.log'

    tee_output = None
    if not args.no_log:
        tee_output = TeeOutput(log_file)
        sys.stdout = tee_output
        sys.stderr = tee_output
        print(f"[INFO] Logging to: {log_file}")
        print(f"[INFO] Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print_header()

    total_start = time.time()

    # ------------------------------------------------------------------
    # Resolve all working directories
    # ------------------------------------------------------------------
    raw_dem_dir = Path(args.input_dir).resolve() if args.input_dir else (BASE_DIR / 'raw_dem')
    reprojected_dir = BASE_DIR / 'reprojected'
    tiles_vrt_dir = BASE_DIR / 'tiles_vrt'
    tmp_dir = BASE_DIR / 'tmp'
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (BASE_DIR / 'output')
    scripts_dir = BASE_DIR / 'scripts'

    tmp_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    print(f"Working directory:    {BASE_DIR}")
    print(f"Raw DEM:              {raw_dem_dir}")
    print(f"Reprojected:          {reprojected_dir}")
    print(f"Target projection:    {args.target_srs}")
    print(f"VRT tile size:        {args.tile_size}km × {args.tile_size}km")
    print(f"Workers:              {args.workers}")
    print(f"Temp outputs:         {tmp_dir}")
    print(f"Clipped outputs:      {output_dir}")
    print(f"State/Region:         {state_upper}")
    if args.skip_export:
        print(f"MBTiles export:       SKIPPED (--skip-export)")
    else:
        print(f"TileServer data dir:  {tileserver_dir}")
    print()

    # ------------------------------------------------------------------
    # Validate raw DEM input
    # ------------------------------------------------------------------
    if not raw_dem_dir.exists():
        print(f"[ERROR] Raw DEM directory not found: {raw_dem_dir}")
        print(f"        Check the path and try again, or omit --input-dir to use raw_dem/")
        return 1

    dem_files = list(raw_dem_dir.glob('*.tif'))
    dem_count = len(dem_files)
    print(f"[FOUND] {dem_count} DEM tile(s) in {raw_dem_dir}")
    print()

    if dem_count == 0:
        print(f"[ERROR] No .tif files found in {raw_dem_dir}")
        print(f"        Download DEM tiles using wget and the lists in USGS_DL_Lists/")
        return 1

    # ------------------------------------------------------------------
    # Pre-run cleanup (unless --skip-cleanup)
    # ------------------------------------------------------------------
    if args.skip_cleanup:
        print()
        print("=" * 70)
        print("SKIPPING PRE-RUN CLEANUP (--skip-cleanup)")
        print("=" * 70)
        print()
        print("[INFO] Reusing existing reprojected/ and tiles_vrt/ directories")
        print()
    else:
        for d in [reprojected_dir, tiles_vrt_dir]:
            if d.exists():
                try:
                    shutil.rmtree(d)
                    print(f"[OK] Cleared {d.name}/")
                except Exception as e:
                    print(f"[WARN] Could not clear {d.name}/: {e}")

    reprojected_dir.mkdir(exist_ok=True)
    tiles_vrt_dir.mkdir(exist_ok=True)

    print()

    # ------------------------------------------------------------------
    # Locate required sub-scripts
    # ------------------------------------------------------------------
    reproject_script = scripts_dir / 'reproject_dem_tiles.py'
    contours_script  = scripts_dir / 'generate_contours.py'
    hillshade_script = scripts_dir / 'generate_hillshade.py'
    clip_script      = scripts_dir / 'clip_to_state.py'
    export_script    = scripts_dir / 'export_mbtiles.py'

    required_scripts = [reproject_script, contours_script, hillshade_script, clip_script]
    if not args.skip_export:
        required_scripts.append(export_script)

    missing_scripts = [s for s in required_scripts if not s.exists()]
    if missing_scripts:
        print("[ERROR] Missing required scripts:")
        for s in missing_scripts:
            print(f"        - {s}")
        print(f"        Ensure you have a complete clone of the terrain_builder repo.")
        return 1

    print("[FOUND] All required scripts")
    print()

    # ------------------------------------------------------------------
    # Stage 1 — Reproject DEM tiles
    # ------------------------------------------------------------------
    if not run_stage(
        reproject_script,
        ['--input-dir',  str(raw_dem_dir),
         '--output-dir', str(reprojected_dir),
         '--target-srs', args.target_srs,
         '--tile-size',  str(args.tile_size),
         '--workers',    str(args.workers)],
        "1. Reproject DEM Tiles"
    ):
        return 1

    # Confirm VRT tiles were actually produced before continuing
    if not tiles_vrt_dir.exists() or not list(tiles_vrt_dir.glob('*.vrt')):
        print(f"[ERROR] No VRT tiles found in {tiles_vrt_dir} after reprojection.")
        print(f"        The reproject script should have created these. Check the log above.")
        return 1

    # ------------------------------------------------------------------
    # Stage 2 — Generate contours
    # ------------------------------------------------------------------
    contours_output = tmp_dir / 'contours.gpkg'

    if not run_stage(
        contours_script,
        [str(tiles_vrt_dir), str(contours_output), '--workers', str(args.workers)],
        "2. Generate Contours from VRT Tiles"
    ):
        return 1

    # ------------------------------------------------------------------
    # Stage 3 — Generate hillshade
    # ------------------------------------------------------------------
    hillshade_output = tmp_dir / 'hillshade.tif'

    if not run_stage(
        hillshade_script,
        [str(tiles_vrt_dir), str(hillshade_output), '--workers', str(args.workers)],
        "3. Generate Hillshade from VRT Tiles"
    ):
        return 1

    # ------------------------------------------------------------------
    # Stage 4 — Clip to state/region boundary
    # ------------------------------------------------------------------
    clipped_contours  = output_dir / f'contours_{state_upper}.gpkg'
    clipped_hillshade = output_dir / f'hillshade_{state_upper}.tif'

    if not run_stage(
        clip_script,
        ['--state',         state_upper,
         '--gpkg-dir',      str(shape_files_dir),
         '--contours-in',   str(contours_output),
         '--contours-out',  str(clipped_contours),
         '--hillshade-in',  str(hillshade_output),
         '--hillshade-out', str(clipped_hillshade),
         '--keep-sources'],
        f"4. Clip to Region: {state_upper}"
    ):
        return 1

    # ------------------------------------------------------------------
    # Stage 5 — Export to MBTiles (optional)
    # ------------------------------------------------------------------
    if args.skip_export:
        print()
        print("=" * 70)
        print("STAGE: 5. Export to MBTiles — SKIPPED")
        print("=" * 70)
        print()
        print("[INFO] MBTiles export skipped (--skip-export)")
        print("[INFO] Clipped outputs saved to output/ and ready for future merge.")
        print("[INFO] When ready to export, run:")
        print(f"[INFO]   python3 scripts/export_mbtiles.py --output-dir {output_dir} --dest-dir <path>")
        print()
    else:
        if not run_stage(
            export_script,
            ['--output-dir', str(output_dir),
             '--dest-dir',   str(tileserver_dir)],
            "5. Export to MBTiles"
        ):
            return 1

    # ------------------------------------------------------------------
    # Post-run cleanup of intermediate directories (unless --skip-cleanup)
    # ------------------------------------------------------------------
    total_elapsed = time.time() - total_start

    print()
    print("=" * 70)
    print("[SUCCESS] DEM Processing Complete")
    print("=" * 70)
    print()
    print(f"Total time: {total_elapsed / 60:.1f} minutes ({total_elapsed / 3600:.1f} hours)")

    if not args.skip_cleanup:
        print()
        print("=" * 70)
        print("Post-Run Cleanup")
        print("=" * 70)
        print()

        # Clean raw_dem/ only if using the default input dir (not a custom --input-dir)
        if not args.input_dir and raw_dem_dir.exists():
            raw_tifs = list(raw_dem_dir.glob('*.tif'))
            if raw_tifs:
                try:
                    for f in raw_tifs:
                        f.unlink()
                    print(f"[OK] raw_dem/ cleared ({len(raw_tifs)} tiles removed)")
                except Exception as e:
                    print(f"[WARN] Could not fully clean raw_dem/: {e}")

        for d in [reprojected_dir, tiles_vrt_dir]:
            if d.exists():
                try:
                    shutil.rmtree(d)
                    d.mkdir()
                    print(f"[OK] {d.name}/ cleared")
                except Exception as e:
                    print(f"[WARN] Could not fully clean {d.name}/: {e}")

        print()

    # ------------------------------------------------------------------
    # Final output summary
    # ------------------------------------------------------------------
    print()
    print("Final outputs:")

    if clipped_contours.exists():
        size_mb = clipped_contours.stat().st_size / (1024 * 1024)
        print(f"  Clipped contours:   {clipped_contours} ({size_mb:.1f} MB)")
    else:
        print(f"  Clipped contours:   [NOT FOUND] {clipped_contours}")

    if clipped_hillshade.exists():
        size_mb = clipped_hillshade.stat().st_size / (1024 * 1024)
        print(f"  Clipped hillshade:  {clipped_hillshade} ({size_mb:.1f} MB)")
    else:
        print(f"  Clipped hillshade:  [NOT FOUND] {clipped_hillshade}")

    if not args.skip_export and tileserver_dir:
        contours_mbtiles  = tileserver_dir / 'contours.mbtiles'
        hillshade_mbtiles = tileserver_dir / 'hillshade.mbtiles'

        if contours_mbtiles.exists():
            size_mb = contours_mbtiles.stat().st_size / (1024 * 1024)
            print(f"  Contours MBTiles:   {contours_mbtiles} ({size_mb:.1f} MB)")
        else:
            print(f"  Contours MBTiles:   [NOT FOUND] {contours_mbtiles}")

        if hillshade_mbtiles.exists():
            size_mb = hillshade_mbtiles.stat().st_size / (1024 * 1024)
            print(f"  Hillshade MBTiles:  {hillshade_mbtiles} ({size_mb:.1f} MB)")
        else:
            print(f"  Hillshade MBTiles:  [NOT FOUND] {hillshade_mbtiles}")

        print()
        print(f"MBTiles written to: {tileserver_dir}")

    # ------------------------------------------------------------------
    # TileServer reload (optional — only if reload script exists)
    # ------------------------------------------------------------------
    if not args.skip_export and tileserver_dir:
        print()
        print("=" * 70)
        print("Final Stage: Refresh TileServer GL")
        print("=" * 70)
        print()

        reload_script = tileserver_dir / 'reload_tileserver.py'
        if reload_script.exists():
            print("Reloading TileServer GL (waiting up to 60 seconds)...")
            try:
                result = subprocess.run(
                    [sys.executable, str(reload_script)],
                    capture_output=True, text=True, timeout=60
                )
                if result.returncode == 0:
                    print("   ✓ TileServer reloaded successfully")
                else:
                    print(f"   ✗ TileServer reload failed: {result.stderr.strip()}")
                    print(f"   Manual reload: python3 {reload_script}")
            except subprocess.TimeoutExpired:
                print("   ✗ TileServer reload timed out after 60 seconds")
                print(f"   Manual reload: python3 {reload_script}")
            except Exception as e:
                print(f"   ✗ Could not reload TileServer: {e}")
                print(f"   Manual reload: python3 {reload_script}")
        else:
            print("[INFO] No reload_tileserver.py found in tileserver dir.")
            print("[INFO] Reload TileServer manually if required.")

    print()

    # ------------------------------------------------------------------
    # Next steps
    # ------------------------------------------------------------------
    print("Next steps:")
    if args.input_dir:
        print(f"  Add more DEM files to:  {Path(args.input_dir).resolve()}")
        print(f"  Run:  python3 {Path(__file__).name} --state XX --input-dir {args.input_dir} --tileserver-dir <path>")
    else:
        print(f"  Add DEM files to:  {raw_dem_dir}")
        print(f"  Run:  python3 {Path(__file__).name} --state XX --tileserver-dir <path>")
    print("  Outputs will be merged automatically on export.")
    print()

    if not args.no_log:
        print(f"[INFO] Log saved to: {log_file}")
        print(f"[INFO] Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    tmp_dir = BASE_DIR / 'tmp'
    exit_code = 1
    tee_output = None

    try:
        exit_code = main()
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] DEM processing cancelled by user")
        exit_code = 130  # Standard SIGINT exit code
    except Exception as e:
        print()
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        # Always clean tmp/ on exit (success, failure, or interrupt)
        # tmp/ holds large unclipped intermediates that are not needed after the run.
        # Note: --skip-cleanup does NOT protect tmp/ — that flag only covers
        # reprojected/ and tiles_vrt/ (the reusable intermediate stages).
        try:
            if tmp_dir.exists():
                print()
                print(f"[INFO] Cleaning temporary directory: {tmp_dir}")
                shutil.rmtree(tmp_dir)
                tmp_dir.mkdir(exist_ok=True)
                print("[OK] tmp/ reset")
        except Exception as e:
            print(f"[WARN] Could not clean tmp/: {e}")

        # Restore stdout/stderr and close log file
        if sys.stdout is not sys.__stdout__:
            tee = sys.stdout
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            try:
                tee.close()
            except Exception:
                pass

    sys.exit(exit_code)
