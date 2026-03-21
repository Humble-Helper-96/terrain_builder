#!/usr/bin/env python3
"""
Terrain Builder — DEM Processing Pipeline

Processes raw DEM tiles into contours, hillshade, and web-ready MBTiles.
All paths are relative to this script's directory, so the project is
fully self-contained and portable.

Workflow:
  1) Reproject raw DEM tiles    --> reprojected/ (may overlap)
  2) Build master VRT + create non-overlapping 100km×100km VRT tiles --> tiles_vrt/
  3) Generate contours from VRT tiles  --> tmp/contours.gpkg
  4) Generate hillshade from VRT tiles --> tmp/hillshade.tif
  5) Clip to state boundary     --> output/contours_XX.gpkg, hillshade_XX.tif
  6) Merge and export MBTiles   --> output/mbtiles/*.mbtiles
  7) Clean up intermediate files (reprojected/, tiles_vrt/, tmp/)

Directory structure:
  terrain_builder/
    process_dem.py        - This script (run from anywhere)
    raw_dem/              - Place your raw DEM .tif files here
    reprojected/          - Reprojected tiles (intermediate, may overlap)
      └── dem_mosaic.vrt  - Master VRT of all reprojected tiles
    tiles_vrt/            - Non-overlapping VRT tiles
    tmp/                  - Unclipped contours and hillshade (intermediate, auto-cleaned)
    output/               - Clipped outputs ready for merging (PERSISTENT)
      └── mbtiles/        - Final MBTiles for TileServer GL or other consumers
    scripts/              - Processing scripts
    shape_files/          - State/region boundary GeoPackages
    logs/                 - Processing logs

The output/ folder accumulates clipped regions. When you add more DEM data
and clip to another state, those outputs are added to output/ and can be
merged together into larger regional MBTiles.

NOTE: reprojected/ and tiles_vrt/ are cleaned AFTER each run to free
disk space. These are intermediate files that can be regenerated.
Pass --skip-cleanup to keep them (useful for debugging or re-runs).

VRT tiles in tiles_vrt/ have NO overlap,
which eliminates duplicate contours at tile boundaries!
"""

import subprocess
import sys
from pathlib import Path
import argparse
import time
import platform
import shutil
from datetime import datetime

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Resolve base directory from script location (not cwd) so it works
# regardless of where you invoke it from.
BASE_DIR = Path(__file__).resolve().parent


class TeeOutput:
    """Class to duplicate output to both terminal and log file"""
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
        
    def close(self):
        self.log.close()


def get_system_info():
    """Get system information for display"""
    info = {}
    
    if platform.system() == "Linux":
        try:
            os_info = {}
            with open("/etc/os-release", "r") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line:
                        key, value = line.split("=", 1)
                        os_info[key] = value.strip('"')
            
            if "PRETTY_NAME" in os_info:
                distro_name = os_info["PRETTY_NAME"]
            elif "NAME" in os_info:
                distro_name = os_info["NAME"]
            else:
                distro_name = f"Linux {platform.release()}"
            
            info['os'] = distro_name
        except FileNotFoundError:
            info['os'] = f"Linux {platform.release()}"
    else:
        info['os'] = f"{platform.system()} {platform.release()}"
    
    disk_usage = shutil.disk_usage(BASE_DIR)
    free_gb = disk_usage.free / (1024**3)
    total_gb = disk_usage.total / (1024**3)
    info['disk_free'] = f"{free_gb:.1f} GB free of {total_gb:.1f} GB"
    
    if HAS_PSUTIL:
        mem = psutil.virtual_memory()
        total_ram_gb = mem.total / (1024**3)
        available_ram_gb = mem.available / (1024**3)
        info['ram_total'] = f"{total_ram_gb:.1f} GB"
        info['ram_available'] = f"{available_ram_gb:.1f} GB ({mem.percent:.1f}% used)"
        
        cpu_cores = psutil.cpu_count(logical=False) or 0
        cpu_threads = psutil.cpu_count(logical=True) or 0
        info['cpu_cores'] = cpu_cores
        info['cpu_threads'] = cpu_threads
    else:
        import os
        info['ram_total'] = 'unknown (install psutil)'
        info['ram_available'] = 'unknown (install psutil)'
        info['cpu_cores'] = os.cpu_count() or 0
        info['cpu_threads'] = os.cpu_count() or 0
    
    return info


def print_header():
    """Print script header with system info"""
    print()
    print("=" * 70)
    print("Terrain Builder — DEM Processing Pipeline")
    print("=" * 70)
    print()
    
    info = get_system_info()
    print(f"Operating System:     {info['os']}")
    print(f"CPU Cores/Threads:    {info['cpu_cores']} / {info['cpu_threads']}")
    print(f"Available RAM:        {info['ram_available']}")
    print(f"Available Disk:       {info['disk_free']}")
    print()


def run_stage(script_path, args, stage_name):
    """Run a pipeline stage and return success status"""
    print()
    print("=" * 70)
    print(f"STAGE: {stage_name}")
    print("=" * 70)
    print()
    
    start_time = time.time()
    
    cmd = [sys.executable, str(script_path)] + args
    
    # Use Popen to capture output in real-time
    # cwd=BASE_DIR ensures sub-scripts resolve relative defaults correctly
    # even when the user invokes process_dem.py from another directory.
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Line buffered
        universal_newlines=True,
        cwd=str(BASE_DIR)
    )
    
    # Stream output line by line to both terminal and log
    for line in process.stdout:
        print(line, end='')  # This goes to TeeOutput (terminal + log)
        sys.stdout.flush()
    
    # Wait for process to complete
    return_code = process.wait()
    
    success = return_code == 0
    elapsed = time.time() - start_time
    
    print()
    if success:
        print(f"[SUCCESS] {stage_name} completed ({elapsed/60:.1f} minutes)")
    else:
        print(f"[FAIL] {stage_name} failed")
    print()
    
    return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Terrain Builder — Process raw DEM tiles into contours, hillshade, and MBTiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 process_dem.py --state CT
  python3 process_dem.py --state AK --target-srs EPSG:3338
  python3 process_dem.py --state CT --workers 4
  python3 process_dem.py --state AK --skip-cleanup
  python3 process_dem.py --state AK --tile-size 50
  python3 process_dem.py --state NV --workers 6 --skip-export
        """
    )
    
    parser.add_argument(
        '--state',
        required=True,
        help='State code for clipping (e.g., CT, NY, MA, AK)'
    )
    
    parser.add_argument(
        '--target-srs',
        default='EPSG:3857',
        help='Target projection code (default: EPSG:3857 - Web Mercator)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=2,
        help='Number of parallel workers (default: 2)'
    )
    
    parser.add_argument(
        '--no-log',
        action='store_true',
        help='Disable logging to file'
    )

    parser.add_argument(
        '--input-dir',
        help='Custom directory containing raw DEM .tif files instead of raw_dem/'
    )

    parser.add_argument(
        '--skip-cleanup',
        action='store_true',
        help='Skip post-run cleanup of reprojected/ and tiles_vrt/ (keep intermediate files)'
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
        help='Skip MBTiles export stage (run manually later with: python3 scripts/export_mbtiles.py --output-dir output --mbtiles-dir output/mbtiles)'
    )
    
    args = parser.parse_args()
    
    base_dir = BASE_DIR
    logs_dir = base_dir / 'logs'
    logs_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    state_upper = args.state.upper()
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
    
    raw_dem_dir = Path(args.input_dir).resolve() if args.input_dir else (base_dir / 'raw_dem')
    reprojected_dir = base_dir / 'reprojected'
    tmp_dir = base_dir / 'tmp'
    output_dir = base_dir / 'output'
    mbtiles_dir = output_dir / 'mbtiles'
    scripts_dir = base_dir / 'scripts'
    shape_files_dir = base_dir / 'shape_files'
    
    tmp_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    mbtiles_dir.mkdir(exist_ok=True)
    
    print(f"Working directory:    {base_dir}")
    print(f"Raw DEM:              {raw_dem_dir}")
    print(f"Reprojected:          {reprojected_dir}")
    print(f"Target projection:    {args.target_srs}")
    print(f"VRT tile size:        {args.tile_size}km × {args.tile_size}km")
    print(f"Temp outputs:         {tmp_dir}")
    print(f"Clipped outputs:      {output_dir}")
    print(f"MBTiles output:       {mbtiles_dir}")
    print(f"State:                {state_upper}")
    if args.skip_export:
        print(f"MBTiles export:       SKIPPED (--skip-export)")
    print()
    
    if not raw_dem_dir.exists():
        print(f"[ERROR] Raw DEM directory not found: {raw_dem_dir.resolve()}")
        print(f"        Please check the path and try again")
        return 1
    
    dem_count = len(list(raw_dem_dir.glob('*.tif')))
    print(f"[FOUND] {dem_count} DEM tile(s) in {raw_dem_dir}")
    print()
    
    if dem_count == 0:
        print(f"[ERROR] No .tif files found in {raw_dem_dir}")
        return 1
    
    tiles_vrt_dir = base_dir / 'tiles_vrt'

    if args.skip_cleanup:
        print()
        print("[INFO] Post-run cleanup disabled (--skip-cleanup)")
        print("[INFO] Intermediate files in reprojected/ and tiles_vrt/ will be kept")
        print()

    reprojected_dir.mkdir(exist_ok=True)
    tiles_vrt_dir.mkdir(exist_ok=True)
    
    print()
    
    reproject_script = scripts_dir / 'reproject_dem_tiles.py'
    contours_script = scripts_dir / 'generate_contours.py'
    hillshade_script = scripts_dir / 'generate_hillshade.py'
    clip_script = scripts_dir / 'clip_to_state.py'
    export_script = scripts_dir / 'export_mbtiles.py'
    
    missing_scripts = []
    for script in [reproject_script, contours_script, hillshade_script, clip_script, export_script]:
        if not script.exists():
            missing_scripts.append(script.name)
    
    if missing_scripts:
        print("[ERROR] Missing required scripts:")
        for script in missing_scripts:
            print(f"        - {scripts_dir / script}")
        return 1
    
    print("[FOUND] All required scripts")
    print()
    
    if not run_stage(
        reproject_script,
        ['--input-dir', str(raw_dem_dir),
         '--output-dir', str(reprojected_dir),
         '--tiles-dir', str(tiles_vrt_dir),
         '--target-srs', args.target_srs,
         '--tile-size', str(args.tile_size),
         '--workers', str(args.workers)],
        "1. Reproject DEM Tiles"
    ):
        return 1
    
    contours_output = tmp_dir / 'contours.gpkg'
    
    if not tiles_vrt_dir.exists() or not list(tiles_vrt_dir.glob('*.vrt')):
        print(f"[ERROR] VRT tiles not found in {tiles_vrt_dir}")
        print(f"        The reproject script should have created these.")
        return 1
    
    if not run_stage(
        contours_script,
        [str(tiles_vrt_dir), str(contours_output), '--workers', str(args.workers)],
        "2. Generate Contours from VRT Tiles"
    ):
        return 1
    
    hillshade_output = tmp_dir / 'hillshade.tif'
    if not run_stage(
        hillshade_script,
        [str(tiles_vrt_dir), str(hillshade_output), '--workers', str(args.workers)],
        "3. Generate Hillshade from VRT Tiles"
    ):
        return 1
    
    state = args.state.upper()
    
    state_gpkg = shape_files_dir / f"{state}.gpkg"
    if not state_gpkg.exists():
        print(f"[ERROR] State GPKG not found: {state_gpkg}")
        print(f"        Place {state}.gpkg in: {shape_files_dir}")
        return 1
    
    clipped_contours = output_dir / f'contours_{state}.gpkg'
    clipped_hillshade = output_dir / f'hillshade_{state}.tif'
    
    if not run_stage(
        clip_script,
        [
            '--state', state,
            '--gpkg-dir', str(shape_files_dir),
            '--contours-in', str(contours_output),
            '--contours-out', str(clipped_contours),
            '--hillshade-in', str(hillshade_output),
            '--hillshade-out', str(clipped_hillshade),
            '--keep-sources'
        ],
        f"4. Clip to State {state}"
    ):
        return 1
    
    if args.skip_export:
        print()
        print("=" * 70)
        print("STAGE: 5. Export to MBTiles — SKIPPED")
        print("=" * 70)
        print()
        print("[INFO] MBTiles export skipped (--skip-export flag set)")
        print("[INFO] Clipped outputs saved to output/ and ready for future merge.")
        print("[INFO] When ready to export, run:")
        print(f"[INFO]   python3 scripts/export_mbtiles.py --output-dir {output_dir} --mbtiles-dir {mbtiles_dir} --tmp-dir {tmp_dir}")
        print()
    else:
        if not run_stage(
            export_script,
            ['--output-dir', str(output_dir), '--mbtiles-dir', str(mbtiles_dir), '--tmp-dir', str(tmp_dir)],
            "5. Export to MBTiles"
        ):
            return 1
    
    total_elapsed = time.time() - total_start
    
    print()
    print("=" * 70)
    print("[SUCCESS] DEM Processing Complete")
    print("=" * 70)
    print()
    print(f"Total time: {total_elapsed/60:.1f} minutes ({total_elapsed/3600:.1f} hours)")
    # Post-run cleanup: remove intermediate directories now that processing is complete
    # NOTE: raw_dem/ is intentionally NOT cleaned — those are your source files.
    if not args.skip_cleanup:
        print()
        print("=" * 70)
        print("Post-Run Cleanup")
        print("=" * 70)
        print()

        # Clean reprojected/
        if reprojected_dir.exists():
            try:
                shutil.rmtree(reprojected_dir)
                reprojected_dir.mkdir()
                print(f"[SUCCESS] reprojected/ cleared")
            except Exception as e:
                print(f"[WARN] Could not fully clean reprojected/: {e}")

        # Clean tiles_vrt/
        if tiles_vrt_dir.exists():
            try:
                shutil.rmtree(tiles_vrt_dir)
                tiles_vrt_dir.mkdir()
                print(f"[SUCCESS] tiles_vrt/ cleared")
            except Exception as e:
                print(f"[WARN] Could not fully clean tiles_vrt/: {e}")

        print()

    print()
    print("Final outputs:")
    
    if clipped_contours.exists():
        size_mb = clipped_contours.stat().st_size / (1024 * 1024)
        print(f"  - Clipped contours:  {clipped_contours} ({size_mb:.1f} MB)")
    
    if clipped_hillshade.exists():
        size_mb = clipped_hillshade.stat().st_size / (1024 * 1024)
        print(f"  - Clipped hillshade: {clipped_hillshade} ({size_mb:.1f} MB)")
    
    if not args.skip_export:
        contours_mbtiles = mbtiles_dir / 'contours.mbtiles'
        hillshade_mbtiles = mbtiles_dir / 'hillshade.mbtiles'
        
        if contours_mbtiles.exists():
            size_mb = contours_mbtiles.stat().st_size / (1024 * 1024)
            print(f"  - Contours MBTiles:  {contours_mbtiles} ({size_mb:.1f} MB)")
        
        if hillshade_mbtiles.exists():
            size_mb = hillshade_mbtiles.stat().st_size / (1024 * 1024)
            print(f"  - Hillshade MBTiles: {hillshade_mbtiles} ({size_mb:.1f} MB)")
        
        print()
        print(f"MBTiles written to: {mbtiles_dir}")
    
    print()
    
    print("Next steps:")
    if args.input_dir:
        print(f"  - Add more DEM files to your input directory")
        print(f"  - Run: python3 process_dem.py --state XX --input-dir {args.input_dir}")
    else:
        print("  - Add DEM files to raw_dem/")
        print("  - Run: python3 process_dem.py --state XX")
    print("  - Outputs will be merged automatically")
    if not args.skip_export:
        print(f"  - Copy MBTiles from {mbtiles_dir} to your tile server's data directory")
    print()
    
    if not args.no_log:
        print(f"[INFO] Log saved to: {log_file}")
        print(f"[INFO] Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return 0


if __name__ == '__main__':
    tmp_dir = BASE_DIR / 'tmp'

    exit_code = 1
    tee_output = None

    try:
        exit_code = main()
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] DEM processing cancelled by user")
        exit_code = 1
    except Exception as e:
        print()
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        try:
            if tmp_dir.exists():
                print()
                print(f"[INFO] Cleaning temporary directory: {tmp_dir}")
                shutil.rmtree(tmp_dir)
                tmp_dir.mkdir(exist_ok=True)
                print("[OK] Temporary directory reset (tmp/ is now empty)")
        except Exception as e:
            print(f"[WARN] Could not fully clean tmp/: {e}")
        
        if sys.stdout != sys.__stdout__:
            if hasattr(sys.stdout, 'close'):
                sys.stdout.close()
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

    sys.exit(exit_code)
