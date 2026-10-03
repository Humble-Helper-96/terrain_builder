#!/usr/bin/env python3
"""
Reproject DEM tiles and create non-overlapping VRT tiles

This script implements a two-stage process:
  1) Reproject raw DEMs to target projection → reprojected/
  2) Build master VRT and create SMART non-overlapping 100km×100km tile VRTs → tiles_vrt/

SMART TILING: Only creates VRT tiles where source data actually exists,
eliminating empty tiles and improving processing efficiency.

The VRT tiles have NO overlap, eliminating duplicate contours at tile boundaries.
"""

import os
import sys
import subprocess
from pathlib import Path
import json
from multiprocessing import Pool, cpu_count
import time
import argparse
import math

# Import resource monitor
try:
    from resource_monitor import ResourceMonitor, print_resource_summary
except ImportError:
    ResourceMonitor = None
    print_resource_summary = None
    print("[WARNING] resource_monitor.py not found - resource monitoring disabled")

def run_command(cmd, description="", quiet=False):
    """Run a shell command and return output"""
    if description and not quiet:
        print(f"  {description}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        if not quiet:
            print(f"Error: {result.stderr}")
        return None
    return result.stdout

def get_projection(tif_path):
    """Get the projection of a GeoTIFF"""
    cmd = f'gdalinfo -json "{tif_path}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        try:
            info = json.loads(result.stdout)
            if 'coordinateSystem' in info and 'wkt' in info['coordinateSystem']:
                wkt = info['coordinateSystem']['wkt']
                # Extract the projection name
                if 'PROJCS[' in wkt:
                    name = wkt.split('PROJCS["')[1].split('"')[0]
                    return name
                elif 'GEOGCS[' in wkt:
                    name = wkt.split('GEOGCS["')[1].split('"')[0]
                    return name
        except:
            pass
    return "Unknown"

def get_vrt_bounds(vrt_path):
    """Get bounds of a VRT in the format: (xmin, ymin, xmax, ymax)"""
    cmd = f'gdalinfo -json "{vrt_path}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        return None
    
    try:
        info = json.loads(result.stdout)
        corners = info['cornerCoordinates']
        
        # Extract coordinates
        upper_left = corners['upperLeft']
        lower_right = corners['lowerRight']
        
        xmin = upper_left[0]
        ymax = upper_left[1]
        xmax = lower_right[0]
        ymin = lower_right[1]
        
        return (xmin, ymin, xmax, ymax)
    except:
        return None

def get_tif_bounds(tif_path):
    """Get bounds of a GeoTIFF in the format: (xmin, ymin, xmax, ymax)"""
    cmd = f'gdalinfo -json "{tif_path}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        return None
    
    try:
        info = json.loads(result.stdout)
        corners = info['cornerCoordinates']
        
        xmin = corners['upperLeft'][0]
        ymax = corners['upperLeft'][1]
        xmax = corners['lowerRight'][0]
        ymin = corners['lowerRight'][1]
        
        return (xmin, ymin, xmax, ymax)
    except:
        return None

def reproject_tile(args):
    """Reproject a single tile - designed for multiprocessing"""
    input_tif, output_tif, target_srs, threads_per_job = args
    
    # Skip if already processed
    if output_tif.exists():
        return (True, str(input_tif.name), "skipped")
    
    cmd = f'''gdalwarp \
        -t_srs {target_srs} \
        -r bilinear \
        -dstnodata -999 \
        -co COMPRESS=LZW \
        -co TILED=YES \
        -co BIGTIFF=YES \
        -co NUM_THREADS={threads_per_job} \
        -overwrite \
        -q \
        "{input_tif}" "{output_tif}"'''
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        return (True, str(input_tif.name), None)
    else:
        return (False, str(input_tif.name), result.stderr)

def create_smart_tile_grid(reprojected_dir, master_vrt_bounds, tile_size_km=100):
    """
    Create a grid of non-overlapping tiles ONLY where source data exists.
    
    This analyzes all reprojected TIF files to determine which grid cells
    contain data, then only creates VRT tiles for those cells.
    
    Args:
        reprojected_dir: Directory containing reprojected .tif files
        master_vrt_bounds: (xmin, ymin, xmax, ymax) of master VRT
        tile_size_km: Size of each tile in kilometers
        
    Returns:
        List of tuples: [(tile_name, xmin, ymin, xmax, ymax), ...]
    """
    xmin, ymin, xmax, ymax = master_vrt_bounds
    tile_size_m = tile_size_km * 1000  # Convert to meters
    
    # Calculate total grid dimensions
    nx_tiles_total = math.ceil((xmax - xmin) / tile_size_m)
    ny_tiles_total = math.ceil((ymax - ymin) / tile_size_m)
    total_possible = nx_tiles_total * ny_tiles_total
    
    print(f"  Full grid would be: {nx_tiles_total} x {ny_tiles_total} = {total_possible} tiles")
    print(f"  Tile size: {tile_size_km}km × {tile_size_km}km")
    print()
    
    # Track which grid cells have data
    grid_with_data = set()
    
    # Find all reprojected TIF files
    tif_files = sorted(reprojected_dir.glob("*.tif"))
    
    print(f"  Analyzing {len(tif_files)} reprojected tiles to find data coverage...")
    print()
    
    processed = 0
    for tif_file in tif_files:
        processed += 1
        if processed % 100 == 0 or processed == len(tif_files):
            print(f"  Progress: {processed}/{len(tif_files)} tiles analyzed", end='\r')
        
        # Get bounds of this source file
        src_bounds = get_tif_bounds(tif_file)
        if not src_bounds:
            continue
            
        src_xmin, src_ymin, src_xmax, src_ymax = src_bounds
        
        # Calculate which grid cells this source overlaps
        # Use floor for min and ceil for max to ensure full coverage
        ix_min = max(0, int(math.floor((src_xmin - xmin) / tile_size_m)))
        ix_max = min(nx_tiles_total - 1, int(math.floor((src_xmax - xmin) / tile_size_m)))
        iy_min = max(0, int(math.floor((src_ymin - ymin) / tile_size_m)))
        iy_max = min(ny_tiles_total - 1, int(math.floor((src_ymax - ymin) / tile_size_m)))
        
        # Mark all overlapping grid cells
        for ix in range(ix_min, ix_max + 1):
            for iy in range(iy_min, iy_max + 1):
                grid_with_data.add((ix, iy))
    
    print()  # New line after progress
    print()
    
    tiles_with_data = len(grid_with_data)
    percent_with_data = (tiles_with_data / total_possible) * 100
    
    print(f"  ✓ Smart analysis complete!")
    print(f"    - Total possible tiles:  {total_possible}")
    print(f"    - Tiles with data:       {tiles_with_data} ({percent_with_data:.1f}%)")
    print(f"    - Empty tiles avoided:   {total_possible - tiles_with_data} ({100 - percent_with_data:.1f}%)")
    print()
    
    # Create VRT tile definitions only for cells with data
    tiles = []
    for ix, iy in sorted(grid_with_data):
        tile_xmin = xmin + ix * tile_size_m
        tile_ymin = ymin + iy * tile_size_m
        tile_xmax = min(tile_xmin + tile_size_m, xmax)
        tile_ymax = min(tile_ymin + tile_size_m, ymax)
        
        tile_name = f"tile_{ix:04d}_{iy:04d}"
        tiles.append((tile_name, tile_xmin, tile_ymin, tile_xmax, tile_ymax))
    
    return tiles

def create_tile_vrt(args):
    """Create a single non-overlapping tile VRT"""
    tile_name, xmin, ymin, xmax, ymax, master_vrt, output_dir = args
    
    output_vrt = output_dir / f"{tile_name}.vrt"
    
    # Use gdalbuildvrt with -te (target extent) to clip to exact bounds
    cmd = [
        'gdalbuildvrt',
        '-te', str(xmin), str(ymin), str(xmax), str(ymax),
        str(output_vrt),
        str(master_vrt)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        return (True, tile_name, None)
    else:
        return (False, tile_name, result.stderr)

def main():
    parser = argparse.ArgumentParser(
        description='Reproject DEM tiles and create SMART non-overlapping VRT tiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default settings (raw_dem → reprojected → tiles_vrt, EPSG:3857)
  python3 reproject_dem_tiles.py
  
  # Reproject to different projection
  python3 reproject_dem_tiles.py --target-srs EPSG:3338
  
  # Custom tile size (default: 100km)
  python3 reproject_dem_tiles.py --tile-size 50
  
  # Custom input/output directories
  python3 reproject_dem_tiles.py --input-dir raw_dem --output-dir reprojected
  
  # Custom number of workers
  python3 reproject_dem_tiles.py --workers 4

SMART TILING:
  This script now uses intelligent tile creation that analyzes your
  reprojected data to determine which grid cells contain actual data.
  
  It only creates VRT tiles where data exists, avoiding the creation
  of hundreds of empty tiles that would waste processing time during
  contour and hillshade generation.
  
  For sparse datasets like Alaska IFSAR, this can reduce tile count
  by 40-50% and significantly improve memory efficiency.

Process:
  Stage 1: Reproject raw DEMs (these may overlap - that's OK)
    raw_dem/*.tif → reprojected/*.tif
  
  Stage 2: Build master VRT from all reprojected DEMs
    reprojected/*.tif → reprojected/dem_mosaic.vrt
  
  Stage 3: Analyze data coverage and create smart VRT tiles
    - Scan all reprojected tiles to find data coverage
    - Create 100km×100km tile VRTs ONLY where data exists
    - Result: tiles_vrt/tile_XXXX_YYYY.vrt (no empty tiles!)
  
  The contour/hillshade scripts then process these VRTs, eliminating
  both duplicates at tile boundaries AND empty tile processing.
        """
    )
    
    parser.add_argument('--input-dir', default='raw_dem',
                       help='Directory containing raw DEM .tif files (default: raw_dem)')
    parser.add_argument('--output-dir', default='reprojected',
                       help='Output directory for reprojected tiles (default: reprojected)')
    parser.add_argument('--tiles-dir', default='tiles_vrt',
                       help='Output directory for VRT tiles (default: tiles_vrt)')
    parser.add_argument('--target-srs', default='EPSG:3857',
                       help='Target projection code (default: EPSG:3857 - Web Mercator)')
    parser.add_argument('--tile-size', type=int, default=100,
                       help='Tile size in kilometers (default: 100)')
    parser.add_argument('--workers', '-w', type=int, default=None,
                       help='Number of parallel workers (default: auto)')
    
    args = parser.parse_args()
    
    INPUT_DIR = Path(args.input_dir)
    OUTPUT_DIR = Path(args.output_dir)
    TILES_DIR = Path(args.tiles_dir)
    TARGET_SRS = args.target_srs
    TILE_SIZE_KM = args.tile_size
    
    # CPU configuration
    total_cpus = cpu_count()
    
    if args.workers is None:
        # Use 75% of CPUs for parallel jobs
        num_parallel_jobs = max(1, int(total_cpus * 0.75))
    else:
        num_parallel_jobs = args.workers
    
    # Each job gets remaining threads
    threads_per_job = max(1, total_cpus // num_parallel_jobs)
    
    print("=" * 70)
    print("   Phase 1/4: DEM Tile Reprojection + SMART VRT Tiling")
    print("=" * 70)
    print(f"Input directory:     {INPUT_DIR}")
    print(f"Output directory:    {OUTPUT_DIR}")
    print(f"VRT tiles directory: {TILES_DIR}")
    print(f"Target SRS:          {TARGET_SRS}")
    print(f"Tile size:           {TILE_SIZE_KM}km × {TILE_SIZE_KM}km")
    print()
    print(f"CPU Configuration:")
    print(f"  Total CPUs:        {total_cpus}")
    print(f"  Parallel jobs:     {num_parallel_jobs}")
    print(f"  Threads per job:   {threads_per_job}")
    print()
    
    # Check if input directory exists
    if not INPUT_DIR.exists():
        print(f"Error: Input directory not found: {INPUT_DIR.absolute()}")
        print(f"Make sure you pass the correct --input-dir path")
        sys.exit(1)
    
    # Create output directories
    OUTPUT_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    print(f"[OK] Output directories ready")
    
    # Start resource monitoring
    monitor = None
    if ResourceMonitor:
        monitor = ResourceMonitor(working_dir=OUTPUT_DIR)
        monitor.start()
    
    # Find all TIFF files
    tif_files = sorted(INPUT_DIR.glob("*.tif"))
    if not tif_files:
        print(f"Error: No .tif files found in {INPUT_DIR.absolute()}")
        sys.exit(1)
    
    print(f"[OK] Found {len(tif_files)} TIFF files")
    print()
    
    # =========================================================================
    # STAGE 1:REPROJECT RAW DEMS
    # =========================================================================
    print("=" * 70)
    print("    Phase 2/4: Reproject Raw DEMs")
    print("=" * 70)
    print()
    
    # Check projections
    print("Checking projections (first 5 files)...")
    projections = {}
    for tif in tif_files[:5]:
        proj = get_projection(tif)
        projections[proj] = projections.get(proj, 0) + 1
        print(f"  {tif.name}: {proj}")
    print()
    
    if len(projections) > 1:
        print(f"! Mixed projections detected: {list(projections.keys())}")
        print(f"  All tiles will be reprojected to {TARGET_SRS}")
    print()
    
    # Check how many need processing
    to_process = [tif for tif in tif_files if not (OUTPUT_DIR / tif.name).exists()]
    already_done = len(tif_files) - len(to_process)
    
    if already_done > 0:
        print(f"[OK] {already_done} tiles already reprojected (will skip)")
    
    if len(to_process) == 0:
        print("[OK] All tiles already reprojected!")
        print()
    else:
        print(f"Reprojecting {len(to_process)} tiles...")
        print("(Using bilinear resampling for elevation data)")
        print()
        
        # Prepare arguments for parallel processing
        args_list = [
            (tif, OUTPUT_DIR / tif.name, TARGET_SRS, threads_per_job)
            for tif in to_process
        ]
        
        # Process tiles in parallel
        failed = []
        completed = 0
        total = len(to_process)
        
        start_time = time.time()
        
        with Pool(processes=num_parallel_jobs) as pool:
            for success, filename, error in pool.imap_unordered(reproject_tile, args_list):
                completed += 1
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (total - completed) / rate if rate > 0 else 0
                
                # Print progress
                percent = (completed / total) * 100
                print(f"  [{completed}/{total}] {percent:.1f}% | "
                      f"{rate*60:.1f} tiles/min | "
                      f"ETA: {int(remaining/60)}m {int(remaining%60)}s | "
                      f"{filename}", end='\r')
                
                if not success and error != "skipped":
                    failed.append((filename, error))
        
        print()  # New line after progress
        print()
        
        if failed:
            print(f"! Failed to reproject {len(failed)} files:")
            for filename, error in failed[:10]:
                print(f"  - {filename}")
                if error:
                    print(f"    Error: {error[:100]}")
            if len(failed) > 10:
                print(f"  ... and {len(failed) - 10} more")
            print()
            sys.exit(1)
        else:
            print("[OK] All tiles reprojected successfully!")
        print()
    
    # =========================================================================
    # STAGE 2: BUILD MASTER VRT
    # =========================================================================
    print("=" * 70)
    print("    Phase 3/4: Build Master VRT")
    print("=" * 70)
    print()
    
    master_vrt = OUTPUT_DIR / "dem_mosaic.vrt"
    
    print("Building VRT from all reprojected tiles...")
    
    cmd = f'gdalbuildvrt -overwrite "{master_vrt}" "{OUTPUT_DIR}"/*.tif'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error creating master VRT: {result.stderr}")
        sys.exit(1)
    
    print(f"[OK] Master VRT created: {master_vrt}")
    print()
    
    # Get VRT bounds
    bounds = get_vrt_bounds(master_vrt)
    if bounds is None:
        print("Error: Could not extract bounds from master VRT")
        sys.exit(1)
    
    xmin, ymin, xmax, ymax = bounds
    width = xmax - xmin
    height = ymax - ymin
    
    print("Master VRT coverage:")
    print(f"  Bounds: ({xmin:.2f}, {ymin:.2f}) to ({xmax:.2f}, {ymax:.2f})")
    print(f"  Width:  {width/1000:.1f} km")
    print(f"  Height: {height/1000:.1f} km")
    print()
    
    # =========================================================================
    # STAGE 3: CREATE SMART NON-OVERLAPPING VRT TILES
    # =========================================================================
    print("=" * 70)
    print("    Phase 4/4: Create SMART Non-Overlapping VRT Tiles")
    print("=" * 70)
    print()
    
    print(f"Using SMART tiling to avoid creating empty tiles.")
    print(f"This analyzes your reprojected data to determine which grid cells")
    print(f"actually contain data, then only creates VRT tiles for those cells.")
    print()
    
    # Create smart tile grid (only where data exists)
    tiles = create_smart_tile_grid(OUTPUT_DIR, bounds, TILE_SIZE_KM)
    
    print(f"Generating {len(tiles)} VRT tiles (only where data exists)...")
    print()
    
    # Prepare arguments for parallel processing
    args_list = [
        (tile_name, xmin, ymin, xmax, ymax, master_vrt, TILES_DIR)
        for tile_name, xmin, ymin, xmax, ymax in tiles
    ]
    
    # Create VRT tiles in parallel
    failed = []
    completed = 0
    total = len(tiles)
    
    start_time = time.time()
    
    with Pool(processes=num_parallel_jobs) as pool:
        for success, tile_name, error in pool.imap_unordered(create_tile_vrt, args_list):
            completed += 1
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            remaining = (total - completed) / rate if rate > 0 else 0
            
            # Print progress
            percent = (completed / total) * 100
            print(f"  [{completed}/{total}] {percent:.1f}% | "
                  f"{rate:.1f} tiles/sec | "
                  f"ETA: {int(remaining)}s | "
                  f"{tile_name}", end='\r')
            
            if not success:
                failed.append((tile_name, error))
    
    print()  # New line after progress
    print()
    
    if failed:
        print(f"! Failed to create {len(failed)} VRT tiles:")
        for tile_name, error in failed[:10]:
            print(f"  - {tile_name}")
            if error:
                print(f"    Error: {error[:100]}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
        print()
        sys.exit(1)
    else:
        print("[OK] All VRT tiles created successfully!")
    print()
    
    # Stop resource monitoring and get stats
    resource_stats = None
    if monitor:
        resource_stats = monitor.stop()
    
    print("=" * 70)
    print("   [SUCCESS] Reprojection + SMART VRT Tiling Complete")
    print("=" * 70)
    print()
    print("Outputs:")
    print(f"  - Reprojected tiles: {OUTPUT_DIR}/ ({len(tif_files)} files)")
    print(f"  - Master VRT:        {master_vrt}")
    print(f"  - VRT tiles:         {TILES_DIR}/ ({len(tiles)} files)")
    print()
    print("SMART tiling benefits:")
    print(f"  - Only tiles with data were created")
    print(f"  - Empty tiles were automatically excluded")
    print(f"  - Contour/hillshade processing will be more efficient")
    print()
    
    # Print resource usage summary
    if resource_stats and print_resource_summary:
        print_resource_summary(resource_stats, "DEM Reprojection & Tiling")
    
    print("Next steps:")
    print(f"   Generate contours and hillshade")
    print()

if __name__ == "__main__":
    main()
