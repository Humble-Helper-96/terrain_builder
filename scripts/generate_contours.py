#!/usr/bin/env python3
"""
Generate contour lines from DEM tiles
Produces a single GeoPackage with elevation contours
"""

import subprocess
import sys
from pathlib import Path
import shutil
from multiprocessing import Pool, cpu_count
import tempfile

# Import resource monitor
try:
    from resource_monitor import ResourceMonitor, print_resource_summary
except ImportError:
    ResourceMonitor = None
    print_resource_summary = None
    print("[WARNING] resource_monitor.py not found - resource monitoring disabled")

def has_valid_data(tile_path):
    """Check if a tile has any valid (non-NoData) pixels"""
    try:
        # Use gdalinfo to check statistics
        cmd = ["gdalinfo", "-stats", str(tile_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        # Check for "STATISTICS_VALID_PERCENT=0" which means no valid data
        if "STATISTICS_VALID_PERCENT=0" in result.stdout:
            return False
        
        # Also check for explicit "no valid pixels" error
        if "no valid pixels found" in result.stderr.lower():
            return False
            
        return True
    except:
        # If we can't determine, assume it has data
        return True

def generate_contour_for_tile(args):
    """Generate contours for a single tile"""
    tile_path, temp_dir, interval_meters = args
    
    base_name = tile_path.stem
    output_gpkg = Path(temp_dir) / f"{base_name}.gpkg"
    
    # Check if tile has valid data first
    if not has_valid_data(tile_path):
        return (True, base_name, "skipped_no_data")
    
    try:
        cmd = [
            "gdal_contour",
            "-i", str(interval_meters),
            "-a", "elev_m",
            "-f", "GPKG",
            str(tile_path),
            str(output_gpkg),
            "-nln", "contours"
        ]
        
        # Set cache size for better performance (preserve existing environment)
        import os
        env = os.environ.copy()
        env["GDAL_CACHEMAX"] = "4096"
        
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        
        if result.returncode == 0:
            return (True, base_name, None)
        else:
            return (False, base_name, result.stderr)
            
    except Exception as e:
        return (False, base_name, str(e))

def generate_contours(tiles_dir, output_gpkg, interval_feet=40, simplify_meters=2, num_workers=None):
    """
    Generate contour lines from DEM tiles
    
    Args:
        tiles_dir: Directory containing DEM tiles
        output_gpkg: Output GeoPackage path
        interval_feet: Contour interval in feet (default: 40)
        simplify_meters: Simplification tolerance in meters (default: 2)
        num_workers: Number of parallel workers (default: 2)
    """
    # Start resource monitoring
    monitor = None
    if ResourceMonitor:
        monitor = ResourceMonitor(working_dir=tiles_dir)
        monitor.start()
    
    tiles_dir = Path(tiles_dir)
    output_gpkg = Path(output_gpkg)
    
    if not tiles_dir.is_dir():
        print(f"Error: {tiles_dir} is not a valid directory")
        return False
    
    # Find all TIF and VRT files
    tif_files = sorted(tiles_dir.glob("*.tif"))
    vrt_files = sorted(tiles_dir.glob("*.vrt"))
    
    # Combine both - prefer VRT if available, fallback to TIF
    input_files = vrt_files if vrt_files else tif_files
    
    if not input_files:
        print(f"Error: No .tif or .vrt files found in {tiles_dir}")
        return False
    
    file_type = "VRT" if vrt_files else "TIF"
    print(f"[INFO] Processing {len(input_files)} {file_type} files")
    print()
    
    total = len(input_files)
    
    # Convert feet to meters (1 foot = 0.3048 meters)
    interval_meters = interval_feet * 0.3048
    
    # Determine number of workers
    if num_workers is None:
        total_cpus = cpu_count()
        # Use 2 worker by default (safe for all systems)
        # Large 200km tiles with gdal_contour are memory intensive
        # Can be increased with --workers flag: 4 for 16GB, 8 for 32GB+ RAM
        num_workers = 2
    
    print("=" * 70)
    print("    Phase 1/1: Contour Generation")
    print("=" * 70)
    print(f"Input directory: {tiles_dir}")
    print(f"Output file: {output_gpkg}")
    print(f"Tiles to process: {total}")
    print(f"Contour interval: {interval_feet} feet ({interval_meters:.3f} meters)")
    print(f"Simplification: {simplify_meters} meters")
    print(f"Parallel workers: {num_workers}")
    print()
    
    # Create temporary directory for individual contours
    with tempfile.TemporaryDirectory(prefix="contours_") as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        print()
        
        # Stage 1: Generate contours for each tile
        print("Stage 1/4: Generating contours from tiles...")
        print("(Skipping tiles with no valid data)")
        print()
        
        args_list = [(tile, temp_dir, interval_meters) for tile in input_files]
        
        failed = []
        skipped = 0
        completed = 0
        
        with Pool(processes=num_workers) as pool:
            for success, filename, error in pool.imap_unordered(generate_contour_for_tile, args_list):
                completed += 1
                percent = (completed / total) * 100
                
                if success:
                    if error == "skipped_no_data":
                        status = "[SKIP]"
                        skipped += 1
                    else:
                        status = "[OK]"
                else:
                    status = "[ERROR]"
                    failed.append((filename, error))
                
                print(f"  [{completed}/{total}] {percent:.1f}% {status} {filename}", end='\r')
                
        print()  # Move to new line after progress is done
        
        print()
        print(f"[INFO] Skipped {skipped} empty tiles (no data)")
        print(f"[INFO] Processed {completed - skipped - len(failed)} tiles with data")
        print()
        
        if failed:
            print()
            print(f"[WARNING] {len(failed)} tiles failed:")
            for name, error in failed[:5]:
                print(f"  - {name}: {error[:100]}")
            return False
        
        # Stage 2: Merge all contours into single GeoPackage
        print("Stage 2/4: Merging contours into single GeoPackage...")
        
        # Find all generated gpkg files
        gpkg_files = sorted(Path(temp_dir).glob("*.gpkg"))
        
        if not gpkg_files:
            print("Error: No contour files were generated")
            print("This might mean all VRT tiles were empty (no valid data)")
            return False
        
        print(f"[INFO] Merging {len(gpkg_files)} contour files...")
        
        # Use ogrmerge.py to merge
        cmd = [
            "ogrmerge.py",
            "-single",
            "-o", str(output_gpkg),
            "-f", "GPKG",
            "-overwrite_ds",
            "-nln", "contours",
            "-lco", "SPATIAL_INDEX=YES",
            "-progress"
        ] + [str(f) for f in gpkg_files]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error merging contours: {result.stderr}")
            return False
        
        print("  [OK] Merged successfully")
        print()
        
        # Stage 3: Add elevation in feet (rounded to 20ft)
        print("Stage 3/4: Adding elevation attributes...")
        
        # Add elev_ft column
        cmd1 = [
            "ogrinfo",
            str(output_gpkg),
            "-sql", "ALTER TABLE contours ADD COLUMN elev_ft INTEGER"
        ]
        
        result = subprocess.run(cmd1, capture_output=True, text=True)
        
        if result.returncode != 0 and "already exists" not in result.stderr.lower():
            print(f"Warning: Could not add elev_ft column: {result.stderr}")
        
        # Update elev_ft column (convert meters to feet, round to nearest 20ft)
        cmd2 = [
            "ogrinfo",
            str(output_gpkg),
            "-sql", "UPDATE contours SET elev_ft = CAST(ROUND((elev_m * 3.28084) / 20.0) * 20 AS INTEGER)"
        ]
        
        result = subprocess.run(cmd2, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Warning: Could not update elev_ft: {result.stderr}")
        
        print("  [OK] Added elev_ft column (rounded to 20ft)")
        print()
        
    # Stage 4: Simplify geometry
    print("Stage 4/4: Simplifying geometry...")

    simplified_gpkg = output_gpkg.parent / f"{output_gpkg.stem}_simplified.gpkg"

    cmd = [
        "ogr2ogr",
        "-f", "GPKG",
        str(simplified_gpkg),
        str(output_gpkg),
        "-nln", "contours",
        "-simplify", str(simplify_meters),
        "-lco", "SPATIAL_INDEX=YES",
        "-progress"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Warning: Could not simplify geometry: {result.stderr}")
        print(f"  Using unsimplified version")
        simplified_gpkg = output_gpkg
    else:
        print(f"  [OK] Simplified (tolerance: {simplify_meters}m)")
        # Replace original with simplified
        shutil.move(str(simplified_gpkg), str(output_gpkg))

    print()

    # Stop resource monitoring and get stats
    resource_stats = None
    if monitor:
        resource_stats = monitor.stop()
    
    # ===== SUMMARY SECTION =====
    print("=" * 70)
    print("    CONTOUR GENERATION SUMMARY")
    print("=" * 70)
    print(f"Total tiles processed:    {total}")
    print(f"  - Successful:           {total - skipped - len(failed)}")
    print(f"  - Skipped (no data):    {skipped}")
    print(f"  - Failed:               {len(failed)}")
    print()
    print(f"Contour interval:         {interval_feet} feet ({interval_meters:.3f} meters)")
    print(f"Simplification:           {simplify_meters} meters")
    print(f"Parallel workers used:    {num_workers}")
    print()
    print(f"Output file:              {output_gpkg}")
    print(f"  - Layer name:           contours")
    print(f"  - Attributes:           elev_m, elev_ft")
    print(f"  - Spatial index:        YES")
    print()
    if failed:
        print("Failed tiles:")
        for name, error in failed[:10]:  # Show first 10
            print(f"  - {name}")
            if error:
                print(f"    Error: {error[:80]}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")
        print()
    print("=" * 70)
    print()
    
    # Print resource usage summary
    if resource_stats and print_resource_summary:
        print_resource_summary(resource_stats, "Contour Generation")
    
    print("=" * 70)
    print("    [OK] Contour generation complete!")
    print("=" * 70)
    print()
    print(f"Output: {output_gpkg}")
    print()
    print("Contour attributes:")
    print(f"  - elev_m: Elevation in meters (original)")
    print(f"  - elev_ft: Elevation in feet (rounded to 20ft intervals)")
    print()
    print("Major contours (200ft intervals): WHERE elev_ft % 200 = 0")
    print("Minor contours (40ft intervals): All others")
    print()
    
    return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate contour lines from DEM tiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (40ft contours)
  python3 generate_contours.py tiles/ contours.gpkg
  
  # 100ft contours
  python3 generate_contours.py tiles/ contours_100ft.gpkg --interval 100

Styling in QGIS:
  1. Load contours.gpkg
  2. Style by expression: elev_ft % 200 = 0
  3. Major contours (200ft): Thick, labeled
  4. Minor contours (40ft): Thin, unlabeled
        """
    )
    
    parser.add_argument('tiles_dir', help='Directory containing DEM tiles')
    parser.add_argument('output', help='Output GeoPackage path')
    parser.add_argument('--interval', '-i', type=int, default=40,
                       help='Contour interval in feet (default: 40)')
    parser.add_argument('--simplify', '-s', type=float, default=2.0,
                       help='Simplification tolerance in meters (default: 2)')
    parser.add_argument('--workers', '-w', type=int, default=None,
                       help='Number of parallel workers (default: auto)')
    
    args = parser.parse_args()
    
    success = generate_contours(
        args.tiles_dir,
        args.output,
        args.interval,
        args.simplify,
        args.workers
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
