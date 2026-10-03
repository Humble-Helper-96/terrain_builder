#!/usr/bin/env python3
"""
Generate hillshade with alpha channel from DEM tiles
Produces grayscale hillshade with transparency for better visualization
"""

import subprocess
import sys
from pathlib import Path
import tempfile
import os
from multiprocessing import Pool, cpu_count

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

def generate_hillshade_for_tile(args):
    """Generate hillshade with alpha channel for a single tile"""
    tile_path, output_dir, contrast_multiplier, shadow_base = args
    
    base_name = tile_path.stem
    output_file = Path(output_dir) / f"{base_name}_hs.tif"
    
    # Check if tile has valid data first
    if not has_valid_data(tile_path):
        return (True, base_name, "skipped_no_data")
    
    # Use /tmp for intermediate files
    temp_gray = f"/tmp/hs_{os.getpid()}_{base_name}_gray.tif"
    temp_alpha = f"/tmp/hs_{os.getpid()}_{base_name}_alpha.tif"
    
    try:
        # Step 1: Generate grayscale hillshade
        cmd_gray = [
            "gdaldem", "hillshade",
            "-multidirectional",
            "-z", "1",
            "-s", "1",
            "-compute_edges",
            "-of", "GTiff",
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            str(tile_path),
            temp_gray
        ]
        
        result = subprocess.run(cmd_gray, capture_output=True, text=True)
        if result.returncode != 0:
            return (False, base_name, f"Grayscale generation failed: {result.stderr[:100]}")
        
        # Step 2: Generate alpha channel
        # Formula: shadow_base - (A * contrast_multiplier)
        # Default: 225 - (A * 0.59)
        calc_formula = f"{shadow_base}-(A*{contrast_multiplier})"
        
        cmd_alpha = [
            "gdal_calc.py",
            "-A", temp_gray,
            "--outfile", temp_alpha,
            "--calc", calc_formula,
            "--type", "Byte",
            "--NoDataValue", "0",
            "--overwrite",
            "--quiet"
        ]
        
        result = subprocess.run(cmd_alpha, capture_output=True, text=True)
        if result.returncode != 0:
            return (False, base_name, f"Alpha generation failed: {result.stderr[:100]}")
        
        # Step 3: Merge gray and alpha into single file
        cmd_merge = [
            "gdal_merge.py",
            "-separate",
            "-o", str(output_file),
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            temp_gray,
            temp_alpha
        ]
        
        result = subprocess.run(cmd_merge, capture_output=True, text=True)
        if result.returncode != 0:
            return (False, base_name, f"Merge failed: {result.stderr[:100]}")
        
        # Step 4: Set color interpretation
        cmd_interp = [
            "gdal_edit.py",
            "-colorinterp_1", "gray",
            "-colorinterp_2", "alpha",
            str(output_file)
        ]
        
        subprocess.run(cmd_interp, capture_output=True)
        
        # Cleanup temp files
        for temp_file in [temp_gray, temp_alpha]:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        return (True, base_name, None)
        
    except Exception as e:
        # Cleanup on error
        for temp_file in [temp_gray, temp_alpha]:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        return (False, base_name, str(e))

def generate_hillshade(tiles_dir, output_tif, contrast_multiplier=0.59, shadow_base=225, num_workers=None):
    """
    Generate hillshade with alpha channel from DEM tiles
    
    Args:
        tiles_dir: Directory containing DEM tiles (should be blurred)
        output_tif: Output hillshade TIF path
        contrast_multiplier: Contrast multiplier (0.0-1.0, default: 0.59)
        shadow_base: Shadow darkness base (0-255, default: 225)
        num_workers: Number of parallel workers (default: 2)
    """
    # Start resource monitoring
    monitor = None
    if ResourceMonitor:
        monitor = ResourceMonitor(working_dir=tiles_dir)
        monitor.start()
    
    tiles_dir = Path(tiles_dir)
    output_tif = Path(output_tif)
    
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
    
    # Determine number of workers (hillshade is CPU intensive)
    if num_workers is None:
        total_cpus = cpu_count()
        # Use 1 worker by default (safe for all systems)
        # Each worker processes: gdaldem + gdal_calc + gdal_merge (memory intensive)
        # Can be increased with --workers flag: 4 for 16GB, 8 for 32GB+ RAM
        num_workers = 2
    
    print(f"Input directory: {tiles_dir}")
    print(f"Output file: {output_tif}")
    print(f"Tiles to process: {total}")
    print(f"Contrast multiplier: {contrast_multiplier}")
    print(f"Shadow base: {shadow_base}")
    print(f"  Formula: {shadow_base} - (gray * {contrast_multiplier})")
    print(f"Parallel workers: {num_workers}")
    print()
    
    # Create temporary directory for hillshade tiles
    with tempfile.TemporaryDirectory(prefix="hillshade_") as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        print()
        
        # Phase 1: Generate hillshade tiles with alpha
        print("=" * 70)
        print("    Phase 1/3: Tiled Hillshade Generation with Transparent Alpha")
        print("=" * 70)
        print("  (Creating grayscale + alpha channel for each tile)")
        print("  (Skipping tiles with no valid data)")
        print()
        
        args_list = [(tile, temp_dir, contrast_multiplier, shadow_base) for tile in input_files]
        
        failed = []
        skipped = 0
        completed = 0
        
        with Pool(processes=num_workers) as pool:
            for success, filename, error in pool.imap_unordered(generate_hillshade_for_tile, args_list):
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
                print(f"  - {name}")
                print(f"    {error}")
            return False
        
        # Phase 2: Build VRT mosaic
        print("=" * 70)
        print("    Phase 2/3: Build Hillshade VRT Mosaic")
        print("=" * 70)
        
        vrt_path = output_tif.parent / f"{output_tif.stem}.vrt"
        
        # Find all generated hillshade tiles
        hs_files = sorted(Path(temp_dir).glob("*_hs.tif"))
        
        if not hs_files:
            print("Error: No hillshade files were generated")
            print("This might mean all VRT tiles were empty (no valid data)")
            return False
        
        print(f"[INFO] Building VRT from {len(hs_files)} hillshade tiles...")
        
        cmd_vrt = [
            "gdalbuildvrt",
            "-resolution", "highest",
            "-srcnodata", "0",
            "-vrtnodata", "0",
            str(vrt_path)
        ] + [str(f) for f in hs_files]
        
        result = subprocess.run(cmd_vrt, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error building VRT: {result.stderr}")
            return False
        
        print(f"  [OK] VRT created: {vrt_path}")
        print()
        
        # Phase 3: Create final TIF
        print("=" * 70)
        print("    Phase 3/3: Build Single Complete Hillshade TIF File")
        print("=" * 70)
        print("  (This may take a while for large datasets)")
        
        cmd_translate = [
            "gdal_translate",
            str(vrt_path),
            str(output_tif),
            "-of", "GTiff",
            "-co", "COMPRESS=DEFLATE",
            "-co", "TILED=YES",
            "-co", "BIGTIFF=YES",
            "-co", "NUM_THREADS=ALL_CPUS"
        ]
        
        result = subprocess.run(cmd_translate, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error creating TIF: {result.stderr}")
            return False
        
        print(f"  [OK] Hillshade TIF created: {output_tif}")
        print()
    
        # Remove the temporary VRT now that the final TIF is created
        try:
            if vrt_path.exists():
                vrt_path.unlink()
                print(f"  [OK] Removed temporary VRT: {vrt_path}")
                print()
        except Exception as e:
            print(f"  Warning: could not remove temporary VRT {vrt_path}: {e}")
            print()

    # Stop resource monitoring and get stats
    resource_stats = None
    if monitor:
        resource_stats = monitor.stop()

    # ===== SUMMARY SECTION =====
    print("=" * 70)
    print("    HILLSHADE GENERATION SUMMARY")
    print("=" * 70)
    print(f"Total tiles processed:    {total}")
    print(f"  - Successful:           {total - skipped - len(failed)}")
    print(f"  - Skipped (no data):    {skipped}")
    print(f"  - Failed:               {len(failed)}")
    print()
    print(f"Contrast multiplier:      {contrast_multiplier}")
    print(f"Shadow base:              {shadow_base}")
    print(f"Alpha formula:            {shadow_base} - (gray * {contrast_multiplier})")
    print(f"Parallel workers used:    {num_workers}")
    print()
    print(f"Output file:              {output_tif}")
    print(f"  - Bands:                2 (grayscale + alpha)")
    print(f"  - Compression:          DEFLATE")
    print(f"  - Tiled:                YES")
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
        print_resource_summary(resource_stats, "Hillshade Generation")
    
    print("=" * 70)
    print("    [OK] Hillshade generation complete!")
    print("=" * 70)
    print()
    print(f"Output: {output_tif}")
    print(f"Temporary VRT (deleted): {vrt_path}")
    print()
    print("The hillshade has two bands:")
    print("  - Band 1: Grayscale hillshade")
    print("  - Band 2: Alpha channel (transparency)")
    print()
    print("Load in QGIS for best visualization!")
    print()
    
    return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate hillshade with alpha channel from DEM tiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (default settings)
  python3 generate_hillshade.py tiles/ hillshade.tif
  
  # Stronger contrast
  python3 generate_hillshade.py tiles/ hillshade.tif --contrast 0.8
  
  # Lighter shadows
  python3 generate_hillshade.py tiles/ hillshade.tif --shadow-base 150
  
  # Dramatic contrast
  python3 generate_hillshade.py tiles/ hillshade.tif --contrast 1.0 --shadow-base 255

Alpha Formula:
  alpha = shadow_base - (gray * contrast_multiplier)
  
  - contrast_multiplier (0.0-1.0):
    Lower = flatter, uniform look
    Higher = stronger relief, brighter highlights
    Default: 0.59 (balanced)
  
  - shadow_base (0-255):
    Lower = lighter shadows
    Higher = darker shadows
    Default: 225 (balanced)

Common presets:
  --contrast 0.59 --shadow-base 225  # Balanced (default)
  --contrast 0.4  --shadow-base 150  # Subtle, soft
  --contrast 1.0  --shadow-base 255  # Dramatic, high contrast
        """
    )
    
    parser.add_argument('tiles_dir', help='Directory containing DEM tiles (blurred)')
    parser.add_argument('output', help='Output hillshade TIF path')
    parser.add_argument('--contrast', '-c', type=float, default=0.59,
                       help='Contrast multiplier 0.0-1.0 (default: 0.59)')
    parser.add_argument('--shadow-base', '-s', type=int, default=225,
                       help='Shadow darkness base 0-255 (default: 225)')
    parser.add_argument('--workers', '-w', type=int, default=None,
                       help='Number of parallel workers (default: auto)')
    
    args = parser.parse_args()
    
    # Validate contrast
    if not 0.0 <= args.contrast <= 1.0:
        print("Error: Contrast must be between 0.0 and 1.0")
        sys.exit(1)
    
    # Validate shadow base
    if not 0 <= args.shadow_base <= 255:
        print("Error: Shadow base must be between 0 and 255")
        sys.exit(1)
    
    success = generate_hillshade(
        args.tiles_dir,
        args.output,
        args.contrast,
        args.shadow_base,
        args.workers
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
