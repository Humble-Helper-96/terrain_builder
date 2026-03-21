#!/usr/bin/env python3
"""
Clip contours and hillshade to a state boundary

A buffer can be applied if needed (default: 0m).

Process:
  1) Load state boundary from GPKG
  2) Optionally apply additional buffer
  3) Clip contours.gpkg to boundary
  4) Clip hillshade.tif to boundary
  5) Clean near-zero elevation contours (offshore artifacts)

Outputs are saved with state suffix (e.g., contours_CT.gpkg, hillshade_CT.tif)
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path
from multiprocessing import cpu_count

# Import resource monitor
try:
    from resource_monitor import ResourceMonitor, print_resource_summary
except ImportError:
    ResourceMonitor = None
    print_resource_summary = None
    print("[WARNING] resource_monitor.py not found - resource monitoring disabled")


def run_command(cmd, desc):
    """Run a command with description and timing"""
    print(f"\n[INFO] {desc}")
    print(f"       Command: {' '.join(str(c) for c in cmd)}")
    
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start
    
    if result.returncode != 0:
        print(f"[ERROR] {desc} failed")
        print(f"        {result.stderr}")
        sys.exit(result.returncode)
    
    print(f"[SUCCESS] {desc} completed ({elapsed:.1f} seconds)")
    return result


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Clip contours and hillshade to state boundary with buffer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Clip to Connecticut
  python3 clip_to_state.py --state CT
  
  # Clip with additional 5km buffer beyond GPKG boundary
  python3 clip_to_state.py --state CT --buffer 5000
  
  # Clip with 1km buffer
  python3 clip_to_state.py --state CT --buffer 1000

Buffer distance:
  The --buffer option adds an ADDITIONAL buffer if needed.
  
  Default: 0
        """
    )
    
    parser.add_argument(
        '--state',
        required=True,
        help='State code (e.g., CT, NY, MA)'
    )
    
    parser.add_argument(
        '--gpkg-dir',
        default='shape_files',
        help='Directory containing state GPKG files (default: shape_files)'
    )
    
    parser.add_argument(
        '--contours-in',
        default='tmp/contours.gpkg',
        help='Input contours GeoPackage (default: tmp/contours.gpkg)'
    )
    
    parser.add_argument(
        '--contours-out',
        help='Output contours GeoPackage (default: output/contours_XX.gpkg)'
    )
    
    parser.add_argument(
        '--hillshade-in',
        default='tmp/hillshade.tif',
        help='Input hillshade GeoTIFF (default: tmp/hillshade.tif)'
    )
    
    parser.add_argument(
        '--hillshade-out',
        help='Output hillshade GeoTIFF (default: output/hillshade_XX.tif)'
    )
    
    parser.add_argument(
        '--buffer',
        type=float,
        default=0,
        help='Buffer distance in meters (default: 0)'
    )
    
    parser.add_argument(
        '--keep-sources',
        action='store_true',
        help='Keep source files after clipping'
    )
    
    args = parser.parse_args()
    
    # Start resource monitoring
    monitor = None
    if ResourceMonitor:
        monitor = ResourceMonitor()
        monitor.start()
    
    print()
    
    state = args.state.upper()
    
    # Setup paths
    gpkg_dir = Path(args.gpkg_dir)
    state_gpkg = gpkg_dir / f"{state}.gpkg"
    
    contours_in = Path(args.contours_in)
    hillshade_in = Path(args.hillshade_in)
    
    # Default output paths
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    # Project temp directory (matches terrain_builder/tmp)
    tmp_dir = Path('tmp')
    tmp_dir.mkdir(exist_ok=True)
    
    if args.contours_out:
        contours_out = Path(args.contours_out)
    else:
        contours_out = output_dir / f'contours_{state}.gpkg'
    
    if args.hillshade_out:
        hillshade_out = Path(args.hillshade_out)
    else:
        hillshade_out = output_dir / f'hillshade_{state}.tif'
    
    print(f"State:               {state}")
    print(f"State GPKG:          {state_gpkg}")
    print(f"Additional buffer:   {args.buffer:.0f} meters ({args.buffer/1000:.1f} km)")
    print()
    print(f"Input contours:      {contours_in}")
    print(f"Output contours:     {contours_out}")
    print()
    print(f"Input hillshade:     {hillshade_in}")
    print(f"Output hillshade:    {hillshade_out}")
    print()
    
    # Check inputs
    if not state_gpkg.exists():
        print(f"[ERROR] State GPKG not found: {state_gpkg}")
        return 1
    
    if not contours_in.exists():
        print(f"[ERROR] Contours not found: {contours_in}")
        return 1
    
    if not hillshade_in.exists():
        print(f"[ERROR] Hillshade not found: {hillshade_in}")
        return 1
    
    # Stage 1: Buffer state boundary (if additional buffer > 0)
    print()
    print("=" * 70)
    print("    Phase 1/4: Prepare Clipping Boundary")
    print("=" * 70)
    print()
    
    if args.buffer > 0:
        # Create buffered boundary from GPKG in project tmp directory
        buffered_gpkg = tmp_dir / f"{state}_buffered_{int(args.buffer)}m.gpkg"
        
        print(f"[INFO] Adding {args.buffer:.0f}m buffer")
        print(f"       Output: {buffered_gpkg}")
        
        cmd = [
            'ogr2ogr',
            '-f', 'GPKG',
            str(buffered_gpkg),
            str(state_gpkg),
            '-dialect', 'SQLite',
            '-sql', f'SELECT ST_Buffer(geom, {args.buffer}) as geom FROM "{state}"'
        ]
        
        run_command(cmd, f"Apply additional {args.buffer:.0f}m buffer")
        
        clip_boundary = buffered_gpkg
    else:
        print("[INFO] Using GPKG boundary")
        print("[INFO] No additional buffer applied")
        clip_boundary = state_gpkg
    
    # Stage 2: Clip contours
    print()
    print("=" * 70)
    print("    Phase 2/4: Clip Contours")
    print("=" * 70)
    print()
    
    cmd = [
        'ogr2ogr',
        '-f', 'GPKG',
        '-nlt', 'MULTILINESTRING',
        '-progress',
        '-overwrite',
        str(contours_out),
        str(contours_in),
        '-clipsrc', str(clip_boundary)
    ]
    
    run_command(cmd, f"Clip contours to {state}")
    
    # Stage 3: Clip hillshade
    print()
    print("=" * 70)
    print("    Phase 3/4: Clip Hillshade")
    print("=" * 70)
    print()
    
    # Use 75% of CPUs for gdalwarp threading
    total_cpus = cpu_count()
    threads = max(1, int(total_cpus * 0.75))
    
    print(f"[INFO] Using {threads} threads (75% of {total_cpus} CPUs)")
    
    cmd = [
        'gdalwarp',
        '--config', 'CHECK_DISK_FREE_SPACE', 'FALSE',
        '-overwrite',
        '-cutline', str(clip_boundary),
        '-crop_to_cutline',
        '-of', 'GTiff',
        '-co', 'COMPRESS=LZW',
        '-co', 'PREDICTOR=2',
        '-co', 'TILED=YES',
        '-co', 'BIGTIFF=YES',
        '-multi',
        '-wo', f'NUM_THREADS={threads}',
        str(hillshade_in),
        str(hillshade_out)
    ]
    
    run_command(cmd, f"Clip hillshade to {state}")
    
    # Stage 4: Clean artifacts
    print()
    print("=" * 70)
    print("    Phase 4/4: Clean Artifacts")
    print("=" * 70)
    print()
    
    print("[INFO] Removing near-zero elevation contours (-0.25m to +0.25m)")
    print("       These are offshore/lake artifacts from DEM processing")
    
    cmd = [
        'ogrinfo',
        str(contours_out),
        '-sql',
        'DELETE FROM contours WHERE elev_m BETWEEN -0.25 AND 0.25'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("[SUCCESS] Cleaned near-zero contours")
    else:
        print(f"[WARN] Cleanup had issues: {result.stderr}")
    
    # Stop resource monitoring and get stats
    resource_stats = None
    if monitor:
        resource_stats = monitor.stop()
    
    # ===== NEW COMPREHENSIVE SUMMARY SECTION =====
    print()
    print()
    print("=" * 70)
    print("    CLIPPING SUMMARY")
    print("=" * 70)
    print(f"State:                 {state}")
    print(f"Clipping boundary:     {clip_boundary.name}")
    if args.buffer > 0:
        print(f"Additional buffer:     {args.buffer:.0f}m ({args.buffer/1000:.1f}km)")
    else:
        print(f"Additional buffer:     None (used GPKG as-is)")
    print()
    
    # Get actual statistics from the contours output
    if contours_out.exists():
        size_mb = contours_out.stat().st_size / (1024 * 1024)
        
        # Count features in clipped contours
        cmd = ['ogrinfo', '-so', '-al', str(contours_out)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        feature_count = "Unknown"
        for line in result.stdout.split('\n'):
            if 'Feature Count:' in line:
                feature_count = line.split(':')[1].strip()
                break
        
        print(f"Contours output:")
        print(f"  - File:              {contours_out}")
        print(f"  - Size:              {size_mb:.1f} MB")
        print(f"  - Feature count:     {feature_count} contour lines")
        print()
    
    # Get actual statistics from the hillshade output
    if hillshade_out.exists():
        size_mb = hillshade_out.stat().st_size / (1024 * 1024)
        
        # Get dimensions and resolution
        cmd = ['gdalinfo', str(hillshade_out)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        width = height = resolution = compression = "Unknown"
        for line in result.stdout.split('\n'):
            if line.startswith('Size is'):
                parts = line.replace('Size is ', '').replace(',', '').split()
                if len(parts) >= 2:
                    width, height = parts[0], parts[1]
            elif 'Pixel Size =' in line:
                res = line.split('=')[1].strip().split(',')[0].replace('(', '')
                resolution = f"{abs(float(res)):.2f}m"
            elif 'COMPRESSION=' in line:
                compression = line.split('=')[1].strip()
        
        print(f"Hillshade output:")
        print(f"  - File:              {hillshade_out}")
        print(f"  - Size:              {size_mb:.1f} MB")
        print(f"  - Dimensions:        {width} x {height} pixels")
        print(f"  - Resolution:        {resolution}")
        print(f"  - Compression:       {compression}")
        print()
    
    # Threading info used
    print(f"Processing info:")
    print(f"  - CPUs available:    {total_cpus}")
    print(f"  - Threads used:      {threads} (75%)")
    print()
    
    # Cleanup info
    print(f"Artifacts removed:")
    print(f"  - Near-zero contours (-0.25m to +0.25m)")
    print()
    
    print("=" * 70)
    print()
    # ===== END NEW SUMMARY SECTION =====
    
    # Print resource usage summary
    if resource_stats and print_resource_summary:
        print_resource_summary(resource_stats, "State Clipping")
    
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] Clipping cancelled by user")
        sys.exit(1)
    except Exception as e:
        print()
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
