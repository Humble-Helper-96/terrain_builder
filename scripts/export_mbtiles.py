#!/usr/bin/env python3
"""
Export MBTiles for GIS Tool Stack

Merges all clipped outputs and exports to MBTiles for TileServer GL.

Process:
  1) Scan output/ for all .tif and .gpkg files
  2) Build VRT mosaic from all .tif files --> tmp/hillshade_merged.vrt
  3) Merge all .gpkg files --> tmp/contours_merged.gpkg
  4) Export hillshade VRT to MBTiles --> ~/server/gis/tileserver/data/hillshade.mbtiles
  5) Export contours to MBTiles --> ~/server/gis/tileserver/data/contours.mbtiles

This allows incremental builds - as you add more clipped regions to output/,
they will be merged into the final MBTiles.
"""

import subprocess
import sys
from pathlib import Path
import argparse
import time
import tempfile
import shutil

# Import resource monitor
try:
    from resource_monitor import ResourceMonitor, print_resource_summary
except ImportError:
    ResourceMonitor = None
    print_resource_summary = None
    print("[WARNING] resource_monitor.py not found - resource monitoring disabled")


def run_command(cmd, desc):
    """Run a command with description and error handling"""
    print(f"\n[INFO] {desc}")
    print(f"       Command: {' '.join(str(c) for c in cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] {desc} failed")
        print(f"        {result.stderr}")
        sys.exit(result.returncode)
    
    print(f"[SUCCESS] {desc} completed")
    return result


def format_size(size_bytes):
    """Format bytes as human-readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Merge clipped outputs and export to MBTiles",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Directory containing clipped outputs (default: output)'
    )
    
    parser.add_argument(
        '--tmp-dir',
        type=str,
        default='tmp',
        help='Temporary directory for merged files (default: tmp)'
    )

    parser.add_argument(
        '--dest-dir',
        type=str,
        default=None,
        help='Destination directory for output MBTiles files (default: ~/server/gis/tileserver/data)'
    )
    
    args = parser.parse_args()
    
    # Start resource monitoring
    monitor = None
    if ResourceMonitor:
        monitor = ResourceMonitor()
        monitor.start()
    
    start_time = time.time()
    
    # Setup paths
    output_dir = Path(args.output_dir)
    tmp_dir = Path(args.tmp_dir)
    tmp_dir.mkdir(exist_ok=True)
    
    if args.dest_dir:
        tileserver_data = Path(args.dest_dir)
    else:
        tileserver_data = Path.home() / 'server' / 'gis' / 'tileserver' / 'data'
    tileserver_data.mkdir(parents=True, exist_ok=True)
    
    print(f"Output directory:     {output_dir}")
    print(f"Temp directory:       {tmp_dir}")
    print(f"TileServer data:      {tileserver_data}")
    print()
    
    # Check for outputs
    if not output_dir.exists():
        print(f"[ERROR] Output directory not found: {output_dir}")
        return 1
    
    tif_files = sorted(output_dir.glob('hillshade_*.tif'))
    gpkg_files = sorted(output_dir.glob('contours_*.gpkg'))
    
    print(f"[FOUND] {len(tif_files)} hillshade file(s)")
    print(f"[FOUND] {len(gpkg_files)} contours file(s)")
    print()
    
    if len(tif_files) == 0 and len(gpkg_files) == 0:
        print("[ERROR] No clipped outputs found in output/")
        print("        Run process_dem.py first to create clipped outputs")
        return 1
    
    # Show files
    if tif_files:
        print("Hillshade files:")
        for f in tif_files:
            size = format_size(f.stat().st_size)
            print(f"  - {f.name} ({size})")
        print()
    
    if gpkg_files:
        print("Contours files:")
        for f in gpkg_files:
            size = format_size(f.stat().st_size)
            print(f"  - {f.name} ({size})")
        print()
    
    # Initialize variables for summary
    vrt_path = None
    merged_gpkg = None
    hillshade_mbtiles = None
    contours_mbtiles = None
    
    # Stage 1: Build hillshade VRT
    if tif_files:
        print()
        print("=" * 70)
        print("    Phase 1/4: Build Hillshade VRT Mosaic")
        print("=" * 70)
        print()
        
        vrt_path = tmp_dir / 'hillshade_merged.vrt'
        
        cmd = [
            'gdalbuildvrt',
            '-resolution', 'highest',
            '-srcnodata', '0',
            '-vrtnodata', '0',
            str(vrt_path)
        ] + [str(f) for f in tif_files]
        
        run_command(cmd, "Build VRT from hillshade files")
        
        if vrt_path.exists():
            print(f"\n[SUCCESS] VRT created: {vrt_path}")
        print()
    
    # Stage 2: Merge contours
    if gpkg_files:
        print()
        print("=" * 70)
        print("    Phase 2/4: Merge Contours")
        print("=" * 70)
        print()
        
        merged_gpkg = tmp_dir / 'contours_merged.gpkg'
        
        # Use ogrmerge.py to merge all contours
        cmd = [
            'ogrmerge.py',
            '-single',
            '-o', str(merged_gpkg),
            '-f', 'GPKG',
            '-overwrite_ds',
            '-nln', 'contours',
            '-lco', 'SPATIAL_INDEX=YES'
        ] + [str(f) for f in gpkg_files]
        
        run_command(cmd, "Merge contours GeoPackages")
        
        if merged_gpkg.exists():
            size = format_size(merged_gpkg.stat().st_size)
            print(f"\n[SUCCESS] Merged contours: {merged_gpkg} ({size})")
        print()
    
    # Stage 3: Export hillshade to MBTiles
    if tif_files and vrt_path and vrt_path.exists():
        print()
        print("=" * 70)
        print("    Phase 3/4: Export Hillshade MBTiles")
        print("=" * 70)
        print()
        
        hillshade_mbtiles = tileserver_data / 'hillshade.mbtiles'
        
        # Convert VRT to MBTiles capped at z12
        # -co ZOOM_LEVEL_STRATEGY=LOWER ensures gdal_translate picks z12 as max
        # rather than auto-detecting a higher native zoom from pixel resolution
        cmd = [
            'gdal_translate',
            '-of', 'MBTILES',
            '-co', 'ZOOM_LEVEL_STRATEGY=LOWER',
            '-co', 'ZOOM_LEVEL=12',
            str(vrt_path),
            str(hillshade_mbtiles)
        ]
        
        run_command(cmd, "Convert VRT to MBTiles (max zoom 12)")
        
        # Add overviews to build down to z6
        # gdaladdo factors are relative to the max zoom tile size:
        # factor 2 = z11, 4 = z10, 8 = z9, 16 = z8, 32 = z7, 64 = z6
        print("\n[INFO] Adding overviews for zoom levels 6-11...")
        levels = [2, 4, 8, 16, 32, 64]
        cmd = [
            'gdaladdo',
            '-r', 'bilinear',
            str(hillshade_mbtiles)
        ] + [str(l) for l in levels]
        
        run_command(cmd, "Add overviews (z6-z11)")
        
        if hillshade_mbtiles.exists():
            size = format_size(hillshade_mbtiles.stat().st_size)
            print(f"\n[SUCCESS] Hillshade MBTiles: {hillshade_mbtiles} ({size})")
        print()
    
    # Stage 4: Export contours to MBTiles
    if gpkg_files and merged_gpkg and merged_gpkg.exists():
        print()
        print("=" * 70)
        print("    Phase 4/4: Export Contours MBTiles")
        print("=" * 70)
        print()
        
        contours_mbtiles = tileserver_data / 'contours.mbtiles'
        
        # Create temp directory for GeoJSONSeq files
        with tempfile.TemporaryDirectory(prefix="contours_export_") as temp_export:
            temp_export_path = Path(temp_export)
            
            major_jsonl = temp_export_path / 'contours_major.jsonl'
            minor_jsonl = temp_export_path / 'contours_minor.jsonl'
            
            # Split into major and minor contours
            print("[INFO] Splitting contours into major (200ft) and minor...")
            
            cmd_major = [
                'ogr2ogr',
                '-f', 'GeoJSONSeq',
                str(major_jsonl),
                str(merged_gpkg),
                '-dialect', 'SQLITE',
                '-sql', 'SELECT * FROM contours WHERE elev_ft % 200 = 0'
            ]
            
            run_command(cmd_major, "Export major contours (200ft)")
            
            cmd_minor = [
                'ogr2ogr',
                '-f', 'GeoJSONSeq',
                str(minor_jsonl),
                str(merged_gpkg),
                '-dialect', 'SQLITE',
                '-sql', 'SELECT * FROM contours WHERE elev_ft % 200 != 0'
            ]
            
            run_command(cmd_minor, "Export minor contours (40ft)")
            
            # Create MBTiles with tippecanoe
            print("\n[INFO] Creating vector MBTiles with tippecanoe...")
            
            cmd = [
                'tippecanoe',
                '-f',  # overwrite
                '-o', str(contours_mbtiles),
                '-Z', '8',   # min zoom
                '-z', '13',  # max zoom (z14 causes exponential tile growth on dense contour data)
                '-L', f'contours_major:{major_jsonl}',
                '-L', f'contours_minor:{minor_jsonl}',
                '--drop-densest-as-needed',
                '--maximum-tile-bytes', '500000'
            ]
            
            run_command(cmd, "Create vector MBTiles")
        
        if contours_mbtiles.exists():
            size = format_size(contours_mbtiles.stat().st_size)
            print(f"\n[SUCCESS] Contours MBTiles: {contours_mbtiles} ({size})")
        print()
    
    elapsed = time.time() - start_time
    elapsed_minutes = int(elapsed / 60)
    elapsed_seconds = int(elapsed % 60)
    
    # Stop resource monitoring and get stats
    resource_stats = None
    if monitor:
        resource_stats = monitor.stop()
    
    # ===== COMPREHENSIVE SUMMARY SECTION =====
    print()
    print()
    print("=" * 70)
    print("    MBTILES EXPORT SUMMARY")
    print("=" * 70)
    print()
    
    # Input statistics
    print("Input sources:")
    print(f"  - Hillshade files:   {len(tif_files)}")
    if tif_files:
        total_hillshade_bytes = sum(f.stat().st_size for f in tif_files)
        total_hillshade_mb = total_hillshade_bytes / (1024 * 1024)
        print(f"    Total size:        {total_hillshade_mb:.1f} MB")
    
    print(f"  - Contours files:    {len(gpkg_files)}")
    if gpkg_files:
        total_contours_bytes = sum(f.stat().st_size for f in gpkg_files)
        total_contours_mb = total_contours_bytes / (1024 * 1024)
        print(f"    Total size:        {total_contours_mb:.1f} MB")
    print()
    
    # Merged intermediate outputs
    print("Merged intermediates (in tmp/):")
    if tif_files and vrt_path and vrt_path.exists():
        # Get VRT info
        cmd = ['gdalinfo', str(vrt_path)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        width = height = "Unknown"
        for line in result.stdout.split('\n'):
            if line.startswith('Size is'):
                parts = line.replace('Size is ', '').replace(',', '').split()
                if len(parts) >= 2:
                    width, height = parts[0], parts[1]
                    break
        
        print(f"  - {vrt_path.name}")
        print(f"    Dimensions:        {width} x {height} pixels")
    
    if gpkg_files and merged_gpkg and merged_gpkg.exists():
        size_mb = merged_gpkg.stat().st_size / (1024 * 1024)
        
        # Get feature count
        cmd = ['ogrinfo', '-so', '-al', str(merged_gpkg)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        feature_count = "Unknown"
        for line in result.stdout.split('\n'):
            if 'Feature Count:' in line:
                feature_count = line.split(':')[1].strip()
                break
        
        print(f"  - {merged_gpkg.name}")
        print(f"    Size:              {size_mb:.1f} MB")
        print(f"    Features:          {feature_count} contour lines")
    print()
    
    # Final MBTiles outputs
    print("Final MBTiles outputs:")
    
    if tif_files and hillshade_mbtiles and hillshade_mbtiles.exists():
        size_mb = hillshade_mbtiles.stat().st_size / (1024 * 1024)
        
        print(f"  - {hillshade_mbtiles.name}")
        print(f"    Path:              {hillshade_mbtiles}")
        print(f"    Size:              {size_mb:.1f} MB")
        print(f"    Type:              Raster")
        print(f"    Overviews:         2, 4, 8, 16, 32, 64")
        print()
    
    if gpkg_files and contours_mbtiles and contours_mbtiles.exists():
        size_mb = contours_mbtiles.stat().st_size / (1024 * 1024)
        
        print(f"  - {contours_mbtiles.name}")
        print(f"    Path:              {contours_mbtiles}")
        print(f"    Size:              {size_mb:.1f} MB")
        print(f"    Type:              Vector")
        print(f"    Layers:            contours_major, contours_minor")
        print(f"    Zoom range:        8-14")
        print(f"    Major contours:    200ft intervals")
        print(f"    Minor contours:    40ft intervals")
        print()
    
    # Compression statistics
    if tif_files and hillshade_mbtiles and hillshade_mbtiles.exists():
        input_size = sum(f.stat().st_size for f in tif_files)
        output_size = hillshade_mbtiles.stat().st_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0
        
        print(f"Hillshade compression:")
        print(f"  - Input:             {format_size(input_size)}")
        print(f"  - Output:            {format_size(output_size)}")
        print(f"  - Reduction:         {ratio:.1f}%")
        print()
    
    if gpkg_files and contours_mbtiles and contours_mbtiles.exists():
        input_size = sum(f.stat().st_size for f in gpkg_files)
        output_size = contours_mbtiles.stat().st_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0
        
        print(f"Contours compression:")
        print(f"  - Input:             {format_size(input_size)}")
        print(f"  - Output:            {format_size(output_size)}")
        print(f"  - Reduction:         {ratio:.1f}%")
        print()
    
    # Processing time
    print(f"Processing time:")
    print(f"  - Total elapsed:     {elapsed_minutes}m {elapsed_seconds}s")
    print()
    
    print("=" * 70)
    print()
    # ===== END SUMMARY SECTION =====
    
    # Print resource usage summary
    if resource_stats and print_resource_summary:
        print_resource_summary(resource_stats, "MBTiles Export")
    
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] Export cancelled by user")
        sys.exit(1)
    except Exception as e:
        print()
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
