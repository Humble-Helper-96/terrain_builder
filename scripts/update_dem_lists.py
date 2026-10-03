#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 echos6
#
# Part of the terrain_builder pipeline — see HOW_TO_USE.md for full documentation.
"""
Update USGS DEM download lists with the latest available tile URLs  (v1.1.0)

Reads each state's USGS_DL_Lists/<STATE>_data.txt, queries the USGS S3 bucket
for the most current 1/3 arc-second GeoTIFF at each tile's grid cell, and
rewrites the download list if any tiles have been updated.

For each state where updates are found:
  - Original file is backed up to USGS_DL_Lists/backup/<STATE>_data.txt.bkup
    (overwrites any existing backup)
  - New file is written with updated URLs
  - States with no changes are left completely untouched

A summary report is printed at the end listing every updated tile.

Why S3 listing instead of the TNM products API:
  The TNM API (tnmaccess.nationalmap.gov/api/v1/products) returns a mix of all
  resolutions (1 arc-second, 1/3 arc-second, etc.) with no working filter for
  resolution -- the 'datasets' field is always empty in API responses. The S3
  bucket is the actual file store; listing it directly gives an authoritative,
  resolution-specific result for each cell with no ambiguity.

S3 bucket: prd-tnm.s3.amazonaws.com  (public, no authentication required)

current/ vs historical/ -- USGS keeps each 1/3 arc-second cell under two
sibling prefixes:

  StagedProducts/Elevation/13/TIFF/current/<CELL>/USGS_13_<CELL>.tif
      The live product for the cell. UNDATED filename.
  StagedProducts/Elevation/13/TIFF/historical/<CELL>/USGS_13_<CELL>_<DATE>.tif
      Superseded versions. DATED filename (YYYYMMDD).

When USGS publishes a new version of a cell, the old file moves into
historical/ and the new one takes its place in current/. A dated
historical/ URL can therefore stop resolving (404) at any time once that
version is promoted to current/.

Lookup order per cell: current/ first (if a tile is there it is the live
product, return it), otherwise the newest dated tile in historical/.

No-regression rule -- a list entry is replaced only when, in order:
  1. candidate URL == existing URL          -> keep
  2. candidate is in current/               -> update (live product wins)
  3. existing is current/, candidate is not -> keep (never downgrade)
  4. both historical/, candidate date newer -> update
  5. otherwise                              -> keep
(YYYYMMDD compares correctly as a plain string.)

(v1.0.0 only looked in historical/ and treated any difference as an update,
so it could rewrite a good list backwards. If you ran v1.0.0, compare your
lists against USGS_DL_Lists/backup/<STATE>_data.txt.bkup.)

--verify HEAD-requests every URL that was NOT updated and reports those that
fail to resolve (e.g. a tile withdrawn upstream with no replacement in either
prefix). These are the URLs that will 404 during wget.

Run time: each tile costs up to two S3 listing requests (current/, then
historical/), plus one HEAD request with --verify. At the default 0.5s delay
expect roughly 2x the v1.0.0 estimate: ~2500 tiles takes about 40-50 minutes
(more with --verify).

Tile URL formats parsed by this script:
  https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/
      current/<CELL>/USGS_13_<CELL>.tif
      historical/<CELL>/USGS_13_<CELL>_<DATE>.tif
  where <CELL> is e.g. n41w104 (1-degree grid cell, NW corner)

Usage:
  # Check and update all states
  python3 scripts/update_dem_lists.py

  # Check and update a single state
  python3 scripts/update_dem_lists.py --state WY

  # Dry run -- report what would change without writing any files
  python3 scripts/update_dem_lists.py --dry-run

  # Also HEAD-check every URL that was not updated
  python3 scripts/update_dem_lists.py --verify

  # Increase request delay (seconds) to be more polite to USGS servers
  python3 scripts/update_dem_lists.py --delay 1.0

System dependencies:
  Python 3 standard library only (urllib, xml, pathlib, re, time, argparse)
  No third-party packages required.
"""

import argparse
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

VERSION = "1.1.0"

# ---------------------------------------------------------------------------
# Base directory -- resolved from script location
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # terrain_builder/

# USGS S3 bucket -- public, no authentication required.
TNM_S3_BASE   = "https://prd-tnm.s3.amazonaws.com"
TNM_S3_PREFIX = "StagedProducts/Elevation/13/TIFF"   # + /current|historical/<CELL>/

# S3 XML namespace used in ListBucketResult responses
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"

# Polite delay between requests (seconds)
DEFAULT_DELAY = 0.5

USER_AGENT = "terrain_builder/" + VERSION

# Matches both URL shapes. Groups: 1=variant, 2=cell, 3=date (None if undated)
URL_PATTERN = re.compile(
    r"https://prd-tnm\.s3\.amazonaws\.com/StagedProducts/Elevation/13/TIFF/"
    r"(current|historical)/([ns]\d+[ew]\d+)/USGS_13_\2(?:_(\d{8}))?\.tif\Z",
    re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt_date(date_str):
    """Render a possibly-None tile date for display."""
    return date_str if date_str else "current"


def should_update(old_url, new_url):
    """
    Decide whether a list entry should be replaced by a candidate URL.
    Returns (update: bool, reason: str). See the no-regression rule in the
    module docstring.
    """
    if new_url.lower() == old_url.lower():
        return False, "identical"

    new_m = URL_PATTERN.match(new_url)
    old_m = URL_PATTERN.match(old_url)
    if not new_m:
        return False, "unrecognised candidate"

    new_var = new_m.group(1).lower()
    if new_var == "current":
        return True, "live product in current/"

    if old_m and old_m.group(1).lower() == "current":
        return False, "would downgrade current/ to historical/"

    if old_m and old_m.group(3) and new_m.group(3) and new_m.group(3) > old_m.group(3):
        return True, "newer historical tile"

    return False, "not newer"


def _head_ok(url):
    """HEAD-request a URL. Returns (ok, detail)."""
    try:
        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return 200 <= resp.status < 300, str(resp.status)
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except (urllib.error.URLError, OSError) as e:
        return False, f"error: {getattr(e, 'reason', e)}"


# ---------------------------------------------------------------------------
# S3 bucket listing query
# ---------------------------------------------------------------------------

def _list_keys(cell_id, variant):
    """
    List .tif keys under <prefix>/<variant>/<cell>/. Raises on HTTP/XML error.
    """
    params = {
        "list-type": "2",
        "prefix":    f"{TNM_S3_PREFIX}/{variant}/{cell_id.lower()}/",
        "max-keys":  "50",
    }
    list_url = f"{TNM_S3_BASE}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(list_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as response:
        xml_data = response.read().decode("utf-8")
    root = ET.fromstring(xml_data)

    keys = []
    for content in root.findall(f"{{{S3_NS}}}Contents"):
        key_el = content.find(f"{{{S3_NS}}}Key")
        key = (key_el.text or "") if key_el is not None else ""
        if key.lower().endswith(".tif"):
            keys.append(key)
    return keys


def query_latest_tile(cell_id, delay=DEFAULT_DELAY):
    """
    Find the best 1/3 arc-second GeoTIFF for a 1-degree grid cell.

    Probes current/ first (the live product); falls back to the newest dated
    tile in historical/.

    Returns:
        (url, date_str_or_None, variant) with variant 'current' or
        'historical'. date_str is None for current/ tiles (undated).
        Returns None on any HTTP/XML error or if neither prefix has a tile,
        so the caller keeps the existing URL.
    """
    cell = re.escape(cell_id.lower())
    tile_re = re.compile(rf"USGS_13_{cell}(?:_(\d{{8}}))?\.tif$", re.IGNORECASE)

    try:
        # --- current/ ---
        try:
            matches = [(k, tile_re.search(k)) for k in _list_keys(cell_id, "current")]
        finally:
            time.sleep(delay)
        matches = [(k, m) for k, m in matches if m]
        if matches:
            # Prefer the undated canonical name
            matches.sort(key=lambda km: km[1].group(1) is not None)
            key, m = matches[0]
            return f"{TNM_S3_BASE}/{key}", m.group(1), "current"

        # --- historical/ ---
        try:
            matches = [(k, tile_re.search(k)) for k in _list_keys(cell_id, "historical")]
        finally:
            time.sleep(delay)
        dated = [(m.group(1), k) for k, m in matches if m and m.group(1)]
        if dated:
            # YYYYMMDD sorts correctly as a plain string
            date_str, key = max(dated)
            return f"{TNM_S3_BASE}/{key}", date_str, "historical"

    except urllib.error.HTTPError as e:
        print(f"  [WARN] S3 HTTP error for {cell_id}: {e.code} {e.reason}")
        return None
    except urllib.error.URLError as e:
        print(f"  [WARN] S3 connection error for {cell_id}: {e.reason}")
        return None
    except ET.ParseError as e:
        print(f"  [WARN] S3 XML parse error for {cell_id}: {e}")
        return None
    except OSError as e:
        print(f"  [WARN] S3 request failed for {cell_id}: {e}")
        return None

    print(f"  [WARN] No 1/3 arc-second .tif found in S3 for cell {cell_id} -- keeping existing URL")
    return None


# ---------------------------------------------------------------------------
# Per-state processing
# ---------------------------------------------------------------------------

def process_state(state, dl_lists_dir, backup_dir, dry_run=False,
                  delay=DEFAULT_DELAY, verify=False):
    """
    Check and optionally update the download list for a single state.

    Returns a dict with keys: state, total, updated, failed, unresolved, skipped.
    """
    result = {
        'state':      state,
        'total':      0,
        'updated':    [],
        'failed':     [],
        'unresolved': [],   # (url, detail) kept URLs that failed --verify
        'skipped':    False,
    }

    dl_file = dl_lists_dir / f"{state}_data.txt"

    if not dl_file.exists():
        print(f"  [SKIP] {dl_file.name} not found")
        result['skipped'] = True
        return result

    existing_urls = [
        line.strip()
        for line in dl_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    if not existing_urls:
        print(f"  [SKIP] {dl_file.name} is empty")
        result['skipped'] = True
        return result

    result['total'] = len(existing_urls)
    print(f"  Checking {len(existing_urls)} tile(s)...")

    new_urls = []

    for i, old_url in enumerate(existing_urls, start=1):
        tag = f"    [{i}/{len(existing_urls)}]"
        m = URL_PATTERN.match(old_url)
        if not m:
            print(f"{tag} [SKIP] Unrecognised URL: {old_url[:80]}")
            kept = True
            new_urls.append(old_url)
        else:
            cell_id  = m.group(2).lower()
            old_date = m.group(3)
            print(f"{tag} {cell_id}  (current: {fmt_date(old_date)})", end="", flush=True)

            found = query_latest_tile(cell_id, delay=delay)
            kept = True

            if found is None:
                print(f"  -> [KEEP] (S3 lookup failed)")
                result['failed'].append(cell_id)
                new_urls.append(old_url)
            else:
                latest_url, new_date, variant = found
                update, reason = should_update(old_url, latest_url)
                if update:
                    print(f"  -> [UPDATE] {fmt_date(old_date)} -> {fmt_date(new_date)} ({variant})")
                    result['updated'].append((cell_id, old_url, latest_url))
                    new_urls.append(latest_url)
                    kept = False
                elif reason == "identical":
                    print(f"  -> [CURRENT]")
                    new_urls.append(old_url)
                else:
                    print(f"  -> [KEEP] ({reason}; S3 has {fmt_date(new_date)}/{variant})")
                    new_urls.append(old_url)

        if kept and verify:
            ok, detail = _head_ok(old_url)
            time.sleep(delay)
            if not ok:
                print(f"      [VERIFY FAIL] {detail}: {old_url}")
                result['unresolved'].append((old_url, detail))

    if not result['updated']:
        print(f"  [OK] No changes needed")
        result['skipped'] = True
        return result

    if dry_run:
        print(f"  [DRY RUN] Would update {len(result['updated'])} tile(s) -- no files written")
    else:
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_file = backup_dir / f"{state}_data.txt.bkup"
        shutil.copy2(str(dl_file), str(backup_file))
        print(f"  [BACKUP]  {backup_file}")

        new_content = "\n".join(new_urls) + "\n"
        dl_file.write_text(new_content, encoding="utf-8")
        print(f"  [WRITTEN] {dl_file}  ({len(result['updated'])} tile(s) updated)")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Check USGS S3 bucket for newer DEM tiles and update download lists",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for Wyoming
  python3 scripts/update_dem_lists.py --state WY --dry-run

  # Update Wyoming
  python3 scripts/update_dem_lists.py --state WY

  # Update all states
  python3 scripts/update_dem_lists.py

  # Also check that every kept URL still resolves
  python3 scripts/update_dem_lists.py --verify

  # Slower requests (more polite)
  python3 scripts/update_dem_lists.py --delay 1.0

Output files:
  Updated:  USGS_DL_Lists/<STATE>_data.txt
  Backup:   USGS_DL_Lists/backup/<STATE>_data.txt.bkup

Notes:
  - States where nothing changes are left completely untouched.
  - If the S3 lookup fails for a tile, the existing URL is preserved.
  - A tile is never replaced by an older one (see module docstring).
  - Backups overwrite any existing backup for that state.
  - Each tile costs up to 2 S3 requests; full CONUS (~2500 tiles at 0.5s
    delay) takes roughly 40-50 minutes.
        """
    )

    parser.add_argument(
        '--state', metavar='CODE', default=None,
        help='Process a single state (e.g. WY). Omit to process all states.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Report changes without writing any files'
    )
    parser.add_argument(
        '--verify', action='store_true',
        help='HEAD-request every URL that was not updated and report any that fail'
    )
    parser.add_argument(
        '--delay', type=float, default=DEFAULT_DELAY,
        help=f'Delay between S3 requests in seconds (default: {DEFAULT_DELAY})'
    )
    parser.add_argument(
        '--dl-lists-dir', default=str(BASE_DIR / 'USGS_DL_Lists'),
        help='Path to USGS_DL_Lists/ directory (default: USGS_DL_Lists/)'
    )
    parser.add_argument(
        '--version', action='version', version=f'%(prog)s {VERSION}'
    )

    args = parser.parse_args()

    dl_lists_dir = Path(args.dl_lists_dir)
    backup_dir   = dl_lists_dir / 'backup'

    if not dl_lists_dir.exists():
        print(f"[ERROR] USGS_DL_Lists directory not found: {dl_lists_dir}")
        return 1

    if args.state:
        states = [args.state.upper()]
    else:
        states = sorted(
            p.stem.replace('_data', '').upper()
            for p in dl_lists_dir.glob('*_data.txt')
        )
        if not states:
            print(f"[ERROR] No *_data.txt files found in {dl_lists_dir}")
            return 1

    print()
    print("=" * 70)
    print("  USGS DEM Download List Updater")
    print("=" * 70)
    print(f"  Source:         USGS S3 bucket (prd-tnm.s3.amazonaws.com)")
    print(f"  Resolution:     1/3 arc-second  (Elevation/13/TIFF/)")
    print(f"  States:         {len(states)}")
    print(f"  Request delay:  {args.delay}s")
    print(f"  Verify URLs:    {'Yes' if args.verify else 'No'}")
    print(f"  Dry run:        {'YES -- no files will be written' if args.dry_run else 'No'}")
    print(f"  Started:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    all_results = []

    for i, state in enumerate(states, start=1):
        print(f"[{i}/{len(states)}] {state}")
        result = process_state(
            state=state,
            dl_lists_dir=dl_lists_dir,
            backup_dir=backup_dir,
            dry_run=args.dry_run,
            delay=args.delay,
            verify=args.verify,
        )
        all_results.append(result)
        print()

    total_tiles      = sum(r['total']          for r in all_results)
    total_updated    = sum(len(r['updated'])   for r in all_results)
    total_failed     = sum(len(r['failed'])    for r in all_results)
    total_unresolved = sum(len(r['unresolved']) for r in all_results)
    states_updated    = [r for r in all_results if r['updated']]
    states_failed     = [r for r in all_results if r['failed']]
    states_unresolved = [r for r in all_results if r['unresolved']]
    states_skipped    = [r for r in all_results if r['skipped'] and not r['updated']]

    print()
    print("=" * 70)
    print("  UPDATE REPORT")
    print("=" * 70)
    print(f"  Run completed:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  States checked: {len(all_results)}")
    print(f"  Total tiles:    {total_tiles}")
    print(f"  Tiles updated:  {total_updated}")
    print(f"  S3 failures:    {total_failed}  (existing URLs preserved)")
    if args.verify:
        print(f"  Unresolvable:   {total_unresolved}  (kept URLs that failed --verify)")
    print(f"  States current: {len(states_skipped)}  (no changes)")
    if args.dry_run:
        print(f"  Mode:           DRY RUN -- no files were written")
    print()

    if states_updated:
        print("-" * 70)
        print("  Updated tiles by state:")
        print("-" * 70)
        for r in states_updated:
            print(f"\n  {r['state']}  ({len(r['updated'])} tile(s) updated)")
            for cell_id, old_url, new_url in r['updated']:
                old_m = URL_PATTERN.match(old_url)
                new_m = URL_PATTERN.match(new_url)
                old_date = fmt_date(old_m.group(3)) if old_m else "unknown"
                new_date = fmt_date(new_m.group(3)) if new_m else "unknown"
                print(f"    {cell_id:<12}  {old_date}  ->  {new_date}")
                print(f"      old: {old_url}")
                print(f"      new: {new_url}")
        print()

    if states_failed:
        print("-" * 70)
        print("  S3 failures (existing URLs preserved -- re-run to retry):")
        print("-" * 70)
        for r in states_failed:
            print(f"\n  {r['state']}:")
            for cell_id in r['failed']:
                print(f"    {cell_id}")
        print()

    if states_unresolved:
        print("-" * 70)
        print("  UNRESOLVABLE URLs (not updated, failed HEAD check -- wget will fail):")
        print("-" * 70)
        for r in states_unresolved:
            print(f"\n  {r['state']}:")
            for url, detail in r['unresolved']:
                print(f"    [{detail}] {url}")
        print()

    if not states_updated and not states_failed and not states_unresolved:
        print("  All tiles are current. No updates needed.")
        print()

    if states_updated and not args.dry_run:
        print("-" * 70)
        print("  Backup files written to:")
        print(f"    {backup_dir}/")
        print()
        print("  Re-download updated tiles with wget:")
        print("    wget -c -i USGS_DL_Lists/<STATE>_data.txt -P raw_dem/")
        print()

    print("=" * 70)
    print()

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        print("[INTERRUPTED] Update cancelled by user")
        sys.exit(130)
    except Exception as e:
        print()
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
