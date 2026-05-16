#!/usr/bin/env python3
"""
MRMS composite reflectivity fetch + tile pipeline.
Usage: python mrms_fetch_tile.py <timestamp>
  timestamp: YYYYMMDD-HHMMSS (e.g. 20260516-052241)
  If omitted, fetches the latest available frame.
"""

import os
import sys
import re
import subprocess
import requests

MRMS_BASE = "https://mrms.ncep.noaa.gov/2D/MergedReflectivityQCComposite"
BBOX      = (-104.5, 49.5, -80.0, 36.0)  # west, north, east, south
ZOOM      = "5-8"

def get_latest_timestamp():
    r = requests.get(f"{MRMS_BASE}/")
    r.raise_for_status()
    matches = re.findall(
        r'MRMS_MergedReflectivityQCComposite_00\.50_(\d{8}-\d{6})\.grib2\.gz',
        r.text
    )
    if not matches:
        raise RuntimeError("No MRMS files found on NOMADS")
    return sorted(set(matches))[-1]

def run(cmd, desc):
    print(f"  {desc}...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        sys.exit(1)

def process(timestamp):
    filename  = f"MRMS_MergedReflectivityQCComposite_00.50_{timestamp}.grib2"
    url       = f"{MRMS_BASE}/{filename}.gz"
    out_dir   = f"mrms_tiles/{timestamp}"

    print(f"\n=== MRMS {timestamp} ===")

    # 1. Fetch
    print(f"  Fetching {filename}.gz ...")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open("mrms_raw.grib2.gz", "wb") as f:
        for chunk in r.iter_content(65536):
            f.write(chunk)
    print(f"  Downloaded {os.path.getsize('mrms_raw.grib2.gz') / 1024:.0f} KB")

    # 2. Decompress
    run("gunzip -f mrms_raw.grib2.gz && mv mrms_raw.grib2 mrms_raw_full.grib2",
        "Decompressing")

    # 3. Crop to Upper Midwest
    w, n, e, s = BBOX
    run(f'gdal_translate -projwin {w} {n} {e} {s} -of GTiff '
        f'mrms_raw_full.grib2 mrms_crop.tif', "Cropping to Upper Midwest")

    # 4. Scale dBZ to 8-bit (−10 to 75 dBZ → 1–254, nodata=255)
    run('gdal_translate -ot Byte -scale -10 75 1 254 -a_nodata 255 '
        'mrms_crop.tif mrms_byte.tif', "Scaling to 8-bit")

    # 5. Colorize with NWS ramp
    run('gdaldem color-relief mrms_byte.tif color_ramp.txt mrms_color.tif -alpha',
        "Applying NWS color ramp")

    # 6. Reproject to Web Mercator
    run('gdalwarp -s_srs EPSG:4326 -t_srs EPSG:3857 -r near -of GTiff '
        'mrms_color.tif mrms_3857.tif', "Reprojecting to EPSG:3857")

    # 7. Generate XYZ tiles, skip empty
    os.makedirs(out_dir, exist_ok=True)
    run(f'gdal2tiles.py --zoom={ZOOM} --resampling=near '
        f'--tiledriver=PNG --xyz -x mrms_3857.tif {out_dir}',
        "Generating tiles")

    # Count
    tile_count = sum(
        len(files) for _, _, files in os.walk(out_dir)
        if any(f.endswith('.png') for f in files)
    )
    print(f"  ✓ {tile_count} tiles → {out_dir}/")

    # Cleanup intermediates
    for f in ["mrms_raw_full.grib2","mrms_crop.tif",
              "mrms_byte.tif","mrms_color.tif","mrms_3857.tif"]:
        if os.path.exists(f):
            os.remove(f)

    return out_dir, tile_count

if __name__ == "__main__":
    ts = sys.argv[1] if len(sys.argv) > 1 else get_latest_timestamp()
    print(f"Timestamp: {ts}")
    out_dir, count = process(ts)
    print(f"\n🎉 Done — {count} tiles in {out_dir}/")
