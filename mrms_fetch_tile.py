#!/usr/bin/env python3
"""
MRMS composite reflectivity fetch + tile pipeline.
Usage: python mrms_fetch_tile.py [timestamp]
  timestamp: YYYYMMDD-HHMMSS (e.g. 20260516-052241)
  If omitted, fetches the latest available frame.
"""

import os
import sys
import re
import json
import subprocess
import requests
import boto3
from botocore.config import Config
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────
MRMS_BASE    = "https://mrms.ncep.noaa.gov/2D/MergedReflectivityQCComposite"
BBOX         = (-104.5, 49.5, -80.0, 36.0)   # west, north, east, south
ZOOM         = "5-7"
HISTORY_HRS  = 2          # rolling window to keep
MAX_FRAMES   = 60         # 2 hrs × 30 frames/hr

# R2 credentials — same env vars as HRRR pipeline
R2_ACCOUNT_ID      = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID   = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET          = os.environ.get("R2_BUCKET")

# Cloudflare API for KV writes
CF_ACCOUNT_ID  = os.environ.get("CF_ACCOUNT_ID", R2_ACCOUNT_ID)
CF_API_TOKEN   = os.environ.get("CF_API_TOKEN")
KV_NAMESPACE   = os.environ.get("KV_NAMESPACE_ID", "77eaa275e835453ca6db4bb0f9b9ae97")

UPLOAD = all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET])

# ── R2 CLIENT ─────────────────────────────────────────────────────────
def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

# ── HELPERS ───────────────────────────────────────────────────────────
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

def ts_to_dt(ts):
    """Convert '20260516-053638' to datetime."""
    return datetime.strptime(ts, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)

# ── UPLOAD ────────────────────────────────────────────────────────────
def upload_tiles(r2, timestamp, tile_dir):
    prefix = f"mrms/tiles/{timestamp}"
    files  = list(Path(tile_dir).rglob("*.png"))
    print(f"  Uploading {len(files)} tiles to R2 ({prefix})...")

    for i, fpath in enumerate(files):
        rel   = fpath.relative_to(tile_dir)
        key   = f"{prefix}/{rel}"
        r2.upload_file(
            str(fpath), R2_BUCKET, key,
            ExtraArgs={"ContentType": "image/png",
                       "CacheControl": "public, max-age=120"}
        )
        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{len(files)} uploaded...")

    print(f"  ✓ Uploaded {len(files)} tiles")
    return len(files)

# ── KV INVENTORY ──────────────────────────────────────────────────────
def update_kv_inventory(all_frames, latest):
    """Write frame inventory to KV via Cloudflare REST API."""
    if not CF_API_TOKEN:
        print("  ⚠ No CF_API_TOKEN — skipping KV update")
        return

    payload = {
        "latest":     latest,
        "frames":     all_frames,
        "lag_minutes": round(
            (datetime.now(timezone.utc) - ts_to_dt(latest)).total_seconds() / 60, 1
        ),
        "bbox":       "upper-midwest",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    url = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
           f"/storage/kv/namespaces/{KV_NAMESPACE}/values/mrms_inventory")

    r = requests.put(
        url,
        headers={"Authorization": f"Bearer {CF_API_TOKEN}",
                 "Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    if r.ok:
        print(f"  ✓ KV inventory updated ({len(all_frames)} frames)")
    else:
        print(f"  ⚠ KV update failed: {r.status_code} {r.text[:200]}")

# ── PURGE OLD FRAMES ──────────────────────────────────────────────────
def purge_old_frames(r2, all_frames):
    """Delete R2 tiles for frames older than HISTORY_HRS."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HISTORY_HRS)
    to_delete = [ts for ts in all_frames if ts_to_dt(ts) < cutoff]

    if not to_delete:
        return all_frames

    for ts in to_delete:
        prefix = f"mrms/tiles/{ts}/"
        paginator = r2.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                r2.delete_object(Bucket=R2_BUCKET, Key=obj["Key"])
        print(f"  🗑 Purged {ts}")

    remaining = [ts for ts in all_frames if ts not in to_delete]
    return remaining

# ── MAIN PIPELINE ─────────────────────────────────────────────────────
def process(timestamp):
    filename = f"MRMS_MergedReflectivityQCComposite_00.50_{timestamp}.grib2"
    url      = f"{MRMS_BASE}/{filename}.gz"
    out_dir  = f"mrms_tiles/{timestamp}"

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
    run(f"gdal_translate -projwin {w} {n} {e} {s} -of GTiff "
        f"mrms_raw_full.grib2 mrms_crop.tif", "Cropping to Upper Midwest")

    # 4. Scale dBZ to 8-bit
    run("gdal_translate -ot Byte -scale -10 75 1 254 -a_nodata 255 "
        "mrms_crop.tif mrms_byte.tif", "Scaling to 8-bit")

    # 5. Colorize
    run("gdaldem color-relief mrms_byte.tif color_ramp.txt mrms_color.tif -alpha",
        "Applying NWS color ramp")

    # 6. Reproject
    run("gdalwarp -s_srs EPSG:4326 -t_srs EPSG:3857 -r near -of GTiff "
        "mrms_color.tif mrms_3857.tif", "Reprojecting to EPSG:3857")

    # 7. Tile
    os.makedirs(out_dir, exist_ok=True)
    run(f"gdal2tiles.py --zoom={ZOOM} --resampling=near "
        f"--tiledriver=PNG --xyz -x mrms_3857.tif {out_dir}",
        "Generating tiles")

    tile_count = sum(
        len([f for f in files if f.endswith(".png")])
        for _, _, files in os.walk(out_dir)
    )
    print(f"  ✓ {tile_count} tiles → {out_dir}/")

    # Cleanup intermediates
    for f in ["mrms_raw_full.grib2", "mrms_crop.tif",
              "mrms_byte.tif", "mrms_color.tif", "mrms_3857.tif"]:
        if os.path.exists(f):
            os.remove(f)

    return out_dir, tile_count


if __name__ == "__main__":
    ts = sys.argv[1] if len(sys.argv) > 1 else get_latest_timestamp()
    print(f"Timestamp: {ts}")

    out_dir, count = process(ts)

    if UPLOAD:
        print("\n── Upload phase ──")
        r2 = get_r2()

        # Upload new tiles
        upload_tiles(r2, ts, out_dir)

        # Load existing inventory from KV or build fresh
        existing = []
        if CF_API_TOKEN:
            url = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
                   f"/storage/kv/namespaces/{KV_NAMESPACE}/values/mrms_inventory")
            resp = requests.get(
                url, headers={"Authorization": f"Bearer {CF_API_TOKEN}"}
            )
            if resp.ok:
                try:
                    existing = resp.json().get("frames", [])
                except Exception:
                    existing = []

        # Merge + deduplicate + sort
        all_frames = sorted(set(existing + [ts]))

        # Purge old frames
        all_frames = purge_old_frames(r2, all_frames)

        # Keep max frames cap
        if len(all_frames) > MAX_FRAMES:
            all_frames = all_frames[-MAX_FRAMES:]

        # Update KV
        update_kv_inventory(all_frames, ts)

        print(f"\n🎉 Done — {count} tiles uploaded, {len(all_frames)} frames in inventory")
    else:
        print("\n⚠ R2 credentials not set — local run only (no upload)")
        print(f"🎉 Done — {count} tiles in {out_dir}/")
