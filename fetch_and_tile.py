"""
Darnoppler HRRR Pipeline
Fetches NOAA HRRR composite reflectivity, reprojects, and tiles it.
"""
import argparse
import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
import pygrib
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.crs import CRS
from pyproj import Transformer

NOMADS_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"


# ── Function: Fetch the .idx file ─────────────────────────────
def fetch_idx(date: str, run_hour: int, forecast_hour: int) -> str:
    """
    Fetch the .idx companion file for a given HRRR run.
    """
    filename = f"hrrr.t{run_hour:02d}z.wrfsfcf{forecast_hour:02d}.grib2.idx"
    url = f"{NOMADS_BASE}/hrrr.{date}/conus/{filename}"

    print(f"Fetching idx: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.text


# ── Function: Find variable byte range in the .idx ────────────
def find_variable_range(idx_text: str, variable: str, level: str) -> tuple[int, int]:
    """
    Find the byte range for a specific variable in a GRIB2 .idx file.
    Returns (start_byte, end_byte) — inclusive.
    """
    lines = idx_text.strip().split('\n')

    for i, line in enumerate(lines):
        fields = line.split(':')
        if len(fields) < 5:
            continue

        if fields[3] == variable and fields[4] == level:
            start = int(fields[1])

            if i + 1 < len(lines) and lines[i + 1].strip():
                next_fields = lines[i + 1].split(':')
                end = int(next_fields[1]) - 1
            else:
                end = -1

            return (start, end)

    raise ValueError(f"Variable {variable!r} at level {level!r} not found in index")


# ── Function: Fetch GRIB2 bytes via HTTP range request ────────
def fetch_grib2_range(
    date: str,
    run_hour: int,
    forecast_hour: int,
    start: int,
    end: int
) -> bytes:
    """
    Fetch only the specified byte range of a GRIB2 file using HTTP Range.
    """
    filename = f"hrrr.t{run_hour:02d}z.wrfsfcf{forecast_hour:02d}.grib2"
    url = f"{NOMADS_BASE}/hrrr.{date}/conus/{filename}"

    range_header = f"bytes={start}-{end}" if end >= 0 else f"bytes={start}-"

    print(f"Fetching {filename} with Range: {range_header}")
    response = requests.get(url, headers={'Range': range_header})
    response.raise_for_status()

    if response.status_code != 206:
        print(f"⚠️  Warning: server returned {response.status_code}, expected 206")

    print(f"Received {len(response.content)} bytes")
    return response.content


# ── Function: Convert GRIB2 bytes → georeferenced GeoTIFF ─────
def write_geotiff(grib2_path: str, output_path: str) -> None:
    """
    Read a GRIB2 file and write it out as a georeferenced GeoTIFF
    in the source Lambert Conformal projection.
    """
    grbs = pygrib.open(grib2_path)
    grb = grbs.message(1)
    data = grb.values.astype(np.float32)
    proj_params = grb.projparams
    lat0 = grb.latitudeOfFirstGridPointInDegrees
    lon0 = grb.longitudeOfFirstGridPointInDegrees
    dx = grb.DxInMetres
    dy = grb.DyInMetres
    nx = grb.Nx
    ny = grb.Ny
    grbs.close()

    print(f"Grid: {nx}×{ny}, {dx}m spacing")

    proj_string = (
        f"+proj=lcc "
        f"+lat_1={proj_params['lat_1']} "
        f"+lat_2={proj_params['lat_2']} "
        f"+lat_0={proj_params['lat_0']} "
        f"+lon_0={proj_params['lon_0']} "
        f"+a={proj_params['a']} "
        f"+b={proj_params['b']} "
        f"+units=m +no_defs"
    )
    crs = CRS.from_proj4(proj_string)

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    sw_x, sw_y = transformer.transform(lon0, lat0)

    nw_x = sw_x
    nw_y = sw_y + (ny - 1) * dy

    data_flipped = np.flipud(data)

    transform = from_origin(nw_x, nw_y, dx, dy)

    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',
        height=ny,
        width=nx,
        count=1,
        dtype=data_flipped.dtype,
        crs=crs,
        transform=transform,
        nodata=-10,
    ) as dst:
        dst.write(data_flipped, 1)

    print(f"✓ Wrote {output_path}")


# ── Function: Convert Float32 GeoTIFF → 8-bit Byte GeoTIFF ────
def convert_to_byte(input_tif: str, output_tif: str) -> None:
    """
    Convert a Float32 GeoTIFF to 8-bit Byte (required for PNG tile output).
    Maps dBZ range [-10, 75] linearly to [0, 255]. Sets 0 as nodata.
    """
    cmd = [
        'gdal_translate',
        '-of', 'GTiff',
        '-ot', 'Byte',
        '-scale', '-10', '75', '0', '255',
        '-a_nodata', '0',
        '-q',
        input_tif,
        output_tif,
    ]

    print(f"Converting to 8-bit: {input_tif} → {output_tif}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ gdal_translate failed:")
        print(result.stderr)
        raise RuntimeError("gdal_translate failed")

    print(f"✓ Wrote {output_tif}")


# ── Function: Generate XYZ tile pyramid ───────────────────────
def generate_tiles(
    input_tif: str,
    output_dir: str,
    min_zoom: int = 4,
    max_zoom: int = 8,
    processes: int = 4,
) -> None:
    """
    Generate an XYZ tile pyramid from a byte-scaled GeoTIFF.
    """
    cmd = [
        'gdal2tiles.py',
        '-z', f'{min_zoom}-{max_zoom}',
        '--xyz',
        f'--processes={processes}',
        '-q',
        input_tif,
        output_dir,
    ]

    print(f"Generating tiles (zoom {min_zoom}-{max_zoom}) → {output_dir}/")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ gdal2tiles.py failed:")
        print(result.stderr)
        raise RuntimeError("gdal2tiles.py failed")

    tile_count = sum(1 for _ in subprocess.run(
        ['find', output_dir, '-name', '*.png'],
        capture_output=True, text=True
    ).stdout.strip().split('\n'))
    print(f"✓ Generated {tile_count} tiles")


# ── Helper: Upload a single file (used by ThreadPoolExecutor) ──
def _upload_one_file(s3, local_path: str, bucket: str, r2_key: str) -> None:
    """
    Upload a single file to R2. Used as the unit of work for parallel uploads.
    """
    content_type = 'image/png' if local_path.endswith('.png') else 'text/html'

    s3.upload_file(
        local_path,
        bucket,
        r2_key,
        ExtraArgs={
            'ContentType': content_type,
            'CacheControl': 'public, max-age=3600',
        }
    )


# ── Function: Upload a folder of tiles to R2 (parallel) ───────
def upload_tiles_to_r2(
    local_dir: str,
    bucket: str,
    prefix: str,
    account_id: str,
    access_key: str,
    secret_key: str,
    max_workers: int = 20,
) -> int:
    """
    Upload every file in local_dir/ to an R2 bucket under the given prefix,
    using a thread pool for parallel uploads.

    Returns the number of files uploaded.
    """
    # boto3 client is thread-safe; all workers share one.
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto',
    )

    # Collect every (local_path, r2_key) pair first.
    upload_tasks = []
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, local_dir)
            r2_key = f"{prefix}/{rel_path}".replace('\\', '/')
            upload_tasks.append((local_path, r2_key))

    uploaded = 0

    # Dispatch uploads to a thread pool for parallel execution.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_upload_one_file, s3, local_path, bucket, r2_key): r2_key
            for local_path, r2_key in upload_tasks
        }

        # Wait for each to complete. Re-raises exceptions if any upload failed.
        for future in as_completed(futures):
            future.result()
            uploaded += 1

    return uploaded


# ── Test / Entry point ────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Fetch HRRR composite reflectivity and generate web map tiles.'
    )
    parser.add_argument('date', help='Run date in YYYYMMDD format (e.g., 20260418)')
    parser.add_argument('run_hour', type=int, help='Run hour in UTC, 0-23 (e.g., 0 for 00Z run)')
    parser.add_argument('forecast_hour', type=int, help='Forecast hour offset, 0-18 (0 = analysis)')
    parser.add_argument('--output-dir', default='tiles', help='Local tiles dir (default: tiles)')
    parser.add_argument('--min-zoom', type=int, default=4, help='Minimum zoom level')
    parser.add_argument('--max-zoom', type=int, default=8, help='Maximum zoom level')
    parser.add_argument('--upload', action='store_true', help='Upload tiles to R2 after generation')

    args = parser.parse_args()

    print(f"=== HRRR {args.date} {args.run_hour:02d}Z — forecast hour {args.forecast_hour} ===\n")

    idx_text = fetch_idx(args.date, args.run_hour, args.forecast_hour)
    start, end = find_variable_range(idx_text, 'REFC', 'entire atmosphere')
    print(f"REFC byte range: {start} to {end} ({end - start + 1} bytes)\n")

    grib2_bytes = fetch_grib2_range(args.date, args.run_hour, args.forecast_hour, start, end)
    with open('refc.grib2', 'wb') as f:
        f.write(grib2_bytes)
    print(f"✓ Saved refc.grib2\n")

    write_geotiff('refc.grib2', 'refc.tif')
    print()

    convert_to_byte('refc.tif', 'refc_byte.tif')
    print()

    generate_tiles(
        'refc_byte.tif',
        args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
    )

    if args.upload:
        print("\n=== Uploading to R2 (parallel) ===")

        account_id = os.environ.get('R2_ACCOUNT_ID')
        access_key = os.environ.get('R2_ACCESS_KEY_ID')
        secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
        bucket = os.environ.get('R2_BUCKET', 'darnoppler-tiles')

        if not all([account_id, access_key, secret_key]):
            print("❌ Missing R2 credentials in environment")
            print("   Required: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
            raise SystemExit(1)

        prefix = f"{args.date}/{args.run_hour:02d}z/f{args.forecast_hour:02d}"

        uploaded = upload_tiles_to_r2(
            args.output_dir,
            bucket,
            prefix,
            account_id,
            access_key,
            secret_key,
        )
        print(f"✓ Uploaded {uploaded} files to s3://{bucket}/{prefix}/")

    print("\n🎉 Pipeline complete!")


if __name__ == '__main__':
    main()
