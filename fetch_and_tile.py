"""
Darnoppler HRRR Pipeline
Fetches NOAA HRRR composite reflectivity, reprojects, and tiles it.
"""
import argparse
import subprocess

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
    
    Args:
        date, run_hour, forecast_hour: Same as fetch_idx
        start: First byte to fetch (inclusive)
        end: Last byte to fetch (inclusive). Use -1 for "read to end of file".
    
    Returns:
        The raw bytes for that range
    """
    filename = f"hrrr.t{run_hour:02d}z.wrfsfcf{forecast_hour:02d}.grib2"
    url = f"{NOMADS_BASE}/hrrr.{date}/conus/{filename}"
    
    # Build the Range header
    # Format: "bytes=START-END" or "bytes=START-" for open-ended
    range_header = f"bytes={start}-{end}" if end >= 0 else f"bytes={start}-"
    
    print(f"Fetching {filename} with Range: {range_header}")
    response = requests.get(url, headers={'Range': range_header})
    response.raise_for_status()
    
    # HTTP 206 = Partial Content (the server honored our range request)
    # HTTP 200 = server ignored the range and sent everything (bad — but handle it)
    if response.status_code != 206:
        print(f"⚠️  Warning: server returned {response.status_code}, expected 206")
    
    print(f"Received {len(response.content)} bytes")
    return response.content

# ── Function: Convert GRIB2 bytes → georeferenced GeoTIFF ─────
def write_geotiff(grib2_path: str, output_path: str) -> None:
    """
    Read a GRIB2 file and write it out as a georeferenced GeoTIFF
    in the source Lambert Conformal projection.
    
    Args:
        grib2_path: Path to input GRIB2 file (single message expected)
        output_path: Path to write the output .tif
    """
    # Read the GRIB2 message
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
    
    # Build the Lambert Conformal CRS from the projection params
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
    
    # Convert SW corner from lat/lon to Lambert meters
    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    sw_x, sw_y = transformer.transform(lon0, lat0)
    
    # GeoTIFF needs the NW (top-left) corner in projected coords
    nw_x = sw_x
    nw_y = sw_y + (ny - 1) * dy
    
    # Flip data vertically — GeoTIFF expects row 0 at the top (north)
    # HRRR stores row 0 at the bottom (south), so we flip
    data_flipped = np.flipud(data)
    
    # Build the affine transform: pixel (col, row) → (x, y) in Lambert meters
    transform = from_origin(nw_x, nw_y, dx, dy)
    
    # Write the GeoTIFF
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
        '-q',                  # quiet mode — suppress progress bar
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
    
    Args:
        input_tif: Path to byte-scaled GeoTIFF
        output_dir: Folder to write the tile pyramid into
        min_zoom, max_zoom: Inclusive zoom range
        processes: Parallel workers for tile generation
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
    
    # Count what we produced
    tile_count = sum(1 for _ in subprocess.run(
        ['find', output_dir, '-name', '*.png'],
        capture_output=True, text=True
    ).stdout.strip().split('\n'))
    print(f"✓ Generated {tile_count} tiles")

# ── Test / Entry point ────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Fetch HRRR composite reflectivity and generate web map tiles.'
    )
    parser.add_argument(
        'date',
        help='Run date in YYYYMMDD format (e.g., 20260418)'
    )
    parser.add_argument(
        'run_hour',
        type=int,
        help='Run hour in UTC, 0-23 (e.g., 0 for 00Z run)'
    )
    parser.add_argument(
        'forecast_hour',
        type=int,
        help='Forecast hour offset, 0-18 (0 = analysis)'
    )
    parser.add_argument(
        '--output-dir',
        default='tiles',
        help='Directory to write tiles into (default: tiles)'
    )
    parser.add_argument(
        '--min-zoom',
        type=int,
        default=4,
        help='Minimum zoom level (default: 4)'
    )
    parser.add_argument(
        '--max-zoom',
        type=int,
        default=8,
        help='Maximum zoom level (default: 8)'
    )
    
    args = parser.parse_args()
    
    print(f"=== HRRR {args.date} {args.run_hour:02d}Z — forecast hour {args.forecast_hour} ===\n")
    
    # Step 1: Fetch the index
    idx_text = fetch_idx(args.date, args.run_hour, args.forecast_hour)
    
    # Step 2: Find REFC byte range
    start, end = find_variable_range(idx_text, 'REFC', 'entire atmosphere')
    print(f"REFC byte range: {start} to {end} ({end - start + 1} bytes)\n")
    
    # Step 3: Fetch just those bytes
    grib2_bytes = fetch_grib2_range(args.date, args.run_hour, args.forecast_hour, start, end)
    with open('refc.grib2', 'wb') as f:
        f.write(grib2_bytes)
    print(f"✓ Saved refc.grib2\n")
    
    # Step 4: Write Float32 GeoTIFF
    write_geotiff('refc.grib2', 'refc.tif')
    print()
    
    # Step 5: Convert to 8-bit
    convert_to_byte('refc.tif', 'refc_byte.tif')
    print()
    
    # Step 6: Generate tiles
    generate_tiles(
        'refc_byte.tif',
        args.output_dir,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
    )
    
    print("\n🎉 Pipeline complete!")


if __name__ == '__main__':
    main()
