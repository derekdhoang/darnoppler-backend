import pygrib
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.crs import CRS
from pyproj import Transformer

# ── Step 1: Read GRIB2 data ─────────────────────────────────────
grbs = pygrib.open('refc.grib2')
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

print(f"Grid: {nx} × {ny}, spacing {dx}m")
print(f"First point (SW corner): {lat0}°N, {lon0}°E")

# ── Step 2: Build the CRS (coordinate reference system) ────────
# The PROJ string describes our Lambert Conformal projection
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
print(f"\nCRS: {crs}")

# ── Step 3: Convert the first-point lat/lon to Lambert meters ──
# GDAL needs the top-left corner in projected coordinates
# pygrib gives us the SW corner in lat/lon; we need to find the top-left in meters
transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
sw_x, sw_y = transformer.transform(lon0, lat0)
print(f"\nSW corner in Lambert meters: x={sw_x:.1f}, y={sw_y:.1f}")

# Top-left corner: same x as SW, but y is at the top of the grid
# (grid goes south-to-north; top = sw_y + (ny-1)*dy)
nw_x = sw_x
nw_y = sw_y + (ny - 1) * dy
print(f"NW corner in Lambert meters: x={nw_x:.1f}, y={nw_y:.1f}")

# ── Step 4: Flip the data so row 0 = north ─────────────────────
# GeoTIFF convention: row 0 is at the TOP (north)
# HRRR data: row 0 is at the BOTTOM (south)
# We flip vertically so the image renders right-side-up
data_flipped = np.flipud(data)

# ── Step 5: Build the affine transform ─────────────────────────
# This maps pixel (col, row) → geographic (x, y) in Lambert meters
transform = from_origin(nw_x, nw_y, dx, dy)

# ── Step 6: Write the GeoTIFF ──────────────────────────────────
with rasterio.open(
    'refc.tif',
    'w',
    driver='GTiff',
    height=ny,
    width=nx,
    count=1,                    # single band (just reflectivity)
    dtype=data_flipped.dtype,
    crs=crs,
    transform=transform,
    nodata=-10,                 # tell GDAL that -10 means "no data"
) as dst:
    dst.write(data_flipped, 1)

print("\n✓ Wrote refc.tif")
