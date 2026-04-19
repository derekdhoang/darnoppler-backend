# Darnoppler Backend — Progress Notes

## Stack
- WSL2 Ubuntu on Windows, Python venv at `~/darnoppler-proxy/venv`
- Python 3.12, pygrib 2.1.8, numpy, Pillow, rasterio, requests
- GDAL 3.8.4 (system install for gdal_translate + gdal2tiles.py)

## Done ✅

### Phase 1–5: Complete End-to-End Pipeline
- ✅ NOMADS exploration — understand .idx + byte-range trick
- ✅ Python script fetches REFC from NOMADS (byte-range HTTP)
- ✅ Parses GRIB2 binary, reprojects Lambert Conformal → GeoTIFF
- ✅ Converts Float32 → Byte, generates XYZ tile pyramid
- ✅ Full CLI tool with argparse: `python fetch_and_tile.py <date> <run_hour> <forecast_hour>`
- ✅ Bash orchestration for all 19 forecast hours of a run
- ✅ 41,705 tiles verified on Leaflet with OpenStreetMap basemap

## Files in ~/darnoppler-proxy/

| File | Purpose |
|------|---------|
| `fetch_and_tile.py` | Main Python pipeline, CLI tool |
| `run_all_hours.sh` | Bash script, loops pipeline over 19 forecast hours |
| `parse.py` | Original exploratory script (kept for reference) |
| `refc.grib2` | Last-fetched raw GRIB2 |
| `refc.tif` | Last-fetched Float32 GeoTIFF |
| `refc_byte.tif` | Last-fetched 8-bit GeoTIFF |
| `tiles/f00` through `tiles/f18` | Full tile pyramids, one per forecast hour |
| `NOTES.md` | This file |

## Next: Phase 6 — Cloud Deployment

Goal: Move the pipeline off Derek's laptop so it runs automatically.

Options to decide between:
- **Netlify Functions** (same ecosystem as Darnoppler frontend, 10s free / 26s paid execution limit — we'd need to architect per-hour)
- **Cheap VPS** (~$5/mo, DigitalOcean/Hetzner, full Linux server, cron-scheduled)
- **GitHub Actions** (free, generous limits, unconventional but clever)
- **Cloudflare Workers + R2** (modern serverless, more flexible)

Pick one based on cost vs. complexity trade-off.

## Phase 7+: 
- Caching layer (R2 or similar)
- Frontend integration (timeline scrubber in Darnell's studio)
- NWS color palette polish
- Animation (auto-play through forecast hours)
- Reveal to Darnell

## Resume Commands

```bash
# Get back into the project
cd ~/darnoppler-proxy
source venv/bin/activate

# Run the full pipeline for one forecast hour
python fetch_and_tile.py 20260418 0 0

# Or run all 19 forecast hours
./run_all_hours.sh 20260418 0

# Quick-verify tiles in browser
cd tiles/f00 && python -m http.server 8000
# Open http://localhost:8000/leaflet.html
```

## Key Numbers Worth Remembering

- HRRR grid: 1799 × 1059 cells at 3 km spacing
- CRS: Lambert Conformal Conic, lat_0=38.5°N, lon_0=262.5°E (−97.5°W)
- Byte-range savings: ~550× vs full file download
- Full pipeline runtime: ~30-90 sec per forecast hour
- Full 19-hour run: ~15-20 min
- Tiles generated per run: 41,705 (2,195 × 19 hours)
