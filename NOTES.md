# Darnoppler Backend — HRRR Tile Pipeline

Python pipeline that fetches NOAA HRRR composite reflectivity forecasts,
reprojects them, generates XYZ map tiles, and uploads to Cloudflare R2.

## What It Does

Every run:

1. Fetches HRRR `.idx` files from NOMADS (tiny companion files listing variables + byte offsets in the GRIB2)
2. Finds the byte range for `REFC` (composite reflectivity) at "entire atmosphere"
3. Fetches only those bytes via HTTP Range request (~400KB vs the full 150MB file — a ~375x bandwidth win)
4. Parses GRIB2 with pygrib, reprojects from HRRR's Lambert Conformal → writes georeferenced GeoTIFF
5. Converts Float32 dBZ → uint8 via `gdal_translate` (PNG can't hold float)
6. Generates XYZ tile pyramid (zoom 4-8, 2,195 tiles) via `gdal2tiles.py`
7. Uploads tiles to R2 via boto3 (S3-compatible API)

## Architecture

```
NOAA NOMADS (upstream data)
        ↓ HTTP byte-range fetch
GitHub Actions (compute)
        ↓ Python pipeline
        ↓ boto3 upload
Cloudflare R2 (storage)
        ↓ fetch by frontend (future)
Darnoppler Frontend
```

Everything free tier:
- GitHub Actions: unlimited on public repos
- R2: 10GB storage, 1M Class A ops/mo, unlimited egress
- Pirate Weather (via Worker): 10k calls/day

## Key Numbers

- HRRR grid: 1799 × 1059 cells at 3km spacing
- HRRR projection: Lambert Conformal, lat_0=38.5°N, lon_0=262.5°E (-97.5°E)
- REFC byte range: ~278-400 KB per forecast hour (out of 150MB full file)
- Tiles per forecast hour: 2,199 files (2,195 PNGs + 4 preview HTML/XML)
- Full 19-hour run: ~41,800 tiles
- R2 ops per full run: ~41,800 PUT ops (→ ~1,003,000/day at hourly schedule)

## Ops Budget Consideration

At the naive "one PUT per tile" strategy, running hourly would blow R2's 1M/mo free tier
in 24 hours. When launching production, either:
- Pay $5/mo for R2 Standard tier (10M Class A ops) — simplest
- Tarball per forecast hour (19 PUTs instead of 2,199) — free tier fits, but needs
  unpacking Worker to serve tiles

Deferring this decision until launch.

## GitHub Actions Workflow

`.github/workflows/build-tiles.yml` — manual trigger only for now.
Schedule (cron) will be enabled when launching to production.

Runner spec:
- ubuntu-latest
- Python 3.12
- apt: gdal-bin, python3-gdal, libgdal-dev
- pip: everything in requirements.txt
- Timeout: 45 minutes (typical run ~20-25 min for 19 forecast hours)

Secrets configured:
- R2_ACCOUNT_ID
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_BUCKET (value: darnoppler-tiles)

## R2 Bucket Structure

```
darnoppler-tiles/
├── 20260419/           # YYYYMMDD (UTC date of run)
│   ├── 00z/            # HHz (UTC run hour)
│   │   ├── f00/        # forecast hour 00
│   │   │   ├── 4/      # zoom level 4
│   │   │   ├── 5/
│   │   │   ├── 6/
│   │   │   ├── 7/
│   │   │   ├── 8/
│   │   │   ├── leaflet.html
│   │   │   ├── openlayers.html
│   │   │   └── mapml.mapml
│   │   ├── f01/
│   │   ├── ...
│   │   └── f18/
│   └── 04z/
└── ...
```

Frontend URL pattern (when implemented):
```
https://<r2-public-url>/<date>/<run_hour>z/f<forecast_hour>/<zoom>/<x>/<y>.png
```

## Known Issues / Deferred Work

### NOAA data retention
NOMADS keeps ~48 hours of runs, then deletes. Scheduled pipeline always fetches fresh
data so this isn't a production concern — but manual "re-test yesterday's run" often
fails. Data is gone.

### Early-hour 404s
NOAA publishes runs ~45-90 min after nominal time. Running the workflow at the top of
the hour often returns 404 because the current hour's run hasn't been published yet.
Current code fetches the current hour's run; may need to add a "1 hour ago" buffer
when the production schedule is enabled.

### Content-type fallback is lazy
`upload_tiles_to_r2` sends PNG as `image/png` and everything else as `text/html`.
The `.mapml` XML file gets mislabeled. Not blocking, but worth fixing eventually.

### Cron not enabled
The workflow is manual-trigger only. Schedule will be added at launch (to avoid
burning R2 ops budget while frontend isn't consuming tiles yet).

## Local Development

```bash
cd ~/darnoppler-proxy
source venv/bin/activate

# Run for one forecast hour, no upload:
python fetch_and_tile.py 20260419 0 0

# Run with R2 upload (needs env vars set):
export R2_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
python fetch_and_tile.py 20260419 0 0 --upload

# Run all 19 forecast hours locally:
./run_all_hours.sh 20260419 0
```

## Phase Status

- [x] Phase 6a: Cloudflare account + R2 bucket + WSL installed
- [x] Phase 6b: Pirate Weather Worker proxy deployed
- [x] Phase 6b.5: Frontend migrated Netlify → Cloudflare Pages
- [x] Phase 6c: HRRR pipeline on GitHub Actions + R2 (parallel uploads, ~22min end-to-end; cron deffered)
- [ ] Phase 7: Frontend integration (timeline scrubber in studio)
- [ ] Phase 8: Polish (NWS colors, animation)
- [ ] Phase 9: Reveal to Darnell
