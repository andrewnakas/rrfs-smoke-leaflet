# rrfs-smoke-leaflet

Leaflet-based viewer for RRFS smoke, deployed via GitHub Pages.

## Current architecture

This version now aims to render **raw RRFS smoke fields** into transparent PNG overlays instead of scraping NOAA's pre-rendered website graphics.

Pipeline:

1. GitHub Actions opens recent RRFS prototype data from AWS using Herbie
2. reads smoke fields for:
   - near-surface smoke (`MASSDEN` for dry particulate organic matter <2.5μm at 8 m AGL)
   - vertically integrated smoke (`COLMD` for dry particulate organic matter <2.5μm)
3. reprojects them to EPSG:4326
4. writes transparent PNG overlays into `public/cache-raw`
5. deploys the app to GitHub Pages

## Local development

```bash
npm install
python3 -m pip install -r requirements-pipeline.txt
npm run render-raw-smoke
npm run dev
```

## Notes

- Source archive: NOAA RRFS prototype data on AWS
- This avoids depending on the flaky RRFS-SD website image paths.
- The repo still keeps the simple Leaflet overlay viewer from the original HRRR app.
- If RRFS field metadata changes, update the search strings in `scripts/render-rrfs-smoke.py`.
