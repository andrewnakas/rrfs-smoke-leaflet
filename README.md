# rrfs-smoke-leaflet

Leaflet-based viewer for RRFS smoke, deployed via GitHub Pages.

## Current architecture

This version uses the same **RRFS byte-range pattern** as the working TreeSixty backend approach:

1. list available RRFS cycles from anonymous S3 XML listings
2. pick the latest cycle with the forecast hours we need
3. fetch the `.idx` for each natlev GRIB
4. locate the smoke message byte range in the index
5. download only that message with an HTTP `Range` request
6. decode it with `pygrib`
7. reproject to a Web Mercator-aligned PNG overlay
8. publish cached PNGs to GitHub Pages

## Smoke fields targeted

- `trc1_full_sfc` → `MASSDEN` at `8 m above ground`
- `trc1_full_int` → `COLMD` for the whole atmospheric column

## Local development

```bash
npm install
python3 -m pip install -r requirements-pipeline.txt
npm run render-raw-smoke
npm run dev
```

## Notes

- Source archive: NOAA RRFS prototype data on AWS S3
- This avoids scraping NOAA graphics and avoids downloading full giant GRIB files when a single message is enough.
- If NOAA changes natlev index metadata strings, update the `matchers` in `scripts/render-rrfs-smoke.py`.
