# rrfs-smoke-leaflet

Leaflet-based viewer for RRFS Smoke/Dust graphics, suitable for GitHub Pages.

## What it does

- pulls recent RRFS Smoke/Dust PNG graphics from NOAA Rapid Refresh
- caches them into `public/cache`
- writes a `public/latest.json` manifest
- serves an animated Leaflet viewer over those cached frames

## Current architecture

This version mirrors the current `hrrr-smoke-leaflet` app structure, but targets the experimental **RRFS-SD** graphics feed instead of HRRR smoke.

Pipeline:

1. fetch RRFS-SD index from `https://rapidrefresh.noaa.gov/RRFS-SD/`
2. resolve a recent runtime
3. download smoke overlay PNGs for supported layers
4. cache them under `public/cache/<runtime>/<layer>/`
5. serve them via Vite / GitHub Pages

## Supported layers

- `trc1_full_sfc` — near-surface smoke
- `trc1_full_int` — vertically integrated smoke

## Local development

```bash
npm install
npm run update-manifest
npm run dev
```

## Notes

- Source archive: NOAA Rapid Refresh experimental RRFS Smoke/Dust graphics
- This app currently follows the same cache-and-manifest approach as the HRRR repo rather than rendering raw RRFS fields locally.
- If NOAA changes the RRFS-SD graphic path layout or field names, update `scripts/update-noaa-manifest.mjs`.
