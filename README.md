# rrfs-smoke-leaflet

Leaflet-based viewer for RRFS smoke, deployed via GitHub Pages.

## Current architecture

This version renders **raw RRFS GRIB fields** into transparent PNG overlays instead of scraping NOAA's image site or depending on Herbie runtime behavior.

Pipeline:

1. GitHub Actions probes documented RRFS AWS GRIB filenames directly
2. downloads the first working RRFS deterministic control GRIB for each frame
3. opens GRIB contents with `cfgrib`
4. finds smoke fields for:
   - near-surface smoke (`MASSDEN` at 8 m AGL for dry particulate organic matter <2.5 μm)
   - vertically integrated smoke (`COLMD` for dry particulate organic matter <2.5 μm)
5. reprojects them to EPSG:4326
6. writes transparent PNG overlays into `public/cache-raw`
7. deploys the app to GitHub Pages

## Local development

```bash
npm install
python3 -m pip install -r requirements-pipeline.txt
npm run render-raw-smoke
npm run dev
```

## Notes

- Source archive: NOAA RRFS prototype data on AWS
- The script tries several known RRFS native-grid filename variants because NOAA naming has shifted over time.
- If the GRIB field metadata changes, update the layer matchers in `scripts/render-rrfs-smoke.py`.
