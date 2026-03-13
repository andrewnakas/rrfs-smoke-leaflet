#!/usr/bin/env python3
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import rasterio
import xarray as xr
from PIL import Image
from pyproj import CRS
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / 'public'
OUT = PUBLIC / 'latest.json'
CACHE = PUBLIC / 'cache-raw'

PROJ4 = '+proj=lcc +a=6371229 +b=6371229 +lat_1=25 +lat_2=25 +lat_0=25 +lon_0=265'
MAX_SHORT_FRAME = 18
MAX_LONG_FRAME = 48

LAYERS = {
    'trc1_full_sfc': {
        'label': 'Near-surface smoke',
        'units': 'kg/m^3',
        'search': ':MASSDEN:8 m above ground:.*aerosol=Particulate organic matter dry.*<2.5e-06',
        'scale_max': 250e-9,
    },
    'trc1_full_int': {
        'label': 'Vertically integrated smoke',
        'units': 'kg/m^2',
        'search': r':COLMD:entire atmosphere \(considered as a single layer\):.*aerosol=Particulate organic matter dry.*<2.5e-06',
        'scale_max': 0.6,
    },
}


def latest_cycle_utc(now=None):
    now = now or datetime.now(timezone.utc)
    cycle_hour = (now.hour // 3) * 3
    return now.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)


def cycle_candidates(long_only=False):
    dt = latest_cycle_utc()
    for step in range(0, 48):
        cand = dt - timedelta(hours=step)
        if long_only and cand.hour not in (0, 6, 12, 18):
            continue
        yield cand


def herbie_open(run_dt, fxx, search):
    from herbie import Herbie

    H = Herbie(
        run_dt,
        model='rrfs',
        product='nat',
        fxx=fxx,
        member='control',
        domain=None,
        priority=['aws'],
        save_dir=ROOT / '.herbie-cache',
        verbose=False,
        overwrite=False,
    )
    ds = H.xarray(search=search, remove_grib=False)
    return H, ds


def choose_data_var(ds):
    for name, var in ds.data_vars.items():
        if getattr(var, 'ndim', 0) >= 2:
            return name
    raise RuntimeError(f'no data variable found; vars={list(ds.data_vars)}')


def extract_xy(var):
    x_name = next((n for n in var.coords if n.lower() in ('x', 'projection_x_coordinate')), None)
    y_name = next((n for n in var.coords if n.lower() in ('y', 'projection_y_coordinate')), None)
    if x_name is None or y_name is None:
        raise RuntimeError(f'could not find x/y coords; coords={list(var.coords)}')
    x = np.asarray(var.coords[x_name].values, dtype='float64')
    y = np.asarray(var.coords[y_name].values, dtype='float64')
    return x, y


def build_source_transform(var):
    attrs = getattr(var, 'gribfile_projection', None)
    src_crs = CRS.from_proj4(PROJ4)
    x, y = extract_xy(var)
    dx = float(np.median(np.diff(x)))
    dy = float(np.median(np.diff(y)))
    west = float(x.min() - dx / 2.0)
    east = float(x.max() + dx / 2.0)
    south = float(y.min() - abs(dy) / 2.0)
    north = float(y.max() + abs(dy) / 2.0)
    transform = from_origin(west, north, abs(dx), abs(dy))
    return src_crs, transform, (west, south, east, north)


def smoke_rgba(data, scale_max):
    arr = np.nan_to_num(data.astype('float64'), nan=0.0, posinf=0.0, neginf=0.0)
    arr[arr < 0] = 0
    if scale_max <= 0:
        scale_max = float(np.nanpercentile(arr, 99)) or 1.0
    norm = np.clip(arr / scale_max, 0, 1)
    alpha = np.clip(np.power(norm, 0.65) * 255, 0, 255).astype('uint8')
    r = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 236, 200, 93]).astype('uint8')
    g = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 186, 93, 33]).astype('uint8')
    b = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 79, 30, 4]).astype('uint8')
    rgba = np.dstack([r, g, b, alpha])
    rgba[alpha == 0] = 0
    return rgba


def warp_rgba(rgba, src_transform, src_crs, src_bounds):
    height, width = rgba.shape[:2]
    left, bottom, right, top = src_bounds
    dst_transform, dst_width, dst_height = calculate_default_transform(
        src_crs, 'EPSG:4326', width, height, left, bottom, right, top, resolution=0.05
    )
    dst = np.zeros((4, dst_height, dst_width), dtype='uint8')
    for band in range(4):
        reproject(
            source=rgba[:, :, band],
            destination=dst[band],
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs='EPSG:4326',
            resampling=Resampling.bilinear,
            src_nodata=0,
            dst_nodata=0,
        )
    bounds = rasterio.transform.array_bounds(dst_height, dst_width, dst_transform)
    leaflet_bounds = [[bounds[1], bounds[0]], [bounds[3], bounds[2]]]
    return np.moveaxis(dst, 0, -1), leaflet_bounds


def save_png(rgba, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgba, mode='RGBA').save(path)


def render_mode(runtime_dt, mode_name, frame_limit, long_only=False):
    runtime = runtime_dt.strftime('%Y%m%d%H')
    manifest = {
        'runtime': runtime,
        'maxFrame': 0,
        'bounds': None,
        'logs': [f'using runtime {runtime} from AWS RRFS prototype ({mode_name})'],
        'layers': {},
    }

    for key, layer in LAYERS.items():
        layer_frames = []
        available = []
        layer_bounds = None
        for frame in range(frame_limit + 1):
            try:
                H, ds = herbie_open(runtime_dt, frame, layer['search'])
                var_name = choose_data_var(ds)
                var = ds[var_name].squeeze()
                if var.ndim != 2:
                    raise RuntimeError(f'unexpected dims for {var_name}: {var.dims}')
                src_crs, src_transform, src_bounds = build_source_transform(var)
                data = np.asarray(var.values)
                rgba = smoke_rgba(data, layer['scale_max'])
                warped, layer_bounds = warp_rgba(rgba, src_transform, src_crs, src_bounds)
                rel = f'./cache-raw/{runtime}/{mode_name}/{key}/f{frame:03d}.png'
                save_png(warped, PUBLIC / rel.replace('./', ''))
                layer_frames.append({
                    'frame': frame,
                    'url': rel,
                    'cached': True,
                    'source': getattr(H, 'grib', None),
                })
                available.append(frame)
                manifest['logs'].append(f'cached {key} F{frame:03d}')
            except Exception as e:
                layer_frames.append({'frame': frame, 'url': None, 'cached': False, 'error': str(e)})
                manifest['logs'].append(f'failed {key} F{frame:03d}: {e}')
        manifest['layers'][key] = {
            'label': layer['label'],
            'units': layer['units'],
            'frames': layer_frames,
            'availableFrames': available,
        }
        if available:
            manifest['maxFrame'] = max(manifest['maxFrame'], available[-1])
        if layer_bounds is not None and manifest['bounds'] is None:
            manifest['bounds'] = layer_bounds
    return manifest


def find_runtime(long_only=False):
    logs = []
    for run_dt in cycle_candidates(long_only=long_only):
        try:
            _, ds = herbie_open(run_dt, 0, LAYERS['trc1_full_sfc']['search'])
            choose_data_var(ds)
            return run_dt, logs
        except Exception as e:
            logs.append(f"failed runtime {run_dt.strftime('%Y%m%d%H')}: {e}")
    raise RuntimeError('\n'.join(logs) or 'no RRFS runtime found')


def main():
    PUBLIC.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)

    short_run, short_logs = find_runtime(long_only=False)
    short_runtime = short_run.strftime('%Y%m%d%H')

    manifest = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'runtime': short_runtime,
        'runtimeSource': 'aws-noaa-rrfs-via-herbie',
        'modes': {},
    }

    hourly = render_mode(short_run, 'hourly', MAX_SHORT_FRAME, long_only=False)
    hourly['logs'] = short_logs + hourly['logs']
    manifest['modes']['hourly'] = hourly

    try:
        long_run, long_logs = find_runtime(long_only=True)
        long_mode = render_mode(long_run, 'long', MAX_LONG_FRAME, long_only=True)
        long_mode['logs'] = long_logs + long_mode['logs']
        manifest['modes']['long'] = long_mode
    except Exception as e:
        manifest['logs'] = [f'long mode unavailable: {e}']

    default_mode = manifest['modes']['hourly']
    manifest['maxFrame'] = default_mode['maxFrame']
    manifest['bounds'] = default_mode['bounds']
    manifest['layers'] = default_mode['layers']
    manifest['logs'] = default_mode.get('logs', [])

    keep = {m['runtime'] for m in manifest['modes'].values()}
    for old in CACHE.iterdir():
        if old.is_dir() and old.name not in keep:
            shutil.rmtree(old, ignore_errors=True)

    OUT.write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Wrote {OUT} for runtime {short_runtime}')


if __name__ == '__main__':
    main()
