#!/usr/bin/env python3
import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
import s3fs
import xarray as xr
import zarr
from PIL import Image
from pyproj import CRS, Transformer
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / 'public'
OUT = PUBLIC / 'latest.json'
CACHE = PUBLIC / 'cache-raw'

PROJ4 = '+proj=lcc +a=6371200.0 +b=6371200.0 +lon_0=262.5 +lat_0=38.5 +lat_1=38.5 +lat_2=38.5'
MAX_RENDER_FRAME = 48
LAYERS = {
    'trc1_full_sfc': {
        'label': 'Near-surface smoke',
        'level': '8m_above_ground',
        'variable': 'MASSDEN',
        'units': 'kg/m^3',
        'scale_max': 250e-9,
    },
    'trc1_full_int': {
        'label': 'Vertically integrated smoke',
        'level': 'entire_atmosphere_single_layer',
        'variable': 'COLMD',
        'units': 'kg/m^2',
        'scale_max': 0.6,
    },
}


def latest_cycle_utc(now=None):
    now = now or datetime.now(timezone.utc)
    cycle_hour = (now.hour // 6) * 6
    dt = now.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    return dt


def cycle_candidates(long_only=False):
    dt = latest_cycle_utc()
    for step in range(0, 36, 1):
        cand = datetime.fromtimestamp(dt.timestamp() - step * 3600, tz=timezone.utc)
        if long_only and cand.hour not in (0, 6, 12, 18):
            continue
        yield cand


def open_dataset(run_dt, layer):
    ymd = run_dt.strftime('%Y%m%d')
    hh = run_dt.strftime('%H')
    store = f"s3://hrrrzarr/sfc/{ymd}/{ymd}_{hh}z_fcst.zarr"
    group = f"{layer['level']}/{layer['variable']}"
    fs = s3fs.S3FileSystem(anon=True)
    mapper = fs.get_mapper(store)
    zg = zarr.open_group(mapper, mode='r', path=group)

    data_key = f"{layer['level']}/{layer['variable']}"
    if data_key not in zg:
        raise RuntimeError(f'data array {data_key} not found in {store} group {group}; keys={list(zg.array_keys())}')

    data = zg[data_key]
    time = np.asarray(zg['time'])
    x = np.asarray(zg['projection_x_coordinate'])
    y = np.asarray(zg['projection_y_coordinate'])

    if data.shape[0] < 2:
        raise RuntimeError(f'insufficient time dimension in {store} group {group}')

    var = xr.DataArray(
        data,
        dims=('time', 'projection_y_coordinate', 'projection_x_coordinate'),
        coords={
            'time': time,
            'projection_x_coordinate': x,
            'projection_y_coordinate': y,
        },
        name=layer['variable'],
    )
    return f"{store}::{group}", None, var


def build_source_transform(var):
    src_crs = CRS.from_proj4(PROJ4)
    x = np.asarray(var.coords['projection_x_coordinate'].values, dtype='float64')
    y = np.asarray(var.coords['projection_y_coordinate'].values, dtype='float64')
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
    # array_bounds returns left,bottom,right,top
    leaflet_bounds = [[bounds[1], bounds[0]], [bounds[3], bounds[2]]]
    return np.moveaxis(dst, 0, -1), leaflet_bounds


def save_png(rgba, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgba, mode='RGBA').save(path)


def resolve_mode_runtime(long_only=False):
    chosen_run = None
    sources = {}
    logs = []
    for run_dt in cycle_candidates(long_only=long_only):
        try:
            logs.append(f"trying runtime {run_dt.strftime('%Y%m%d%H')} long_only={long_only}")
            for key, layer in LAYERS.items():
                store, ds, var = open_dataset(run_dt, layer)
                sources[key] = {'store': store, 'dataset': ds, 'var': var}
                logs.append(f"opened {key} from {store}")
            chosen_run = run_dt
            break
        except Exception as e:
            logs.append(f"failed runtime {run_dt.strftime('%Y%m%d%H')}: {e}")
            sources = {}
            continue
    return chosen_run, sources, logs


def render_mode(runtime, sources, mode_name, startup_logs, long_only=False):
    runtime_cache = CACHE / runtime / mode_name
    runtime_cache.mkdir(parents=True, exist_ok=True)

    mode_manifest = {
        'runtime': runtime,
        'longOnlyCycle': long_only,
        'maxFrame': 0,
        'bounds': None,
        'logs': list(startup_logs),
        'layers': {},
    }
    mode_manifest['logs'].append(f'using runtime {runtime} from public AWS HRRR Zarr mode={mode_name}')

    for key, layer in LAYERS.items():
        ds = sources[key]['dataset']
        var = sources[key]['var']
        src_crs, src_transform, src_bounds = build_source_transform(var)
        mode_manifest['logs'].append(f"source bounds {key}: {src_bounds}")
        layer_cache = runtime_cache / key
        layer_cache.mkdir(parents=True, exist_ok=True)
        frames = []
        available = []
        layer_bounds = None
        frame_limit = 49 if long_only else 19
        frame_count = min(int(var.sizes['time']), frame_limit)
        mode_manifest['logs'].append(f"frame count {key}: {frame_count}")
        for frame in range(frame_count):
            try:
                data = var.isel(time=frame).values
                rgba = smoke_rgba(data, layer['scale_max'])
                warped, layer_bounds = warp_rgba(rgba, src_transform, src_crs, src_bounds)
                rel = f'./cache-raw/{runtime}/{key}/f{frame:03d}.png'
                save_png(warped, PUBLIC / rel.replace('./', ''))
                frames.append({'frame': frame, 'url': rel, 'cached': True})
                available.append(frame)
            except Exception as e:
                frames.append({'frame': frame, 'url': None, 'cached': False, 'error': str(e)})
                mode_manifest['logs'].append(f'failed {key} F{frame:03d}: {e}')
        mode_manifest['layers'][key] = {
            'label': layer['label'],
            'units': layer['units'],
            'frames': frames,
            'availableFrames': available,
            'store': sources[key]['store'],
        }
        mode_manifest['maxFrame'] = max(mode_manifest['maxFrame'], (available[-1] if available else 0))
        if layer_bounds is not None and mode_manifest['bounds'] is None:
            mode_manifest['bounds'] = layer_bounds
    return mode_manifest


def main():
    PUBLIC.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)

    short_run, short_sources, short_logs = resolve_mode_runtime(long_only=False)
    if short_run is None:
        raise SystemExit('Could not open any recent HRRR smoke zarr run\n' + '\n'.join(short_logs))

    long_run, long_sources, long_logs = resolve_mode_runtime(long_only=True)
    if long_run is None:
        long_logs = long_logs + ['long-range mode unavailable']

    # clean old runtimes
    keep = {short_run.strftime('%Y%m%d%H')}
    if long_run is not None:
        keep.add(long_run.strftime('%Y%m%d%H'))
    for old in CACHE.iterdir():
        if old.is_dir() and old.name not in keep:
            shutil.rmtree(old, ignore_errors=True)

    short_runtime = short_run.strftime('%Y%m%d%H')
    manifest = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'runtime': short_runtime,
        'runtimeSource': 'aws-hrrrzarr',
        'modes': {
            'hourly': render_mode(short_runtime, short_sources, 'hourly', short_logs, long_only=False)
        }
    }

    if long_run is not None:
        long_runtime = long_run.strftime('%Y%m%d%H')
        manifest['modes']['long'] = render_mode(long_runtime, long_sources, 'long', long_logs, long_only=True)

    default_mode = manifest['modes']['hourly']
    manifest['maxFrame'] = default_mode['maxFrame']
    manifest['bounds'] = default_mode['bounds']
    manifest['layers'] = default_mode['layers']
    manifest['logs'] = default_mode['logs']

    OUT.write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Wrote {OUT} for runtime {short_runtime}')


if __name__ == '__main__':
    main()
