#!/usr/bin/env python3
import json
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import cfgrib
import numpy as np
import rasterio
import requests
from PIL import Image
from pyproj import CRS
from rasterio.transform import from_origin
from rasterio.warp import Resampling, calculate_default_transform, reproject

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / 'public'
OUT = PUBLIC / 'latest.json'
CACHE = PUBLIC / 'cache-raw'
DOWNLOADS = ROOT / '.rrfs-grib-cache'

S3_BUCKET = 'https://noaa-rrfs-pds.s3.amazonaws.com'
PREFIX_ROOT = 'rrfs_a'
PROJ4 = '+proj=lcc +a=6371229 +b=6371229 +lat_1=25 +lat_2=25 +lat_0=25 +lon_0=265'
MAX_SHORT_FRAME = 18
MAX_LONG_FRAME = 48

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Claw/1.0 (+rrfs-smoke-leaflet)'})

LAYERS = {
    'trc1_full_sfc': {
        'label': 'Near-surface smoke',
        'units': 'kg/m^3',
        'scale_max': 250e-9,
        'idx_tokens': [':MASSDEN:', ':8 m above ground:', 'Particulate organic matter dry'],
        'matcher': lambda da: (
            str(da.attrs.get('GRIB_shortName', '')).lower() == 'massden'
            and 'particulate organic matter dry' in json.dumps(da.attrs).lower()
        ),
    },
    'trc1_full_int': {
        'label': 'Vertically integrated smoke',
        'units': 'kg/m^2',
        'scale_max': 0.6,
        'idx_tokens': [':COLMD:', 'Particulate organic matter dry'],
        'matcher': lambda da: (
            str(da.attrs.get('GRIB_shortName', '')).lower() == 'colmd'
            and 'particulate organic matter dry' in json.dumps(da.attrs).lower()
        ),
    },
}


def latest_cycle_utc(now=None):
    now = now or datetime.now(timezone.utc)
    cycle_hour = (now.hour // 1) * 1
    return now.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)


def cycle_candidates(long_only=False):
    dt = latest_cycle_utc()
    for step in range(0, 48):
        cand = dt - timedelta(hours=step)
        if long_only and cand.hour not in (0, 6, 12, 18):
            continue
        yield cand


def list_bucket(prefix: str):
    params = {'delimiter': '/', 'prefix': prefix}
    r = SESSION.get(S3_BUCKET + '/', params=params, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.text)


def list_cycle_files(day_ymd: str, hh: str):
    root = list_bucket(f'{PREFIX_ROOT}/rrfs.{day_ymd}/{hh}/')
    files = []
    ns = '{http://s3.amazonaws.com/doc/2006-03-01/}'
    for ct in root.findall(f'{ns}Contents'):
        key_elem = ct.find(f'{ns}Key')
        if key_elem is None or not key_elem.text:
            continue
        key = key_elem.text
        if '/rrfs.t' not in key or not key.endswith('.grib2'):
            continue
        if '.subh.' in key:
            continue
        if '.natlev.' not in key and '.nat.' not in key:
            continue
        if '.na.grib2' not in key:
            continue
        files.append(key)
    return sorted(files)


def find_cycle_file(run_dt, fxx):
    day_ymd = run_dt.strftime('%Y%m%d')
    hh = run_dt.strftime('%H')
    ff = f'.f{fxx:03d}.'
    files = list_cycle_files(day_ymd, hh)
    preferred = [k for k in files if ff in k and '.natlev.' in k]
    if preferred:
        return preferred[0]
    fallback = [k for k in files if ff in k]
    if fallback:
        return fallback[0]
    raise RuntimeError(f'no RRFS nat/natlev file found for {day_ymd} {hh}z F{fxx:03d}')


def parse_idx_range(idx_text: str, tokens):
    lines = [ln for ln in idx_text.strip().splitlines() if ln.strip()]
    selected = None
    for i, line in enumerate(lines):
        if all(token in line for token in tokens):
            parts = line.split(':')
            if len(parts) < 3:
                continue
            start = int(parts[1])
            end = int(lines[i + 1].split(':')[1]) - 1 if i + 1 < len(lines) else None
            selected = (start, end, line)
            break
    return selected


def download_field_subset(run_dt, fxx, layer_key):
    key = find_cycle_file(run_dt, fxx)
    grib_url = f'{S3_BUCKET}/{key}'
    idx_url = grib_url + '.idx'
    idx_res = SESSION.get(idx_url, timeout=30)
    idx_res.raise_for_status()
    parsed = parse_idx_range(idx_res.text, LAYERS[layer_key]['idx_tokens'])
    if not parsed:
        raise RuntimeError(f'field not found in idx for {Path(key).name}')
    start, end, matched_line = parsed

    ymdh = run_dt.strftime('%Y%m%d%H')
    out_dir = DOWNLOADS / ymdh / layer_key
    out_dir.mkdir(parents=True, exist_ok=True)
    local = out_dir / f'f{fxx:03d}.grib2'
    if local.exists() and local.stat().st_size > 1024:
        return local, grib_url, matched_line

    headers = {'Range': f'bytes={start}-{end}' if end is not None else f'bytes={start}-'}
    with SESSION.get(grib_url, headers=headers, timeout=180, stream=True) as r:
        r.raise_for_status()
        with open(local, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return local, grib_url, matched_line


def open_subset_dataset(grib_path):
    datasets = cfgrib.open_datasets(str(grib_path), backend_kwargs={'indexpath': '', 'errors': 'ignore'})
    return datasets


def find_matching_var(datasets, matcher):
    attempts = []
    for ds in datasets:
        for name, da in ds.data_vars.items():
            meta = {
                'name': name,
                'shortName': da.attrs.get('GRIB_shortName'),
                'typeOfLevel': da.attrs.get('GRIB_typeOfLevel'),
                'level': da.attrs.get('GRIB_level'),
                'name_full': da.attrs.get('GRIB_name'),
            }
            attempts.append(meta)
            if matcher(da):
                return da.squeeze(), attempts
    raise RuntimeError(f'no matching variable found; scanned={attempts[:20]}')


def extract_xy(var):
    x_name = next((n for n in var.coords if n.lower() in ('x', 'projection_x_coordinate')), None)
    y_name = next((n for n in var.coords if n.lower() in ('y', 'projection_y_coordinate')), None)
    if x_name is not None and y_name is not None:
        return np.asarray(var.coords[x_name].values, dtype='float64'), np.asarray(var.coords[y_name].values, dtype='float64')

    lon_name = next((n for n in var.coords if n.lower() == 'longitude'), None)
    lat_name = next((n for n in var.coords if n.lower() == 'latitude'), None)
    if lon_name and lat_name:
        lon = np.asarray(var.coords[lon_name].values, dtype='float64')
        lat = np.asarray(var.coords[lat_name].values, dtype='float64')
        if lon.ndim == 2 and lat.ndim == 2:
            return lon[0, :], lat[:, 0]

    raise RuntimeError(f'could not find x/y coords; coords={list(var.coords)}')


def build_source_transform(var):
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
    dst_transform, dst_width, dst_height = calculate_default_transform(src_crs, 'EPSG:4326', width, height, left, bottom, right, top, resolution=0.05)
    dst = np.zeros((4, dst_height, dst_width), dtype='uint8')
    for band in range(4):
        reproject(source=rgba[:, :, band], destination=dst[band], src_transform=src_transform, src_crs=src_crs, dst_transform=dst_transform, dst_crs='EPSG:4326', resampling=Resampling.bilinear, src_nodata=0, dst_nodata=0)
    bounds = rasterio.transform.array_bounds(dst_height, dst_width, dst_transform)
    leaflet_bounds = [[bounds[1], bounds[0]], [bounds[3], bounds[2]]]
    return np.moveaxis(dst, 0, -1), leaflet_bounds


def save_png(rgba, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgba, mode='RGBA').save(path)


def load_layer_data(run_dt, frame, layer_key):
    grib_path, url, matched_line = download_field_subset(run_dt, frame, layer_key)
    datasets = open_subset_dataset(grib_path)
    var, scanned = find_matching_var(datasets, LAYERS[layer_key]['matcher'])
    return grib_path, url, matched_line, var, scanned


def render_mode(runtime_dt, mode_name, frame_limit):
    runtime = runtime_dt.strftime('%Y%m%d%H')
    manifest = {'runtime': runtime, 'maxFrame': 0, 'bounds': None, 'logs': [f'using runtime {runtime} from RRFS byte-range GRIB extraction ({mode_name})'], 'layers': {}}
    for key, layer in LAYERS.items():
        layer_frames, available, layer_bounds = [], [], None
        for frame in range(frame_limit + 1):
            try:
                grib_path, url, matched_line, var, _ = load_layer_data(runtime_dt, frame, key)
                if var.ndim != 2:
                    raise RuntimeError(f'unexpected dims: {var.dims}')
                src_crs, src_transform, src_bounds = build_source_transform(var)
                rgba = smoke_rgba(np.asarray(var.values), layer['scale_max'])
                warped, layer_bounds = warp_rgba(rgba, src_transform, src_crs, src_bounds)
                rel = f'./cache-raw/{runtime}/{mode_name}/{key}/f{frame:03d}.png'
                save_png(warped, PUBLIC / rel.replace('./', ''))
                layer_frames.append({'frame': frame, 'url': rel, 'cached': True, 'source': url, 'idxMatch': matched_line})
                available.append(frame)
                manifest['logs'].append(f'cached {key} F{frame:03d} from {grib_path.name}')
            except Exception as e:
                layer_frames.append({'frame': frame, 'url': None, 'cached': False, 'error': str(e)})
                manifest['logs'].append(f'failed {key} F{frame:03d}: {e}')
        manifest['layers'][key] = {'label': layer['label'], 'units': layer['units'], 'frames': layer_frames, 'availableFrames': available}
        if available:
            manifest['maxFrame'] = max(manifest['maxFrame'], available[-1])
        if layer_bounds is not None and manifest['bounds'] is None:
            manifest['bounds'] = layer_bounds
    return manifest


def runtime_has_smoke(run_dt):
    grib_path, url, matched_line, var, _ = load_layer_data(run_dt, 0, 'trc1_full_sfc')
    return grib_path, url, matched_line, var


def find_runtime(long_only=False):
    logs = []
    for run_dt in cycle_candidates(long_only=long_only):
        try:
            grib_path, url, matched_line, var = runtime_has_smoke(run_dt)
            logs.append(f'validated runtime {run_dt.strftime("%Y%m%d%H")} with {grib_path.name}')
            return run_dt, logs
        except Exception as e:
            logs.append(f'failed runtime {run_dt.strftime("%Y%m%d%H")}: {e}')
    raise RuntimeError('\n'.join(logs) or 'no RRFS runtime found')


def main():
    PUBLIC.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)

    short_run, short_logs = find_runtime(long_only=False)
    short_runtime = short_run.strftime('%Y%m%d%H')
    manifest = {'generatedAt': datetime.now(timezone.utc).isoformat(), 'runtime': short_runtime, 'runtimeSource': 'aws-noaa-rrfs-byte-range', 'modes': {}}

    hourly = render_mode(short_run, 'hourly', MAX_SHORT_FRAME)
    hourly['logs'] = short_logs + hourly['logs']
    manifest['modes']['hourly'] = hourly

    try:
        long_run, long_logs = find_runtime(long_only=True)
        long_mode = render_mode(long_run, 'long', MAX_LONG_FRAME)
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
