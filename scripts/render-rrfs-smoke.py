#!/usr/bin/env python3
import json
import os
import re
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pygrib
import requests
from PIL import Image
from pyproj import Transformer
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / 'public'
OUT = PUBLIC / 'latest.json'
CACHE = PUBLIC / 'cache-raw'
RRFS_S3_BUCKET = 'https://noaa-rrfs-pds.s3.amazonaws.com'
MAX_SHORT_FRAME = 18
MAX_LONG_FRAME = 48
REQUEST_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 300
HTTP_RETRIES = 4
HTTP_BACKOFF_SECONDS = 2.0
RRFS_REPROJ_CACHE = {}

LAYERS = {
    'trc1_full_sfc': {
        'label': 'Near-surface smoke',
        'units': 'kg/m^3',
        'scale_max': 250e-9,
    },
    'trc1_full_int': {
        'label': 'Vertically integrated smoke',
        'units': 'kg/m^2 (proxy)',
        'scale_max': 5e-6,
    },
}


def log(msg):
    print(msg, flush=True)


def http_get_with_retries(url: str, *, params=None, headers=None, timeout=REQUEST_TIMEOUT, stream=False):
    last_error = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                stream=stream,
            )
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_error = exc
            if attempt == HTTP_RETRIES:
                break
            sleep_s = HTTP_BACKOFF_SECONDS * attempt
            log(f'HTTP retry {attempt}/{HTTP_RETRIES - 1} for {url}: {exc}; sleeping {sleep_s:.1f}s')
            time.sleep(sleep_s)
    raise RuntimeError(f'HTTP request failed after {HTTP_RETRIES} attempts for {url}: {last_error}') from last_error


def _s3_list(prefix: str) -> str:
    resp = http_get_with_retries(
        f'{RRFS_S3_BUCKET}/',
        params={'delimiter': '/', 'prefix': prefix},
        timeout=REQUEST_TIMEOUT,
    )
    return resp.text


def _parse_common_prefixes(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    return [
        pref.text for cp in root.findall('s3:CommonPrefixes', ns)
        if (pref := cp.find('s3:Prefix', ns)) is not None and pref.text
    ]


def _parse_contents(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    return [
        key.text for ct in root.findall('s3:Contents', ns)
        if (key := ct.find('s3:Key', ns)) is not None and key.text
    ]


def discover_latest_rrfs_cycle(min_hours=1, long_only=False):
    day_prefixes = _parse_common_prefixes(_s3_list('rrfs_a/'))
    day_prefixes = sorted([p for p in day_prefixes if 'rrfs_a/rrfs.' in p])
    for day_prefix in reversed(day_prefixes):
        day = day_prefix.replace('rrfs_a/rrfs.', '').strip('/ ')
        hour_prefixes = _parse_common_prefixes(_s3_list(day_prefix))
        hours = sorted([h.strip('/').split('/')[-1] for h in hour_prefixes])
        for hour in reversed(hours):
            if long_only and hour not in ('00', '06', '12', '18'):
                continue
            contents = _parse_contents(_s3_list(f'{day_prefix}{hour}/'))
            natlev_keys = [k for k in contents if '.natlev.3km.' in k and '.na.grib2' in k and not k.endswith('.idx')]
            forecast_hours = []
            for k in natlev_keys:
                try:
                    forecast_hours.append(int(k.split('.f')[1].split('.')[0]))
                except Exception:
                    pass
            if forecast_hours and max(forecast_hours) >= (min_hours - 1):
                return day, hour
    return None, None


def build_rrfs_key(cycle_date: str, cycle_hour: str, forecast_hour: int) -> str:
    return f'rrfs_a/rrfs.{cycle_date}/{cycle_hour}/rrfs.t{cycle_hour}z.natlev.3km.f{forecast_hour:03d}.na.grib2'


def parse_idx_entries(idx_url: str):
    resp = http_get_with_retries(idx_url, timeout=REQUEST_TIMEOUT)

    rows = []
    for line in resp.text.strip().splitlines():
        parts = line.split(':')
        if len(parts) < 4:
            continue
        try:
            rows.append({'start': int(parts[1]), 'meta': ':'.join(parts[3:]), 'line': line})
        except Exception:
            continue

    for i in range(len(rows) - 1):
        rows[i]['end'] = rows[i + 1]['start'] - 1
    if rows:
        rows[-1]['end'] = None
    return rows


def select_smoke_entries(rows, layer_key):
    pom = [r for r in rows if 'massden' in r['meta'].lower() and 'particulate organic matter dry' in r['meta'].lower()]
    if layer_key == 'trc1_full_sfc':
        hybrid = []
        for r in pom:
            m = re.search(r'MASSDEN:(\d+) hybrid level', r['meta'], re.I)
            if m:
                hybrid.append((int(m.group(1)), r))
        if not hybrid:
            raise RuntimeError('no particulate organic matter dry MASSDEN hybrid levels found')
        hybrid.sort(key=lambda x: x[0])
        return [hybrid[0][1]]

    if layer_key == 'trc1_full_int':
        hybrid = []
        for r in pom:
            m = re.search(r'MASSDEN:(\d+) hybrid level', r['meta'], re.I)
            if m:
                hybrid.append((int(m.group(1)), r))
        if not hybrid:
            raise RuntimeError('no particulate organic matter dry MASSDEN hybrid levels found for integration')
        hybrid.sort(key=lambda x: x[0])
        return [r for _, r in hybrid]

    raise RuntimeError(f'unknown layer {layer_key}')


def download_entry_bytes(grib_url: str, entry: dict) -> bytes:
    headers = {'Range': f'bytes={entry["start"]}-{"" if entry["end"] is None else entry["end"]}'}
    resp = http_get_with_retries(grib_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
    return resp.content


def build_rrfs_webmercator_grid(lats, lons, resolution_m=8000, sample_stride=25):
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:3857', always_xy=True)
    merc_max = 85.05112878
    lats_clip = np.clip(lats, -merc_max, merc_max)
    xs_full, ys_full = transformer.transform(lons, lats_clip)
    x_min, x_max = xs_full.min(), xs_full.max()
    y_min, y_max = ys_full.min(), ys_full.max()

    nx = int(np.floor((x_max - x_min) / resolution_m)) + 1
    ny = int(np.floor((y_max - y_min) / resolution_m)) + 1
    x_grid = np.linspace(x_min, x_max, nx)
    y_grid = np.linspace(y_min, y_max, ny)
    Xi, Yi = np.meshgrid(x_grid, y_grid)

    flat_idx = np.arange(xs_full.size, dtype=np.int64)[::sample_stride]
    xs_sample = xs_full.ravel()[flat_idx]
    ys_sample = ys_full.ravel()[flat_idx]
    tree = cKDTree(np.column_stack((xs_sample, ys_sample)))
    _, nearest_idx = tree.query(np.column_stack((Xi.ravel(), Yi.ravel())), k=1, workers=-1)

    return {
        'x_grid': x_grid,
        'y_grid': y_grid,
        'nearest_idx': nearest_idx,
        'sample_indices': flat_idx,
        'shape': Xi.shape,
        'transformer': transformer,
    }


def reproject_rrfs_to_webmercator(data, reproj_grid):
    flat = data.ravel()[reproj_grid['sample_indices']]
    return flat[reproj_grid['nearest_idx']].reshape(reproj_grid['shape'])


def smoke_rgba(data, scale_max):
    arr = np.nan_to_num(data.astype('float64'), nan=0.0, posinf=0.0, neginf=0.0)
    arr[arr < 0] = 0
    norm = np.clip(arr / scale_max, 0, 1)
    alpha = np.clip(np.power(norm, 0.65) * 255, 0, 255).astype('uint8')
    r = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 236, 200, 93]).astype('uint8')
    g = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 186, 93, 33]).astype('uint8')
    b = np.interp(norm, [0, 0.1, 0.25, 0.5, 0.75, 1.0], [0, 150, 201, 79, 30, 4]).astype('uint8')
    rgba = np.dstack([r, g, b, alpha])
    rgba[alpha == 0] = 0
    return rgba


def generate_png_from_grib(values, lats, lons, scale_max, output_path):
    cache_key = (values.shape, round(float(np.nanmin(lats)), 4), round(float(np.nanmax(lats)), 4), round(float(np.nanmin(lons)), 4), round(float(np.nanmax(lons)), 4))
    if cache_key not in RRFS_REPROJ_CACHE:
        RRFS_REPROJ_CACHE[cache_key] = build_rrfs_webmercator_grid(lats, lons)
    reproj = RRFS_REPROJ_CACHE[cache_key]

    clean = values.astype(np.float32)
    invalid_mask = (~np.isfinite(clean)) | (clean < 0) | (clean > 1e6)
    clean[invalid_mask] = np.nan
    grid_data = reproject_rrfs_to_webmercator(clean, reproj)
    rgba = smoke_rgba(grid_data, scale_max)

    alpha = rgba[..., 3]
    if not np.any(alpha):
        raise RuntimeError('tile fully transparent')

    full_rgba = np.flipud(rgba)

    x_min, x_max = reproj['x_grid'][0], reproj['x_grid'][-1]
    y_min, y_max = reproj['y_grid'][0], reproj['y_grid'][-1]
    lon_tl, lat_tl = reproj['transformer'].transform(x_min, y_max, direction='INVERSE')
    lon_tr, lat_tr = reproj['transformer'].transform(x_max, y_max, direction='INVERSE')
    lon_bl, lat_bl = reproj['transformer'].transform(x_min, y_min, direction='INVERSE')
    lon_br, lat_br = reproj['transformer'].transform(x_max, y_min, direction='INVERSE')

    bounds = {
        'north': float(max(lat_tl, lat_tr, lat_bl, lat_br)),
        'south': float(min(lat_tl, lat_tr, lat_bl, lat_br)),
        'east': float(max(lon_tl, lon_tr, lon_bl, lon_br)),
        'west': float(min(lon_tl, lon_tr, lon_bl, lon_br)),
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(full_rgba, 'RGBA').save(output_path, 'PNG', optimize=True, compress_level=9)
    return bounds


def fetch_layer_slice(cycle_date: str, cycle_hour: str, forecast_hour: int, layer_key: str):
    key = build_rrfs_key(cycle_date, cycle_hour, forecast_hour)
    grib_url = f'{RRFS_S3_BUCKET}/{key}'
    rows = parse_idx_entries(f'{grib_url}.idx')
    entries = select_smoke_entries(rows, layer_key)

    if layer_key == 'trc1_full_sfc':
        entry = entries[0]
        with tempfile.NamedTemporaryFile(delete=False, suffix='.grib2') as tmp:
            tmp.write(download_entry_bytes(grib_url, entry))
            tmp_path = tmp.name
        try:
            grbs = pygrib.open(tmp_path)
            grb = grbs[1]
            values = grb.values
            lats, lons = grb.latlons()
            grbs.close()
            lons = np.where(lons > 180, lons - 360, lons)
            return values, lats, lons, grib_url, [entry['meta']]
        finally:
            os.unlink(tmp_path)

    values = None
    lats = lons = None
    metas = []
    for idx, entry in enumerate(entries, start=1):
        log(f'[{cycle_date}{cycle_hour}] {layer_key} F{forecast_hour:03d}: level {idx}/{len(entries)}')
        with tempfile.NamedTemporaryFile(delete=False, suffix='.grib2') as tmp:
            tmp.write(download_entry_bytes(grib_url, entry))
            tmp_path = tmp.name
        try:
            grbs = pygrib.open(tmp_path)
            grb = grbs[1]
            arr = grb.values.astype(np.float64)
            if lats is None:
                lats, lons = grb.latlons()
                lons = np.where(lons > 180, lons - 360, lons)
                values = np.zeros_like(arr, dtype=np.float64)
            values += np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
            metas.append(entry['meta'])
            grbs.close()
        finally:
            os.unlink(tmp_path)
    return values, lats, lons, grib_url, metas


def render_mode(cycle_date: str, cycle_hour: str, mode_name: str, frame_limit: int):
    runtime = f'{cycle_date}{cycle_hour}'
    manifest = {
        'runtime': runtime,
        'maxFrame': 0,
        'bounds': None,
        'logs': [f'using runtime {runtime} via RRFS S3 idx byte ranges ({mode_name})'],
        'layers': {},
    }

    for layer_key, layer in LAYERS.items():
        frames = []
        available = []
        for frame in range(frame_limit + 1):
            try:
                log(f'[{runtime}] {layer_key} F{frame:03d}: fetching smoke slices')
                values, lats, lons, grib_url, metas = fetch_layer_slice(cycle_date, cycle_hour, frame, layer_key)
                rel = f'./cache-raw/{runtime}/{mode_name}/{layer_key}/f{frame:03d}.png'
                bounds = generate_png_from_grib(values, lats, lons, layer['scale_max'], PUBLIC / rel.replace('./', ''))
                frames.append({'frame': frame, 'url': rel, 'cached': True, 'source': grib_url, 'meta': metas[:3]})
                available.append(frame)
                if manifest['bounds'] is None:
                    manifest['bounds'] = [[bounds['south'], bounds['west']], [bounds['north'], bounds['east']]]
                manifest['logs'].append(f'cached {layer_key} F{frame:03d}')
                manifest['maxFrame'] = max(manifest['maxFrame'], frame)
            except Exception as e:
                frames.append({'frame': frame, 'url': None, 'cached': False, 'error': str(e)})
                manifest['logs'].append(f'failed {layer_key} F{frame:03d}: {e}')
                log(f'[{runtime}] {layer_key} F{frame:03d}: FAILED {e}')
        manifest['layers'][layer_key] = {
            'label': layer['label'],
            'units': layer['units'],
            'frames': frames,
            'availableFrames': available,
        }
    return manifest


def find_runtime(long_only=False, min_hours=1):
    cycle_date, cycle_hour = discover_latest_rrfs_cycle(min_hours=min_hours, long_only=long_only)
    if not cycle_date:
        raise RuntimeError(f'no RRFS cycle discovered from S3 listing with min_hours={min_hours}')
    return cycle_date, cycle_hour, [f'discovered runtime {cycle_date}{cycle_hour} from RRFS S3 listing (min_hours={min_hours})']


def main():
    PUBLIC.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)

    cycle_date, cycle_hour, short_logs = find_runtime(long_only=False, min_hours=MAX_SHORT_FRAME + 1)
    runtime = f'{cycle_date}{cycle_hour}'
    manifest = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'runtime': runtime,
        'runtimeSource': 'aws-noaa-rrfs-direct-byte-range',
        'modes': {},
    }

    hourly = render_mode(cycle_date, cycle_hour, 'hourly', MAX_SHORT_FRAME)
    hourly['logs'] = short_logs + hourly['logs']
    manifest['modes']['hourly'] = hourly

    try:
        long_date, long_hour, long_logs = find_runtime(long_only=True, min_hours=MAX_LONG_FRAME + 1)
        long_mode = render_mode(long_date, long_hour, 'long', MAX_LONG_FRAME)
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
    log(f'Wrote {OUT} for runtime {runtime}')


if __name__ == '__main__':
    main()
