const DEFAULT_BOUNDS = [[21.5, -129.5], [52.5, -61.0]];

const els = {
  layerSelect: document.querySelector('#layerSelect'),
  frameSlider: document.querySelector('#frameSlider'),
  frameLabel: document.querySelector('#frameLabel'),
  playButton: document.querySelector('#playButton'),
  refreshButton: document.querySelector('#refreshButton'),
  opacitySlider: document.querySelector('#opacitySlider'),
  openNoaaLink: document.querySelector('#openNoaaLink'),
  statusBox: document.querySelector('#statusBox'),
  runtimeMeta: document.querySelector('#runtimeMeta'),
  debugConsole: document.querySelector('#debugConsole'),
  copyConsoleButton: document.querySelector('#copyConsoleButton'),
  clearConsoleButton: document.querySelector('#clearConsoleButton'),
  plumeCount: document.querySelector('#plumeCount'),
  smokyArea: document.querySelector('#smokyArea'),
  opaquePixels: document.querySelector('#opaquePixels'),
};

const state = {
  manifest: null,
  frame: 0,
  layer: els.layerSelect.value,
  playing: false,
  timer: null,
  opacity: Number(els.opacitySlider.value),
  overlay: null,
  logLines: [],
  diagnosticsToken: 0,
};

function log(message, extra) {
  const timestamp = new Date().toISOString();
  const line = `[${timestamp}] ${message}${extra ? ` ${JSON.stringify(extra)}` : ''}`;
  state.logLines.push(line);
  if (state.logLines.length > 300) state.logLines.shift();
  els.debugConsole.textContent = state.logLines.join('\n');
  els.debugConsole.scrollTop = els.debugConsole.scrollHeight;
  console.log(message, extra || '');
}

function setDiagnostics(plumes, areaSqMi, opaquePixels) {
  els.plumeCount.textContent = plumes == null ? '—' : String(plumes);
  els.smokyArea.textContent = areaSqMi == null ? '—' : `${Math.round(areaSqMi).toLocaleString()} sq mi`;
  els.opaquePixels.textContent = opaquePixels == null ? '—' : opaquePixels.toLocaleString();
}

const map = L.map('map', { zoomControl: true, minZoom: 3, maxZoom: 10 }).setView([39.5, -98.35], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap contributors' }).addTo(map);

const smokeBoundsRect = L.rectangle(DEFAULT_BOUNDS, { color: '#ffb347', weight: 1, fillOpacity: 0.05, dashArray: '6 6' }).addTo(map);
smokeBoundsRect.bindTooltip('Approximate RRFS smoke domain extent');

const metaControl = L.control({ position: 'topright' });
metaControl.onAdd = () => {
  const div = L.DomUtil.create('div', 'map-badge');
  div.id = 'mapBadge';
  div.textContent = 'Loading smoke…';
  return div;
};
metaControl.addTo(map);

function setMapBadge(text) {
  const badge = document.querySelector('#mapBadge');
  if (badge) badge.textContent = text;
}

function setStatus(text) {
  els.statusBox.textContent = text;
  log(`status: ${text}`);
}

function padFrame(frame) {
  return String(frame).padStart(3, '0');
}

function getLayerData() {
  return state.manifest?.layers?.[state.layer] || null;
}

function getFrameRecord() {
  const layer = getLayerData();
  return layer?.frames?.find((f) => f.frame === state.frame) || null;
}

function currentLocalUrl() {
  return getFrameRecord()?.url || null;
}

async function analyzeOverlay(url, bounds) {
  const token = ++state.diagnosticsToken;
  setDiagnostics(null, null, null);
  try {
    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.src = `${url}${url.includes('?') ? '&' : '?'}diag=${Date.now()}`;
    await new Promise((resolve, reject) => {
      img.onload = resolve;
      img.onerror = reject;
    });
    if (token !== state.diagnosticsToken) return;

    const canvas = document.createElement('canvas');
    canvas.width = img.width;
    canvas.height = img.height;
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    ctx.drawImage(img, 0, 0);
    const { data } = ctx.getImageData(0, 0, img.width, img.height);

    const width = img.width;
    const height = img.height;
    const mask = new Uint8Array(width * height);
    let opaquePixels = 0;
    for (let i = 0; i < width * height; i += 1) {
      const alpha = data[i * 4 + 3];
      if (alpha > 8) {
        mask[i] = 1;
        opaquePixels += 1;
      }
    }

    let plumes = 0;
    const queue = new Uint32Array(width * height);
    for (let i = 0; i < mask.length; i += 1) {
      if (!mask[i]) continue;
      plumes += 1;
      let head = 0;
      let tail = 0;
      queue[tail++] = i;
      mask[i] = 0;
      while (head < tail) {
        const idx = queue[head++];
        const x = idx % width;
        const y = (idx / width) | 0;
        const neighbors = [idx - 1, idx + 1, idx - width, idx + width];
        if (x === 0) neighbors[0] = -1;
        if (x === width - 1) neighbors[1] = -1;
        if (y === 0) neighbors[2] = -1;
        if (y === height - 1) neighbors[3] = -1;
        for (const n of neighbors) {
          if (n >= 0 && mask[n]) {
            mask[n] = 0;
            queue[tail++] = n;
          }
        }
      }
    }

    const latSpan = Math.abs(bounds[1][0] - bounds[0][0]);
    const lonSpan = Math.abs(bounds[1][1] - bounds[0][1]);
    const centerLat = (bounds[0][0] + bounds[1][0]) / 2;
    const milesPerLat = 69.0;
    const milesPerLon = 69.172 * Math.cos((centerLat * Math.PI) / 180);
    const totalAreaSqMi = latSpan * milesPerLat * lonSpan * milesPerLon;
    const smokyAreaSqMi = (opaquePixels / (width * height || 1)) * totalAreaSqMi;

    setDiagnostics(plumes, smokyAreaSqMi, opaquePixels);
    log('diagnostics updated', { plumes, smokyAreaSqMi: Math.round(smokyAreaSqMi), opaquePixels, width, height });
  } catch (error) {
    log('diagnostics failed', { message: error.message, url });
    setDiagnostics(null, null, null);
  }
}

function applyOverlay(url) {
  const bounds = state.manifest?.bounds || DEFAULT_BOUNDS;
  if (state.overlay) {
    state.overlay.setBounds(bounds);
    state.overlay.setUrl(url);
    state.overlay.setOpacity(state.opacity);
    analyzeOverlay(url, bounds);
    return;
  }

  state.overlay = L.imageOverlay(url, bounds, { opacity: state.opacity, interactive: false }).addTo(map);
  state.overlay.on('load', () => {
    log('overlay loaded', { layer: state.layer, frame: state.frame, url: currentLocalUrl() });
  });
  state.overlay.on('error', () => {
    const record = getFrameRecord();
    log('overlay error', record || { layer: state.layer, frame: state.frame });
    setStatus(`Cached image missing for ${state.layer} F${padFrame(state.frame)}.`);
  });
  analyzeOverlay(url, bounds);
}

function availableFrames() {
  return getLayerData()?.availableFrames || [];
}

function nearestAvailableFrame(requested) {
  const available = availableFrames();
  if (available.includes(requested)) return requested;
  const next = available.find((f) => f >= requested);
  if (next != null) return next;
  return available[available.length - 1] ?? 0;
}

function updateMap() {
  if (!state.manifest) return;
  const adjusted = nearestAvailableFrame(state.frame);
  if (adjusted !== state.frame) {
    log('adjusted frame to available cache', { requested: state.frame, adjusted });
    state.frame = adjusted;
    els.frameSlider.value = String(adjusted);
  }

  const frame = padFrame(state.frame);
  const layer = getLayerData();
  const label = layer?.label || state.layer;
  const record = getFrameRecord();
  if (!record?.url) {
    setStatus(`No cached frame available for ${label} F${frame}.`);
    setDiagnostics(null, null, null);
    return;
  }

  applyOverlay(record.url);
  setMapBadge(`${label} · runtime ${state.manifest.runtime} · F${frame}`);
  els.frameLabel.textContent = `F${frame}`;
  els.openNoaaLink.href = record?.remoteUrl || 'https://rapidrefresh.noaa.gov/RRFS-SD/';
  setStatus(`Showing ${label}, runtime ${state.manifest.runtime}, forecast F${frame}.`);
}

async function loadManifest() {
  setStatus('Loading smoke manifest…');
  try {
    const response = await fetch(`./latest.json?ts=${Date.now()}`, { cache: 'no-store' });
    if (!response.ok) throw new Error(`Manifest HTTP ${response.status}`);
    const manifest = await response.json();
    state.manifest = manifest;
    smokeBoundsRect.setBounds(manifest.bounds || DEFAULT_BOUNDS);
    els.frameSlider.max = String(manifest.maxFrame ?? 18);
    els.runtimeMeta.textContent = `Runtime: ${manifest.runtime} · source: ${manifest.runtimeSource} · generated: ${manifest.generatedAt}`;
    log('manifest loaded', { runtime: manifest.runtime, source: manifest.runtimeSource, logs: manifest.logs?.slice(-8) });
    updateMap();
  } catch (error) {
    log('manifest load failed', { message: error.message });
    setStatus('Failed to load local smoke manifest from GitHub Pages.');
    setMapBadge('Manifest load failed');
  }
}

function stopPlayback() {
  state.playing = false;
  els.playButton.textContent = 'Play';
  if (state.timer) clearInterval(state.timer);
  state.timer = null;
  log('playback stopped');
}

function startPlayback() {
  state.playing = true;
  els.playButton.textContent = 'Pause';
  const available = availableFrames();
  log('playback started', { available });
  state.timer = setInterval(() => {
    const idx = available.indexOf(state.frame);
    const next = available[(idx + 1) % available.length] ?? available[0] ?? 0;
    state.frame = next;
    els.frameSlider.value = String(state.frame);
    updateMap();
  }, 1200);
}

els.layerSelect.addEventListener('change', () => {
  state.layer = els.layerSelect.value;
  log('layer changed', { layer: state.layer });
  updateMap();
});

els.frameSlider.addEventListener('input', () => {
  state.frame = Number(els.frameSlider.value);
  log('frame changed', { frame: state.frame });
  updateMap();
});

els.opacitySlider.addEventListener('input', () => {
  state.opacity = Number(els.opacitySlider.value);
  if (state.overlay) state.overlay.setOpacity(state.opacity);
  log('opacity changed', { opacity: state.opacity });
});

els.playButton.addEventListener('click', () => {
  if (state.playing) stopPlayback();
  else startPlayback();
});

els.refreshButton.addEventListener('click', () => {
  log('manual manifest reload');
  loadManifest();
});

els.copyConsoleButton.addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(els.debugConsole.textContent || '');
    log('console copied');
  } catch (error) {
    log('console copy failed', { message: error.message });
  }
});

els.clearConsoleButton.addEventListener('click', () => {
  state.logLines = [];
  els.debugConsole.textContent = '';
  log('console cleared');
});

loadManifest();
