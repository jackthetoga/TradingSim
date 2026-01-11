"""
Simulator/Simulator.py

HTML server + real-time replay UI for Databento parquet files:
- MBP-10 (L2 top 10 book)
- trades (time & sales)
- ohlcv-1s (chart)

Run:
  python /Users/zizizink/Documents/TradingProject/Simulator/Simulator.py

Then open:
  http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import time
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


APP = FastAPI()

# Default data location (Databento parquet outputs)
DATA_DIR_DEFAULT = Path("/Users/zizizink/Documents/TradingProject/databento_out")
LOCAL_TZ_NAME_DEFAULT = "America/New_York"
FIXED_PRICE_SCALE = 1_000_000_000  # legacy: Databento "fixed" price_type uses 1e9 scaling

# Lightweight cache for the data catalog scan (prevents repeated heavy scans on page load / refresh).
_CATALOG_CACHE: Dict[Tuple[str, str, int], Tuple[float, Dict[str, Any]]] = {}
_CATALOG_CACHE_TTL_S = 5.0

# Config persistence (saved to disk)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_DIR = PROJECT_ROOT / "Configs"
CONFIGS_ALLOWED = {
    "layout": "layout.json",
    "hotkeys": "hotkeys.json",
    "commands": "commands.json",
    "settings": "settings.json",
}


INDEX_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Sim Replay: LVL2 + Tape + Chart</title>
  <!-- TradingView Lightweight Charts (standalone build). -->
  <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    :root {
      --bg: #0b0f14;
      --panel: #111826;
      --fg: #e6edf3;
      --muted: #9aa4b2;
      --grid: #223045;
      --ask: #ff4d4d;
      --bid: #25d366;
      --mid: #e6edf3;
      --warn: #ffcc00;
    }
    body { margin: 0; background: var(--bg); color: var(--fg); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    .topbar { padding: 12px 14px; border-bottom: 1px solid var(--grid); display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
    .topbar input, .topbar select, .topbar button {
      background: #0f1723; border: 1px solid var(--grid); color: var(--fg); padding: 6px 8px; border-radius: 8px;
    }
    .topbar button { cursor: pointer; }
    .topbar button.primary { background: #14304f; border-color: #2f5a8a; }
    .topbar button.danger { background: #3a1820; border-color: #7f2a3a; }
    .topbar label { display: inline-flex; gap: 6px; align-items: center; color: var(--muted); font-size: 12px; }
    .topbar .spacer { flex: 1; }
    .error { color: #ff6b6b; font-size: 12px; }
    .hint { color: var(--muted); font-size: 12px; }

    .layout { display: grid; grid-template-columns: 420px 1fr; gap: 12px; padding: 12px; }
    .panel { background: var(--panel); border: 1px solid var(--grid); border-radius: 12px; overflow: hidden; }
    .panel h3 { margin: 0; padding: 10px 12px; font-size: 13px; color: var(--muted); border-bottom: 1px solid var(--grid); }
    .panel .content { padding: 10px 12px; }

    .right { display: grid; grid-template-rows: 1fr 420px; gap: 12px; }

    /* LVL2 */
    table.l2 { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
    table.l2 th, table.l2 td { font-size: 15px; padding: 4px 8px; border-bottom: 1px solid rgba(34,48,69,0.55); }
    table.l2 th { color: var(--muted); font-weight: 600; }
    table.l2 td { white-space: nowrap; }
    .l2row { height: 26px; }
    .bidx { text-align: right; }
    .askx { text-align: left; }
    .px { width: 88px; }
    .sz { width: 70px; }
    .lvl { width: 34px; color: var(--muted); }
    .meta { margin-top: 8px; color: var(--muted); font-size: 12px; }
    .l2Wrap { position: relative; }
    .l2TopRow { display:flex; justify-content:flex-end; align-items:center; gap:8px; margin-bottom: 8px; }
    .l2Gear { width: 30px; height: 30px; border-radius: 10px; background:#0f1723; border:1px solid var(--grid); color:var(--fg); cursor:pointer; display:flex; align-items:center; justify-content:center; }
    .l2Panel {
      position:absolute; right: 0; top: 0;
      width: 300px; max-width: calc(100% - 8px);
      border: 1px solid var(--grid); border-radius: 12px;
      background: rgba(15,23,35,0.96);
      box-shadow: 0 20px 60px rgba(0,0,0,0.55);
      padding: 10px;
      display:none;
      z-index: 1000;
    }
    .l2PanelTitle { color: var(--muted); font-size: 12px; font-weight: 900; margin-bottom: 8px; }
    .l2ColorsList { display:flex; flex-direction:column; gap: 8px; }
    .l2ColorRow { display:flex; align-items:center; gap: 8px; }
    .l2ColorIdx { color: var(--muted); font-size: 12px; width: 22px; text-align:right; font-variant-numeric: tabular-nums; }
    .l2ColorRow input[type="color"] { width: 44px; height: 30px; padding: 0; border: 1px solid var(--grid); background:#0f1723; border-radius: 10px; }
    .l2PanelBtns { display:flex; flex-wrap:wrap; gap: 8px; margin-top: 10px; }
    .l2Small { color: var(--muted); font-size: 12px; margin-top: 8px; }

    /* Tape */
    /* NOTE: keep tape window id as `tape` (for popout), but the scroll container is `tapeList`. */
    #tapeList { height: 360px; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 14px; background: #000; }
    .tline { padding: 3px 8px; border-bottom: 1px solid rgba(34,48,69,0.35); display: grid; grid-template-columns: 140px 1fr 1fr; gap: 10px; background: #000; }
    .tline .ts { color: inherit; opacity: 0.9; font-weight: 900; }
    /* Keep the same trade classification rules, but move the color to the font (background stays black). */
    .t-ask { color: rgba(37,211,102,0.95); font-weight: 900; }
    .t-bid { color: rgba(255,77,77,0.92); font-weight: 900; }
    .t-mid { color: rgba(200,200,200,0.85); font-weight: 900; }

    /* Chart */
    #chartWrap { height: 560px; }
    #chart { width: 100%; height: 100%; display: block; background: #0b1220; }
    .chartHud { display: flex; gap: 10px; align-items: center; padding: 8px 10px; border-top: 1px solid var(--grid); color: var(--muted); font-size: 12px; }
    .pill { padding: 2px 8px; border-radius: 999px; border: 1px solid var(--grid); background: rgba(0,0,0,0.1); }
  </style>
</head>
<body>
  <div class="topbar">
    <label>Symbol <input id="symbol" value="MNTS" size="6"/></label>
    <label>Day <input id="day" value="2026-01-05" size="10"/></label>
    <label>Time (ET) <input id="ts" value="2026-01-05 09:30:00" size="24"/></label>
    <label>Session
      <select id="catalog">
        <option value="">(scan sessions)</option>
      </select>
    </label>
    <label>Speed
      <select id="speed">
        <option value="0.5">0.5x</option>
        <option value="1" selected>1x</option>
        <option value="2">2x</option>
      </select>
    </label>
    <button id="load" class="primary">Load</button>
    <button id="play" class="primary">Play</button>
    <button id="pause" class="danger">Pause</button>
    <div class="spacer"></div>
    <div id="now" class="hint"></div>
    <div id="status" class="hint"></div>
    <div id="err" class="error"></div>
  </div>

  <div id="workspace"></div>

<script>
// Disk-backed configs (injected by backend). If null, we fall back to localStorage/defaults.
const DISK_LAYOUT = __DISK_LAYOUT__;
const DISK_HOTKEYS = __DISK_HOTKEYS__;
const DISK_COMMANDS = __DISK_COMMANDS__;
const DISK_SETTINGS = __DISK_SETTINGS__;

// ---------------- Windowing (drag/resize/popout) ----------------
function ensureWorkspaceStyles(){
  // convert panels into floating windows
}

// URL params
const QS = new URLSearchParams(location.search);
const POPOUT = QS.get('popout'); // l2 | tape | chart-<id>

function $(id){ return document.getElementById(id); }

// ---------------- Session catalog dropdown ----------------
let catalogMap = new Map();
async function initCatalogDropdown(){
  const sel = $('catalog');
  if (!sel) return;
  sel.disabled = true;
  sel.innerHTML = `<option value="">(scan sessions)</option>`;
  catalogMap = new Map();
  try {
    const resp = await fetch(`/api/catalog`);
    const data = await resp.json();
    if (!resp.ok){
      const detail = data?.detail || 'Failed to load catalog';
      sel.innerHTML = `<option value="">(catalog error)</option>`;
      setErr(String(detail));
      sel.disabled = false;
      return;
    }
    const items = Array.isArray(data?.items) ? data.items : [];
    sel.innerHTML = `<option value="">Sessions…</option>`;
    for (const it of items){
      const sym = String(it.symbol || '').trim();
      const day = String(it.day || '').trim();
      const ts = String(it.start_et || '').trim();
      const label = String(it.label || '').trim() || `${sym} ${day}`;
      const id = `${sym}__${day}__${Number(it.start_ts_ns ?? 0)}`;
      catalogMap.set(id, {symbol: sym, day, ts, label});
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = label;
      sel.appendChild(opt);
    }
    sel.disabled = false;
  } catch (e) {
    sel.innerHTML = `<option value="">(catalog error)</option>`;
    sel.disabled = false;
    setErr(`Catalog error: ${e?.message ?? e}`);
  }

  sel.addEventListener('change', async ()=>{
    const id = sel.value;
    if (!id) return;
    const it = catalogMap.get(id);
    if (!it) return;
    try {
      $('symbol').value = it.symbol;
      $('day').value = it.day;
      $('ts').value = it.ts;
      await doLoad(true);
    } catch (e) {
      setErr(`Failed to load session: ${e?.message ?? e}`);
    }
  });
}

let zTop = 10;
function bringToFront(win){
  zTop += 1;
  win.style.zIndex = String(zTop);
}

// Hotkey context (set based on focused window)
let activeContext = 'global'; // global | entry | chart | tape | l2
function contextForWindowId(id){
  if (!id) return 'global';
  if (id === 'entry') return 'entry';
  if (id === 'tape') return 'tape';
  if (id === 'l2') return 'l2';
  if (String(id).startsWith('chart-')) return 'chart';
  return 'global';
}

function closeWindow(win){
  if (!win) return;
  const id = String(win.id || '');
  // Charts: remove entirely (do not persist in layout, do not keep DOM node)
  if (id.startsWith('chart-')){
    try {
      const ch = charts?.get?.(id);
      try { if (ch?.sse) { ch.sse.close(); ch.sse = null; } } catch {}
      try { ch?._ro?.disconnect?.(); } catch {}
      try { ch?.tv?.remove?.(); } catch {}
      try { ch?.macdTv?.remove?.(); } catch {}
      try { charts?.delete?.(id); } catch {}
    } catch {}
    try { win.remove(); } catch { try { win.parentNode?.removeChild?.(win); } catch {} }
    try { scheduleLayoutSave(); } catch {}
    return;
  }
  // Regular windows: hide, but do not persist any layout state for this "closed" window.
  try { win.style.display = 'none'; } catch {}
  try { win.dataset.closed = '1'; } catch {}
  // Reset to defaults so re-opening doesn't "remember" the last position/size
  try {
    if (win.dataset.defaultX != null) win.style.left = `${Number(win.dataset.defaultX)}px`;
    if (win.dataset.defaultY != null) win.style.top = `${Number(win.dataset.defaultY)}px`;
    if (win.dataset.defaultW != null) win.style.width = `${Number(win.dataset.defaultW)}px`;
    if (win.dataset.defaultH != null) win.style.height = `${Number(win.dataset.defaultH)}px`;
  } catch {}
  try { scheduleLayoutSave(); } catch {}
}

function makeWindow({id, title, x, y, w, h, bodyHtml}){
  const ws = $('workspace');
  const win = document.createElement('div');
  win.className = 'win';
  win.id = id;
  win.style.left = `${x}px`;
  win.style.top = `${y}px`;
  win.style.width = `${w}px`;
  win.style.height = `${h}px`;
  win.style.zIndex = String(++zTop);
  // Remember defaults for "close" behavior (do not remember last position)
  try {
    win.dataset.defaultX = String(x);
    win.dataset.defaultY = String(y);
    win.dataset.defaultW = String(w);
    win.dataset.defaultH = String(h);
  } catch {}
  win.innerHTML = `
    <div class="wtitle">
      <div class="wlabel">${title}</div>
      <div class="wbtns">
        <button class="wbtn" data-act="pop">Pop out</button>
        <button class="wbtn" data-act="close">×</button>
      </div>
    </div>
    <div class="wbody">${bodyHtml}</div>
    <div class="wdragEdge wdragEdgeL" data-drag="edge"></div>
    <div class="wdragEdge wdragEdgeR" data-drag="edge"></div>
    <div class="wresizer"></div>
  `;
  ws.appendChild(win);
  function clampWinToWorkspace(){
    try {
      const wsW = ws?.clientWidth ?? 0;
      const wsH = ws?.clientHeight ?? 0;
      if (!Number.isFinite(wsW) || !Number.isFinite(wsH) || wsW <= 0 || wsH <= 0) return;
      const winW = parseInt(win.style.width, 10) || win.offsetWidth || 0;
      const winH = parseInt(win.style.height, 10) || win.offsetHeight || 0;
      // Keep at least part of the window accessible so it can't be "lost".
      const minVisibleX = 80;   // px of window that must remain visible horizontally
      const minVisibleY = 34;   // px (title bar) that must remain visible vertically
      let x = parseInt(win.style.left, 10) || 0;
      let y = parseInt(win.style.top, 10) || 0;
      const minX = -(Math.max(0, winW - minVisibleX));
      const maxX = wsW - minVisibleX;
      const minY = 0;
      const maxY = wsH - minVisibleY;
      x = Math.min(maxX, Math.max(minX, x));
      y = Math.min(maxY, Math.max(minY, y));
      win.style.left = `${x}px`;
      win.style.top = `${y}px`;
    } catch {}
  }
  // drag
  const bar = win.querySelector('.wtitle');
  let dragging=false, sx=0, sy=0, ox=0, oy=0, dragPid=null;
  const startDrag = (e)=>{
    if (e.target && e.target.closest('.wbtns')) return;
    dragging=true;
    bringToFront(win);
    dragPid = e.pointerId;
    win.setPointerCapture(e.pointerId);
    sx=e.clientX; sy=e.clientY;
    ox=parseInt(win.style.left,10); oy=parseInt(win.style.top,10);
  };
  bar.addEventListener('pointerdown', startDrag);
  // Chart windows can also be dragged by their side edges (helps prevent losing a chart off-screen).
  win.querySelectorAll('[data-drag="edge"]').forEach(h=>{
    h.addEventListener('pointerdown', startDrag);
  });
  // IMPORTANT: pointer capture is on `win`, so move/up/cancel handlers must be on `win`
  win.addEventListener('pointermove', (e)=>{
    if (!dragging) return;
    if (dragPid != null && e.pointerId !== dragPid) return;
    win.style.left = `${ox + (e.clientX - sx)}px`;
    win.style.top = `${oy + (e.clientY - sy)}px`;
    clampWinToWorkspace();
  });
  const endDrag = (e)=>{
    if (!dragging) return;
    if (dragPid != null && e && e.pointerId !== dragPid) return;
    dragging=false;
    try { if (dragPid != null) win.releasePointerCapture(dragPid); } catch {}
    dragPid=null;
    clampWinToWorkspace();
    try { scheduleLayoutSave(); } catch {}
  };
  win.addEventListener('pointerup', endDrag);
  win.addEventListener('pointercancel', endDrag);
  win.addEventListener('lostpointercapture', ()=>{ dragging=false; dragPid=null; });
  // resize (bottom-right)
  const rz = win.querySelector('.wresizer');
  let resizing=false, rsx=0, rsy=0, ow=0, oh=0, rzPid=null;
  rz.addEventListener('pointerdown', (e)=>{
    resizing=true;
    bringToFront(win);
    rzPid = e.pointerId;
    win.setPointerCapture(e.pointerId);
    rsx=e.clientX; rsy=e.clientY;
    ow=parseInt(win.style.width,10); oh=parseInt(win.style.height,10);
  });
  win.addEventListener('pointermove', (e)=>{
    if (!resizing) return;
    if (rzPid != null && e.pointerId !== rzPid) return;
    win.style.width = `${Math.max(260, ow + (e.clientX - rsx))}px`;
    win.style.height = `${Math.max(200, oh + (e.clientY - rsy))}px`;
    clampWinToWorkspace();
  });
  const endResize = (e)=>{
    if (!resizing) return;
    if (rzPid != null && e && e.pointerId !== rzPid) return;
    resizing=false;
    try { if (rzPid != null) win.releasePointerCapture(rzPid); } catch {}
    rzPid=null;
    clampWinToWorkspace();
    try { scheduleLayoutSave(); } catch {}
  };
  win.addEventListener('pointerup', endResize);
  win.addEventListener('pointercancel', endResize);
  win.addEventListener('lostpointercapture', ()=>{ resizing=false; rzPid=null; });

  // buttons
  win.querySelectorAll('.wbtn').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const act = btn.getAttribute('data-act');
      if (act === 'close') { closeWindow(win); }
      if (act === 'pop') {
        const url = new URL(location.href);
        url.searchParams.set('popout', id);
        url.searchParams.set('symbol', $('symbol')?.value ?? 'MNTS');
        url.searchParams.set('day', $('day')?.value ?? '2026-01-05');
        url.searchParams.set('ts', $('ts')?.value ?? '2026-01-05 09:30:00');
        url.searchParams.set('speed', $('speed')?.value ?? '1');
        window.open(url.toString(), '_blank', 'noopener,noreferrer,width=900,height=700');
      }
    });
  });

  win.addEventListener('mousedown', ()=>{
    bringToFront(win);
    try { activeContext = contextForWindowId(win.id); } catch {}
  });
  return win;
}

// Add CSS for windowing
const style = document.createElement('style');
style.textContent = `
  #workspace { position: relative; height: calc(100vh - 58px); overflow: hidden; }
  .win { position: absolute; background: var(--panel); border: 1px solid var(--grid); border-radius: 12px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.35); }
  .wtitle { height: 34px; display: flex; align-items: center; justify-content: space-between; padding: 0 10px; background: rgba(15,23,35,0.85); border-bottom: 1px solid var(--grid); cursor: grab; user-select: none; }
  .wlabel { color: var(--muted); font-size: 12px; font-weight: 700; }
  .wbtns { display: flex; gap: 6px; }
  .wbtn { padding: 4px 8px; border-radius: 8px; background: #0f1723; border: 1px solid var(--grid); color: var(--fg); font-size: 12px; cursor: pointer; }
  .wbody { height: calc(100% - 34px); padding: 10px 12px; overflow: auto; }
  /* Chart-only drag handles on the side edges (prevents losing the chart if title bar goes off-screen). */
  .wdragEdge { position: absolute; top: 34px; bottom: 16px; width: 12px; z-index: 40; display: none; background: transparent; }
  .wdragEdgeL { left: 0; cursor: grab; }
  .wdragEdgeR { right: 0; cursor: grab; }
  .win.chartWin .wdragEdge { display: block; }
  .win.chartWin .wdragEdge:hover { background: rgba(230,237,243,0.04); }
  .wresizer { position: absolute; right: 0; bottom: 0; width: 16px; height: 16px; cursor: nwse-resize;
              background: linear-gradient(135deg, transparent 50%, rgba(230,237,243,0.18) 50%); }
  /* Multi-chart canvases must fill their wrapper; otherwise indicators/volume can be drawn "below" the visible area. */
  .chartWrap { position: relative; flex: 1 1 auto; min-height: 220px; }
  .tvChart { position: absolute; inset: 0; }

  /* Settings */
  .setWrap { display:flex; gap:12px; height:100%; min-height:0; }
  .setNav { width: 170px; flex: 0 0 auto; border-right: 1px solid rgba(34,48,69,0.65); padding-right: 12px; }
  .setNavTitle { color: var(--muted); font-size: 12px; font-weight: 900; margin-bottom: 10px; }
  .setTab { width: 100%; text-align:left; padding: 8px 10px; border-radius: 10px; border: 1px solid var(--grid); background:#0f1723; color: var(--fg); cursor:pointer; font-size:12px; font-weight:900; margin-bottom: 8px; }
  .setTab.on { background:#14304f; border-color:#2f5a8a; }
  .setMain { flex: 1 1 auto; min-width: 0; height: 100%; min-height:0; overflow:hidden; }
  .setPane { height:100%; min-height:0; overflow:auto; }
  .setSection { border: 1px solid rgba(34,48,69,0.55); border-radius: 12px; padding: 10px; background: rgba(15,23,35,0.55); }
  .setRow { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
  .setRow input, .setRow select, .setRow textarea { background:#0f1723; border:1px solid var(--grid); color:var(--fg); padding:6px 8px; border-radius: 8px; }
  .setRow textarea { width: 100%; min-height: 72px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; }
  .setHint { color: var(--muted); font-size: 12px; }

  /* Order entry (DAS-like) */
  .entryWrap { display: flex; flex-direction: column; gap: 10px; height: 100%; }
  .entryGrid { display: grid; grid-template-columns: 96px 1fr; gap: 8px 10px; align-items: center; }
  .entryGrid .lbl { color: var(--muted); font-size: 12px; font-weight: 700; }
  .entryGrid input, .entryGrid select {
    width: 100%;
    background: #0f1723;
    border: 1px solid var(--grid);
    color: var(--fg);
    padding: 6px 8px;
    border-radius: 8px;
    font-variant-numeric: tabular-nums;
  }
  .entryGrid input.num { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
  .entryRow2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .entryBtns { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .entryBtn { padding: 10px 10px; border-radius: 10px; border: 1px solid var(--grid); cursor: pointer; font-weight: 900; letter-spacing: 0.3px; }
  .entryBuy { background: rgba(37,211,102,0.85); color: #000; border-color: rgba(37,211,102,0.95); }
  .entrySell { background: rgba(255,77,77,0.80); color: #000; border-color: rgba(255,77,77,0.95); }
  .entrySmallBtns { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .entrySmallBtns .wbtn { padding: 4px 8px; }
  .entryHint { color: var(--muted); font-size: 12px; }
  .entryDisabled { opacity: 0.55; pointer-events: none; }

  /* Positions / Portfolio */
  .posWrap { display:flex; flex-direction:column; gap:10px; height:100%; min-height:0; }
  .posTop { display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .posTitle { color: var(--muted); font-size: 12px; font-weight: 800; letter-spacing: 0.2px; }
  .posGear { width: 30px; height: 30px; border-radius: 10px; background:#0f1723; border:1px solid var(--grid); color:var(--fg); cursor:pointer; display:flex; align-items:center; justify-content:center; }
  .posPanel { display:none; padding:10px; border:1px solid var(--grid); border-radius: 10px; background: rgba(15,23,35,0.85); }
  .posPanel .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  .posPanel label { color: var(--muted); font-size: 12px; display:flex; gap:6px; align-items:center; }
  .posTable { width:100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
  .posTable th, .posTable td { font-size: 12px; padding: 6px 8px; border-bottom: 1px solid rgba(34,48,69,0.55); white-space: nowrap; }
  .posTable th { color: var(--muted); font-weight: 800; text-align:left; position: sticky; top: 0; background: rgba(15,23,35,0.95); }
  .posEmpty { color: var(--muted); font-size: 12px; padding: 10px 0; }
  .posUp { color: rgba(37,211,102,0.95); font-weight: 800; }
  .posDn { color: rgba(255,77,77,0.95); font-weight: 800; }

  /* Hotkeys */
  .hkWrap { display:flex; flex-direction:column; gap:10px; height:100%; min-height:0; }
  .hkTop { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .hkTop input {
    background: #0f1723; border: 1px solid var(--grid); color: var(--fg); padding: 6px 8px; border-radius: 8px;
    width: 260px;
  }
  .hkTabs { display:flex; gap:6px; flex-wrap:wrap; }
  .hkTab { padding: 6px 10px; border-radius: 10px; border: 1px solid var(--grid); background:#0f1723; color: var(--fg); cursor:pointer; font-size:12px; font-weight:800; }
  .hkTab.on { background:#14304f; border-color:#2f5a8a; }
  .hkBadge { display:inline-flex; align-items:center; gap:6px; padding:2px 8px; border-radius:999px; border:1px solid var(--grid); background: rgba(0,0,0,0.15); color: var(--muted); font-size:12px; font-weight:800; }
  .hkWarn { color: var(--warn); font-weight: 900; }
  .hkOverlay {
    position: fixed; inset: 0; background: rgba(0,0,0,0.55); z-index: 100000;
    display:none; align-items:center; justify-content:center;
  }
  .hkModal {
    width: 520px; max-width: calc(100vw - 40px);
    border: 1px solid var(--grid); border-radius: 14px; background: rgba(15,23,35,0.98);
    box-shadow: 0 20px 60px rgba(0,0,0,0.55);
    padding: 14px;
  }
  .hkModal .title { color: var(--muted); font-size: 12px; font-weight: 900; }
  .hkModal .big { margin-top:10px; font-size: 20px; font-weight: 1000; letter-spacing: 0.3px; }
  .hkModal .small { margin-top:10px; color: var(--muted); font-size: 12px; }

  /* Commands (DAS-like scripting) */
  .cmdWrap { display:grid; grid-template-columns: 280px 1fr; gap: 10px; height: 100%; min-height: 0; }
  .cmdLeft { display:flex; flex-direction:column; gap:10px; min-height:0; }
  .cmdRight { display:flex; flex-direction:column; gap:10px; min-height:0; overflow:auto; padding-bottom: 6px; }
  .cmdList { flex:1 1 auto; min-height:0; overflow:auto; border:1px solid rgba(34,48,69,0.55); border-radius:10px; }
  .cmdItem { padding:8px 10px; border-bottom:1px solid rgba(34,48,69,0.35); cursor:pointer; }
  .cmdItem:hover { background: rgba(255,255,255,0.03); }
  .cmdItem.on { background: rgba(20,48,79,0.55); }
  .cmdName { font-weight: 900; }
  .cmdSub { color: var(--muted); font-size: 12px; margin-top: 2px; }
  .cmdRow { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .cmdTa {
    flex: 0 0 220px; height: 220px; min-height: 220px; width: 100%; resize: vertical;
    background: #0f1723; border: 1px solid var(--grid); color: var(--fg);
    padding: 10px; border-radius: 10px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    font-size: 12px;
  }
  .cmdBtns { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .badge { display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px; border:1px solid var(--grid); background: rgba(0,0,0,0.15); color: var(--muted); font-size:12px; font-weight:800; }
  .cmdFooter { position: sticky; bottom: 0; background: rgba(15,23,35,0.97); padding-top: 8px; }
`;
document.head.appendChild(style);

// ---------------- Replay globals ----------------
let sseBookTape = null;
let currentBook = {bids: [], asks: [], ts_event: null};
let playheadNs = null; // authoritative "now" (monotonic; driven by incoming stream events)
let loadedStartNs = null;    // snapshot anchor (requested time)
let isPaused = true;
let lastTapeTsSeen = null;   // for monotonic tape rendering (prevents "time going backwards" on resume/reconnect)
let lastTrade = null;        // last trade seen (for initializing live higher-TF candles)
let sessionStats = { open: null, hi: null, lo: null, pcl: null }; // best-effort quote-like fields

// ---------------- Simulator settings (disk-backed via Configs/settings.json) ----------------
const SIM_SETTINGS_STORAGE_KEY = 'sim-settings-v1';
const SIM_SETTINGS_DEFAULTS = {
  version: 1,
  allowShorting: false,
  buyingPowerEnabled: true,
  buyingPower: 1800,
};
let simSettings = null;

function _normalizeSimSettings(obj){
  const out = JSON.parse(JSON.stringify(SIM_SETTINGS_DEFAULTS));
  if (!obj || typeof obj !== 'object') return out;
  if (Number(obj.version || 0) !== 1) return out;
  out.allowShorting = !!obj.allowShorting;
  out.buyingPowerEnabled = (obj.buyingPowerEnabled == null) ? out.buyingPowerEnabled : !!obj.buyingPowerEnabled;
  const bp = Number(obj.buyingPower);
  out.buyingPower = (Number.isFinite(bp) && bp >= 0) ? bp : out.buyingPower;
  return out;
}

function loadSimSettings(){
  try {
    if (typeof DISK_SETTINGS !== 'undefined' && DISK_SETTINGS && typeof DISK_SETTINGS === 'object') {
      return _normalizeSimSettings(DISK_SETTINGS);
    }
    const raw = localStorage.getItem(SIM_SETTINGS_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return _normalizeSimSettings(parsed);
  } catch {
    return JSON.parse(JSON.stringify(SIM_SETTINGS_DEFAULTS));
  }
}

function saveSimSettings(){
  if (!simSettings) return;
  try { localStorage.setItem(SIM_SETTINGS_STORAGE_KEY, JSON.stringify(simSettings)); } catch {}
  try { scheduleConfigSave('settings', simSettings); } catch {}
}

function _allowShorting(){
  if (!simSettings) simSettings = loadSimSettings();
  return !!simSettings.allowShorting;
}

function _buyingPowerEnabled(){
  if (!simSettings) simSettings = loadSimSettings();
  return !!simSettings.buyingPowerEnabled;
}

function _buyingPower(){
  if (!simSettings) simSettings = loadSimSettings();
  const bp = Number(simSettings.buyingPower);
  return (Number.isFinite(bp) && bp >= 0) ? bp : 0;
}

function _wouldGoShort(symbol, side, qty){
  const sym = String(symbol || '').toUpperCase();
  const s = String(side || '').toUpperCase();
  const q = Math.floor(Number(qty));
  if (!sym || !Number.isFinite(q) || q <= 0) return false;
  if (s !== 'SELL') return false;
  const pos = positions?.get?.(sym);
  const sh = Number(pos?.shares ?? 0);
  if (!Number.isFinite(sh)) return true;
  return (sh - q) < 0;
}

function _currentAskPx(){
  const a = Number(currentBook?.asks?.[0]?.[0]);
  if (Number.isFinite(a) && a > 0) return a;
  const lastPx = Number(lastTrade?.price);
  if (Number.isFinite(lastPx) && lastPx > 0) return lastPx;
  return null;
}

function _estimateBuyPxForOrder(orderType, limitPx, stopPx){
  const t = String(orderType || '').toUpperCase();
  if (t === 'LMT' || t === 'STOPLMT') {
    const lim = Number(limitPx);
    return (Number.isFinite(lim) && lim > 0) ? lim : null;
  }
  if (t === 'STOP') {
    const stp = Number(stopPx);
    return (Number.isFinite(stp) && stp > 0) ? stp : _currentAskPx();
  }
  // MKT and unknown: best-effort current ask/last
  return _currentAskPx();
}

function _calcLongSharesAfterFill(prevShares, buyQty){
  const prev = Number(prevShares || 0);
  const q = Math.floor(Number(buyQty));
  if (!Number.isFinite(prev) || !Number.isFinite(q)) return null;
  const next = prev + q;
  const curLong = Math.max(0, prev);
  const nextLong = Math.max(0, next);
  return { curLong, nextLong, deltaLong: Math.max(0, nextLong - curLong) };
}

function _openBuyNotional(sym){
  // Conservative: sum remaining BUY order notional using their own limit/stop/current ask estimate.
  const symbol = String(sym || '').toUpperCase();
  if (!symbol) return 0;
  let sum = 0;
  for (const o of orders.values()){
    if (!o) continue;
    if (String(o.symbol).toUpperCase() !== symbol) continue;
    if (o.status !== 'open' && o.status !== 'partial') continue;
    if (o.cancelledAtNs != null) continue;
    if (String(o.side).toUpperCase() !== 'BUY') continue;
    const rem = _orderRemaining(o);
    if (rem <= 0) continue;
    const px = _estimateBuyPxForOrder(o.type, o.limitPx, o.stopPx);
    if (px == null) continue;
    sum += rem * px;
  }
  return Number.isFinite(sum) ? sum : 0;
}

function _longNotional(sym){
  const symbol = String(sym || '').toUpperCase();
  if (!symbol) return 0;
  const pos = positions?.get?.(symbol);
  const sh = Number(pos?.shares ?? 0);
  if (!Number.isFinite(sh)) return 0;
  const longSh = Math.max(0, sh);
  if (longSh <= 0) return 0;
  const px = _currentAskPx();
  if (px == null) return 0;
  return longSh * px;
}

function _wouldExceedBuyingPowerOnBuy({ symbol, qty, type, limitPx, stopPx }){
  if (!_buyingPowerEnabled()) return { ok: true };
  const sym = String(symbol || '').toUpperCase();
  const q = Math.floor(Number(qty));
  if (!sym || !Number.isFinite(q) || q <= 0) return { ok: true };

  const pos = positions?.get?.(sym);
  const prevSh = Number(pos?.shares ?? 0);
  const shCalc = _calcLongSharesAfterFill(prevSh, q);
  if (!shCalc) return { ok: true };
  if (shCalc.deltaLong <= 0) return { ok: true }; // covering short or flat->less flat

  const px = _estimateBuyPxForOrder(type, limitPx, stopPx);
  if (px == null) return { ok: false, reason: 'Buying power check needs a price (book/last or limit/stop)' };

  const curLong = _longNotional(sym);
  const openBuys = _openBuyNotional(sym);
  const add = shCalc.deltaLong * px;
  const total = curLong + openBuys + add;
  const bp = _buyingPower();
  if (total > bp + 1e-9) {
    return { ok: false, reason: `Buying power exceeded: est $${total.toFixed(2)} > limit $${bp.toFixed(2)}` };
  }
  return { ok: true };
}

// Perf: coalesce bursty SSE events into a single render per animation frame.
let _pendingBook = null;
let _pendingTrades = [];
let _pendingMaxTs = null;
let _replayFlushRaf = null;
const MAX_TAPE_RENDER_PER_FRAME = 120; // render cap; prevents DOM blowups during intense bursts
const MAX_TRADES_PROCESS_PER_FRAME = 2000; // processing cap; prevents long JS tasks during bursts

function _scheduleReplayFlush(){
  if (_replayFlushRaf != null) return;
  _replayFlushRaf = requestAnimationFrame(()=>{
    _replayFlushRaf = null;
    const book = _pendingBook;
    const trades = _pendingTrades;
    const maxTs = _pendingMaxTs;
    _pendingBook = null;
    _pendingTrades = [];
    _pendingMaxTs = null;

    if (book) {
      currentBook = book;
      renderL2(currentBook);
    }

    let remainder = [];
    let procTrades = trades;
    if (Array.isArray(trades) && trades.length > MAX_TRADES_PROCESS_PER_FRAME) {
      procTrades = trades.slice(0, MAX_TRADES_PROCESS_PER_FRAME);
      remainder = trades.slice(MAX_TRADES_PROCESS_PER_FRAME);
    }

    if (Array.isArray(procTrades) && procTrades.length) {
      // Process fills/chart logic in chronological order, but render tape (newest-first) in a bounded batch.
      for (let i=0; i<procTrades.length; i++){
        const tr = procTrades[i];
        if (!tr) continue;
        // Guard against rewinds (e.g., resume from an older playhead) which makes the tape "jump backwards".
        if (lastTapeTsSeen != null && tr.ts_event < lastTapeTsSeen) continue;
        lastTapeTsSeen = tr.ts_event;
        lastTrade = tr;
        try { maybeTriggerStopsFromTrade(tr); } catch {}
        try { maybeFillFromTrade(tr); } catch {}
        // drive higher-TF charts' in-progress bar close/high/low/volume in real-time
        try {
          for (const ch of charts.values()) {
            updateChartFromTrade(ch, Number(tr.ts_event), Number(tr.price), Number(tr.size));
          }
        } catch {}
      }

      // Tape render: newest at top
      const slice = procTrades.slice(-MAX_TAPE_RENDER_PER_FRAME);
      slice.reverse(); // newest-first for appendTapeBatch
      appendTapeBatch(slice);
    }

    // Trading UI refresh once per frame (positions / working orders / etc.)
    if (book || (Array.isArray(procTrades) && procTrades.length)) {
      try { renderPositions(); } catch {}
      try { maybeFillOrders(); } catch {}
    }

    // If we still have backlog, continue next frame to avoid freezing the UI.
    if (Array.isArray(remainder) && remainder.length) {
      _pendingTrades = remainder;
      _pendingMaxTs = maxTs; // keep the "true" max so the final frame can advance playhead fully
      // Advance playhead only to what we actually processed so state stays consistent.
      const lastProcTs = procTrades?.length ? procTrades[procTrades.length-1]?.ts_event : (book?.ts_event ?? null);
      if (lastProcTs != null) updatePlayhead(lastProcTs);
      _scheduleReplayFlush();
      return;
    }

    if (maxTs != null) updatePlayhead(maxTs);
  });
}

// Cross-tab control sync (popouts): pause/play/load in one tab controls the others.
const TAB_ID = Math.random().toString(36).slice(2);
const BC = ('BroadcastChannel' in window) ? new BroadcastChannel('sim-replay') : null;
let _suppressBroadcast = false;

function _broadcast(msg){
  if (_suppressBroadcast) return;
  const full = Object.assign({tab: TAB_ID, at: Date.now()}, msg);
  try { BC?.postMessage(full); } catch {}
  try { localStorage.setItem('sim-replay-msg', JSON.stringify(full)); } catch {}
}

async function _handleExternalControl(msg){
  if (!msg || msg.tab === TAB_ID) return;
  if (!msg.cmd) return;
  _suppressBroadcast = true;
  try {
    if (msg.symbol) $('symbol').value = msg.symbol;
    if (msg.day) $('day').value = msg.day;
    if (msg.ts) $('ts').value = msg.ts;
    if (msg.speed) $('speed').value = msg.speed;
    if (msg.playheadNs != null) {
      // accept the external playhead only if it advances us
      updatePlayhead(msg.playheadNs);
    }
    if (msg.cmd === 'pause') {
      doPause(false);
    } else if (msg.cmd === 'load') {
      await doLoad(false);
    } else if (msg.cmd === 'play') {
      await doPlay(false);
    }
  } finally {
    _suppressBroadcast = false;
  }
}

if (BC) {
  BC.onmessage = (ev)=>_handleExternalControl(ev.data);
}
window.addEventListener('storage', (ev)=>{
  if (ev.key !== 'sim-replay-msg' || !ev.newValue) return;
  try { _handleExternalControl(JSON.parse(ev.newValue)); } catch {}
});

function updatePlayhead(ns){
  if (ns == null) return;
  const v = Number(ns);
  if (!Number.isFinite(v)) return;
  if (playheadNs == null || v > playheadNs) {
    playheadNs = v;
    // Perf: throttle "Now" label updates (Intl formatting + layout) during bursts.
    // Only refresh if we crossed into a new second.
    const sec = Math.floor(playheadNs / 1e9);
    if (updatePlayhead._lastSec == null || sec !== updatePlayhead._lastSec) {
      updatePlayhead._lastSec = sec;
      setNow(`Now: ${nsToEt(playheadNs)} ET`);
    }
    // Allow buffered candle updates (e.g., higher TF) to become visible only once complete.
    for (const ch of charts?.values?.() ?? []) {
      flushPendingCandles(ch);
    }
  }
}

function resetTapeMonotonic(tsNs){
  lastTapeTsSeen = tsNs == null ? null : Number(tsNs);
}

// chart view state
let zoom = 1.0;     // higher = more zoomed in (fewer candles visible)
let pan = 0.0;      // 0..1 normalized offset from right edge
let isDragging = false;
let dragStartX = 0;
let panStart = 0;

function setErr(msg){ $('err').textContent = msg || ''; }
function setStatus(msg){ $('status').textContent = msg || ''; }
function setNow(msg){ $('now').textContent = msg || ''; }

// Hard-fail visibility: surface any JS errors into the UI instead of failing silently.
window.addEventListener('error', (ev)=>{
  try {
    const msg = `JS error: ${ev?.message ?? ev}`;
    console.error(msg, ev);
    setErr(msg);
  } catch {}
});
window.addEventListener('unhandledrejection', (ev)=>{
  try {
    const msg = `JS promise error: ${ev?.reason?.message ?? ev?.reason ?? ev}`;
    console.error(msg, ev);
    setErr(msg);
  } catch {}
});

const ET = 'America/New_York';
// Perf: reuse Intl formatters (creating them and using formatToParts per tick is expensive).
const _ET_DTF_FULL = new Intl.DateTimeFormat('en-CA', {
  timeZone: ET,
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
  hour12: false
});
const _ET_DTF_HMS = new Intl.DateTimeFormat('en-US', {
  timeZone: ET,
  hour: '2-digit', minute: '2-digit', second: '2-digit',
  hour12: false
});
function nsToEt(ns){
  const ms = Math.floor(ns / 1e6);
  const d = new Date(ms);
  // en-CA gives YYYY-MM-DD ordering; format() is much cheaper than formatToParts().
  // Some runtimes include a comma between date and time; normalize to a single space.
  return String(_ET_DTF_FULL.format(d)).replace(', ', ' ');
}
function nsToEtHms(ns){
  const ms = Math.floor(ns / 1e6);
  const d = new Date(ms);
  return String(_ET_DTF_HMS.format(d));
}

function fmtPx(p){
  if (p === null || p === undefined) return '';
  return Number(p).toFixed(4);
}

function _roundTo(x, decimals){
  const d = Math.max(0, Math.floor(Number(decimals || 0)));
  const f = Math.pow(10, d);
  // Add a tiny epsilon to reduce floating rounding artifacts (e.g. 1.005).
  return Math.round((Number(x) + Number.EPSILON) * f) / f;
}
// Stocks >= $1: round to cents. Stocks < $1: round to mills.
function _roundLmtPx(px){
  const p = Number(px);
  if (!Number.isFinite(p)) return null;
  const dec = (p < 1) ? 3 : 2;
  return { px: _roundTo(p, dec), dec };
}
function _applyLmtRoundingEl(el){
  if (!el) return;
  const raw = String(el.value || '').trim();
  if (!raw) return;
  const r = _roundLmtPx(raw);
  if (!r) return;
  try { el.step = (r.dec === 3) ? '0.001' : '0.01'; } catch {}
  el.value = Number(r.px).toFixed(r.dec);
}

// ---------------- LVL2 appearance settings ----------------
const L2_TIER_COLORS_KEY = 'sim-l2-tier-colors-v1';
const L2_DEFAULT_TIER_COLORS = ['#ffd400', '#ffffff', '#31ff69', '#ff3b3b'];
let l2TierColors = null;
let _l2Dom = null; // cached DOM refs for incremental LVL2 updates
let _l2TierBgCache = null; // array[10] -> {bidBg, askBg}

function _isHexColor(s){
  return (typeof s === 'string') && /^#[0-9a-fA-F]{6}$/.test(s.trim());
}

function loadL2TierColors(){
  try {
    const raw = localStorage.getItem(L2_TIER_COLORS_KEY);
    if (!raw) return [...L2_DEFAULT_TIER_COLORS];
    const v = JSON.parse(raw);
    if (!Array.isArray(v)) return [...L2_DEFAULT_TIER_COLORS];
    const out = v.map(x=>String(x).trim()).filter(_isHexColor);
    return (out.length > 0) ? out : [...L2_DEFAULT_TIER_COLORS];
  } catch {
    return [...L2_DEFAULT_TIER_COLORS];
  }
}

function saveL2TierColors(){
  try { localStorage.setItem(L2_TIER_COLORS_KEY, JSON.stringify(l2TierColors || [])); } catch {}
}

function initL2Window(){
  l2TierColors = loadL2TierColors();
  const gear = $('l2Gear');
  const panel = $('l2Panel');
  const list = $('l2ColorsList');
  if (!gear || !panel || !list) return;

  // Build LVL2 rows once and reuse DOM nodes (avoid innerHTML rebuild per tick).
  (function ensureL2Dom(){
    const tb = $('l2body');
    if (!tb) return;
    if (_l2Dom && _l2Dom.tb === tb && _l2Dom.rows && _l2Dom.rows.length === 10) return;
    tb.innerHTML = '';
    const rows = [];
    for (let i=0; i<10; i++){
      const tr = document.createElement('tr');
      tr.className = 'l2row';
      const bidPx = document.createElement('td'); bidPx.className = 'px bidx';
      const bidSz = document.createElement('td'); bidSz.className = 'sz bidx';
      const askPx = document.createElement('td'); askPx.className = 'px askx';
      const askSz = document.createElement('td'); askSz.className = 'sz askx';
      // keep the bold style, but do it once
      bidPx.style.color = '#000'; bidPx.style.fontWeight = '1000';
      bidSz.style.color = '#000'; bidSz.style.fontWeight = '1000';
      askPx.style.color = '#000'; askPx.style.fontWeight = '1000';
      askSz.style.color = '#000'; askSz.style.fontWeight = '1000';
      tr.appendChild(bidPx); tr.appendChild(bidSz); tr.appendChild(askPx); tr.appendChild(askSz);
      tb.appendChild(tr);
      rows.push({tr, bidPx, bidSz, askPx, askSz});
    }
    _l2Dom = { tb, rows, meta: $('l2meta') };
  })();

  function _recomputeL2TierBgs(){
    const colors = (Array.isArray(l2TierColors) && l2TierColors.length) ? l2TierColors : L2_DEFAULT_TIER_COLORS;
    const bg = (hex, a)=> {
      const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
      return `rgba(${r},${g},${b},${a})`;
    };
    _l2TierBgCache = [];
    for (let i=0; i<10; i++){
      const tier = colors[i % colors.length];
      const c = bg(tier, 0.92);
      _l2TierBgCache.push({bidBg: c, askBg: c});
    }
  }
  _recomputeL2TierBgs();

  function openPanel(){ panel.style.display = 'block'; }
  function closePanel(){ panel.style.display = 'none'; }
  gear.addEventListener('click', (e)=>{ e.preventDefault(); (panel.style.display === 'block') ? closePanel() : openPanel(); });
  $('l2ColorClose')?.addEventListener('click', (e)=>{ e.preventDefault(); closePanel(); });

  function rerenderColors(){
    if (!list) return;
    list.innerHTML = '';
    const colors = (Array.isArray(l2TierColors) && l2TierColors.length) ? l2TierColors : [...L2_DEFAULT_TIER_COLORS];
    colors.forEach((hex, idx)=>{
      const row = document.createElement('div');
      row.className = 'l2ColorRow';
      row.innerHTML = `
        <div class="l2ColorIdx">${idx+1}</div>
        <input type="color" value="${hex}">
        <button class="wbtn" data-act="up" title="Move up">↑</button>
        <button class="wbtn" data-act="dn" title="Move down">↓</button>
        <button class="wbtn" data-act="rm" title="Remove">✕</button>
      `;
      const inp = row.querySelector('input[type="color"]');
      inp?.addEventListener('input', ()=>{
        const v = String(inp.value || '').trim();
        if (_isHexColor(v)) {
          l2TierColors[idx] = v;
          saveL2TierColors();
          _recomputeL2TierBgs();
          try { if (currentBook) renderL2(currentBook); } catch {}
        }
      });
      row.querySelector('[data-act="up"]')?.addEventListener('click', (e)=>{
        e.preventDefault();
        if (idx <= 0) return;
        const tmp = l2TierColors[idx-1]; l2TierColors[idx-1] = l2TierColors[idx]; l2TierColors[idx] = tmp;
        saveL2TierColors(); rerenderColors();
        _recomputeL2TierBgs();
        try { if (currentBook) renderL2(currentBook); } catch {}
      });
      row.querySelector('[data-act="dn"]')?.addEventListener('click', (e)=>{
        e.preventDefault();
        if (idx >= l2TierColors.length-1) return;
        const tmp = l2TierColors[idx+1]; l2TierColors[idx+1] = l2TierColors[idx]; l2TierColors[idx] = tmp;
        saveL2TierColors(); rerenderColors();
        _recomputeL2TierBgs();
        try { if (currentBook) renderL2(currentBook); } catch {}
      });
      row.querySelector('[data-act="rm"]')?.addEventListener('click', (e)=>{
        e.preventDefault();
        if (l2TierColors.length <= 1) return;
        l2TierColors.splice(idx, 1);
        saveL2TierColors(); rerenderColors();
        _recomputeL2TierBgs();
        try { if (currentBook) renderL2(currentBook); } catch {}
      });
      list.appendChild(row);
    });
  }

  $('l2ColorAdd')?.addEventListener('click', (e)=>{
    e.preventDefault();
    l2TierColors = (Array.isArray(l2TierColors) ? l2TierColors : [...L2_DEFAULT_TIER_COLORS]);
    l2TierColors.push('#ffffff');
    saveL2TierColors();
    rerenderColors();
  });
  $('l2ColorReset')?.addEventListener('click', (e)=>{
    e.preventDefault();
    l2TierColors = [...L2_DEFAULT_TIER_COLORS];
    saveL2TierColors();
    _recomputeL2TierBgs();
    rerenderColors();
    try { if (currentBook) renderL2(currentBook); } catch {}
  });

  // Close if user clicks outside the panel (within the LVL2 window body).
  const l2win = $('l2');
  l2win?.addEventListener('mousedown', (ev)=>{
    if (!panel || panel.style.display !== 'block') return;
    const t = ev.target;
    if (t && (panel.contains(t) || gear.contains(t))) return;
    closePanel();
  });

  rerenderColors();
}

function renderL2(book){
  const bids = book?.bids || [];
  const asks = book?.asks || [];
  if (!_l2Dom || !_l2Dom.rows || _l2Dom.rows.length !== 10) {
    try { initL2Window(); } catch {}
  }
  if (!_l2Dom || !_l2Dom.rows) return;
  const bgs = Array.isArray(_l2TierBgCache) && _l2TierBgCache.length === 10 ? _l2TierBgCache : null;
  for (let i=0; i<10; i++){
    const b = bids[i] || [null, null];
    const a = asks[i] || [null, null];
    const row = _l2Dom.rows[i];
    row.bidPx.textContent = fmtPx(b[0]);
    row.bidSz.textContent = (b[1] == null) ? '' : String(b[1]);
    row.askPx.textContent = fmtPx(a[0]);
    row.askSz.textContent = (a[1] == null) ? '' : String(a[1]);
    if (bgs) {
      row.bidPx.style.background = bgs[i].bidBg;
      row.bidSz.style.background = bgs[i].bidBg;
      row.askPx.style.background = bgs[i].askBg;
      row.askSz.style.background = bgs[i].askBg;
    }
  }
  const bb = bids[0]?.[0], aa = asks[0]?.[0];
  const spr = (bb!=null && aa!=null) ? (aa - bb) : null;
  // Use the authoritative playhead time for display to avoid flicker/backwards jumps.
  const shownTs = (playheadNs != null) ? playheadNs : (book.ts_event ?? null);
  const meta = _l2Dom.meta || $('l2meta');
  if (meta) meta.textContent = `ts_event=${shownTs ? nsToEt(shownTs) : ''} ET   bid=${fmtPx(bb)}   ask=${fmtPx(aa)}   spread=${spr!=null ? spr.toFixed(4) : ''}`;
}

function classifyTrade(price){
  const bid = currentBook.bids?.[0]?.[0];
  const ask = currentBook.asks?.[0]?.[0];
  if (ask != null && price >= ask) return 'ask';
  if (bid != null && price <= bid) return 'bid';
  return 'mid';
}

function appendTape(tr){
  const tape = $('tapeList');
  if (!tape) return;
  const line = document.createElement('div');
  const cls = classifyTrade(tr.price);
  line.className = 'tline ' + (cls === 'ask' ? 't-ask' : cls === 'bid' ? 't-bid' : 't-mid');
  line.innerHTML = `
    <div class="ts">${nsToEtHms(tr.ts_event)}</div>
    <div>${fmtPx(tr.price)}</div>
    <div>${tr.size}</div>
  `;
  tape.prepend(line);
  while (tape.childNodes.length > 250) tape.removeChild(tape.lastChild);
}

function appendTapeBatch(trades){
  const tape = $('tapeList');
  if (!tape) return;
  if (!Array.isArray(trades) || trades.length === 0) return;
  // Render newest at top (prepend). Building a fragment avoids repeated layout thrash.
  const frag = document.createDocumentFragment();
  for (let i=0; i<trades.length; i++){
    const tr = trades[i];
    if (!tr) continue;
    const line = document.createElement('div');
    const cls = classifyTrade(tr.price);
    line.className = 'tline ' + (cls === 'ask' ? 't-ask' : cls === 'bid' ? 't-bid' : 't-mid');
    line.innerHTML = `
      <div class="ts">${nsToEtHms(tr.ts_event)}</div>
      <div>${fmtPx(tr.price)}</div>
      <div>${tr.size}</div>
    `;
    frag.appendChild(line);
  }
  tape.insertBefore(frag, tape.firstChild);
  while (tape.childNodes.length > 250) tape.removeChild(tape.lastChild);
}

const TF_NS = {
  '1s': 1e9,
  '10s': 10e9,
  '1m': 60e9,
  '5m': 300e9,
};

function tfNs(tf){
  return TF_NS[tf] || 1e9;
}

// ---------------- TradingView Lightweight Charts ----------------
function _tvSecFromNs(ns){
  const v = Number(ns);
  if (!Number.isFinite(v)) return 0;
  return Math.floor(v / 1e9);
}

function _tvCandleFromInternal(c){
  // Lightweight Charts expects {time: unixSeconds, open, high, low, close}
  const time = _tvSecFromNs(c.t);
  const open = Number(c.o), high = Number(c.h), low = Number(c.l), close = Number(c.c);
  if (!Number.isFinite(time) || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) return null;
  return { time, open, high, low, close };
}

function _tvVolFromInternal(c){
  const up = Number(c.c) >= Number(c.o);
  const time = _tvSecFromNs(c.t);
  if (!Number.isFinite(time)) return null;
  const vv = Number(c.v ?? 0);
  return {
    time,
    value: Number.isFinite(vv) ? vv : 0,
    color: up ? 'rgba(37,211,102,0.55)' : 'rgba(255,77,77,0.55)',
  };
}

function tvInit(chart){
  if (!window.LightweightCharts) {
    const msg = 'TradingView Lightweight Charts failed to load (no LightweightCharts global).';
    console.error(msg);
    setErr(msg);
    return;
  }
  if (!chart?.container) return;

  function _tvCrosshairModeNormal(){
    // Backwards/forwards compatible: some builds expose CrosshairMode enum, others don't.
    try { return LightweightCharts?.CrosshairMode?.Normal; } catch {}
    return undefined;
  }

  function _etTimeLabelFromSec(sec, tf){
    // Render timestamps in America/New_York (ET) so 09:30 ET shows as 09:30 (not 14:30 UTC).
    // Lightweight Charts provides UTC seconds; we format in ET.
    const ms = Number(sec) * 1000;
    if (!Number.isFinite(ms)) return '';
    const d = new Date(ms);
    const showSeconds = (tf === '1s' || tf === '10s');
    const opts = showSeconds
      ? { timeZone: ET, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }
      : { timeZone: ET, hour: '2-digit', minute: '2-digit', hour12: false };
    try {
      return new Intl.DateTimeFormat('en-US', opts).format(d);
    } catch {
      // Fallback: use existing ns->ET formatter
      const ns = Number(sec) * 1e9;
      return showSeconds ? nsToEt(ns).slice(11,19) : nsToEt(ns).slice(11,16);
    }
  }

  function _tvTimeFormatter(time){
    // time can be a number (unix seconds) or a business day object in some cases.
    if (typeof time === 'number') return _etTimeLabelFromSec(time, chart.tf);
    if (time && typeof time === 'object' && time.year && time.month && time.day) {
      // Treat business day as midnight UTC and format in ET.
      const d = new Date(Date.UTC(time.year, time.month - 1, time.day, 0, 0, 0));
      const sec = Math.floor(d.getTime() / 1000);
      return _etTimeLabelFromSec(sec, chart.tf);
    }
    return String(time ?? '');
  }

  function _tvTickMarkFormatter(time /*, tickMarkType, locale */){
    // Tick labels (x-axis) use tickMarkFormatter, not timeFormatter, in Lightweight Charts.
    return _tvTimeFormatter(time);
  }

  function _tvAddCandles(api, options){
    // v3/v4: api.addCandlestickSeries
    if (api && typeof api.addCandlestickSeries === 'function') return api.addCandlestickSeries(options);
    // v5+: api.addSeries(CandlestickSeries, options) or api.addSeries('Candlestick', options)
    if (api && typeof api.addSeries === 'function') {
      const C = LightweightCharts?.CandlestickSeries;
      if (C) return api.addSeries(C, options);
      // Some docs mention string-based type.
      try { return api.addSeries('Candlestick', options); } catch {}
      try { return api.addSeries('CandlestickSeries', options); } catch {}
    }
    throw new Error('No candlestick series API found (addCandlestickSeries/addSeries unavailable).');
  }

  function _tvAddHistogram(api, options){
    if (api && typeof api.addHistogramSeries === 'function') return api.addHistogramSeries(options);
    if (api && typeof api.addSeries === 'function') {
      const H = LightweightCharts?.HistogramSeries;
      if (H) return api.addSeries(H, options);
      try { return api.addSeries('Histogram', options); } catch {}
      try { return api.addSeries('HistogramSeries', options); } catch {}
    }
    throw new Error('No histogram series API found (addHistogramSeries/addSeries unavailable).');
  }

  try {
    // Create chart
    chart.tv = LightweightCharts.createChart(chart.container, {
      layout: { background: { color: '#0b1220' }, textColor: 'rgba(230,237,243,0.9)' },
      grid: { vertLines: { color: 'rgba(34,48,69,0.55)' }, horzLines: { color: 'rgba(34,48,69,0.55)' } },
      rightPriceScale: {
        borderColor: 'rgba(34,48,69,0.8)',
        // Reduce wasted headroom while keeping space for the volume band at the bottom.
        scaleMargins: { top: 0.05, bottom: 0.22 },
      },
      timeScale: {
        borderColor: 'rgba(34,48,69,0.8)',
        timeVisible: true,
        secondsVisible: (chart.tf === '1s' || chart.tf === '10s'),
        tickMarkFormatter: _tvTickMarkFormatter,
      },
      crosshair: (_tvCrosshairModeNormal() != null) ? { mode: _tvCrosshairModeNormal() } : undefined,
      localization: { timeFormatter: _tvTimeFormatter },
    });
    // Some versions use applyOptions for localization; do both, safely.
    try { chart.tv.applyOptions({
      localization: { timeFormatter: _tvTimeFormatter },
      timeScale: { tickMarkFormatter: _tvTickMarkFormatter, secondsVisible: (chart.tf === '1s' || chart.tf === '10s') },
      rightPriceScale: { scaleMargins: { top: 0.05, bottom: 0.22 } },
    }); } catch {}
  } catch (e) {
    const msg = `LightweightCharts.createChart failed: ${e?.message ?? e}`;
    console.error(msg, e);
    setErr(msg);
    return;
  }

  try {
    chart.candleSeries = _tvAddCandles(chart.tv, {
      upColor: 'rgba(37,211,102,0.9)',
      downColor: 'rgba(255,77,77,0.9)',
      wickUpColor: 'rgba(37,211,102,0.9)',
      wickDownColor: 'rgba(255,77,77,0.9)',
      borderUpColor: 'rgba(37,211,102,0.9)',
      borderDownColor: 'rgba(255,77,77,0.9)',
    });
  } catch (e) {
    const msg = `addCandlestickSeries failed: ${e?.message ?? e}`;
    console.error(msg, e);
    setErr(msg);
    return;
  }

  try {
    chart.volSeries = _tvAddHistogram(chart.tv, {
      priceScaleId: '',
      priceFormat: { type: 'volume' },
      lastValueVisible: false,
    });
    // Put volume at the bottom
    chart.volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
  } catch (e) {
    const msg = `addHistogramSeries failed: ${e?.message ?? e}`;
    console.error(msg, e);
    setErr(msg);
    // keep going without volume
    chart.volSeries = null;
  }

  // Crosshair -> OHLC HUD
  try {
    chart.tv.subscribeCrosshairMove((param)=>{
      if (!param || !param.time) { chart.ohlcEl.textContent = ''; return; }
      const data = param.seriesData?.get?.(chart.candleSeries);
      if (!data) { chart.ohlcEl.textContent = ''; return; }
      const vData = chart.volSeries ? (param.seriesData?.get?.(chart.volSeries) ?? null) : null;
      const vv = (vData && Number.isFinite(Number(vData.value))) ? Math.trunc(Number(vData.value)) : null;
      const sec = Number(param.time);
      const tNs = Number.isFinite(sec) ? (sec * 1e9) : null;
      if (tNs == null) { chart.ohlcEl.textContent = ''; return; }
      chart.ohlcEl.textContent =
        `${nsToEt(tNs)}  O ${fmtPx(data.open)}  H ${fmtPx(data.high)}  L ${fmtPx(data.low)}  C ${fmtPx(data.close)}  V ${vv ?? ''}`;
    });
  } catch (e) {
    // non-fatal
  }

  // Load more history when the user scrolls near the start.
  chart.tv.timeScale().subscribeVisibleLogicalRangeChange((range)=>{
    if (!range) return;
    if (chart.loadingMore || !chart.canLoadMore) return;
    // When close to the left edge, fetch more candles.
    if (range.from < 50) loadMoreHistory(chart, 500);
  });

  tvResize(chart);
}

function tvResize(chart){
  // IMPORTANT: size the chart to its actual container, not the whole wrap.
  // When MACD pane is enabled, the wrap includes both panes; sizing the main chart to the wrap
  // causes the bottom (volume band) to be clipped because the main container is smaller.
  if (!chart?.tv || !chart?.container) return;
  const rect = chart.container.getBoundingClientRect();
  if (!rect || rect.width <= 2 || rect.height <= 2) return;
  chart.tv.applyOptions({ width: Math.floor(rect.width), height: Math.floor(rect.height) });
  // If MACD pane is enabled, resize it too.
  try { if (chart.macdTv && chart.macdContainer && chart.macdWrap && chart.macdWrap.style.display !== 'none') {
    const r2 = chart.macdContainer.getBoundingClientRect();
    chart.macdTv.applyOptions({ width: Math.floor(r2.width), height: Math.floor(r2.height) });
  }} catch {}
}

function tvSetData(chart){
  if (!chart?.candleSeries) return;
  const candles = (chart.candles || []).filter(c=>c && Number.isFinite(c.t));
  const data = candles.map(_tvCandleFromInternal).filter(x=>x);
  const vols = candles.map(_tvVolFromInternal).filter(x=>x);
  const ts = chart.tv?.timeScale?.();
  const prevRange = ts?.getVisibleLogicalRange?.() || null;
  const prevLen = (chart._lastSetDataLen != null) ? Number(chart._lastSetDataLen) : null;
  try {
    console.debug(`[tv] setData ${chart.id} tf=${chart.tf} candles=${data.length} vols=${vols.length}`);
    chart.candleSeries.setData(data);
    if (chart.volSeries) chart.volSeries.setData(vols);
    chart._lastSetDataLen = data.length;
    // IMPORTANT: do NOT fitContent() on every setData. That "zooms out" the chart periodically
    // (especially on higher TF when completed candles are applied). Only auto-fit on initial load
    // or explicit snapshot reload.
    if (chart._fitNext) {
      chart._fitNext = false;
      try { ts?.fitContent?.(); } catch {}
    } else if (prevRange && ts?.setVisibleLogicalRange) {
      try {
        // If we prepended history (len grew), shift the logical range so the user stays on the same bars.
        if (prevLen != null && Number.isFinite(prevLen) && data.length > prevLen) {
          const delta = data.length - prevLen;
          ts.setVisibleLogicalRange({ from: prevRange.from + delta, to: prevRange.to + delta });
        } else {
          ts.setVisibleLogicalRange(prevRange);
        }
      } catch {}
    }
    if (chart.hudEl) chart.hudEl.textContent = `Bars: ${data.length}`;
    chart._indDirty = true;
  } catch (e) {
    const msg = `Chart setData failed (tf=${chart.tf}): ${e?.message ?? e}`;
    console.error(msg, e);
    setErr(msg);
  }
}

function tvUpdateLast(chart){
  if (!chart?.candleSeries) return;
  const n = chart.candles?.length || 0;
  if (n === 0) return;
  const last = chart.candles[n-1];
  const c = _tvCandleFromInternal(last);
  const v = _tvVolFromInternal(last);
  try {
    if (c) chart.candleSeries.update(c);
    if (v && chart.volSeries) chart.volSeries.update(v);
  } catch (e) {
    const msg = `Chart update failed (tf=${chart.tf}): ${e?.message ?? e}`;
    console.error(msg, e);
    setErr(msg);
  }
  // HUD uses last candle (lightweight; avoids depending on crosshair)
  chart.hudEl.textContent = last ? `Last: ${fmtPx(last.c)} @ ${nsToEt(last.t).slice(11,19)} ET` : '';
  chart._indDirty = true;
}

function _emaSeries(values, period){
  const out = new Array(values.length).fill(null);
  const k = 2/(period+1);
  let prev = null;
  for (let i=0;i<values.length;i++){
    const v = values[i];
    if (!Number.isFinite(v)) { out[i] = prev; continue; }
    prev = (prev==null) ? v : (v*k + prev*(1-k));
    out[i] = prev;
  }
  return out;
}

function _computeSmaPoints(candles, period){
  const out = [];
  const win = [];
  let sum = 0;
  for (let i=0;i<candles.length;i++){
    const c = candles[i];
    const v = Number(c.c);
    win.push(v);
    if (Number.isFinite(v)) sum += v;
    if (win.length > period){
      const old = win.shift();
      if (Number.isFinite(old)) sum -= old;
    }
    if (win.length === period){
      const time = _tvSecFromNs(c.t);
      if (Number.isFinite(time)) out.push({ time, value: sum/period });
    }
  }
  return out;
}

function _computeEmaPoints(candles, period){
  const out = [];
  const p = Math.max(2, Math.floor(Number(period || 9)));
  const closes = candles.map(c=>Number(c.c));
  const em = _emaSeries(closes, p);
  for (let i=0;i<candles.length;i++){
    const time = _tvSecFromNs(candles[i].t);
    const v = em[i];
    if (!Number.isFinite(time) || !Number.isFinite(v)) continue;
    out.push({ time, value: v });
  }
  return out;
}

function _computeVwapPoints(candles, period){
  // VWAP using typical price * volume, either cumulative (period=0) or rolling N bars
  const out = [];
  let pv = 0, vv = 0;
  const q = []; // {pv, v}
  for (let i=0;i<candles.length;i++){
    const c = candles[i];
    const v = Number(c.v ?? 0);
    const tp = (Number(c.h) + Number(c.l) + Number(c.c)) / 3;
    const pv_i = (Number.isFinite(tp) && Number.isFinite(v)) ? (tp * v) : 0;
    const v_i = Number.isFinite(v) ? v : 0;
    if (period > 0) {
      q.push({pv: pv_i, v: v_i});
      pv += pv_i; vv += v_i;
      while (q.length > period){
        const x = q.shift();
        pv -= x.pv; vv -= x.v;
      }
    } else {
      pv += pv_i; vv += v_i;
    }
    const val = (vv > 0) ? (pv/vv) : null;
    if (val != null && Number.isFinite(val)) {
      const time = _tvSecFromNs(c.t);
      if (Number.isFinite(time)) out.push({ time, value: val });
    }
  }
  return out;
}

function _computeMacdPoints(candles, fast, slow, signal){
  const closes = candles.map(c=>Number(c.c));
  const eF = _emaSeries(closes, fast);
  const eS = _emaSeries(closes, slow);
  const mac = closes.map((_,i)=> (Number(eF[i]) - Number(eS[i])));
  const sig = _emaSeries(mac, signal);
  const hist = mac.map((v,i)=> (Number(v) - Number(sig[i])));
  const macPts = [], sigPts = [], histPts = [];
  for (let i=0;i<candles.length;i++){
    const time = _tvSecFromNs(candles[i].t);
    if (!Number.isFinite(time)) continue;
    const m = mac[i], s = sig[i], h = hist[i];
    if (Number.isFinite(m)) macPts.push({ time, value: m });
    if (Number.isFinite(s)) sigPts.push({ time, value: s });
    if (Number.isFinite(h)) histPts.push({ time, value: h, color: h>=0 ? 'rgba(37,211,102,0.55)' : 'rgba(255,77,77,0.55)' });
  }
  return { macPts, sigPts, histPts };
}

function tvEnsureIndicators(chart){
  if (!chart?.tv) return;
  const emaOn = !!chart.indEmaEl?.checked;
  const smaOn = !!chart.indSmaEl?.checked;
  const vwapOn = !!chart.indVwapEl?.checked;
  const macdOn = !!chart.indMacdEl?.checked;

  // EMA
  if (emaOn && !chart.emaSeries) {
    try {
      chart.emaSeries = (typeof chart.tv.addLineSeries === 'function')
        ? chart.tv.addLineSeries({ color: 'rgba(177,108,255,0.95)', lineWidth: 2 })
        : (typeof chart.tv.addSeries === 'function' && LightweightCharts?.LineSeries)
          ? chart.tv.addSeries(LightweightCharts.LineSeries, { color: 'rgba(177,108,255,0.95)', lineWidth: 2 })
          : null;
    } catch {}
  }
  if (!emaOn && chart.emaSeries) { try { chart.tv.removeSeries(chart.emaSeries); } catch {} chart.emaSeries = null; }

  // SMA
  if (smaOn && !chart.smaSeries) {
    try {
      chart.smaSeries = (typeof chart.tv.addLineSeries === 'function')
        ? chart.tv.addLineSeries({ color: 'rgba(255,212,0,0.95)', lineWidth: 2 })
        : (typeof chart.tv.addSeries === 'function' && LightweightCharts?.LineSeries)
          ? chart.tv.addSeries(LightweightCharts.LineSeries, { color: 'rgba(255,212,0,0.95)', lineWidth: 2 })
          : null;
    } catch {}
  }
  if (!smaOn && chart.smaSeries) { try { chart.tv.removeSeries(chart.smaSeries); } catch {} chart.smaSeries = null; }

  // VWAP
  if (vwapOn && !chart.vwapSeries) {
    try {
      chart.vwapSeries = (typeof chart.tv.addLineSeries === 'function')
        ? chart.tv.addLineSeries({ color: 'rgba(51,161,255,0.95)', lineWidth: 2 })
        : (typeof chart.tv.addSeries === 'function' && LightweightCharts?.LineSeries)
          ? chart.tv.addSeries(LightweightCharts.LineSeries, { color: 'rgba(51,161,255,0.95)', lineWidth: 2 })
          : null;
    } catch {}
  }
  if (!vwapOn && chart.vwapSeries) { try { chart.tv.removeSeries(chart.vwapSeries); } catch {} chart.vwapSeries = null; }

  // MACD pane
  if (macdOn) {
    chart.macdWrap.style.display = 'block';
    if (!chart.macdTv) {
      try {
        const _macdTimeFmt = (t)=> {
          if (typeof t === 'number') {
            const showSeconds = (chart.tf === '1s' || chart.tf === '10s');
            const opts = showSeconds
              ? { timeZone: ET, hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false }
              : { timeZone: ET, hour:'2-digit', minute:'2-digit', hour12:false };
            return new Intl.DateTimeFormat('en-US', opts).format(new Date(t*1000));
          }
          return String(t ?? '');
        };
        const _macdTickFmt = (t /*, tickMarkType, locale */)=> _macdTimeFmt(t);
        chart.macdTv = LightweightCharts.createChart(chart.macdContainer, {
          layout: { background: { color: '#0b1220' }, textColor: 'rgba(230,237,243,0.85)' },
          grid: { vertLines: { color: 'rgba(34,48,69,0.55)' }, horzLines: { color: 'rgba(34,48,69,0.55)' } },
          rightPriceScale: { borderColor: 'rgba(34,48,69,0.8)' },
          timeScale: {
            borderColor: 'rgba(34,48,69,0.8)',
            timeVisible: true,
            secondsVisible: (chart.tf === '1s' || chart.tf === '10s'),
            tickMarkFormatter: _macdTickFmt,
          },
          handleScroll: false,
          handleScale: false,
          localization: { timeFormatter: _macdTimeFmt },
        });
      } catch (e) {
        console.error('MACD chart create failed', e);
        chart.macdTv = null;
      }
      if (chart.macdTv) {
        try {
          // series
          chart.macdHistSeries = (typeof chart.macdTv.addHistogramSeries === 'function')
            ? chart.macdTv.addHistogramSeries({ priceScaleId:'', lastValueVisible:false })
            : (typeof chart.macdTv.addSeries === 'function' && LightweightCharts?.HistogramSeries)
              ? chart.macdTv.addSeries(LightweightCharts.HistogramSeries, { priceScaleId:'', lastValueVisible:false })
              : null;
          chart.macdLineSeries = (typeof chart.macdTv.addLineSeries === 'function')
            ? chart.macdTv.addLineSeries({ color:'rgba(255,212,0,0.9)', lineWidth:2 })
            : (typeof chart.macdTv.addSeries === 'function' && LightweightCharts?.LineSeries)
              ? chart.macdTv.addSeries(LightweightCharts.LineSeries, { color:'rgba(255,212,0,0.9)', lineWidth:2 })
              : null;
          chart.macdSignalSeries = (typeof chart.macdTv.addLineSeries === 'function')
            ? chart.macdTv.addLineSeries({ color:'rgba(51,161,255,0.9)', lineWidth:2 })
            : (typeof chart.macdTv.addSeries === 'function' && LightweightCharts?.LineSeries)
              ? chart.macdTv.addSeries(LightweightCharts.LineSeries, { color:'rgba(51,161,255,0.9)', lineWidth:2 })
              : null;
        } catch (e) {
          console.error('MACD series create failed', e);
        }

        // Sync main chart time scale -> MACD
        try {
          chart.tv.timeScale().subscribeVisibleLogicalRangeChange((range)=>{
            try { chart.macdTv?.timeScale?.().setVisibleLogicalRange?.(range); } catch {}
          });
        } catch {}
      }
    }
  } else {
    chart.macdWrap.style.display = 'none';
  }
}

function tvRenderIndicators(chart){
  if (!chart?._indDirty) return;
  chart._indDirty = false;
  tvEnsureIndicators(chart);
  const candles = (chart.candles || []).filter(c=>c && Number.isFinite(c.t));
  if (!candles.length) return;
  if (chart.emaSeries && chart.indEmaEl?.checked) {
    try { chart.emaSeries.setData(_computeEmaPoints(candles, chart.emaPeriod || 9)); } catch (e) { console.error('EMA setData failed', e); }
  }
  if (chart.smaSeries && chart.indSmaEl?.checked) {
    try { chart.smaSeries.setData(_computeSmaPoints(candles, chart.smaPeriod || 20)); } catch (e) { console.error('SMA setData failed', e); }
  }
  if (chart.vwapSeries && chart.indVwapEl?.checked) {
    try { chart.vwapSeries.setData(_computeVwapPoints(candles, chart.vwapPeriod || 0)); } catch (e) { console.error('VWAP setData failed', e); }
  }
  if (chart.indMacdEl?.checked && chart.macdTv) {
    const m = _computeMacdPoints(candles, chart.macdFast || 12, chart.macdSlow || 26, chart.macdSignal || 9);
    try { chart.macdLineSeries?.setData?.(m.macPts); } catch (e) { console.error('MACD line setData failed', e); }
    try { chart.macdSignalSeries?.setData?.(m.sigPts); } catch (e) { console.error('MACD signal setData failed', e); }
    try { chart.macdHistSeries?.setData?.(m.histPts); } catch (e) { console.error('MACD hist setData failed', e); }
  }
  tvResize(chart);
}

function flushPendingCandles(chart){
  if (!chart || chart.tf === '1s') return;
  if (!chart.pendingCandles || chart.pendingCandles.size === 0) return;
  if (playheadNs == null) return;
  const tfn = tfNs(chart.tf);
  // Only apply candles that are fully in the past relative to the playhead.
  const ready = [];
  for (const [t, c] of chart.pendingCandles.entries()){
    if (!Number.isFinite(t)) continue;
    if (Number(t) + tfn <= Number(playheadNs)) ready.push(Number(t));
  }
  if (!ready.length) return;
  ready.sort((a,b)=>a-b);
  for (const t of ready){
    const c = chart.pendingCandles.get(t);
    chart.pendingCandles.delete(t);
    // Don't overwrite the live candle for the current bucket.
    if (chart.liveBucket != null && c?.t === chart.liveBucket) continue;
    _upsertCandleRaw(chart, c);
  }
  chart._tvNeedsSetData = true;
  _scheduleRedraw(chart);
}

function _scheduleRedraw(chart){
  if (chart._raf) return;
  chart._raf = requestAnimationFrame(()=>{
    chart._raf = 0;
    drawChart(chart);
  });
}

function _upsertCandleRaw(chart, c){
  if (!chart || !c || !Number.isFinite(c.t)) return;
  const n = chart.candles.length;
  if (n === 0 || c.t > chart.candles[n-1].t) chart.candles.push(c);
  else if (c.t === chart.candles[n-1].t) chart.candles[n-1] = c;
  else {
    let lo=0, hi=n-1;
    while (lo<=hi){
      const mid=(lo+hi)>>1;
      if (chart.candles[mid].t < c.t) lo=mid+1;
      else if (chart.candles[mid].t > c.t) hi=mid-1;
      else { chart.candles[mid]=c; return; }
    }
    chart.candles.splice(lo,0,c);
  }
}

function updateChartFromTrade(chart, tsNs, price, size){
  if (!chart || chart.tf === '1s') return;
  if (!Number.isFinite(tsNs) || !Number.isFinite(price)) return;
  const tfn = tfNs(chart.tf);
  const bucket = Math.floor(tsNs / tfn) * tfn;
  chart.liveBucket = bucket;
  const last = chart.candles.length ? chart.candles[chart.candles.length-1] : null;
  const vAdd = Number(size || 0);
  if (last && last.t === bucket){
    last.c = price;
    last.h = Math.max(last.h, price);
    last.l = Math.min(last.l, price);
    last.v = Number(last.v || 0) + vAdd;
    _scheduleRedraw(chart);
    flushPendingCandles(chart);
    return;
  }
  if (last && last.t < bucket){
    const o = (last.c != null) ? last.c : price;
    chart.candles.push({t: bucket, o, h: price, l: price, c: price, v: vAdd});
    _scheduleRedraw(chart);
    flushPendingCandles(chart);
    return;
  }
  if (!last){
    chart.candles.push({t: bucket, o: price, h: price, l: price, c: price, v: vAdd});
    _scheduleRedraw(chart);
    flushPendingCandles(chart);
    return;
  }
}

// ---------------- Chart ----------------
function resizeCanvas(){
  const c = $('chart');
  const rect = c.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  c.width = Math.floor(rect.width * dpr);
  c.height = Math.floor(rect.height * dpr);
  drawChart();
}

function visibleWindow(){
  const n = candles.length;
  if (n === 0) return {i0: 0, i1: 0};
  const maxVisible = Math.max(20, Math.floor(240 / zoom));
  const i1 = n - 1 - Math.floor(pan * Math.max(0, n - maxVisible));
  const i0 = Math.max(0, i1 - maxVisible + 1);
  return {i0, i1};
}

function drawChart(){
  const c = $('chart');
  const ctx = c.getContext('2d');
  if (!ctx) return;
  const dpr = window.devicePixelRatio || 1;
  ctx.setTransform(1,0,0,1,0,0);
  ctx.clearRect(0,0,c.width,c.height);
  ctx.scale(dpr,dpr);
  const w = c.getBoundingClientRect().width;
  const h = c.getBoundingClientRect().height;

  // background
  ctx.fillStyle = '#0b1220';
  ctx.fillRect(0,0,w,h);
  ctx.strokeStyle = 'rgba(34,48,69,0.55)';
  ctx.lineWidth = 1;
  // reserve bottom area for volume bars
  const volH = 110;
  const priceH = h - volH;
  const roundTick = (x, tick)=> Math.round(x / tick) * tick;
  // divider
  ctx.beginPath(); ctx.moveTo(0, priceH + 0.5); ctx.lineTo(w, priceH + 0.5); ctx.stroke();

  if (candles.length === 0) return;
  const {i0,i1} = visibleWindow();
  const slice = candles.slice(i0, i1+1);
  let lo = Infinity, hi = -Infinity;
  for (const k of slice){ lo = Math.min(lo, k.l); hi = Math.max(hi, k.h); }
  if (!isFinite(lo) || !isFinite(hi)) return;
  if (hi === lo) {
    // flat candle(s): create an artificial range so we still render candlesticks
    const bump = Math.max(0.01, Math.abs(hi) * 0.001);
    hi += bump;
    lo -= bump;
  }

  const pad = (hi-lo) * 0.08;
  lo -= pad; hi += pad;

  const pxY = (p)=> priceH - ((p - lo) / (hi - lo)) * (priceH - 22) - 12;
  const n = slice.length;
  const candleW = Math.max(3, Math.floor((w - 60) / n));
  const x0 = 50;

  // y-axis labels + gridlines (align labels to lines, round to nearest 5 cents)
  ctx.fillStyle = 'rgba(154,164,178,0.92)';
  ctx.font = '12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace';
  ctx.textBaseline = 'middle';
  for (let i=0;i<=5;i++){
    const p = roundTick(lo + (i/5)*(hi-lo), 0.05);
    const y = Math.floor(pxY(p)) + 0.5;
    // gridline at exact label position
    ctx.strokeStyle = 'rgba(34,48,69,0.55)';
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    // label close to the line for easy association
    const label = p.toFixed(2);
    const padX = 6, padY = 2;
    const tw = ctx.measureText(label).width;
    ctx.fillStyle = 'rgba(11,18,32,0.75)';
    ctx.fillRect(4, y - 7 - padY, tw + padX*2, 14 + padY*2);
    ctx.fillStyle = 'rgba(154,164,178,0.92)';
    ctx.fillText(label, 4 + padX, y);
  }
  ctx.textBaseline = 'alphabetic';

  // candles
  for (let i=0;i<n;i++){
    const k = slice[i];
    const x = x0 + i*candleW;
    const yH = pxY(k.h), yL = pxY(k.l), yO = pxY(k.o), yC = pxY(k.c);
    const up = k.c >= k.o;
    ctx.strokeStyle = up ? 'rgba(37,211,102,0.9)' : 'rgba(255,77,77,0.9)';
    ctx.fillStyle = up ? 'rgba(37,211,102,0.55)' : 'rgba(255,77,77,0.55)';
    // wick
    ctx.beginPath();
    ctx.moveTo(x + candleW/2, yH);
    ctx.lineTo(x + candleW/2, yL);
    ctx.stroke();
    // body
    const top = Math.min(yO,yC);
    const bot = Math.max(yO,yC);
    const bh = Math.max(1, bot-top);
    ctx.fillRect(x+1, top, candleW-2, bh);
  }

  // volume bars
  let vmax = 0;
  for (const k of slice) vmax = Math.max(vmax, k.v || 0);
  const vY0 = priceH + 8;
  const vH = volH - 18;
  for (let i=0;i<n;i++){
    const k = slice[i];
    const x = x0 + i*candleW;
    const up = k.c >= k.o;
    const vh = vmax > 0 ? Math.max(1, Math.floor((k.v / vmax) * vH)) : 1;
    ctx.fillStyle = up ? 'rgba(37,211,102,0.55)' : 'rgba(255,77,77,0.55)';
    ctx.fillRect(x+2, vY0 + (vH - vh), Math.max(1, candleW-4), vh);
  }
  ctx.fillStyle = 'rgba(154,164,178,0.9)';
  ctx.fillText(`Vol (max ${vmax})`, 6, priceH + 18);

  // x-axis time labels (few)
  ctx.fillStyle = 'rgba(154,164,178,0.9)';
  const steps = 4;
  for (let i=0;i<=steps;i++){
    const idx = Math.floor(i0 + (i/steps)*(i1-i0));
    const t = candles[idx]?.t;
    if (!t) continue;
    const label = nsToEt(t).slice(11,19); // HH:MM:SS (ET)
    const x = x0 + (idx - i0) * candleW;
    ctx.fillText(label, x, h - 6);
  }

  // HUD
  const last = candles[candles.length-1];
  $('hud').textContent = last ? `Last: ${fmtPx(last.c)} @ ${nsToEt(last.t).slice(11,19)} ET` : '';
}

// Chart manager (multiple charts)
const charts = new Map(); // id -> chartObj
let chartSeq = 0;

// ---------------- Order Entry (UI-only; no fills/PnL yet) ----------------
function makeEntryHtml(){
  return `
    <div class="entryWrap">
      <div class="entryGrid">
        <div class="lbl">Order</div>
        <select id="entry-ordtype">
          <option value="MKT">Market</option>
          <option value="LMT" selected>Limit</option>
          <option value="STOP">Stop</option>
          <option value="STOPLMT">Stop-Limit</option>
        </select>

        <div class="lbl">Shares</div>
        <input id="entry-shares" class="num" type="number" min="0" step="1" value="100"/>

        <div class="lbl">Limit</div>
        <div class="entryRow2">
          <input id="entry-lmt" class="num" type="number" step="0.001" placeholder="LMT Price"/>
          <div class="entrySmallBtns">
            <button class="wbtn" id="entry-px-bid" title="Set to current bid">Bid</button>
            <button class="wbtn" id="entry-px-ask" title="Set to current ask">Ask</button>
            <button class="wbtn" id="entry-px-mid" title="Set to mid">Mid</button>
            <button class="wbtn" id="entry-px-last" title="Set to last trade">Last</button>
          </div>
        </div>

        <div class="lbl">Stop</div>
        <input id="entry-stop" class="num" type="number" step="0.01" placeholder="Stop Trigger"/>

        <div class="lbl">Route</div>
        <input id="entry-route" placeholder="ROUTE (e.g. SMRTL)" />

        <div class="lbl">Display</div>
        <input id="entry-display" class="num" type="number" min="0" step="1" value="0" title="0 = show full size"/>
      </div>

      <div class="entryBtns">
        <button id="entry-send-buy" class="entryBtn entryBuy">BUY</button>
        <button id="entry-send-sell" class="entryBtn entrySell">SELL</button>
      </div>

      <div class="entryHint">
        Local fill sim (MKT/LMT/STOP/STOP-LMT). Price helpers use current Book/Tape.
      </div>
    </div>
  `;
}

function initEntryWindow(){
  const ordEl = $('entry-ordtype');
  const sharesEl = $('entry-shares');
  const lmtEl = $('entry-lmt');
  const stopEl = $('entry-stop');
  const routeEl = $('entry-route');
  const buyBtn = $('entry-send-buy');
  const sellBtn = $('entry-send-sell');

  const setNum = (el, v)=>{
    if (!el) return;
    if (v == null || !Number.isFinite(Number(v))) return;
    el.value = String(Number(v));
    if (el === lmtEl) _applyLmtRoundingEl(lmtEl);
  };
  const bid = ()=> currentBook?.bids?.[0]?.[0];
  const ask = ()=> currentBook?.asks?.[0]?.[0];
  const mid = ()=>{
    const b = bid(), a = ask();
    if (b == null || a == null) return null;
    return (Number(b) + Number(a)) / 2;
  };
  const last = ()=> lastTrade?.price ?? null;

  $('entry-px-bid')?.addEventListener('click', (e)=>{ e.preventDefault(); setNum(lmtEl, bid()); });
  $('entry-px-ask')?.addEventListener('click', (e)=>{ e.preventDefault(); setNum(lmtEl, ask()); });
  $('entry-px-mid')?.addEventListener('click', (e)=>{ e.preventDefault(); setNum(lmtEl, mid()); });
  $('entry-px-last')?.addEventListener('click', (e)=>{ e.preventDefault(); setNum(lmtEl, last()); });

  const syncFields = ()=>{
    const t = ordEl?.value || 'LMT';
    const needsLmt = (t === 'LMT' || t === 'STOPLMT');
    const needsStop = (t === 'STOP' || t === 'STOPLMT');

    if (lmtEl) {
      lmtEl.disabled = !needsLmt;
      lmtEl.parentElement?.classList?.toggle('entryDisabled', !needsLmt);
    }
    if (stopEl) {
      stopEl.disabled = !needsStop;
      stopEl.classList.toggle('entryDisabled', !needsStop);
    }
  };

  ordEl?.addEventListener('change', syncFields);
  syncFields();
  lmtEl?.addEventListener('blur', ()=>_applyLmtRoundingEl(lmtEl));

  // Place order (simple MKT/LMT for now).
  const send = (forcedSide)=>{
    const symbol = $('symbol')?.value?.trim?.() ?? '';
    const side = forcedSide || 'BUY';
    const ordType = ordEl?.value || 'LMT';
    const shares = Number(sharesEl?.value || 0);
    if (ordType === 'LMT') _applyLmtRoundingEl(lmtEl);
    const lmt = lmtEl?.value ? Number(lmtEl.value) : null;
    const stop = stopEl?.value ? Number(stopEl.value) : null;
    const route = String(routeEl?.value || '').trim();
    const display = Number($('entry-display')?.value || 0);

    if (!symbol) { setErr('Entry: missing symbol'); return; }
    if (!Number.isFinite(shares) || shares <= 0) { setErr('Entry: invalid share size'); return; }
    if ((ordType === 'LMT' || ordType === 'STOPLMT') && (!Number.isFinite(lmt) || lmt == null)) { setErr('Entry: limit price required'); return; }
    if ((ordType === 'STOP' || ordType === 'STOPLMT') && (!Number.isFinite(stop) || stop == null)) { setErr('Entry: stop trigger required'); return; }
    if (ordType !== 'MKT' && ordType !== 'LMT' && ordType !== 'STOP' && ordType !== 'STOPLMT') { setErr('Entry: invalid order type'); return; }

    setErr('');
    placeOrder({
      symbol,
      side,
      type: ordType,
      qty: Math.floor(shares),
      limitPx: (ordType === 'LMT' || ordType === 'STOPLMT') ? Number(lmt) : null,
      stopPx: (ordType === 'STOP' || ordType === 'STOPLMT') ? Number(stop) : null,
      route: route || null,
      display,
    });
  };

  buyBtn?.addEventListener('click', (e)=>{ e.preventDefault(); send('BUY'); });
  sellBtn?.addEventListener('click', (e)=>{ e.preventDefault(); send('SELL'); });
}

// ---------------- Positions / Portfolio (UI-only; no fills/PnL yet) ----------------
const POS_COLS_ALL = [
  { key: 'ticker', label: 'Ticker' },
  { key: 'open_pl', label: 'Open P/L' },
  { key: 'total_pl', label: 'Total P/L' },
  { key: 'cost', label: 'Cost Basis' },
  { key: 'shares', label: '# Shares' },
  { key: 'last', label: 'Last Price' },
  { key: 'plps', label: 'P/L / Sh' },
];
const POS_COLS_KEY = 'sim-pos-cols-v1';
let posCols = null; // array of keys

// positions: symbol -> { shares:number (signed; short negative), avgCost:number|null, realized:number }
const positions = new Map();

// orders + fills (simple simulator)
let _orderSeq = 0;
let _fillSeq = 0;
const orders = new Map(); // id -> order
let fills = []; // newest last; each: {id, orderId, ts_ns, symbol, side, qty, price, type}

// partial fill tuning (simple + deterministic)
const TAKE_PARTICIPATION = 0.85;    // aggressive fills vs book liquidity
const PASSIVE_PARTICIPATION = 0.40; // passive limit fills vs prints at our price (after queueAhead clears)

// ---------------- Layout persistence ----------------
const LAYOUT_STORAGE_KEY = 'sim-layout-v1';
function _numPx(s, d=0){
  const v = parseFloat(String(s || '').replace('px',''));
  return Number.isFinite(v) ? v : d;
}
function captureLayout(){
  const wins = {};
  document.querySelectorAll('.win').forEach(w=>{
    const id = w.id;
    if (!id) return;
    // Windows explicitly "closed" via the X button should not be persisted in layout at all.
    if (w?.dataset?.closed === '1') return;
    wins[id] = {
      x: _numPx(w.style.left, 0),
      y: _numPx(w.style.top, 0),
      w: _numPx(w.style.width, 420),
      h: _numPx(w.style.height, 300),
      z: _numPx(w.style.zIndex, 0),
      hidden: (w.style.display === 'none'),
    };
  });
  const chartLayouts = [];
  try {
    for (const ch of charts.values()){
      const w = ch?.win;
      if (!w) continue;
      if (w?.dataset?.closed === '1') continue;
      chartLayouts.push({
        id: ch.id,
        tf: ch.tf,
        x: _numPx(w.style.left, 0),
        y: _numPx(w.style.top, 0),
        w: _numPx(w.style.width, 820),
        h: _numPx(w.style.height, 680),
      });
    }
  } catch {}
  return { version: 1, windows: wins, charts: chartLayouts };
}

function loadLayout(){
  try {
    if (typeof DISK_LAYOUT !== 'undefined' && DISK_LAYOUT && typeof DISK_LAYOUT === 'object') {
      return JSON.parse(JSON.stringify(DISK_LAYOUT));
    }
    const raw = localStorage.getItem(LAYOUT_STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function applyLayoutToExistingWindows(layout){
  if (!layout || typeof layout !== 'object') return;
  const wins = layout.windows || {};
  for (const [id, r] of Object.entries(wins)){
    const w = document.getElementById(id);
    if (!w || !r) continue;
    if (Number.isFinite(Number(r.x))) w.style.left = `${Number(r.x)}px`;
    if (Number.isFinite(Number(r.y))) w.style.top = `${Number(r.y)}px`;
    if (Number.isFinite(Number(r.w))) w.style.width = `${Math.max(260, Number(r.w))}px`;
    if (Number.isFinite(Number(r.h))) w.style.height = `${Math.max(200, Number(r.h))}px`;
    if (r.hidden) w.style.display = 'none';
  }
}

let _layoutSaveTimer = null;
function scheduleLayoutSave(){
  if (_layoutSaveTimer) clearTimeout(_layoutSaveTimer);
  _layoutSaveTimer = setTimeout(()=>{
    try {
      const lay = captureLayout();
      localStorage.setItem(LAYOUT_STORAGE_KEY, JSON.stringify(lay));
      scheduleConfigSave('layout', lay);
    } catch {}
  }, 500);
}

// ---------------- Hotkeys manager ----------------
const HK_CONTEXTS = ['global','entry','chart','tape','l2'];
const HK_STORAGE_KEY = 'sim-hotkeys-v1';
const HK_DEFAULTS = {
  global: {
    'Shift+B': { cmd: 'trade.buy', args: {} },
    'Shift+S': { cmd: 'trade.sell', args: {} },
    'Shift+F': { cmd: 'risk.flatten_all', args: {} },
    'Shift+C': { cmd: 'risk.cancel_all_orders', args: {} },
  },
  entry: {
    'Enter': { cmd: 'trade.buy', args: {} },
    'Shift+Enter': { cmd: 'trade.sell', args: {} },
  },
  chart: {},
  tape: {},
  l2: {},
};

const BASE_COMMANDS = [
  { id: 'trade.buy', label: 'Buy (uses Order Entry settings)', contexts: ['global','entry'] },
  { id: 'trade.sell', label: 'Sell (uses Order Entry settings)', contexts: ['global','entry'] },
  { id: 'risk.flatten_all', label: 'Close all positions (flatten)', contexts: ['global'] },
  { id: 'risk.cancel_all_orders', label: 'Close all open orders (cancel all)', contexts: ['global'] },
];

let hk = null; // bindings only: {context: {chord: {cmd,args}}}
let hkCustom = {}; // { "custom:foo": {label, contexts?, script?, alias?, args?, steps? } }
let hkFilter = '';
let hkTab = 'global';
let hkRecording = null; // {context, commandId}

function _clone(x){ return JSON.parse(JSON.stringify(x)); }

// ---------------- Commands (DAS-like scripting; saved to Configs/commands.json) ----------------
const CMD_STORAGE_KEY = 'sim-commands-v1';
const CMD_DEFAULTS = { version: 1, commands: [] }; // commands: [{id,name,script}]
let cmdState = null;
let cmdSelectedId = null;

function _normalizeCmdState(obj){
  const out = _clone(CMD_DEFAULTS);
  if (!obj || typeof obj !== 'object') return out;
  if (Number(obj.version || 0) !== 1) return out;
  const arr = Array.isArray(obj.commands) ? obj.commands : [];
  out.commands = arr
    .filter(x=>x && typeof x === 'object')
    .map(x=>({ id: String(x.id || ''), name: String(x.name || ''), script: String(x.script || '') }))
    .filter(x=> x.id && x.name);
  return out;
}

function _newCmdId(){
  return 'c_' + Math.random().toString(16).slice(2) + '_' + Date.now().toString(16);
}

function loadCommands(){
  try {
    if (typeof DISK_COMMANDS !== 'undefined' && DISK_COMMANDS && typeof DISK_COMMANDS === 'object') {
      return _normalizeCmdState(DISK_COMMANDS);
    }
    const raw = localStorage.getItem(CMD_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return _normalizeCmdState(parsed);
  } catch {
    return _clone(CMD_DEFAULTS);
  }
}

// Initialize command state early so hotkeys/settings can see the catalog even if the Commands window is closed.
try { cmdState = loadCommands(); } catch { cmdState = _clone(CMD_DEFAULTS); }

function saveCommands(){
  if (!cmdState) return;
  try { localStorage.setItem(CMD_STORAGE_KEY, JSON.stringify(cmdState)); } catch {}
  try { scheduleConfigSave('commands', cmdState); } catch {}
  // Hotkeys UI includes a command catalog; refresh if it exists.
  try { renderHotkeys(); } catch {}
}

function _cmdCatalog(){
  const cmds = [];
  try {
    const arr = cmdState?.commands || [];
    for (const c of arr){
      const id = String(c?.id || '');
      const name = String(c?.name || id);
      if (!id) continue;
      cmds.push({ id: `das:${id}`, label: `CMD: ${name}`, contexts: ['global','entry'] });
    }
  } catch {}
  return cmds;
}

// ---------------- Global variables ($name), persisted to localStorage ----------------
const GLOBAL_VARS_KEY = 'sim-global-vars-v1';
let GLOBAL_VARS = null; // {lowerKey: any}

function _normGlobalName(name){
  const s = String(name || '').trim();
  if (!s) throw new Error('Global var name is required');
  const n = s.startsWith('$') ? s.slice(1) : s;
  if (!n) throw new Error('Global var name is required');
  return n.toLowerCase();
}

function _loadGlobalVars(){
  try {
    const raw = localStorage.getItem(GLOBAL_VARS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return (parsed && typeof parsed === 'object') ? parsed : {};
  } catch {
    return {};
  }
}

function _saveGlobalVars(){
  try { localStorage.setItem(GLOBAL_VARS_KEY, JSON.stringify(GLOBAL_VARS || {})); } catch {}
}

function getGlobalVar(name){
  if (!GLOBAL_VARS) GLOBAL_VARS = _loadGlobalVars();
  const k = _normGlobalName(name);
  return GLOBAL_VARS[k];
}

function setGlobalVar(name, value){
  if (!GLOBAL_VARS) GLOBAL_VARS = _loadGlobalVars();
  const k = _normGlobalName(name);
  GLOBAL_VARS[k] = value;
  _saveGlobalVars();
}

function delGlobalVar(name){
  if (!GLOBAL_VARS) GLOBAL_VARS = _loadGlobalVars();
  const k = _normGlobalName(name);
  delete GLOBAL_VARS[k];
  _saveGlobalVars();
}

function _normalizeHotkeysState(obj){
  // Accept:
  // - v1: bindings map {global:{...}, entry:{...}, ...}
  // - v2: {version:2, bindings:{...}, customCommands:{...}}
  const out = { version: 2, bindings: _clone(HK_DEFAULTS), customCommands: {} };
  if (!obj || typeof obj !== 'object') return out;
  if (obj.bindings && typeof obj.bindings === 'object') {
    out.bindings = _clone(obj.bindings);
    if (obj.customCommands && typeof obj.customCommands === 'object') out.customCommands = _clone(obj.customCommands);
    return out;
  }
  // v1
  out.bindings = _clone(obj);
  return out;
}

function exportHotkeysState(){
  return { version: 2, bindings: _clone(hk || HK_DEFAULTS), customCommands: _clone(hkCustom || {}) };
}

function _allCommands(){
  const cmds = [...BASE_COMMANDS, ..._cmdCatalog()];
  return cmds;
}

function _isTypingTarget(ev){
  const t = ev?.target;
  const tag = (t?.tagName || '').toLowerCase();
  if (tag === 'input' || tag === 'textarea' || tag === 'select') return true;
  if (t?.isContentEditable) return true;
  return false;
}

function _normalizeKey(k){
  if (!k) return '';
  if (k === ' ') return 'Space';
  if (k === 'Escape') return 'Esc';
  if (k === 'ArrowUp') return 'Up';
  if (k === 'ArrowDown') return 'Down';
  if (k === 'ArrowLeft') return 'Left';
  if (k === 'ArrowRight') return 'Right';
  if (k.length === 1) return k.toUpperCase();
  return k;
}

function chordFromEvent(ev){
  if (!ev) return null;
  const parts = [];
  if (ev.ctrlKey) parts.push('Ctrl');
  if (ev.metaKey) parts.push('Meta');
  if (ev.altKey) parts.push('Alt');
  if (ev.shiftKey) parts.push('Shift');
  const key = _normalizeKey(ev.key);
  if (!key || key === 'Control' || key === 'Shift' || key === 'Alt' || key === 'Meta') return null;
  parts.push(key);
  return parts.join('+');
}

function loadHotkeys(){
  try {
    if (typeof DISK_HOTKEYS !== 'undefined' && DISK_HOTKEYS && typeof DISK_HOTKEYS === 'object') {
      const st = _normalizeHotkeysState(DISK_HOTKEYS);
      hkCustom = st.customCommands || {};
      return _clone(st.bindings);
    }
    const raw = localStorage.getItem(HK_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && typeof parsed === 'object') {
      const st = _normalizeHotkeysState(parsed);
      hkCustom = st.customCommands || {};
      return _clone(st.bindings);
    }
  } catch {}
  hkCustom = {};
  return JSON.parse(JSON.stringify(HK_DEFAULTS));
}
function saveHotkeys(){
  const state = exportHotkeysState();
  try { localStorage.setItem(HK_STORAGE_KEY, JSON.stringify(state)); } catch {}
  try { scheduleConfigSave('hotkeys', state); } catch {}
}
function resetHotkeysToDefault(){
  hk = JSON.parse(JSON.stringify(HK_DEFAULTS));
  hkCustom = {};
  saveHotkeys();
  renderHotkeys();
}

// Disk config save (debounced)
let _cfgSaveTimers = {};
function scheduleConfigSave(name, data){
  const key = String(name || '');
  if (!key) return;
  if (_cfgSaveTimers[key]) clearTimeout(_cfgSaveTimers[key]);
  _cfgSaveTimers[key] = setTimeout(async ()=>{
    try {
      await fetch('/api/config/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: key, data }),
      });
    } catch {}
  }, 350);
}

function _knownCmd(cmd){
  if (!cmd) return false;
  if (String(cmd).startsWith('custom:')) return true;
  if (String(cmd).startsWith('das:')) return true;
  return BASE_COMMANDS.some(c=>c.id === cmd);
}

function findHotkeyWarnings(){
  const out = [];
  for (const ctx of HK_CONTEXTS){
    const map = hk?.[ctx] || {};
    for (const [ch, b] of Object.entries(map)){
      const cmd = b?.cmd;
      if (!_knownCmd(cmd)) out.push({ctx, chord: ch, cmd, kind:'unknown'});
      // shadow warning: chord differs between ctx and global
      if (ctx !== 'global' && hk?.global?.[ch] && hk.global[ch]?.cmd !== cmd) {
        out.push({ctx, chord: ch, cmd, kind:'shadows global'});
      }
    }
  }
  return out;
}

function executeCommand(cmdId, args){
  const id = String(cmdId || '');
  if (id === 'trade.buy') { document.getElementById('entry-send-buy')?.click?.(); return; }
  if (id === 'trade.sell') { document.getElementById('entry-send-sell')?.click?.(); return; }
  if (id.startsWith('das:')) {
    const cid = id.slice(4);
    const c = (cmdState?.commands || []).find(x=>String(x?.id||'') === cid) || null;
    if (!c) { setErr(`Unknown DAS command: ${id}`); return; }
    try { runDasScript(String(c.script || ''), { name: String(c.name||cid), id: cid }); }
    catch (e) { setErr(`Command failed: ${e?.message ?? e}`); }
    return;
  }
  if (id === 'risk.cancel_all_orders') {
    const nowNs = (playheadNs ?? loadedStartNs ?? null);
    for (const o of orders.values()){
      if (o && (o.status === 'open' || o.status === 'partial')) cancelOrder(o.id, nowNs);
    }
    renderOpenOrders();
    setStatus('Cancelled all open orders');
    return;
  }
  if (id === 'risk.flatten_all') {
    let did = 0;
    for (const [sym, p] of positions.entries()){
      const sh = Number(p?.shares ?? 0);
      if (!Number.isFinite(sh) || sh === 0) continue;
      const side = (sh > 0) ? 'SELL' : 'BUY';
      const qty = Math.abs(Math.floor(sh));
      const px = _bestPxForMarket(side);
      if (px == null) continue;
      placeOrder({ symbol: sym, side, type:'MKT', qty, limitPx:null, display:0 });
      did += 1;
    }
    setStatus(did ? 'Flattened positions (best-effort)' : 'No positions to flatten (or no price feed)');
    return;
  }
  if (id.startsWith('custom:')) {
    const meta = hkCustom?.[id] || null;
    // minimal safe "custom command" model:
    // - alias: run a single known command id
    // - steps: [{cmd,args}] (runs sequentially)
    try {
      const seen = new Set();
      const run = (cmd, a)=>{
        const cid = String(cmd || '');
        if (!cid) return;
        if (seen.has(cid)) { setErr(`Custom command loop: ${cid}`); return; }
        seen.add(cid);
        if (cid.startsWith('custom:')) {
          const m2 = hkCustom?.[cid] || null;
          if (Array.isArray(m2?.steps) && m2.steps.length) {
            for (const st of m2.steps) run(st?.cmd, st?.args);
            return;
          }
          if (m2?.alias) { run(m2.alias, m2?.args || {}); return; }
          setStatus(`Custom command invoked: ${cid}`);
          return;
        }
        executeCommand(cid, a || {});
      };
      if (Array.isArray(meta?.steps) && meta.steps.length) {
        for (const st of meta.steps) run(st?.cmd, st?.args);
        return;
      }
      if (meta?.alias) { run(meta.alias, meta?.args || {}); return; }
    } catch {}
    setStatus(`Custom command invoked: ${id}`);
    return;
  }
  setErr(`Unknown command: ${id}`);
}

// ---------------- DAS-like script execution (client-side) ----------------
function _splitStatements(src){
  const s = String(src || '');
  const out = [];
  let buf = '';
  let depth = 0;
  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (ch === '(') depth += 1;
    if (ch === ')') depth = Math.max(0, depth - 1);
    if ((ch === ';' || ch === ',') && depth === 0){
      if (buf.trim()) out.push(buf.trim());
      buf = '';
      continue;
    }
    buf += ch;
  }
  if (buf.trim()) out.push(buf.trim());
  return out;
}

function _tokenizeExpr(src){
  const s = String(src || '');
  const toks = [];
  let i = 0;
  const isWS = (c)=> /\s/.test(c);
  const isNumStart = (c)=> (/\d/.test(c) || (c === '.' && i+1 < s.length && /\d/.test(s[i+1])));
  const isIdStart = (c)=> /[A-Za-z_$]/.test(c);
  const isIdCont = (c)=> /[A-Za-z0-9_]/.test(c);
  while (i < s.length){
    const c = s[i];
    if (isWS(c)) { i++; continue; }
    if (c === '(' || c === ')'){ toks.push({k:c, v:c}); i++; continue; }
    if (c === '+' || c === '-' || c === '*' || c === '/'){ toks.push({k:'op', v:c}); i++; continue; }
    if (isNumStart(c)){
      let j = i;
      while (j < s.length && /[\d.]/.test(s[j])) j++;
      const txt = s.slice(i, j);
      const v = Number(txt);
      if (!Number.isFinite(v)) throw new Error(`Invalid number: ${txt}`);
      toks.push({k:'num', v});
      i = j;
      continue;
    }
    if (isIdStart(c)){
      let j = i+1;
      while (j < s.length && /[A-Za-z0-9_$]/.test(s[j])) j++;
      toks.push({k:'id', v: s.slice(i, j)});
      i = j;
      continue;
    }
    throw new Error(`Unexpected character in expression: '${c}'`);
  }
  return toks;
}

function _evalExpr(expr, envGet){
  const toks = _tokenizeExpr(expr);
  const out = [];
  const ops = [];
  const prec = {'u+':3,'u-':3,'*':2,'/':2,'+':1,'-':1};
  const isOpTok = (t)=> t && t.k === 'op';
  let prev = null;
  for (const t of toks){
    if (t.k === 'num' || t.k === 'id') { out.push(t); prev = t; continue; }
    if (t.k === '(') { ops.push(t); prev = t; continue; }
    if (t.k === ')'){
      while (ops.length && ops[ops.length-1].k !== '(') out.push(ops.pop());
      if (!ops.length) throw new Error('Mismatched parentheses');
      ops.pop();
      prev = t;
      continue;
    }
    if (isOpTok(t)){
      let op = t.v;
      const unary = (!prev || (prev.k === 'op') || (prev.k === '('));
      if (unary && (op === '+' || op === '-')) op = (op === '+') ? 'u+' : 'u-';
      while (ops.length){
        const top = ops[ops.length-1];
        if (top.k === '(') break;
        const topOp = top.v;
        if ((prec[topOp] ?? 0) >= (prec[op] ?? 0)) out.push(ops.pop());
        else break;
      }
      ops.push({k:'op', v: op});
      prev = t;
      continue;
    }
    throw new Error('Invalid expression token');
  }
  while (ops.length){
    const top = ops.pop();
    if (top.k === '(') throw new Error('Mismatched parentheses');
    out.push(top);
  }
  const st = [];
  for (const t of out){
    if (t.k === 'num'){ st.push(Number(t.v)); continue; }
    if (t.k === 'id'){
      const v = envGet(String(t.v||'').toLowerCase());
      if (!Number.isFinite(Number(v))) throw new Error(`Variable '${t.v}' is not numeric`);
      st.push(Number(v));
      continue;
    }
    if (t.k === 'op'){
      const op = t.v;
      if (op === 'u+' || op === 'u-'){
        const a = st.pop();
        if (!Number.isFinite(a)) throw new Error('Bad unary operand');
        st.push(op === 'u-' ? -a : +a);
        continue;
      }
      const b = st.pop();
      const a = st.pop();
      if (!Number.isFinite(a) || !Number.isFinite(b)) throw new Error('Bad operands');
      if (op === '+') st.push(a + b);
      else if (op === '-') st.push(a - b);
      else if (op === '*') st.push(a * b);
      else if (op === '/') st.push(a / b);
      else throw new Error(`Unknown op ${op}`);
      continue;
    }
  }
  if (st.length !== 1) throw new Error('Invalid expression');
  return st[0];
}

function _envSnapshot(){
  const bidPx = ()=> currentBook?.bids?.[0]?.[0] ?? null;
  const askPx = ()=> currentBook?.asks?.[0]?.[0] ?? null;
  const frozenAsk = (askPx() == null ? null : Number(askPx()));
  const frozenBid = (bidPx() == null ? null : Number(bidPx()));
  const getTicker = ()=> String(document.getElementById('symbol')?.value?.trim?.() ?? '').toUpperCase();
  const getPosShares = ()=>{
    const sym = getTicker();
    const p = positions.get(sym);
    return p ? Number(p.shares ?? 0) : 0;
  };
  const getCostBasis = ()=>{
    const sym = getTicker();
    const p = positions.get(sym);
    const c = (p && p.avgCost != null) ? Number(p.avgCost) : null;
    return (c == null || !Number.isFinite(c)) ? null : c;
  };
  return {
    get: (name)=>{
      const n = String(name||'').toLowerCase();
      if (n === 'ask') return frozenAsk;
      if (n === 'bid') return frozenBid;
      if (n === 'last') return Number(lastTrade?.price ?? null);
      if (n === 'hi') return (sessionStats?.hi == null) ? null : Number(sessionStats.hi);
      if (n === 'lo') return (sessionStats?.lo == null) ? null : Number(sessionStats.lo);
      if (n === 'open') return (sessionStats?.open == null) ? null : Number(sessionStats.open);
      if (n === 'pcl') return (sessionStats?.pcl == null) ? null : Number(sessionStats.pcl);
      if (n === 'l2bid') return frozenBid;
      if (n === 'l2ask') return frozenAsk;
      if (n === 'buypower') return _buyingPower();
      if (n === 'ticker') return getTicker();
      if (n === 'position') return getPosShares();
      if (n === 'pos') return getPosShares();
      if (n === 'costbasis') return getCostBasis();
      if (n === 'shares') return Number(document.getElementById('entry-shares')?.value || 0);
      if (n === 'share') return Number(document.getElementById('entry-shares')?.value || 0);
      if (n === 'lmtprice') return Number(document.getElementById('entry-lmt')?.value || 0);
      if (n === 'price') return Number(document.getElementById('entry-lmt')?.value || 0);
      if (n === 'stopprice') return Number(document.getElementById('entry-stop')?.value || 0);
      if (n === 'route') return String(document.getElementById('entry-route')?.value || '');
      if (n === 'stoptype') return String(document.getElementById('entry-ordtype')?.value || '');
      // Global vars ($name), case-insensitive
      if (n.startsWith('$')) return getGlobalVar(n);
      throw new Error(`Unknown variable: ${name}`);
    },
    set: (name, v)=>{
      const n = String(name||'').toLowerCase();
      if (n === 'shares') { const el=document.getElementById('entry-shares'); if (el) el.value=String(Math.max(0, Math.floor(Number(v)))); return; }
      if (n === 'share') { const el=document.getElementById('entry-shares'); if (el) el.value=String(Math.max(0, Math.floor(Number(v)))); return; }
      if (n === 'lmtprice') { const el=document.getElementById('entry-lmt'); if (el) { el.value=String(Number(v)); _applyLmtRoundingEl(el); } return; }
      if (n === 'price') { const el=document.getElementById('entry-lmt'); if (el) { el.value=String(Number(v)); _applyLmtRoundingEl(el); } return; }
      if (n === 'stopprice') { const el=document.getElementById('entry-stop'); if (el) el.value=String(Number(v)); return; }
      if (n === 'route') { const el=document.getElementById('entry-route'); if (el) el.value=String(v); return; }
      if (n === 'stoptype') {
        const el=document.getElementById('entry-ordtype');
        if (el) {
          el.value = String(v).toUpperCase();
          // best-effort: trigger field enable/disable updates
          try { el.dispatchEvent(new Event('change', {bubbles:true})); } catch {}
        }
        return;
      }
      if (n.startsWith('$')) { setGlobalVar(n, v); return; }
      throw new Error(`Variable '${name}' is read-only`);
    },
  };
}

async function validateDasScriptRemote(src){
  try {
    const res = await fetch('/api/commands/validate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ script: String(src||'') }),
    });
    return await res.json();
  } catch (e) {
    return { ok:false, errors:[{message: `Validate failed: ${e?.message ?? e}`, line:1, col:1}] };
  }
}

function runDasScript(src, meta){
  const env = _envSnapshot();
  const parts = _splitStatements(src);
  for (const st of parts){
    const s = String(st || '').trim();
    if (!s) continue;
    const up = s.toUpperCase();
    if (up === 'BUY') { document.getElementById('entry-send-buy')?.click?.(); continue; }
    if (up === 'SELL') { document.getElementById('entry-send-sell')?.click?.(); continue; }
    if (up === 'CANCELALL' || up === 'CANCEL_ALL' || up === 'CANCELALLORDERS') { executeCommand('risk.cancel_all_orders', {}); continue; }

    // SetVar(name, value) / DelVar(name)
    if (/^SETVAR\s*\(/i.test(s) || /^DELVAR\s*\(/i.test(s)) {
      const m = s.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(([\s\S]*)\)\s*$/);
      if (!m) throw new Error(`Invalid call syntax: '${s}'`);
      const fn = String(m[1]||'').toUpperCase();
      const argsRaw = String(m[2]||'');
      // naive split on comma at depth 0 (parens only)
      const args = [];
      let buf = '', depth = 0;
      for (let i=0;i<argsRaw.length;i++){
        const ch = argsRaw[i];
        if (ch === '(') depth += 1;
        if (ch === ')') depth = Math.max(0, depth - 1);
        if (ch === ',' && depth === 0){
          args.push(buf.trim()); buf=''; continue;
        }
        buf += ch;
      }
      if (buf.trim()) args.push(buf.trim());
      const parseName = (x)=>{
        const t = String(x||'').trim();
        const q = t.match(/^["']([\s\S]*)["']$/);
        return q ? q[1] : t;
      };
      if (fn === 'DELVAR'){
        if (args.length !== 1) throw new Error('DelVar(name) expects 1 argument');
        delGlobalVar(parseName(args[0]));
        continue;
      }
      if (fn === 'SETVAR'){
        if (args.length !== 2) throw new Error('SetVar(name, value) expects 2 arguments');
        const nm = parseName(args[0]);
        const rhs = String(args[1]||'').trim();
        const q = rhs.match(/^["']([\s\S]*)["']$/);
        const val = q ? q[1] : _evalExpr(rhs, (n)=>env.get(n));
        setGlobalVar(nm, val);
        continue;
      }
    }

    const eq = s.indexOf('=');
    if (eq < 0) throw new Error(`Expected assignment or command: '${s}'`);
    const lhs = s.slice(0, eq).trim().toLowerCase();
    const rhs = s.slice(eq+1).trim();
    // $globals: allow string literals or numeric expressions
    if (lhs.startsWith('$')){
      const q = rhs.match(/^["']([\s\S]*)["']$/);
      const val = q ? q[1] : _evalExpr(rhs, (n)=>env.get(n));
      env.set(lhs, val);
      continue;
    }

    // string-like assignments (order-entry props)
    const strWritable = new Set(['route','stoptype']);
    if (strWritable.has(lhs)){
      const q = rhs.match(/^["']([\s\S]*)["']$/);
      env.set(lhs, q ? q[1] : rhs);
      continue;
    }

    const writable = new Set(['shares','share','lmtprice','price','stopprice']);
    if (!writable.has(lhs)) throw new Error(`Variable '${lhs}' is not writable`);
    const val = _evalExpr(rhs, (n)=>env.get(n));
    env.set(lhs, val);
  }
  setStatus(meta?.name ? `Ran command: ${meta.name}` : 'Ran command');
}

function handleHotkey(ev){
  if (!hk) hk = loadHotkeys();
  if (!ev) return;
  if (hkRecording && ev.key === 'Escape') {
    hkRecording = null;
    document.getElementById('hkOverlay')?.style && (document.getElementById('hkOverlay').style.display = 'none');
    ev.preventDefault();
    return;
  }
  if (_isTypingTarget(ev)) return;

  // record mode has priority
  if (hkRecording){
    const ch = chordFromEvent(ev);
    if (!ch) return;
    ev.preventDefault();
    ev.stopPropagation();
    const ctx = hkRecording.context;
    const cmd = hkRecording.commandId;
    hk[ctx] = hk[ctx] || {};
    const prev = hk[ctx][ch]?.cmd;
    hk[ctx][ch] = { cmd, args: {} };
    saveHotkeys();
    hkRecording = null;
    document.getElementById('hkOverlay').style.display = 'none';
    renderHotkeys();
    setStatus(prev && prev !== cmd ? `Rebound ${ch} (${ctx}) from ${prev} → ${cmd}` : `Bound ${cmd} (${ctx}) to ${ch}`);
    return;
  }

  const ch = chordFromEvent(ev);
  if (!ch) return;
  const ctx = activeContext || 'global';
  const b = hk?.[ctx]?.[ch] || hk?.global?.[ch];
  if (!b) return;
  ev.preventDefault();
  try { executeCommand(b.cmd, b.args || {}); } catch (e) { setErr(`Hotkey failed: ${e?.message ?? e}`); }
}
window.addEventListener('keydown', handleHotkey, true);

function makeHotkeysHtml(){
  const tabBtns = HK_CONTEXTS.map(c=>`<button class="hkTab" data-ctx="${c}">${c.toUpperCase()}</button>`).join('');
  return `
    <div class="hkWrap">
      <div class="hkTop">
        <input id="hkFilter" placeholder="Search commands…" />
        <div class="hkTabs">${tabBtns}</div>
        <div style="flex:1;"></div>
        <button class="wbtn" id="hkReset" title="Reset to defaults">Reset</button>
      </div>
      <div id="hkWarns" class="entryHint"></div>
      <div style="flex:1 1 auto; min-height:0; overflow:auto;">
        <table class="posTable">
          <thead>
            <tr>
              <th>Command</th>
              <th>Binding</th>
              <th>Context</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="hkBody"></tbody>
        </table>
      </div>
      <div class="setSection">
        <div class="setRow">
          <div style="color:var(--muted); font-size:12px; font-weight:900;">Hotkeys</div>
          <div style="flex:1;"></div>
          <button class="wbtn" id="hkSaveAs" title="Save hotkeys to a custom file">Save As…</button>
        </div>
        <div style="height:8px;"></div>
        <div class="setHint">
          Use the <code>Commands</code> window to create DAS scripts, then bind them here (command id <code>das:&lt;id&gt;</code>).
        </div>
      </div>
    </div>
    <div class="hkOverlay" id="hkOverlay">
      <div class="hkModal">
        <div class="title">Record a hotkey</div>
        <div class="big" id="hkRecText">Press a key combo…</div>
        <div class="small">Press Esc to cancel. This will overwrite any existing binding in this context.</div>
      </div>
    </div>
  `;
}

function renderHotkeys(){
  if (!hk) hk = loadHotkeys();
  const body = document.getElementById('hkBody');
  const warns = document.getElementById('hkWarns');
  if (!body || !warns) return;

  document.querySelectorAll('.hkTab').forEach(b=>{
    const c = b.getAttribute('data-ctx');
    b.classList.toggle('on', c === hkTab);
  });

  const ws = findHotkeyWarnings();
  warns.innerHTML = ws.length
    ? `<span class="hkWarn">Warnings:</span> ` + ws.slice(0,6).map(x=>`[${x.ctx}] ${x.chord} → ${x.cmd} (${x.kind})`).join('  ')
    : 'Tip: click a window (Entry/Chart/Tape/L2) to activate that hotkey context.';

  const q = String(hkFilter || '').trim().toLowerCase();
  const allowed = _allCommands().filter(c=> c.contexts.includes(hkTab));
  const ctxMap = hk?.[hkTab] || {};

  const rows = [];
  for (const c of allowed){
    // reverse lookup binding(s)
    const bindings = [];
    for (const [ch, b] of Object.entries(ctxMap)){
      if (b?.cmd === c.id) bindings.push(ch);
    }
    const binding = bindings.length ? bindings.join(', ') : '';
    const text = (c.label + ' ' + c.id + ' ' + binding).toLowerCase();
    if (q && !text.includes(q)) continue;
    rows.push(`
      <tr>
        <td><div style="font-weight:900;">${c.label}</div><div class="entryHint">${c.id}</div></td>
        <td>${binding ? `<span class="hkBadge">${binding}</span>` : `<span class="entryHint">(unbound)</span>`}</td>
        <td>${hkTab.toUpperCase()}</td>
        <td style="display:flex; gap:8px; justify-content:flex-end;">
          <button class="wbtn" data-act="rec" data-cmd="${c.id}">Record</button>
          <button class="wbtn" data-act="clr" data-cmd="${c.id}">Clear</button>
        </td>
      </tr>
    `);
  }
  body.innerHTML = rows.join('');

  body.querySelectorAll('button[data-act="rec"]').forEach(btn=>{
    btn.addEventListener('click', (e)=>{
      e.preventDefault();
      const cmd = btn.getAttribute('data-cmd');
      hkRecording = { context: hkTab, commandId: cmd };
      document.getElementById('hkOverlay').style.display = 'flex';
      document.getElementById('hkRecText').textContent = `Press keys for ${cmd} (${hkTab.toUpperCase()})…`;
    });
  });
  body.querySelectorAll('button[data-act="clr"]').forEach(btn=>{
    btn.addEventListener('click', (e)=>{
      e.preventDefault();
      const cmd = btn.getAttribute('data-cmd');
      for (const [ch, b] of Object.entries(ctxMap)){
        if (b?.cmd === cmd) delete ctxMap[ch];
      }
      hk[hkTab] = ctxMap;
      saveHotkeys();
      renderHotkeys();
    });
  });

}

function initHotkeysWindow(){
  hk = loadHotkeys();
  const f = document.getElementById('hkFilter');
  f?.addEventListener('input', ()=>{
    hkFilter = f.value || '';
    renderHotkeys();
  });
  document.getElementById('hkReset')?.addEventListener('click', (e)=>{
    e.preventDefault();
    resetHotkeysToDefault();
  });
  document.querySelectorAll('.hkTab').forEach(b=>{
    b.addEventListener('click', (e)=>{
      e.preventDefault();
      hkTab = b.getAttribute('data-ctx') || 'global';
      renderHotkeys();
    });
  });
  document.getElementById('hkSaveAs')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    try {
      await saveConfigAs('hotkeys', exportHotkeysState(), 'hotkeys.custom.json');
    } catch (err) {
      setErr(String(err?.message ?? err));
    }
  });
  document.getElementById('hkOverlay')?.addEventListener('click', (e)=>{
    if (e.target?.id === 'hkOverlay') {
      hkRecording = null;
      document.getElementById('hkOverlay').style.display = 'none';
    }
  });
  renderHotkeys();
}

// ---------------- Settings ----------------
let settingsTab = 'hotkeys'; // hotkeys | trading | layout | about

async function saveConfigAs(kind, data, suggestedFilename){
  const def = String(suggestedFilename || `${kind}.json`);
  const filename = prompt(`Save ${kind} as… (will be written under Configs/)`, def);
  if (!filename) return;
  const res = await fetch('/api/config/save_as', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind, filename, data }),
  });
  const j = await res.json().catch(()=>null);
  if (!res.ok) throw new Error(j?.detail || `Save failed (${res.status})`);
  setStatus(`Saved ${kind} as ${j?.filename || filename}`);
}

function makeSettingsHtml(){
  return `
    <div class="setWrap">
      <div class="setNav">
        <div class="setNavTitle">Settings</div>
        <button class="setTab" data-tab="hotkeys">Hotkeys</button>
        <button class="setTab" data-tab="trading">Trading</button>
        <button class="setTab" data-tab="layout">Layout</button>
        <button class="setTab" data-tab="about">About</button>
      </div>
      <div class="setMain">
        <div class="setPane" id="set-pane-hotkeys">
          ${makeHotkeysHtml()}
        </div>
        <div class="setPane" id="set-pane-trading" style="display:none;">
          <div class="setSection">
            <div style="color:var(--muted); font-size:12px; font-weight:900;">Trading</div>
            <div style="height:10px;"></div>
            <label class="hint" style="display:flex; align-items:center; gap:10px;">
              <input type="checkbox" id="set-allow-shorting"/>
              Allow shorting (SELL can open a short position)
            </label>
            <div style="height:8px;"></div>
            <div class="setHint">
              If disabled, any order that would take your position below 0 shares is rejected (including from hotkeys).
            </div>
            <div style="height:14px;"></div>
            <div style="height:1px; background: rgba(34,48,69,0.65);"></div>
            <div style="height:14px;"></div>
            <div style="color:var(--muted); font-size:12px; font-weight:900;">Buying Power</div>
            <div style="height:10px;"></div>
            <label class="hint" style="display:flex; align-items:center; gap:10px;">
              <input type="checkbox" id="set-bp-enabled"/>
              Enforce max long buying power
            </label>
            <div style="height:8px;"></div>
            <label class="hint" style="display:flex; align-items:center; gap:10px;">
              Max long ($)
              <input id="set-bp" class="num" type="number" min="0" step="1" value="1800" style="width:120px;"/>
            </label>
            <div style="height:8px;"></div>
            <div class="setHint">
              When enabled, BUY orders that would increase net long exposure above this limit are rejected (uses best-effort estimates).
            </div>
          </div>
        </div>
        <div class="setPane" id="set-pane-layout" style="display:none;">
          <div class="setSection">
            <div class="setRow">
              <div style="color:var(--muted); font-size:12px; font-weight:900;">Layout</div>
              <div style="flex:1;"></div>
              <button class="wbtn" id="layoutSaveAs" title="Save the current layout to a custom file">Save As…</button>
            </div>
            <div style="height:10px;"></div>
            <div class="setHint">
              Layout auto-saves to <code>Configs/layout.json</code>. “Save As…” writes an additional JSON file under <code>Configs/</code>.
            </div>
          </div>
        </div>
        <div class="setPane" id="set-pane-about" style="display:none;">
          <div class="setSection">
            <div style="color:var(--muted); font-size:12px; font-weight:900;">About</div>
            <div style="height:8px;"></div>
            <div class="setHint">Trading simulator settings. (More tabs can be added here over time.)</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function initSettingsWindow(){
  const setTabTo = (tab)=>{
    settingsTab = tab;
    document.querySelectorAll('.setTab').forEach(b=>{
      const t = b.getAttribute('data-tab');
      b.classList.toggle('on', t === settingsTab);
    });
    const panes = ['hotkeys','trading','layout','about'];
    for (const p of panes){
      const el = document.getElementById(`set-pane-${p}`);
      if (el) el.style.display = (p === settingsTab) ? 'block' : 'none';
    }
  };
  document.querySelectorAll('.setTab').forEach(b=>{
    b.addEventListener('click', (e)=>{
      e.preventDefault();
      setTabTo(b.getAttribute('data-tab') || 'hotkeys');
    });
  });
  setTabTo(settingsTab);
  initHotkeysWindow();
  try {
    if (!simSettings) simSettings = loadSimSettings();
    const cb = document.getElementById('set-allow-shorting');
    if (cb) cb.checked = !!simSettings.allowShorting;
    cb?.addEventListener('change', ()=>{
      if (!simSettings) simSettings = loadSimSettings();
      simSettings.allowShorting = !!cb.checked;
      saveSimSettings();
      if (!simSettings.allowShorting) {
        // If disabling, cancel any currently-open sell orders that would go net short if filled.
        let cancelled = 0;
        for (const o of orders.values()){
          if (!o) continue;
          if (o.status !== 'open' && o.status !== 'partial') continue;
          if (String(o.side).toUpperCase() !== 'SELL') continue;
          const rem = _orderRemaining(o);
          if (_wouldGoShort(o.symbol, 'SELL', rem)) { cancelOrder(o.id); cancelled += 1; }
        }
        if (cancelled) setStatus(`Shorting disabled; cancelled ${cancelled} open SELL orders that could short`);
        else setStatus('Shorting disabled');
      } else {
        setStatus('Shorting enabled');
      }
    });

    // Buying power controls
    const bpOn = document.getElementById('set-bp-enabled');
    const bpEl = document.getElementById('set-bp');
    if (bpOn) bpOn.checked = !!simSettings.buyingPowerEnabled;
    if (bpEl) bpEl.value = String(Number.isFinite(Number(simSettings.buyingPower)) ? Number(simSettings.buyingPower) : 1800);
    const syncBpUi = ()=>{
      const on = !!bpOn?.checked;
      if (bpEl) bpEl.disabled = !on;
      bpEl?.classList?.toggle?.('entryDisabled', !on);
    };
    syncBpUi();
    bpOn?.addEventListener('change', ()=>{
      if (!simSettings) simSettings = loadSimSettings();
      simSettings.buyingPowerEnabled = !!bpOn.checked;
      saveSimSettings();
      syncBpUi();
      setStatus(simSettings.buyingPowerEnabled ? 'Buying power limit enabled' : 'Buying power limit disabled');
    });
    bpEl?.addEventListener('change', ()=>{
      if (!simSettings) simSettings = loadSimSettings();
      const v = Number(bpEl.value);
      simSettings.buyingPower = (Number.isFinite(v) && v >= 0) ? v : 1800;
      bpEl.value = String(simSettings.buyingPower);
      saveSimSettings();
      setStatus(`Buying power set to $${Number(simSettings.buyingPower).toFixed(0)}`);
    });
  } catch {}
  document.getElementById('layoutSaveAs')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    try {
      await saveConfigAs('layout', captureLayout(), 'layout.custom.json');
    } catch (err) {
      setErr(String(err?.message ?? err));
    }
  });
}

// ---------------- Commands window (DAS-like scripting) ----------------
function makeCommandsHtml(){
  return `
    <div class="cmdWrap">
      <div class="cmdLeft">
        <div class="cmdBtns">
          <button class="wbtn" id="cmdNew">New</button>
          <button class="wbtn" id="cmdCopy">Copy</button>
          <button class="wbtn" id="cmdDel">Delete</button>
          <button class="wbtn" id="cmdSave">Save</button>
        </div>
        <div class="cmdList" id="cmdList"></div>
        <div class="entryHint">
          Bind via Hotkeys using command id: <span class="badge">das:&lt;id&gt;</span>
        </div>
      </div>
      <div class="cmdRight">
        <div class="cmdRow">
          <label class="hint" style="display:flex; gap:6px; align-items:center;">
            Name <input id="cmdName" placeholder="My DAS Command" style="width:260px;"/>
          </label>
          <span class="badge" id="cmdId">(no selection)</span>
        </div>
        <textarea class="cmdTa" id="cmdScript" spellcheck="false" placeholder="lmtprice = ask - .10; shares = 5000 / lmtprice; BUY;"></textarea>
        <div class="cmdBtns">
          <button class="wbtn" id="cmdCheck">Check</button>
          <button class="wbtn primary" id="cmdRun">Run</button>
          <span class="entryHint" id="cmdMsg"></span>
        </div>
        <div class="entryHint cmdFooter">
          Vars: <span class="badge">ask</span> <span class="badge">bid</span> <span class="badge">shares</span> <span class="badge">ticker</span> <span class="badge">BuyPower</span>
          <span class="badge">position</span> <span class="badge">lmtprice</span> <span class="badge">stopprice</span> <span class="badge">costbasis</span>
          &nbsp; Commands: <span class="badge">BUY</span> <span class="badge">SELL</span> <span class="badge">CancelAll</span>
        </div>
      </div>
    </div>
  `;
}

function _cmdById(id){
  return (cmdState?.commands || []).find(x=>String(x?.id||'')===String(id||'')) || null;
}

function renderCommands(){
  const list = document.getElementById('cmdList');
  if (!list) return;
  const arr = cmdState?.commands || [];
  if (!cmdSelectedId && arr.length) cmdSelectedId = String(arr[0].id);
  const rows = [];
  for (const c of arr){
    const id = String(c.id||'');
    const name = String(c.name||id);
    const on = (id === cmdSelectedId);
    rows.push(`<div class="cmdItem ${on?'on':''}" data-id="${id}">
      <div class="cmdName">${name}</div>
      <div class="cmdSub">das:${id}</div>
    </div>`);
  }
  list.innerHTML = rows.join('') || `<div class="cmdItem"><div class="cmdSub">No commands yet. Click “New”.</div></div>`;
  list.querySelectorAll('.cmdItem[data-id]').forEach(el=>{
    el.addEventListener('click', (e)=>{
      e.preventDefault();
      cmdSelectedId = el.getAttribute('data-id');
      _loadCmdToEditor();
      renderCommands();
    });
  });
}

function _loadCmdToEditor(){
  const c = _cmdById(cmdSelectedId);
  const nameEl = document.getElementById('cmdName');
  const idEl = document.getElementById('cmdId');
  const scEl = document.getElementById('cmdScript');
  const msgEl = document.getElementById('cmdMsg');
  if (msgEl) msgEl.textContent = '';
  if (!c){
    if (nameEl) nameEl.value = '';
    if (idEl) idEl.textContent = '(no selection)';
    if (scEl) scEl.value = '';
    return;
  }
  if (nameEl) nameEl.value = String(c.name||'');
  if (idEl) idEl.textContent = `das:${String(c.id||'')}`;
  if (scEl) scEl.value = String(c.script||'');
}

function _saveEditorToCmd(){
  const c = _cmdById(cmdSelectedId);
  if (!c) return;
  const nameEl = document.getElementById('cmdName');
  const scEl = document.getElementById('cmdScript');
  const nm = String(nameEl?.value || '').trim();
  if (nm) c.name = nm;
  c.script = String(scEl?.value || '');
}

function initCommandsWindow(){
  // cmdState is already initialized; just ensure shape.
  cmdState = _normalizeCmdState(cmdState);
  const msgEl = document.getElementById('cmdMsg');
  const nameEl = document.getElementById('cmdName');
  const scEl = document.getElementById('cmdScript');

  const touch = ()=>{
    try { _saveEditorToCmd(); } catch {}
    try { saveCommands(); } catch {}
    try { renderCommands(); } catch {}
  };
  nameEl?.addEventListener('input', touch);
  scEl?.addEventListener('input', touch);

  document.getElementById('cmdNew')?.addEventListener('click', (e)=>{
    e.preventDefault();
    const id = _newCmdId();
    const c = { id, name: `Command ${ (cmdState.commands.length + 1) }`, script: '' };
    cmdState.commands.push(c);
    cmdSelectedId = id;
    saveCommands();
    renderCommands();
    _loadCmdToEditor();
  });

  document.getElementById('cmdCopy')?.addEventListener('click', (e)=>{
    e.preventDefault();
    const src = _cmdById(cmdSelectedId);
    if (!src) return;
    const id = _newCmdId();
    const nm = String(src.name || 'Command').trim() || 'Command';
    const c = { id, name: `Copy of ${nm}`, script: String(src.script || '') };
    // insert next to the source command for convenience
    const arr = cmdState.commands || [];
    const idx = arr.findIndex(x=>String(x?.id||'') === String(src.id||''));
    if (idx >= 0) arr.splice(idx+1, 0, c);
    else arr.push(c);
    cmdState.commands = arr;
    cmdSelectedId = id;
    saveCommands();
    renderCommands();
    _loadCmdToEditor();
    if (msgEl) msgEl.textContent = 'Copied.';
  });

  document.getElementById('cmdDel')?.addEventListener('click', (e)=>{
    e.preventDefault();
    if (!cmdSelectedId) return;
    cmdState.commands = (cmdState.commands || []).filter(x=>String(x.id||'') !== String(cmdSelectedId||''));
    cmdSelectedId = cmdState.commands[0]?.id || null;
    saveCommands();
    renderCommands();
    _loadCmdToEditor();
  });

  document.getElementById('cmdSave')?.addEventListener('click', (e)=>{
    e.preventDefault();
    touch();
    if (msgEl) msgEl.textContent = 'Saved.';
  });

  document.getElementById('cmdCheck')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    const c = _cmdById(cmdSelectedId);
    if (!c) return;
    const res = await validateDasScriptRemote(String(c.script||''));
    if (res?.ok) { if (msgEl) msgEl.textContent = 'OK'; }
    else {
      const er = (res?.errors && res.errors[0]) ? res.errors[0] : {message:'Invalid', line:1, col:1};
      if (msgEl) msgEl.textContent = `Error @ ${er.line}:${er.col} — ${er.message}`;
    }
  });

  document.getElementById('cmdRun')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    const c = _cmdById(cmdSelectedId);
    if (!c) return;
    const res = await validateDasScriptRemote(String(c.script||''));
    if (!res?.ok) {
      const er = (res?.errors && res.errors[0]) ? res.errors[0] : {message:'Invalid', line:1, col:1};
      if (msgEl) msgEl.textContent = `Error @ ${er.line}:${er.col} — ${er.message}`;
      return;
    }
    if (msgEl) msgEl.textContent = '';
    try { runDasScript(String(c.script||''), { name: String(c.name||''), id: String(c.id||'') }); }
    catch (err) { if (msgEl) msgEl.textContent = `Run failed: ${err?.message ?? err}`; }
  });

  renderCommands();
  _loadCmdToEditor();
}

function loadPosCols(){
  try {
    const raw = localStorage.getItem(POS_COLS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    const allowed = new Set(POS_COLS_ALL.map(x=>x.key));
    if (Array.isArray(parsed)) {
      // migration: old key 'pl' -> 'open_pl'
      const mapped = parsed.map(k=> (k === 'pl' ? 'open_pl' : k));
      const clean = mapped.filter(k=>allowed.has(k));
      if (clean.length) return clean;
    }
  } catch {}
  return POS_COLS_ALL.map(x=>x.key);
}
function savePosCols(cols){
  try { localStorage.setItem(POS_COLS_KEY, JSON.stringify(cols)); } catch {}
}

function makePositionsHtml(){
  const checks = POS_COLS_ALL.map(c=>{
    return `<label><input type="checkbox" class="posColCb" data-col="${c.key}"/> ${c.label}</label>`;
  }).join('');
  return `
    <div class="posWrap">
      <div class="posTop">
        <div style="flex:1;"></div>
        <button class="posGear" id="pos-gear" title="Columns">⚙</button>
      </div>
      <div class="posPanel" id="pos-panel">
        <div class="row" style="margin-bottom:8px;">
          <div style="color:var(--muted); font-size:12px; font-weight:800;">Columns</div>
          <div style="flex:1;"></div>
          <button class="wbtn" id="pos-cols-all" style="padding:4px 8px;">All</button>
          <button class="wbtn" id="pos-cols-none" style="padding:4px 8px;">None</button>
          <button class="wbtn" id="pos-cols-reset" style="padding:4px 8px;">Reset</button>
        </div>
        <div class="row" id="pos-cols">
          ${checks}
        </div>
        <div style="height:8px;"></div>
        <div class="entryHint">UI-only for now. Column visibility is saved locally.</div>
      </div>
      <div style="flex:1 1 auto; min-height:0; overflow:auto;">
        <table class="posTable">
          <thead><tr id="pos-head"></tr></thead>
          <tbody id="pos-body"></tbody>
        </table>
        <div class="posEmpty" id="pos-empty" style="display:none;">No positions yet.</div>
      </div>
    </div>
  `;
}

function _posMarkPxForSymbol(symbol){
  // Until we have per-symbol book, just use last trade (if it matches) else current book mid/last.
  const sym = String(symbol || '').toUpperCase();
  const curSym = String($('symbol')?.value?.trim?.() ?? '').toUpperCase();
  if (sym && curSym && sym !== curSym) return null;
  const lastPx = Number(lastTrade?.price);
  if (Number.isFinite(lastPx)) return lastPx;
  const b = Number(currentBook?.bids?.[0]?.[0]);
  const a = Number(currentBook?.asks?.[0]?.[0]);
  if (Number.isFinite(b) && Number.isFinite(a)) return (b+a)/2;
  if (Number.isFinite(b)) return b;
  if (Number.isFinite(a)) return a;
  return null;
}

function renderPositions(){
  const head = $('pos-head');
  const body = $('pos-body');
  const empty = $('pos-empty');
  if (!head || !body || !empty) return;

  // header
  const cols = Array.isArray(posCols) && posCols.length ? posCols : POS_COLS_ALL.map(x=>x.key);
  const colMeta = new Map(POS_COLS_ALL.map(x=>[x.key, x]));
  head.innerHTML = cols.map(k=>`<th>${colMeta.get(k)?.label ?? k}</th>`).join('');

  // rows: show if non-zero shares OR realized P/L exists for the day
  const rows = [];
  for (const [symbol, p] of positions.entries()){
    const sh = Number(p?.shares ?? 0);
    const realized = Number(p?.realized ?? 0);
    const hasReal = Number.isFinite(realized) && Math.abs(realized) > 1e-9;
    if (!Number.isFinite(sh)) continue;
    if (sh === 0 && !hasReal) continue;
    const cost = (p?.avgCost == null) ? null : Number(p.avgCost);
    const lastPx = _posMarkPxForSymbol(symbol);
    const openPl = (Number.isFinite(lastPx) && Number.isFinite(cost) && sh !== 0) ? ((lastPx - cost) * sh) : 0;
    const totalPl = (Number.isFinite(realized) ? realized : 0) + (Number.isFinite(openPl) ? openPl : 0);
    const plps = (Number.isFinite(lastPx) && Number.isFinite(cost) && sh !== 0)
      ? ((lastPx - cost) * (sh >= 0 ? 1 : -1))
      : null;

    const cell = (k)=>{
      if (k === 'ticker') return String(symbol);
      if (k === 'shares') return String(sh);
      if (k === 'cost') return (cost == null || !Number.isFinite(cost)) ? '' : cost.toFixed(4);
      if (k === 'last') return (lastPx == null || !Number.isFinite(lastPx)) ? '' : lastPx.toFixed(4);
      if (k === 'plps') {
        if (plps == null || !Number.isFinite(plps)) return '';
        const cls = plps >= 0 ? 'posUp' : 'posDn';
        return `<span class="${cls}">${plps.toFixed(4)}</span>`;
      }
      if (k === 'open_pl') {
        if (!Number.isFinite(openPl)) return '';
        const cls = openPl >= 0 ? 'posUp' : 'posDn';
        return `<span class="${cls}">${Number(openPl).toFixed(2)}</span>`;
      }
      if (k === 'total_pl') {
        if (!Number.isFinite(totalPl)) return '';
        const cls = totalPl >= 0 ? 'posUp' : 'posDn';
        return `<span class="${cls}">${Number(totalPl).toFixed(2)}</span>`;
      }
      return '';
    };
    rows.push(`<tr>${cols.map(k=>`<td>${cell(k)}</td>`).join('')}</tr>`);
  }
  body.innerHTML = rows.join('');
  empty.style.display = rows.length ? 'none' : 'block';
}

function initPositionsWindow(){
  posCols = loadPosCols();

  const panel = $('pos-panel');
  const gear = $('pos-gear');
  const cbs = Array.from(document.querySelectorAll('.posColCb'));

  const setCbs = (cols)=>{
    const set = new Set(cols);
    for (const cb of cbs){
      const k = cb.getAttribute('data-col');
      cb.checked = set.has(k);
    }
  };

  const applyCols = (cols)=>{
    const allowed = new Set(POS_COLS_ALL.map(x=>x.key));
    const clean = cols.filter(k=>allowed.has(k));
    posCols = clean.length ? clean : [];
    savePosCols(posCols);
    setCbs(posCols);
    renderPositions();
  };

  gear?.addEventListener('click', ()=>{
    const on = panel?.style?.display === 'block';
    if (panel) panel.style.display = on ? 'none' : 'block';
  });

  // initialize checkbox UI to saved cols
  setCbs(posCols);

  cbs.forEach(cb=>{
    cb.addEventListener('change', ()=>{
      const next = cbs.filter(x=>x.checked).map(x=>x.getAttribute('data-col')).filter(Boolean);
      applyCols(next);
    });
  });

  $('pos-cols-all')?.addEventListener('click', (e)=>{ e.preventDefault(); applyCols(POS_COLS_ALL.map(x=>x.key)); });
  $('pos-cols-none')?.addEventListener('click', (e)=>{ e.preventDefault(); applyCols([]); });
  $('pos-cols-reset')?.addEventListener('click', (e)=>{ e.preventDefault(); applyCols(POS_COLS_ALL.map(x=>x.key)); });

  renderPositions();
}

// ---------------- Trading History (fills) ----------------
function makeHistoryHtml(){
  return `
    <div style="display:flex; flex-direction:column; gap:10px; height:100%; min-height:0;">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="color:var(--muted); font-size:12px; font-weight:800;">Trading History</div>
        <button class="wbtn" id="hist-clear" title="Clear (local session)">Clear</button>
      </div>
      <div style="flex:1 1 auto; min-height:0; overflow:auto;">
        <table class="posTable">
          <thead>
            <tr>
              <th>Time (ET)</th>
              <th>Ticker</th>
              <th>Side</th>
              <th>Qty</th>
              <th>Price</th>
              <th>Type</th>
              <th>Order ID</th>
              <th>Realized</th>
            </tr>
          </thead>
          <tbody id="hist-body"></tbody>
        </table>
        <div class="posEmpty" id="hist-empty" style="display:none;">No fills yet.</div>
      </div>
    </div>
  `;
}

function renderHistory(){
  const body = $('hist-body');
  const empty = $('hist-empty');
  if (!body || !empty) return;
  const rows = [];
  for (const f of fills){
    const t = f.ts_ns ? nsToEt(f.ts_ns) : '';
    const cls = (f.realized != null && Number.isFinite(f.realized)) ? (f.realized >= 0 ? 'posUp' : 'posDn') : '';
    const rp = (f.realized != null && Number.isFinite(f.realized)) ? `<span class="${cls}">${Number(f.realized).toFixed(2)}</span>` : '';
    rows.push(`
      <tr>
        <td>${t}</td>
        <td>${String(f.symbol)}</td>
        <td>${String(f.side)}</td>
        <td>${String(f.qty)}</td>
        <td>${Number(f.price).toFixed(4)}</td>
        <td>${String(f.type)}</td>
        <td>${String(f.orderId)}</td>
        <td>${rp}</td>
      </tr>
    `);
  }
  body.innerHTML = rows.join('');
  empty.style.display = rows.length ? 'none' : 'block';
}

// ---------------- Open Orders ----------------
function makeOpenOrdersHtml(){
  return `
    <div style="display:flex; flex-direction:column; gap:10px; height:100%; min-height:0;">
      <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="color:var(--muted); font-size:12px; font-weight:800;">Open Orders</div>
        <button class="wbtn" id="ord-clear" title="Cancel all open orders">Cancel All</button>
      </div>
      <div style="flex:1 1 auto; min-height:0; overflow:auto;">
        <table class="posTable">
          <thead>
            <tr>
              <th>Time (ET)</th>
              <th>Order ID</th>
              <th>Ticker</th>
              <th>Side</th>
              <th>Type</th>
              <th>Qty</th>
              <th>Filled</th>
              <th>Rem</th>
              <th>Stop</th>
              <th>Limit</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="ord-body"></tbody>
        </table>
        <div class="posEmpty" id="ord-empty" style="display:none;">No open orders.</div>
      </div>
    </div>
  `;
}

function _orderRemaining(o){
  const q = Number(o?.qty ?? 0);
  const f = Number(o?.filledQty ?? 0);
  if (!Number.isFinite(q) || !Number.isFinite(f)) return 0;
  return Math.max(0, Math.floor(q - f));
}

function cancelOrder(orderId, ts_ns){
  const o = orders.get(orderId);
  if (!o) return;
  if (o.status !== 'open' && o.status !== 'partial') return;
  o.status = 'cancelled';
  o.cancelledAtNs = (ts_ns ?? playheadNs ?? loadedStartNs ?? null);
  orders.set(o.id, o);
  renderOpenOrders();
}

function renderOpenOrders(){
  const body = $('ord-body');
  const empty = $('ord-empty');
  if (!body || !empty) return;
  const rows = [];
  const list = Array.from(orders.values())
    .filter(o=> o && (o.status === 'open' || o.status === 'partial'))
    .sort((a,b)=> Number(a.ts_intent_ns ?? 0) - Number(b.ts_intent_ns ?? 0));

  for (const o of list){
    const t = o.ts_intent_ns ? nsToEt(o.ts_intent_ns) : '';
    const stop = (o.stopPx != null && Number.isFinite(Number(o.stopPx))) ? Number(o.stopPx).toFixed(4) : '';
    const lim = ((String(o.type).toUpperCase() === 'LMT' || String(o.type).toUpperCase() === 'STOPLMT') && o.limitPx != null && Number.isFinite(Number(o.limitPx))) ? Number(o.limitPx).toFixed(4) : '';
    const rem = _orderRemaining(o);
    rows.push(`
      <tr>
        <td>${t}</td>
        <td>${String(o.id)}</td>
        <td>${String(o.symbol)}</td>
        <td>${String(o.side)}</td>
        <td>${String(o.type)}</td>
        <td>${String(o.qty)}</td>
        <td>${String(o.filledQty ?? 0)}</td>
        <td>${String(rem)}</td>
        <td>${stop}</td>
        <td>${lim}</td>
        <td>${String(o.status)}</td>
        <td><button class="wbtn" data-act="cancel" data-oid="${String(o.id)}" style="padding:4px 8px;">Cancel</button></td>
      </tr>
    `);
  }
  body.innerHTML = rows.join('');
  empty.style.display = rows.length ? 'none' : 'block';

  body.querySelectorAll('button[data-act="cancel"]').forEach(btn=>{
    btn.addEventListener('click', (e)=>{
      e.preventDefault();
      const oid = btn.getAttribute('data-oid');
      if (oid) cancelOrder(oid);
    });
  });
}

function _posGet(symbol){
  const key = String(symbol || '').toUpperCase();
  if (!positions.has(key)) positions.set(key, { shares: 0, avgCost: null, realized: 0 });
  return positions.get(key);
}

function _applyFillToPositions(fill){
  const sym = String(fill.symbol || '').toUpperCase();
  const qty = Math.floor(Number(fill.qty));
  const px = Number(fill.price);
  const side = String(fill.side || 'BUY').toUpperCase();
  if (!sym || !Number.isFinite(qty) || qty <= 0 || !Number.isFinite(px) || px <= 0) return { realized: null };

  const pos = _posGet(sym);
  const prevSh = Number(pos.shares || 0);
  const prevCost = (pos.avgCost == null) ? null : Number(pos.avgCost);
  const delta = (side === 'BUY') ? qty : -qty;
  const nextSh = prevSh + delta;

  let realized = 0;
  if (prevSh === 0 || prevCost == null || !Number.isFinite(prevCost)) {
    // opening new position
    pos.shares = nextSh;
    pos.avgCost = (nextSh === 0) ? null : px;
  } else if ((prevSh > 0 && delta > 0) || (prevSh < 0 && delta < 0)) {
    // increasing same direction: weighted average cost
    const a0 = Math.abs(prevSh);
    const a1 = Math.abs(delta);
    const aN = Math.abs(nextSh);
    pos.avgCost = (aN > 0) ? ((a0 * prevCost + a1 * px) / aN) : null;
    pos.shares = nextSh;
  } else {
    // reducing/closing/flipping
    const closeQty = Math.min(Math.abs(delta), Math.abs(prevSh));
    const signPrev = (prevSh >= 0) ? 1 : -1;
    realized = closeQty * (px - prevCost) * signPrev;
    pos.realized = Number(pos.realized || 0) + realized;
    pos.shares = nextSh;
    if (nextSh === 0) {
      pos.avgCost = null;
    } else if ((prevSh > 0 && nextSh < 0) || (prevSh < 0 && nextSh > 0)) {
      // flipped: leftover opened at fill price
      pos.avgCost = px;
    } else {
      // partially reduced: keep same cost
      pos.avgCost = prevCost;
    }
  }
  return { realized };
}

function _bestPxForMarket(side){
  const s = String(side || 'BUY').toUpperCase();
  const bid = Number(currentBook?.bids?.[0]?.[0]);
  const ask = Number(currentBook?.asks?.[0]?.[0]);
  const lastPx = Number(lastTrade?.price);
  if (s === 'BUY') {
    if (Number.isFinite(ask) && ask > 0) return ask;
    if (Number.isFinite(lastPx) && lastPx > 0) return lastPx;
    if (Number.isFinite(bid) && bid > 0) return bid;
  } else {
    if (Number.isFinite(bid) && bid > 0) return bid;
    if (Number.isFinite(lastPx) && lastPx > 0) return lastPx;
    if (Number.isFinite(ask) && ask > 0) return ask;
  }
  return null;
}

function _canFillLimitNow(order){
  const side = String(order.side).toUpperCase();
  const lim = Number(order.limitPx);
  if (!Number.isFinite(lim)) return null;
  const bid = Number(currentBook?.bids?.[0]?.[0]);
  const ask = Number(currentBook?.asks?.[0]?.[0]);
  if (side === 'BUY') {
    if (Number.isFinite(ask) && ask > 0 && ask <= lim) return { price: ask };
  } else {
    if (Number.isFinite(bid) && bid > 0 && bid >= lim) return { price: bid };
  }
  return null;
}

function _recordFill(order, price, fillQty, ts_ns){
  const qty = Math.floor(Number(fillQty));
  const px = Number(price);
  if (!Number.isFinite(qty) || qty <= 0 || !Number.isFinite(px) || px <= 0) return;
  const fill = {
    id: `F${++_fillSeq}`,
    orderId: order.id,
    ts_ns,
    symbol: order.symbol,
    side: order.side,
    qty,
    price: px,
    type: order.type,
    realized: null,
  };
  const r = _applyFillToPositions(fill);
  if (r && r.realized != null && Number.isFinite(r.realized)) fill.realized = r.realized;
  fills.push(fill);
  order.filledQty = Number(order.filledQty || 0) + qty;
  const rem = _orderRemaining(order);
  order.status = rem === 0 ? 'filled' : 'partial';
  orders.set(order.id, order);
  renderPositions();
  renderHistory();
  renderOpenOrders();
}

function maybeTriggerStopsFromTrade(tr){
  if (!tr) return;
  const ts = Number(tr.ts_event ?? null);
  const px = Number(tr.price ?? null);
  if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0) return;

  const candidates = Array.from(orders.values())
    .filter(o=>{
      if (!o) return false;
      const t = String(o.type || '').toUpperCase();
      if (t !== 'STOP' && t !== 'STOPLMT') return false;
      if (o.status !== 'open' && o.status !== 'partial') return false;
      if (o.cancelledAtNs != null) return false;
      if (!Number.isFinite(Number(o.stopPx))) return false;
      return true;
    })
    .sort((a,b)=> Number(a.ts_intent_ns ?? 0) - Number(b.ts_intent_ns ?? 0));

  for (const o of candidates){
    const side = String(o.side || '').toUpperCase();
    const stopPx = Number(o.stopPx);
    if (!Number.isFinite(stopPx) || stopPx <= 0) continue;
    const trig = (side === 'BUY') ? (px >= stopPx) : (px <= stopPx);
    if (!trig) continue;

    // Buying power check at trigger-time for BUY stops/stop-limits (best-effort)
    if (side === 'BUY') {
      const rem = _orderRemaining(o);
      const chk = _wouldExceedBuyingPowerOnBuy({ symbol: o.symbol, qty: rem, type: o.type, limitPx: o.limitPx, stopPx: o.stopPx });
      if (!chk.ok) {
        o.status = 'rejected';
        orders.set(o.id, o);
        renderOpenOrders();
        setStatus(`Rejected ${o.id}: ${chk.reason || 'buying power exceeded'}`);
        continue;
      }
    }

    // Enforce no-shorting at trigger time too (positions could have changed since placement).
    if (!_allowShorting() && side === 'SELL') {
      const rem = _orderRemaining(o);
      if (_wouldGoShort(o.symbol, 'SELL', rem)) {
        o.status = 'rejected';
        orders.set(o.id, o);
        renderOpenOrders();
        setStatus(`Rejected ${o.id}: shorting disabled`);
        continue;
      }
    }

    // Promote STOP -> MKT, STOPLMT -> LMT
    const t = String(o.type || '').toUpperCase();
    o.triggeredAtNs = ts;
    if (t === 'STOP') o.type = 'MKT';
    if (t === 'STOPLMT') o.type = 'LMT';
    orders.set(o.id, o);

    // Try to fill immediately if it became aggressive/marketable.
    if (String(o.type).toUpperCase() === 'MKT') {
      const filled = _sweepAgainstBook(o, ts);
      if (filled === 0) {
        const mpx = _bestPxForMarket(side);
        if (mpx != null) _recordFill(o, mpx, _orderRemaining(o), ts);
      }
      continue;
    }
    if (String(o.type).toUpperCase() === 'LMT') {
      const ok = _canFillLimitNow(o);
      if (ok) _sweepAgainstBook(o, ts);
      else {
        // If passive, queueAhead estimate at our limit price (rough).
        const lim = Number(o.limitPx);
        const bookSide = (side === 'BUY') ? (currentBook?.bids || []) : (currentBook?.asks || []);
        let qa = 0;
        for (const lv of bookSide){
          const pxx = Number(lv?.[0]);
          const sz = Math.floor(Number(lv?.[1] ?? 0));
          if (Number.isFinite(pxx) && Number.isFinite(sz) && sz > 0 && pxx === lim) { qa = sz; break; }
        }
        o.queueAhead = qa;
        orders.set(o.id, o);
        renderOpenOrders();
      }
    }
  }
}

function _sweepAgainstBook(order, ts_ns){
  if (!order || (order.status !== 'open' && order.status !== 'partial')) return 0;
  if (order.cancelledAtNs != null) return 0;
  let rem = _orderRemaining(order);
  if (rem <= 0) return 0;
  const side = String(order.side).toUpperCase();
  const type = String(order.type).toUpperCase();
  const lim = (type === 'LMT') ? Number(order.limitPx) : null;

  const levels = (side === 'BUY') ? (currentBook?.asks || []) : (currentBook?.bids || []);
  let filled = 0;
  for (const lv of levels){
    if (rem <= 0) break;
    const px = Number(lv?.[0]);
    const sz = Math.floor(Number(lv?.[1] ?? 0));
    if (!Number.isFinite(px) || px <= 0 || !Number.isFinite(sz) || sz <= 0) continue;
    if (type === 'LMT' && Number.isFinite(lim)) {
      if (side === 'BUY' && px > lim) break;
      if (side === 'SELL' && px < lim) break;
    }
    const avail = Math.max(0, Math.floor(sz * TAKE_PARTICIPATION));
    if (avail <= 0) continue;
    const q = Math.min(rem, avail);
    _recordFill(order, px, q, ts_ns);
    rem -= q;
    filled += q;
  }
  // if we couldn't fill anything against book, don't change status
  if (filled === 0) return 0;
  return filled;
}

function maybeFillOrders(){
  const nowNs = (playheadNs ?? loadedStartNs ?? null);
  // aggressive fills (MKT + marketable LMT) vs book liquidity, partial fill supported
  for (const o of orders.values()){
    if (!o) continue;
    if (o.status !== 'open' && o.status !== 'partial') continue;
    if (o.cancelledAtNs != null) continue;
    if (String(o.type).toUpperCase() === 'MKT') {
      _sweepAgainstBook(o, nowNs);
      continue;
    }
    if (String(o.type).toUpperCase() === 'LMT') {
      const ok = _canFillLimitNow(o);
      if (ok) _sweepAgainstBook(o, nowNs);
    }
  }
}

function maybeFillFromTrade(tr){
  if (!tr) return;
  const ts = Number(tr.ts_event ?? null);
  const px = Number(tr.price ?? null);
  const sz = Math.floor(Number(tr.size ?? 0));
  if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0 || !Number.isFinite(sz) || sz <= 0) return;

  const cls = classifyTrade(px); // 'bid' (sell hitting bid) or 'ask' (buy lifting ask) or 'mid'
  if (cls !== 'bid' && cls !== 'ask') return;

  const wantSide = (cls === 'bid') ? 'BUY' : 'SELL';
  const candidates = Array.from(orders.values())
    .filter(o=>{
      if (!o) return false;
      if (o.status !== 'open' && o.status !== 'partial') return false;
      if (o.cancelledAtNs != null) return false;
      if (String(o.type).toUpperCase() !== 'LMT') return false;
      if (String(o.side).toUpperCase() !== wantSide) return false;
      if (!Number.isFinite(Number(o.limitPx))) return false;
      // passive fills only at the order's limit price
      if (Number(o.limitPx) !== px) return false;
      // do not double-fill marketable LMTs here; those are handled against book
      return _canFillLimitNow(o) == null;
    })
    .sort((a,b)=> Number(a.ts_intent_ns ?? 0) - Number(b.ts_intent_ns ?? 0));

  if (!candidates.length) return;

  let vol = sz;
  for (const o of candidates){
    if (vol <= 0) break;
    let rem = _orderRemaining(o);
    if (rem <= 0) continue;
    // queueAhead: volume in front of us at that price (very rough FIFO approximation)
    if (o.queueAhead == null) o.queueAhead = 0;
    const qAhead = Math.max(0, Math.floor(Number(o.queueAhead || 0)));
    const consume = Math.min(qAhead, vol);
    o.queueAhead = qAhead - consume;
    vol -= consume;
    if (vol <= 0) { orders.set(o.id, o); continue; }
    const fillable = Math.max(0, Math.floor(vol * PASSIVE_PARTICIPATION));
    if (fillable <= 0) { orders.set(o.id, o); continue; }
    const q = Math.min(rem, fillable);
    _recordFill(o, px, q, ts);
    vol -= q;
  }
}

function placeOrder({symbol, side, type, qty, limitPx, stopPx, route, tif, display}){
  const sym = String(symbol || '').trim().toUpperCase();
  const s = String(side || 'BUY').toUpperCase();
  const t = String(type || 'MKT').toUpperCase();
  const q = Math.floor(Number(qty));
  const lim = (limitPx == null) ? null : Number(limitPx);
  const stp = (stopPx == null) ? null : Number(stopPx);
  const ts_ns = (playheadNs ?? loadedStartNs ?? null);
  if (!sym) { setErr('Order: missing symbol'); return null; }
  if (s !== 'BUY' && s !== 'SELL') { setErr('Order: invalid side'); return null; }
  if (t !== 'MKT' && t !== 'LMT' && t !== 'STOP' && t !== 'STOPLMT') { setErr('Order: invalid type'); return null; }
  if (!Number.isFinite(q) || q <= 0) { setErr('Order: invalid qty'); return null; }
  if ((t === 'LMT' || t === 'STOPLMT') && (!Number.isFinite(lim) || lim == null || lim <= 0)) { setErr('Order: invalid limit price'); return null; }
  if ((t === 'STOP' || t === 'STOPLMT') && (!Number.isFinite(stp) || stp == null || stp <= 0)) { setErr('Order: invalid stop price'); return null; }

  if (s === 'BUY') {
    const chk = _wouldExceedBuyingPowerOnBuy({ symbol: sym, qty: q, type: t, limitPx: lim, stopPx: stp });
    if (!chk.ok) { setErr(`Order: ${chk.reason || 'buying power exceeded'}`); return null; }
  }

  if (!_allowShorting() && s === 'SELL') {
    if (_wouldGoShort(sym, 'SELL', q)) { setErr('Order: shorting disabled'); return null; }
  }

  const id = `O${++_orderSeq}`;
  const order = {
    id,
    ts_intent_ns: ts_ns,
    symbol: sym,
    side: s,
    type: t,
    qty: q,
    stopPx: (t === 'STOP' || t === 'STOPLMT') ? stp : null,
    triggeredAtNs: null,
    limitPx: (t === 'LMT' || t === 'STOPLMT') ? lim : null,
    route,
    tif,
    display,
    status: 'open',
    filledQty: 0,
    cancelledAtNs: null,
    queueAhead: 0,
  };
  orders.set(id, order);
  renderOpenOrders();

  if (t === 'STOP' || t === 'STOPLMT') {
    setStatus(`Accepted ${s} ${q} ${sym} ${t} (stop ${Number(stp).toFixed(4)}${t==='STOPLMT' ? `, limit ${Number(lim).toFixed(4)}` : ''})`);
    return id;
  }

  if (t === 'MKT') {
    // fill as much as available from book now; remainder stays open and will continue to fill on subsequent book updates
    const filled = _sweepAgainstBook(order, ts_ns);
    if (filled === 0) {
      const px = _bestPxForMarket(s);
      if (px == null) { setErr('Order: no market price available (need book or last trade)'); order.status='rejected'; orders.set(id, order); renderOpenOrders(); return null; }
      // if we only have last trade, fill entire order at that print price (best-effort)
      _recordFill(order, px, _orderRemaining(order), ts_ns);
    }
    const rem = _orderRemaining(order);
    setStatus(rem === 0 ? `Filled ${s} ${q} ${sym} MKT` : `Partially filled ${s} ${sym} MKT (rem ${rem})`);
    return id;
  }

  // LMT: if marketable now, fill against book up to liquidity; remainder rests.
  // If passive (not marketable), estimate queueAhead using current L2 size at that price level (rough).
  const ok = _canFillLimitNow(order);
  if (ok) {
    _sweepAgainstBook(order, ts_ns);
    const rem = _orderRemaining(order);
    setStatus(rem === 0
      ? `Filled ${s} ${q} ${sym} LMT (limit ${Number(lim).toFixed(4)})`
      : `Partially filled ${s} ${sym} LMT (rem ${rem}, limit ${Number(lim).toFixed(4)})`);
  } else {
    // queueAhead estimation: existing size on our side at the same price
    const bookSide = (s === 'BUY') ? (currentBook?.bids || []) : (currentBook?.asks || []);
    let qa = 0;
    for (const lv of bookSide){
      const px = Number(lv?.[0]);
      const sz = Math.floor(Number(lv?.[1] ?? 0));
      if (Number.isFinite(px) && Number.isFinite(sz) && sz > 0 && px === Number(lim)) { qa = sz; break; }
    }
    order.queueAhead = qa;
    orders.set(id, order);
    renderOpenOrders();
    setStatus(`Accepted ${s} ${q} ${sym} LMT @ ${Number(lim).toFixed(4)} (resting; queue≈${qa})`);
  }
  return id;
}

function resetTradingToTime(ts_ns){
  const t = (ts_ns == null) ? null : Number(ts_ns);
  if (t == null || !Number.isFinite(t)) return;

  // prune fills after time
  fills = fills.filter(f=> Number(f.ts_ns) <= t);

  // reset positions
  positions.clear();

  // reset orders to open/filled based on remaining fills
  const fillsByOrder = new Map();
  for (const f of fills){
    if (!fillsByOrder.has(f.orderId)) fillsByOrder.set(f.orderId, []);
    fillsByOrder.get(f.orderId).push(f);
  }
  for (const o of orders.values()){
    const oi = Number(o.ts_intent_ns);
    if (Number.isFinite(oi) && oi > t) {
      orders.delete(o.id);
      continue;
    }
    const fs = fillsByOrder.get(o.id) || [];
    const filledQty = fs.reduce((a,x)=>a + Number(x.qty || 0), 0);
    o.filledQty = filledQty;
    // respect cancels that occurred before/at t
    if (o.cancelledAtNs != null && Number(o.cancelledAtNs) <= t) {
      o.status = 'cancelled';
    } else {
      o.cancelledAtNs = null;
      o.status = (filledQty >= Number(o.qty)) ? 'filled' : (filledQty > 0 ? 'partial' : 'open');
    }
    orders.set(o.id, o);
  }

  // rebuild positions by reapplying fills in chronological order
  const sorted = fills.slice().sort((a,b)=>Number(a.ts_ns)-Number(b.ts_ns));
  for (const f of sorted){
    const r = _applyFillToPositions(f);
    if (r && r.realized != null && Number.isFinite(r.realized)) f.realized = r.realized;
  }
  // keep fills in original order (chronological display is nicer)
  fills = sorted;
  renderPositions();
  renderHistory();
  renderOpenOrders();
}

function makeChartHtml(chartId){
  return `
    <div class="chartCol" style="display:flex; flex-direction:column; gap:8px; height:100%; min-height:0;">
    <div class="controlsRow" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
    </div>
    <div class="ohlcHud" style="margin-top:6px; color: var(--muted); font-size: 12px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace; min-height: 16px;"></div>
    <div class="chartWrap" style="flex: 1 1 auto; min-height: 220px;">
      <div class="tfOverlay" style="position:absolute; left:10px; top:10px; z-index:5; display:flex; gap:8px; align-items:center; padding:6px; border:1px solid var(--grid); border-radius:10px; background: rgba(15,23,35,0.85);">
        <button class="wbtn tfBtn" data-tf="1s" style="padding:4px 8px;">1s</button>
        <button class="wbtn tfBtn" data-tf="10s" style="padding:4px 8px;">10s</button>
        <button class="wbtn tfBtn" data-tf="1m" style="padding:4px 8px;">1m</button>
        <button class="wbtn tfBtn" data-tf="5m" style="padding:4px 8px;">5m</button>
        <button class="wbtn indGear" title="Indicator settings" style="padding:4px 8px;">⚙</button>
      </div>
      <div class="indPanel" style="display:none; position:absolute; left:10px; top:52px; z-index:6; width:260px; padding:10px; border:1px solid var(--grid); border-radius:10px; background: rgba(15,23,35,0.95);">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
          <div style="color:var(--muted); font-weight:700; font-size:12px;">Indicator settings</div>
          <button class="wbtn indClose" style="padding:4px 8px;">Close</button>
        </div>
        <div style="height:10px;"></div>
        <div style="display:flex; flex-direction:column; gap:8px;">
          <label class="hint" style="display:flex; align-items:center; gap:8px;"><input type="checkbox" class="indEma" checked/> EMA</label>
          <label class="hint" style="display:flex; align-items:center; gap:8px;"><input type="checkbox" class="indSma"/> SMA</label>
          <label class="hint" style="display:flex; align-items:center; gap:8px;"><input type="checkbox" class="indVwap"/> VWAP</label>
          <label class="hint" style="display:flex; align-items:center; gap:8px;"><input type="checkbox" class="indMacd"/> MACD</label>
        </div>
        <div style="height:14px;"></div>
        <div style="color:var(--muted); font-size:12px; font-weight:700;">EMA</div>
        <div style="height:6px;"></div>
        <label class="hint">Length <input class="emaPeriod" type="number" min="2" step="1" value="9" style="width:90px;"/></label>
        <div style="height:12px;"></div>
        <div style="color:var(--muted); font-size:12px; font-weight:700;">SMA</div>
        <div style="height:6px;"></div>
        <label class="hint">Period <input class="smaPeriod" type="number" min="2" step="1" value="20" style="width:90px;"/></label>
        <div style="height:12px;"></div>
        <div style="color:var(--muted); font-size:12px; font-weight:700;">VWAP</div>
        <div style="height:6px;"></div>
        <label class="hint">Rolling bars (0=session) <input class="vwapPeriod" type="number" min="0" step="1" value="0" style="width:90px;"/></label>
        <div style="height:12px;"></div>
        <div style="color:var(--muted); font-size:12px; font-weight:700;">MACD</div>
        <div style="height:6px;"></div>
        <label class="hint">Fast <input class="macdFast" type="number" min="2" step="1" value="12" style="width:70px;"/></label>
        <label class="hint" style="margin-left:8px;">Slow <input class="macdSlow" type="number" min="2" step="1" value="26" style="width:70px;"/></label>
        <label class="hint" style="margin-left:8px;">Signal <input class="macdSignal" type="number" min="1" step="1" value="9" style="width:70px;"/></label>
        <div style="height:12px;"></div>
        <button class="wbtn indApply" style="padding:6px 10px;">Apply</button>
      </div>
      <div class="tvStack" style="position:absolute; inset:0; display:flex; flex-direction:column;">
        <div class="tvMain" style="flex:1 1 auto; min-height:120px;"></div>
        <div class="tvMacdWrap" style="height:140px; display:none; border-top:1px solid rgba(34,48,69,0.55);">
          <div class="tvMacd" style="width:100%; height:100%;"></div>
        </div>
      </div>
    </div>
    <div class="chartHud">
      <span class="pill">Scroll: pan</span>
      <span class="pill">Wheel: zoom</span>
      <span class="pill hud"></span>
    </div>
    </div>
  `;
}

function createChartWindow(tf){
  const id = `chart-${++chartSeq}`;
  const win = makeWindow({
    id,
    title: `Chart (${tf})`,
    x: 460 + (chartSeq-1)*30,
    y: 12 + (chartSeq-1)*30,
    w: 820,
    h: 680,
    bodyHtml: makeChartHtml(id)
  });
  // Chart-specific window behavior (edge-drag handles, etc.)
  try { win.classList.add('chartWin'); } catch {}
  const wrap = win.querySelector('.chartWrap');
  const container = win.querySelector('.tvMain');
  const macdWrap = win.querySelector('.tvMacdWrap');
  const macdContainer = win.querySelector('.tvMacd');
  const hudEl = win.querySelector('.hud');
  const ohlcEl = win.querySelector('.ohlcHud');

  const chart = {
    id,
    win,
    wrap,
    container,
    macdWrap,
    macdContainer,
    tv: null,
    candleSeries: null,
    volSeries: null,
    // indicator UI + settings
    indEmaEl: win.querySelector('.indEma'),
    indSmaEl: win.querySelector('.indSma'),
    indVwapEl: win.querySelector('.indVwap'),
    indMacdEl: win.querySelector('.indMacd'),
    indGearEl: win.querySelector('.indGear'),
    indPanelEl: win.querySelector('.indPanel'),
    emaPeriodEl: win.querySelector('.emaPeriod'),
    smaPeriodEl: win.querySelector('.smaPeriod'),
    vwapPeriodEl: win.querySelector('.vwapPeriod'),
    macdFastEl: win.querySelector('.macdFast'),
    macdSlowEl: win.querySelector('.macdSlow'),
    macdSignalEl: win.querySelector('.macdSignal'),
    indCloseEl: win.querySelector('.indClose'),
    indApplyEl: win.querySelector('.indApply'),
    emaPeriod: 9,
    smaPeriod: 20,
    vwapPeriod: 0,
    macdFast: 12,
    macdSlow: 26,
    macdSignal: 9,
    emaSeries: null,
    smaSeries: null,
    vwapSeries: null,
    macdTv: null,
    macdLineSeries: null,
    macdSignalSeries: null,
    macdHistSeries: null,
    _indDirty: true,
    hudEl,
    ohlcEl,
    tf: tf,
    candles: [],
    sse: null,
    // lightweight redraw throttle
    _raf: 0,
    liveBucket: null, // current in-progress TF bucket (higher TF only)
    pendingCandles: new Map(), // t -> candle (buffered until complete so we don't leak future OHLC)
    loadingMore: false,
    canLoadMore: true,
    _tvNeedsSetData: true,
    // Zoom/scroll behavior:
    // - We only auto-fit on initial snapshot load (otherwise setData causes periodic zoom-outs).
    _fitNext: true,
    // Used to preserve the user's view when we prepend history (loadMoreHistory).
    _lastSetDataLen: null,
  };
  charts.set(id, chart);

  // init chart + resize observer
  tvInit(chart);
  const ro = new ResizeObserver(()=> tvResize(chart));
  ro.observe(wrap);
  chart._ro = ro;
  function setTf(tfNext){
    chart.tf = tfNext;
    win.querySelector('.wlabel').textContent = `Chart (${chart.tf})`;
    // highlight selected
    win.querySelectorAll('.tfBtn').forEach(b=>{
      const on = (b.getAttribute('data-tf') === tfNext);
      b.style.background = on ? '#14304f' : '#0f1723';
      b.style.borderColor = on ? '#2f5a8a' : 'var(--grid)';
    });
    // Update seconds visibility for axis labels (1s/10s only)
    try { chart.tv?.applyOptions?.({ timeScale: { secondsVisible: (tfNext === '1s' || tfNext === '10s') } }); } catch {}
    try { chart.macdTv?.applyOptions?.({ timeScale: { secondsVisible: (tfNext === '1s' || tfNext === '10s') } }); } catch {}
  }
  setTf(tf);

  win.querySelectorAll('.tfBtn').forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const next = btn.getAttribute('data-tf') || '1s';
      // Stop old TF candle stream immediately to avoid mixing old/new TF updates while the snapshot loads.
      try { if (chart.sse) { chart.sse.close(); chart.sse = null; } } catch {}
      setTf(next);
      await loadChartSnapshot(chart);
      if (!isPaused && playheadNs != null) startChartStream(chart, playheadNs);
    });
  });

  // indicator panel wiring
  const openPanel = ()=>{ chart.indPanelEl.style.display = 'block'; };
  const closePanel = ()=>{ chart.indPanelEl.style.display = 'none'; };
  chart.indGearEl?.addEventListener('click', ()=>{
    // sync UI to current values
    chart.emaPeriodEl.value = String(chart.emaPeriod);
    chart.smaPeriodEl.value = String(chart.smaPeriod);
    chart.vwapPeriodEl.value = String(chart.vwapPeriod);
    chart.macdFastEl.value = String(chart.macdFast);
    chart.macdSlowEl.value = String(chart.macdSlow);
    chart.macdSignalEl.value = String(chart.macdSignal);
    openPanel();
  });
  chart.indCloseEl?.addEventListener('click', closePanel);
  chart.indApplyEl?.addEventListener('click', ()=>{
    const emaP = parseInt(chart.emaPeriodEl.value || '9', 10);
    const smaP = parseInt(chart.smaPeriodEl.value || '20', 10);
    const vwapP = parseInt(chart.vwapPeriodEl.value || '0', 10);
    const mf = parseInt(chart.macdFastEl.value || '12', 10);
    const ms = parseInt(chart.macdSlowEl.value || '26', 10);
    const msi = parseInt(chart.macdSignalEl.value || '9', 10);
    chart.emaPeriod = Math.max(2, Number.isFinite(emaP) ? emaP : 9);
    chart.smaPeriod = Math.max(2, Number.isFinite(smaP) ? smaP : 20);
    chart.vwapPeriod = Math.max(0, Number.isFinite(vwapP) ? vwapP : 0);
    chart.macdFast = Math.max(2, Number.isFinite(mf) ? mf : 12);
    chart.macdSlow = Math.max(chart.macdFast + 1, Number.isFinite(ms) ? ms : 26);
    chart.macdSignal = Math.max(1, Number.isFinite(msi) ? msi : 9);
    chart._indDirty = true;
    drawChart(chart);
    closePanel();
  });
  const indToggle = ()=>{
    chart._indDirty = true;
    drawChart(chart);
  };
  chart.indEmaEl?.addEventListener('change', indToggle);
  chart.indSmaEl?.addEventListener('change', indToggle);
  chart.indVwapEl?.addEventListener('change', indToggle);
  chart.indMacdEl?.addEventListener('change', indToggle);

  return chart;
}

async function loadMoreHistory(chart, bars=400){
  if (!chart || chart.loadingMore || !chart.canLoadMore) return;
  if (!chart.candles.length) return;
  const symbol = $('symbol').value.trim();
  const day = $('day').value.trim();
  const oldest = Number(chart.candles[0]?.t);
  if (!Number.isFinite(oldest)) return;
  chart.loadingMore = true;
  try {
    const end = oldest - 1; // strictly before current oldest (avoid dup)
    const resp = await fetch(`/api/candles_window?symbol=${encodeURIComponent(symbol)}&day=${encodeURIComponent(day)}&tf=${encodeURIComponent(chart.tf)}&end_ts_ns=${encodeURIComponent(end)}&bars=${encodeURIComponent(bars)}`);
    const data = await resp.json();
    if (!resp.ok) return;
    const newCandles = (data.candles || []).filter(c=>c && Number.isFinite(c.t));
    if (!newCandles.length) { chart.canLoadMore = false; return; }
    // prepend without duplicating timestamps
    const existing = new Set(chart.candles.map(c=>c.t));
    const toAdd = newCandles.filter(c=>!existing.has(c.t));
    if (!toAdd.length) { chart.canLoadMore = false; return; }
    toAdd.sort((a,b)=>a.t-b.t);
    chart.candles = toAdd.concat(chart.candles);
    chart._tvNeedsSetData = true;
    drawChart(chart);
  } finally {
    chart.loadingMore = false;
  }
}

function sma(values, period){
  const out = new Array(values.length).fill(null);
  // Robust SMA: ignore/poison windows containing non-finite values.
  const win = [];
  let sum = 0;
  let bad = 0;
  for (let i=0;i<values.length;i++){
    const v = values[i];
    const ok = Number.isFinite(v);
    win.push(v);
    if (!ok) bad += 1;
    else sum += v;
    if (win.length > period){
      const old = win.shift();
      if (!Number.isFinite(old)) bad -= 1;
      else sum -= old;
    }
    if (win.length === period && bad === 0) out[i] = sum/period;
  }
  return out;
}

function ema(values, period){
  const out = new Array(values.length).fill(null);
  const k = 2/(period+1);
  let prev = null;
  for (let i=0;i<values.length;i++){
    const v = values[i];
    prev = prev==null ? v : (v*k + prev*(1-k));
    out[i] = prev;
  }
  return out;
}

function macd(values, fast=12, slow=26, signal=9){
  const eF = ema(values, fast);
  const eS = ema(values, slow);
  const m = values.map((_,i)=> (eF[i]-eS[i]));
  const sig = ema(m, signal);
  const hist = m.map((v,i)=> v - sig[i]);
  return {m, sig, hist};
}

function quantile(values, q){
  const arr = (values || []).filter(v=>Number.isFinite(v)).slice().sort((a,b)=>a-b);
  if (!arr.length) return 0;
  const idx = Math.min(arr.length-1, Math.max(0, Math.floor(q * (arr.length-1))));
  return arr[idx];
}

function log1p(x){
  const v = Number(x);
  if (!Number.isFinite(v) || v <= -1) return NaN;
  return Math.log(1 + v);
}

function ffillFinite(values){
  // Forward-fill non-finite values; if there is no previous value yet, use the first future finite value, else 0.
  const out = new Array(values.length);
  let first = null;
  for (let i=0;i<values.length;i++){
    const v = values[i];
    if (Number.isFinite(v)) { first = v; break; }
  }
  let last = (first != null) ? first : 0;
  for (let i=0;i<values.length;i++){
    const v = values[i];
    if (Number.isFinite(v)) last = v;
    out[i] = last;
  }
  return out;
}

function xLabelEveryNs(tf){
  // User-requested label cadence by TF
  if (tf === '10s') return 30e9;   // every 30s
  if (tf === '1s') return 10e9;    // every 10s
  if (tf === '1m') return 300e9;   // every 5m
  if (tf === '5m') return 900e9;   // every 15m
  return 60e9;
}

function xLabelText(tf, tNs){
  const et = nsToEt(tNs);
  return (tf === '1m' || tf === '5m') ? et.slice(11,16) : et.slice(11,19);
}

function _chartLayout(chart){
  if (!chart || !chart.canvas || !chart.candles || chart.candles.length === 0) return null;
  const rect = chart.canvas.getBoundingClientRect();
  const w = rect.width;
  const h = rect.height;
  const {i0,i1} = visibleWindow(chart);
  const sliceN = Math.max(0, i1 - i0 + 1);
  if (sliceN <= 0) return null;
  const macdOn = !!chart.indMacd?.checked;
  const macdH = macdOn ? 110 : 0;
  const volH = 110;
  const priceH = h - volH - macdH;
  const candleW = Math.max(3, Math.floor((w - 60) / sliceN));
  const x0 = 50;
  return {w,h,i0,i1,sliceN,macdOn,macdH,volH,priceH,candleW,x0};
}

function _hitTestCandle(chart, clientX, clientY){
  const lay = _chartLayout(chart);
  if (!lay) return null;
  const rect = chart.canvas.getBoundingClientRect();
  const x = clientX - rect.left;
  const y = clientY - rect.top;
  // only consider hits inside the canvas plot area (ignore left price axis)
  if (x < lay.x0 || x > (lay.x0 + lay.sliceN * lay.candleW)) return null;
  if (y < 0 || y > lay.h) return null;
  const i = Math.floor((x - lay.x0) / Math.max(1, lay.candleW));
  const idx = Math.min(lay.i1, Math.max(lay.i0, lay.i0 + i));
  return Number.isFinite(idx) ? idx : null;
}

function _updateOhlcHud(chart){
  if (!chart?.ohlcEl) return;
  const idx = chart.hoverIdx;
  if (idx == null) { chart.ohlcEl.textContent = ''; return; }
  const c = chart.candles[idx];
  if (!c) { chart.ohlcEl.textContent = ''; return; }
  const ts = nsToEt(c.t);
  const o = fmtPx(c.o), h = fmtPx(c.h), l = fmtPx(c.l), cl = fmtPx(c.c);
  const v = Number(c.v ?? 0);
  chart.ohlcEl.textContent = `${ts}  O ${o}  H ${h}  L ${l}  C ${cl}  V ${Number.isFinite(v) ? Math.trunc(v) : ''}`;
}

function vwap(candles, period=0){
  const out = new Array(candles.length).fill(null);
  // cumulative VWAP (period=0) or rolling N bars (period>0)
  let pv=0, vv=0;
  const q = []; // {pv, v}
  for (let i=0;i<candles.length;i++){
    const c = candles[i];
    const v = Number(c.v || 0);
    const tp = (c.h + c.l + c.c)/3;
    const pv_i = tp * v;
    if (period > 0) {
      q.push({pv: pv_i, v});
      pv += pv_i; vv += v;
      while (q.length > period) {
        const x = q.shift();
        pv -= x.pv; vv -= x.v;
      }
      out[i] = vv > 0 ? pv/vv : null;
    } else {
      pv += pv_i; vv += v;
      out[i] = vv > 0 ? pv/vv : null;
    }
  }
  return out;
}

function computeIndicatorSeries(chart){
  // Compute indicator series on the full loaded history (chart.candles),
  // then we render only the visible segment. This makes indicators "look into the past"
  // even when those past candles aren't currently displayed.
  const n = chart?.candles?.length || 0;
  if (n === 0) return null;
  const last = chart.candles[n-1] || {};
  const key = [
    n,
    chart.smaPeriod, chart.vwapPeriod,
    chart.macdFast, chart.macdSlow, chart.macdSignal,
    last.t, last.c, last.v, last.h, last.l,
    // include toggles so turning indicators on recomputes immediately
    !!chart.indSma?.checked, !!chart.indVwap?.checked, !!chart.indMacd?.checked,
  ].join('|');
  if (chart._indKey === key && chart._indData) return chart._indData;

  const closesRaw = chart.candles.map(c=>Number(c.c));
  const closes = ffillFinite(closesRaw);

  const out = { closes, sma: null, vwap: null, macd: null };
  if (chart.indSma?.checked) out.sma = sma(closes, chart.smaPeriod || 20);
  if (chart.indVwap?.checked) out.vwap = vwap(chart.candles, chart.vwapPeriod || 0);
  if (chart.indMacd?.checked) out.macd = macd(closes, chart.macdFast || 12, chart.macdSlow || 26, chart.macdSignal || 9);

  chart._indKey = key;
  chart._indData = out;
  return out;
}

function drawChart(chart){
  // Lightweight Charts renderer: keep the name so the existing replay wiring stays simple.
  if (!chart?.tv || !chart?.candleSeries) return;
  if (chart._tvNeedsSetData) {
    chart._tvNeedsSetData = false;
    tvSetData(chart);
  } else {
    tvUpdateLast(chart);
  }
  // indicators update (only uses past/current candles -> no future leak)
  tvRenderIndicators(chart);
}

function upsertCandle(chart, c){
  if (!chart || !c || !Number.isFinite(c.t)) return;
  // For higher TF charts, DO NOT leak future OHLC:
  // - buffer candles until they are fully complete relative to playheadNs
  // - never overwrite the current live bucket candle (built from tape trades)
  if (chart.tf !== '1s'){
    const tfn = tfNs(chart.tf);
    if (chart.liveBucket != null && c.t === chart.liveBucket) return;
    if (playheadNs != null && (Number(c.t) + tfn) > Number(playheadNs)) {
      chart.pendingCandles?.set?.(Number(c.t), c);
      return;
    }
  }
  const prevLastT = chart.candles.length ? Number(chart.candles[chart.candles.length-1].t) : null;
  _upsertCandleRaw(chart, c);
  const newLastT = chart.candles.length ? Number(chart.candles[chart.candles.length-1].t) : null;
  // If this candle isn't the newest bar, we need a full setData() (Lightweight Charts can't insert in the middle via update()).
  if (newLastT == null || Number(c.t) !== newLastT || (prevLastT != null && newLastT < prevLastT)) {
    chart._tvNeedsSetData = true;
  }
  drawChart(chart);
}

// ---------------- Replay wiring ----------------
async function loadSnapshot(){
  setErr('');
  setStatus('Loading snapshot…');
  const symbol = $('symbol').value.trim();
  const day = $('day').value.trim();
  const ts = $('ts').value.trim();
  // IMPORTANT: reset per-symbol/session state so we don't seed charts with stale data.
  lastTrade = null;
  // Use 1s as the authoritative playhead bucket; each chart has its own TF selector.
  const resp = await fetch(`/api/snapshot?symbol=${encodeURIComponent(symbol)}&day=${encodeURIComponent(day)}&ts=${encodeURIComponent(ts)}&tf=1s`);
  const data = await resp.json();
  if (!resp.ok){
    const detail = data?.detail || 'Failed to load snapshot';
    setErr(detail);
    setStatus('');
    return null;
  }
  currentBook = data.book;
  renderL2(currentBook);
  const tl = $('tapeList');
  if (tl) tl.innerHTML = '';
  // Set lastTrade from the snapshot payload (newest by ts_event) BEFORE we load chart snapshots.
  // (Tape rendering order can be newest-first and must not clobber lastTrade.)
  try {
    const trs = Array.isArray(data.trades) ? data.trades : [];
    let best = null;
    for (const tr of trs){
      if (!tr) continue;
      const t = Number(tr.ts_event);
      if (!Number.isFinite(t)) continue;
      if (!best || t > Number(best.ts_event)) best = tr;
    }
    lastTrade = best;
  } catch {
    lastTrade = null;
  }
  // Snapshot trades are in ascending time; prepend oldest->newest so newest ends up at top.
  try {
    const ts = Array.isArray(data.trades) ? data.trades : [];
    const copy = ts.slice();
    copy.reverse(); // newest-first for appendTapeBatch
    appendTapeBatch(copy);
  } catch {
    for (const tr of (data.trades || [])) appendTape(tr);
  }
  // Seed session-like quote stats from the snapshot candle window (best-effort).
  try {
    const cs = Array.isArray(data.candles) ? data.candles : [];
    if (cs.length) {
      const o = Number(cs[0]?.o);
      const hi = Math.max(...cs.map(c=>Number(c?.h)).filter(Number.isFinite));
      const lo = Math.min(...cs.map(c=>Number(c?.l)).filter(Number.isFinite));
      const pcl = (cs.length >= 2) ? Number(cs[cs.length-2]?.c) : Number(cs[0]?.o);
      sessionStats = {
        open: Number.isFinite(o) ? o : null,
        hi: Number.isFinite(hi) ? hi : null,
        lo: Number.isFinite(lo) ? lo : null,
        pcl: Number.isFinite(pcl) ? pcl : null,
      };
    } else {
      sessionStats = { open: null, hi: null, lo: null, pcl: null };
    }
  } catch {
    sessionStats = { open: null, hi: null, lo: null, pcl: null };
  }
  loadedStartNs = data.ts_effective;
  // Reset playhead + tape monotonic guard at snapshot time (prevents rewinds on resume).
  playheadNs = loadedStartNs;
  setNow(playheadNs ? `Now: ${nsToEt(playheadNs)} ET` : '');
  resetTapeMonotonic(playheadNs);
  // If user "rewound" by loading an earlier snapshot, prune trading state to match.
  try { resetTradingToTime(playheadNs); } catch {}
  // Ensure trading UI reflects the reset state (renderL2 no longer triggers these).
  try { renderPositions(); } catch {}
  try { renderOpenOrders(); } catch {}
  try { renderHistory(); } catch {}
  // update all charts
  for (const ch of charts.values()){
    await loadChartSnapshot(ch);
  }
  // Seed higher-TF live candles using snapshot trades so initial H/L/C reflect "so far" (not future).
  for (const tr of (data.trades || [])) {
    for (const ch of charts.values()) {
      updateChartFromTrade(ch, Number(tr.ts_event), Number(tr.price), Number(tr.size));
    }
  }
  if (data.warning) setErr(String(data.warning));
  setStatus(`Ready (snapshot @ ${nsToEt(data.ts_effective)} ET)`);
  isPaused = true;
  return data;
}

function stopStream(){
  if (sseBookTape){ sseBookTape.close(); sseBookTape = null; }
  for (const ch of charts.values()){
    if (ch.sse){ ch.sse.close(); ch.sse = null; }
  }
}

function startBookTapeStream(tsNs){
  setErr('');
  // Defensive: ensure we never leave a zombie book/tape stream running.
  if (sseBookTape){ try { sseBookTape.close(); } catch {} sseBookTape = null; }
  const symbol = $('symbol').value.trim();
  const day = $('day').value.trim();
  const speed = $('speed').value;
  setStatus(`Playing @ ${speed}x…`);
  sseBookTape = new EventSource(`/api/stream?symbol=${encodeURIComponent(symbol)}&day=${encodeURIComponent(day)}&ts_ns=${encodeURIComponent(tsNs)}&speed=${encodeURIComponent(speed)}&tf=1s&what=booktrades`);
  sseBookTape.onmessage = (ev)=>{
    const msg = JSON.parse(ev.data);
    const handleOne = (m)=>{
      if (!m || !m.type) return;
      if (m.type === 'book'){
        _pendingBook = m;
        _pendingMaxTs = (_pendingMaxTs == null) ? m.ts_event : Math.max(Number(_pendingMaxTs), Number(m.ts_event));
        _scheduleReplayFlush();
      } else if (m.type === 'trade'){
        // Update quote-like stats (best-effort)
        try {
          const px = Number(m.price);
          if (Number.isFinite(px)) {
            if (sessionStats.open == null) sessionStats.open = px;
            sessionStats.hi = (sessionStats.hi == null) ? px : Math.max(sessionStats.hi, px);
            sessionStats.lo = (sessionStats.lo == null) ? px : Math.min(sessionStats.lo, px);
          }
        } catch {}
        _pendingTrades.push(m);
        _pendingMaxTs = (_pendingMaxTs == null) ? m.ts_event : Math.max(Number(_pendingMaxTs), Number(m.ts_event));
        _scheduleReplayFlush();
      } else if (m.type === 'eos'){
        setStatus('Paused (end of data)');
        stopStream();
        isPaused = true;
      }
    };
    if (msg.type === 'batch' && Array.isArray(msg.items)){
      for (const it of msg.items) handleOne(it);
    } else {
      handleOne(msg);
    }
  };
  sseBookTape.onerror = async ()=>{
    // If the stream ends or errors, EventSource fires onerror.
    setStatus('');
    setErr('Stream error (check server logs).');
    stopStream();
    isPaused = true;
  };
}

function startChartStream(chart, tsNs){
  if (chart.sse){ chart.sse.close(); chart.sse = null; }
  const symbol = $('symbol').value.trim();
  const day = $('day').value.trim();
  const speed = $('speed').value;
  chart.sse = new EventSource(`/api/stream?symbol=${encodeURIComponent(symbol)}&day=${encodeURIComponent(day)}&ts_ns=${encodeURIComponent(tsNs)}&speed=${encodeURIComponent(speed)}&tf=${encodeURIComponent(chart.tf)}&what=candles`);
  chart.sse.onmessage = (ev)=>{
    const msg = JSON.parse(ev.data);
    const handleOne = (m)=>{
      if (!m || !m.type) return;
      if (m.type === 'candle') upsertCandle(chart, m);
      if (m.type === 'eos') { chart.sse?.close(); chart.sse=null; }
    };
    if (msg.type === 'batch' && Array.isArray(msg.items)){
      for (const it of msg.items) handleOne(it);
    } else {
      handleOne(msg);
    }
  };
  chart.sse.onerror = ()=>{
    // leave global stream alone; show error once
    setErr('Stream error (check server logs).');
    chart.sse?.close(); chart.sse=null;
  };
}

async function loadChartSnapshot(chart){
  const symbol = $('symbol').value.trim();
  const day = $('day').value.trim();
  const tsNs = (playheadNs != null) ? playheadNs : loadedStartNs;
  if (tsNs == null) return;
  const barsByTf = { '1s': 900, '10s': 1200, '1m': 1200, '5m': 1200 };
  const bars = barsByTf[chart.tf] || 900;
  const resp = await fetch(`/api/candles_window?symbol=${encodeURIComponent(symbol)}&day=${encodeURIComponent(day)}&tf=${encodeURIComponent(chart.tf)}&end_ts_ns=${encodeURIComponent(tsNs)}&bars=${encodeURIComponent(bars)}`);
  const data = await resp.json();
  if (!resp.ok){
    const detail = data?.detail || 'Failed to load chart';
    setErr(detail);
    // Fallback chart TF to 1s if higher-TF candles are unavailable (missing or empty parquet).
    const d = String(detail).toLowerCase();
    if (chart.tf !== '1s' && (
      d.includes('missing parquet') ||
      d.includes('no ohlcv') ||
      d.includes('ohlcv parquet is empty') ||
      d.includes('0 rows matched') ||
      d.includes('selected time does not exist in data range')
    )){
      chart.tf = '1s';
      chart.win.querySelector('.wlabel').textContent = `Chart (1s)`;
      return await loadChartSnapshot(chart);
    }
    return;
  }
  chart.pendingCandles?.clear?.();
  chart.canLoadMore = true;
  chart._fitNext = true;
  chart._lastSetDataLen = null;
  chart.candles = [];
  const candles = (data.candles || []).filter(c=>c && Number.isFinite(c.t));
  candles.sort((a,b)=>a.t-b.t);
  for (const c of candles) _upsertCandleRaw(chart, c);
  console.debug(`[tv] snapshot ${chart.id} tf=${chart.tf} loaded=${chart.candles.length} end=${tsNs}`);

  // For higher TF charts, replace the current in-progress bucket candle (which can contain "future" OHLC)
  // with a live candle that is updated from tape trades in real time.
  if (chart.tf !== '1s' && playheadNs != null) {
    const tfn = tfNs(chart.tf);
    const liveBucket = Math.floor(Number(playheadNs) / tfn) * tfn;
    chart.liveBucket = liveBucket;
    // Remove any candle at liveBucket (if present) so we don't leak its precomputed H/L.
    let openPx = null;
    if (chart.candles.length && chart.candles[chart.candles.length-1].t === liveBucket) {
      const popped = chart.candles.pop();
      openPx = popped?.o ?? null;
    } else {
      // if it exists earlier in list
      const idx = chart.candles.findIndex(x=>x.t === liveBucket);
      if (idx >= 0) {
        openPx = chart.candles[idx]?.o ?? null;
        chart.candles.splice(idx, 1);
      }
    }
    const initPx = Number(lastTrade?.price ?? chart.candles[chart.candles.length-1]?.c ?? openPx);
    const o = Number.isFinite(openPx) ? openPx : (Number.isFinite(initPx) ? initPx : 0);
    const p = Number.isFinite(initPx) ? initPx : o;
    chart.candles.push({t: liveBucket, o, h: p, l: p, c: p, v: 0});
    // Seed the live candle using the last snapshot trades (best-effort).
    // This updates close/high/low/vol up to the most recent seen trades.
    // Note: trades list is limited; remaining intra-bar will update during playback.
    // (We only use this on load; during playback updateChartFromTrade is authoritative.)
  }
  chart._tvNeedsSetData = true;
  drawChart(chart);
  // Do not touch playhead here.
}

async function doLoad(shouldBroadcast=true){
  stopStream();
  const snap = await loadSnapshot();
  if (!snap) return;
  if (shouldBroadcast) {
    _broadcast({cmd:'load', symbol:$('symbol').value.trim(), day:$('day').value.trim(), ts:$('ts').value.trim(), speed:$('speed').value, playheadNs});
  }
}

async function doPlay(shouldBroadcast=true){
  // Prevent duplicate/overlapping streams (which breaks pause and causes timestamp flicker).
  // If user hits Play while already playing, we stop the old streams first.
  if (!isPaused) stopStream();
  // If paused and we have a playhead, resume from there without reloading.
  if (isPaused && playheadNs != null) {
    isPaused = false;
    startBookTapeStream(playheadNs);
    for (const ch of charts.values()) startChartStream(ch, playheadNs);
    $('pause').textContent = 'Pause';
    if (shouldBroadcast) {
      _broadcast({cmd:'play', symbol:$('symbol').value.trim(), day:$('day').value.trim(), ts:$('ts').value.trim(), speed:$('speed').value, playheadNs});
    }
    return;
  }
  const snap = await loadSnapshot();
  if (!snap) return;
  isPaused = false;
  const start = playheadNs ?? loadedStartNs;
  startBookTapeStream(start);
  for (const ch of charts.values()) startChartStream(ch, start);
  $('pause').textContent = 'Pause';
  if (shouldBroadcast) {
    _broadcast({cmd:'play', symbol:$('symbol').value.trim(), day:$('day').value.trim(), ts:$('ts').value.trim(), speed:$('speed').value, playheadNs:start});
  }
}

function doPause(shouldBroadcast=true){
  setStatus('Paused');
  stopStream();
  isPaused = true;
  $('pause').textContent = 'Resume';
  if (shouldBroadcast) {
    _broadcast({cmd:'pause', symbol:$('symbol').value.trim(), day:$('day').value.trim(), speed:$('speed').value, playheadNs});
  }
}

$('load').addEventListener('click', ()=>doLoad(true));

// Play always plays (kept for convenience)
$('play').addEventListener('click', ()=>doPlay(true));

// Pause button is a toggle: Pause <-> Resume
$('pause').addEventListener('click', ()=>{
  if (isPaused) doPlay(true);
  else doPause(true);
});

// Build initial windows
const ws = $('workspace');
ws.style.height = `calc(100vh - ${document.querySelector('.topbar').getBoundingClientRect().height}px)`;

// Restore some state from URL (popouts)
if (QS.get('symbol')) $('symbol').value = QS.get('symbol');
if (QS.get('day')) $('day').value = QS.get('day');
if (QS.get('ts')) $('ts').value = QS.get('ts');
if (QS.get('speed')) $('speed').value = QS.get('speed');

// Populate session catalog (non-blocking; user can still type manual symbol/day/time).
initCatalogDropdown();

// Create windows
makeWindow({
  id: 'l2',
  title: 'LVL2 (Top 10)',
  x: 12, y: 12, w: 420, h: 380,
  bodyHtml: `
    <div class="l2Wrap">
      <div class="l2TopRow">
        <button class="l2Gear" id="l2Gear" title="LVL2 color settings">⚙</button>
      </div>
      <div class="l2Panel" id="l2Panel">
        <div class="l2PanelTitle">LVL2 Tier Colors</div>
        <div class="l2ColorsList" id="l2ColorsList"></div>
        <div class="l2PanelBtns">
          <button class="wbtn" id="l2ColorAdd">Add</button>
          <button class="wbtn" id="l2ColorReset">Reset</button>
          <button class="wbtn" id="l2ColorClose">Close</button>
        </div>
        <div class="l2Small">Applied by level (row 1..10), looping through the list.</div>
      </div>

      <table class="l2">
        <thead>
          <tr>
            <th class="px bidx">Bid Px</th>
            <th class="sz bidx">Bid Sz</th>
            <th class="px askx">Ask Px</th>
            <th class="sz askx">Ask Sz</th>
          </tr>
        </thead>
        <tbody id="l2body"></tbody>
      </table>
      <div class="meta" id="l2meta"></div>
    </div>
  `
});
initL2Window();

makeWindow({
  id: 'tape',
  title: 'Time and Sales',
  x: 444, y: 12, w: 420, h: 520,
  bodyHtml: `
    <div id="tapeList"></div>
  `
});

makeWindow({
  id: 'entry',
  title: 'Order Entry',
  x: 876, y: 12, w: 520, h: 420,
  bodyHtml: makeEntryHtml()
});
initEntryWindow();

makeWindow({
  id: 'commands',
  title: 'Commands',
  x: 876, y: 444, w: 520, h: 360,
  bodyHtml: makeCommandsHtml()
});
initCommandsWindow();

makeWindow({
  id: 'positions',
  title: 'Positions',
  x: 12, y: 404, w: 640, h: 220,
  bodyHtml: makePositionsHtml()
});
initPositionsWindow();

makeWindow({
  id: 'history',
  title: 'Trading History',
  x: 660, y: 404, w: 736, h: 220,
  bodyHtml: makeHistoryHtml()
});
// history wiring
document.getElementById('hist-clear')?.addEventListener('click', (e)=>{
  e.preventDefault();
  fills = [];
  // NOTE: leaving orders/positions untouched; user may want to keep state but clear prints.
  renderHistory();
});
renderHistory();

makeWindow({
  id: 'orders',
  title: 'Open Orders',
  x: 12, y: 634, w: 640, h: 240,
  bodyHtml: makeOpenOrdersHtml()
});
document.getElementById('ord-clear')?.addEventListener('click', (e)=>{
  e.preventDefault();
  const nowNs = (playheadNs ?? loadedStartNs ?? null);
  for (const o of orders.values()){
    if (o && (o.status === 'open' || o.status === 'partial')) {
      cancelOrder(o.id, nowNs);
    }
  }
  renderOpenOrders();
});
renderOpenOrders();

makeWindow({
  id: 'settings',
  title: 'Settings',
  x: 660, y: 634, w: 736, h: 420,
  bodyHtml: makeSettingsHtml()
});
initSettingsWindow();

// Primary chart TF selector in topbar now drives "add chart"
const addBtn = document.createElement('button');
addBtn.textContent = 'Add Chart';
addBtn.className = 'primary';
addBtn.style.marginLeft = '6px';
document.querySelector('.topbar').insertBefore(addBtn, document.querySelector('.spacer'));
addBtn.addEventListener('click', ()=>{
  const ch = createChartWindow('1s');
  tvResize(ch);
  if (loadedStartNs != null) loadChartSnapshot(ch);
});

// Window picker (spawn copies via popout; charts can be spawned in-workspace)
function openPopout(id){
  const url = new URL(location.href);
  url.searchParams.set('popout', id);
  url.searchParams.set('symbol', $('symbol')?.value ?? 'MNTS');
  url.searchParams.set('day', $('day')?.value ?? '2026-01-05');
  url.searchParams.set('ts', $('ts')?.value ?? '2026-01-05 09:30:00');
  url.searchParams.set('speed', $('speed')?.value ?? '1');
  window.open(url.toString(), '_blank', 'noopener,noreferrer,width=900,height=700');
}

function spawnWindowInPage(id){
  const w = document.getElementById(id);
  if (!w) return;
  try { if (w.dataset?.closed === '1') delete w.dataset.closed; } catch {}
  w.style.display = 'block';
  // nudge so repeated spawns are visible
  const x = _numPx(w.style.left, 10);
  const y = _numPx(w.style.top, 10);
  w.style.left = `${x + 12}px`;
  w.style.top = `${y + 12}px`;
  try { bringToFront(w); } catch {}
  try { scheduleLayoutSave(); } catch {}
}

const winBtn = document.createElement('button');
winBtn.textContent = 'Windows';
winBtn.className = 'primary';
winBtn.style.marginLeft = '6px';
document.querySelector('.topbar').insertBefore(winBtn, document.querySelector('.spacer'));

// Quick access: Settings button (shows the Settings window)
const settingsBtn = document.createElement('button');
settingsBtn.textContent = 'Settings';
settingsBtn.className = 'primary';
settingsBtn.style.marginLeft = '6px';
document.querySelector('.topbar').insertBefore(settingsBtn, document.querySelector('.spacer'));
settingsBtn.addEventListener('click', (e)=>{
  e.preventDefault();
  try { spawnWindowInPage('settings'); } catch {}
});

const winPanel = document.createElement('div');
winPanel.style.position = 'fixed';
winPanel.style.right = '14px';
winPanel.style.top = '58px';
winPanel.style.zIndex = '9999';
winPanel.style.display = 'none';
winPanel.style.width = '320px';
winPanel.style.padding = '10px';
winPanel.style.border = '1px solid var(--grid)';
winPanel.style.borderRadius = '12px';
winPanel.style.background = 'rgba(15,23,35,0.97)';
winPanel.style.boxShadow = '0 10px 30px rgba(0,0,0,0.45)';
winPanel.innerHTML = `
  <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
    <div style="color:var(--muted); font-weight:800; font-size:12px;">Window Picker</div>
    <button class="wbtn" id="wp-close" style="padding:4px 8px;">Close</button>
  </div>
  <div style="height:10px;"></div>
  <div style="color:var(--muted); font-weight:800; font-size:12px;">Spawn window</div>
  <div style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px;">
    <button class="wbtn" data-win="l2">LVL2</button>
    <button class="wbtn" data-win="tape">Tape</button>
    <button class="wbtn" data-win="entry">Order Entry</button>
    <button class="wbtn" data-win="commands">Commands</button>
    <button class="wbtn" data-win="positions">Positions</button>
    <button class="wbtn" data-win="history">History</button>
    <button class="wbtn" data-win="orders">Open Orders</button>
    <button class="wbtn" data-win="settings">Settings</button>
  </div>
  <div style="height:12px;"></div>
  <div style="color:var(--muted); font-weight:800; font-size:12px;">Spawn chart</div>
  <div style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px;">
    <button class="wbtn" data-chart="1s">Chart 1s</button>
    <button class="wbtn" data-chart="10s">Chart 10s</button>
    <button class="wbtn" data-chart="1m">Chart 1m</button>
    <button class="wbtn" data-chart="5m">Chart 5m</button>
  </div>
  <div style="height:10px;"></div>
  <div class="entryHint">Spawn shows the window in the current page. Charts can be spawned repeatedly.</div>
`;
document.body.appendChild(winPanel);

function _closeWinPanel(){ winPanel.style.display = 'none'; }
winBtn.addEventListener('click', ()=>{
  winPanel.style.display = (winPanel.style.display === 'none') ? 'block' : 'none';
});
winPanel.querySelector('#wp-close')?.addEventListener('click', (e)=>{ e.preventDefault(); _closeWinPanel(); });
winPanel.querySelectorAll('button[data-win]').forEach(b=>{
  b.addEventListener('click', (e)=>{
    e.preventDefault();
    const id = b.getAttribute('data-win');
    if (id) spawnWindowInPage(id);
  });
});
winPanel.querySelectorAll('button[data-chart]').forEach(b=>{
  b.addEventListener('click', async (e)=>{
    e.preventDefault();
    const tf = b.getAttribute('data-chart') || '1s';
    const ch = createChartWindow(tf);
    tvResize(ch);
    if (loadedStartNs != null) await loadChartSnapshot(ch);
    try { scheduleLayoutSave(); } catch {}
  });
});

function _placeWin(win, x, y, w, h){
  win.style.left = `${x}px`;
  win.style.top = `${y}px`;
  win.style.width = `${w}px`;
  win.style.height = `${h}px`;
}

// Apply saved layout (from disk/localStorage)
const _layout = loadLayout();
applyLayoutToExistingWindows(_layout);

// Charts: restore from layout if present, otherwise create defaults
const _chartLayouts = Array.isArray(_layout?.charts) ? _layout.charts : [];
if (_chartLayouts.length) {
  // remove any existing charts created earlier (none by default currently)
  for (const c of _chartLayouts){
    const tf = c?.tf || '1s';
    const ch = createChartWindow(tf);
    _placeWin(ch.win, Number(c.x ?? 12), Number(c.y ?? 548), Number(c.w ?? 460), Number(c.h ?? 520));
    tvResize(ch);
  }
} else {
  // Default charts: 5m, 1m, 10s (arranged left-to-right beneath L2 + Tape)
  const ch5m = createChartWindow('5m');
  _placeWin(ch5m.win, 12, 548, 460, 520);
  tvResize(ch5m);

  const ch1m = createChartWindow('1m');
  _placeWin(ch1m.win, 484, 548, 460, 520);
  tvResize(ch1m);

  const ch10s = createChartWindow('10s');
  _placeWin(ch10s.win, 956, 548, 460, 520);
  tvResize(ch10s);
}

// Save layout shortly after initial render (creates/updates Configs/layout.json)
setTimeout(()=>{ try { scheduleLayoutSave(); } catch {} }, 800);

// Popout mode: show only the requested window and maximize it
if (POPOUT){
  document.querySelectorAll('.win').forEach(w=>{
    if (w.id !== POPOUT) w.style.display = 'none';
  });
  const w = document.getElementById(POPOUT);
  if (w){
    w.style.display = 'block';
    w.style.left = '10px';
    w.style.top = '10px';
    w.style.width = 'calc(100% - 20px)';
    w.style.height = 'calc(100% - 20px)';
  }
}

// init snapshot
loadSnapshot();
</script>
</body>
</html>
"""


def _sanitize_cfg_filename(filename: str) -> str:
    """
    Allow user-provided filenames for Configs/, but prevent path traversal and weird device names.
    Returns a safe filename ending in .json.
    """
    name = str(filename or "").strip()
    if not name:
        raise ValueError("filename is required")
    # prevent traversal / separators
    if Path(name).name != name:
        raise ValueError("invalid filename")
    # normalize extension
    if not name.lower().endswith(".json"):
        name = f"{name}.json"
    # keep it simple and cross-platform safe
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _.\-]{0,120}\.json", name):
        raise ValueError("filename must be simple (letters/numbers/space/_/./-) and <= 125 chars, ending with .json")
    return name


def _price_to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        # Databento can emit float prices (double) or legacy fixed-int prices.
        if isinstance(x, int):
            return float(x) / FIXED_PRICE_SCALE
        return float(x)
    except Exception:
        return None


def _ts_to_ns(x: Any) -> int:
    """
    Normalize parquet ts_event values to UTC epoch ns int.
    Handles:
    - int/uint64 ns
    - datetime with tzinfo (pyarrow timestamp[ns, tz=UTC] becomes datetime in to_pydict())
    """
    if x is None:
        return 0
    if isinstance(x, int):
        return int(x)
    if hasattr(x, "timestamp"):
        return int(x.timestamp() * 1e9)
    return int(x)


def _parse_ts_et_to_ns(ts_text: str, tz_name: str) -> int:
    """
    Parse user input datetime (assumed in America/New_York by default) into UTC epoch ns.
    Accepted formats:
      - YYYY-MM-DD HH:MM:SS
      - YYYY-MM-DD HH:MM:SS.sss
      - ISO 8601 variants (space or 'T')
    """
    s = ts_text.strip().replace("T", " ")
    # allow trailing Z
    if s.endswith("Z"):
        # treat as UTC
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return int(dt_utc.timestamp() * 1e9)

    # Python's fromisoformat supports "YYYY-MM-DD HH:MM:SS[.ffffff]" and offset forms
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:
        raise ValueError(f"Invalid datetime format: {ts_text}") from e

    if dt.tzinfo is None:
        if ZoneInfo is None:
            raise ValueError("Timezone support unavailable (zoneinfo missing). Provide an explicit UTC offset, e.g. 2026-01-05T14:30:00+00:00")
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    dt_utc = dt.astimezone(timezone.utc)
    return int(dt_utc.timestamp() * 1e9)


@dataclass(frozen=True)
class LoadedDay:
    symbol: str
    day: str
    data_dir: Path
    tz_name: str
    tf: str  # "1s" | "10s" | "1m" | "5m"

    # MBP10
    mbp_ts: List[int]
    bid_px: List[List[Any]]
    bid_sz: List[List[int]]
    ask_px: List[List[Any]]
    ask_sz: List[List[int]]

    # trades
    trd_ts: List[int]
    trd_px: List[Any]
    trd_sz: List[int]

    # ohlcv (1s or 10s)
    ohl_ts: List[int]
    ohl_o: List[Any]
    ohl_h: List[Any]
    ohl_l: List[Any]
    ohl_c: List[Any]
    ohl_v: List[int]

    def bounds(self) -> Tuple[int, int]:
        lo = min(
            self.mbp_ts[0] if self.mbp_ts else 2**63 - 1,
            self.trd_ts[0] if self.trd_ts else 2**63 - 1,
            self.ohl_ts[0] if self.ohl_ts else 2**63 - 1,
        )
        hi = max(
            self.mbp_ts[-1] if self.mbp_ts else 0,
            self.trd_ts[-1] if self.trd_ts else 0,
            self.ohl_ts[-1] if self.ohl_ts else 0,
        )
        return lo, hi


_CACHE: Dict[Tuple[str, str, str, str], LoadedDay] = {}


def _read_parquet_cols(path: Path, columns: List[str]) -> Dict[str, List[Any]]:
    pf = pq.ParquetFile(path)
    tab = pf.read(columns=columns)
    # Avoid pyarrow converting timestamp[ns] columns into python datetime (microsecond-only)
    # which can throw without pandas installed. We keep timestamps as raw int64 ns.
    out: Dict[str, List[Any]] = {}
    for name in columns:
        col = tab[name]  # ChunkedArray
        try:
            if pa.types.is_timestamp(col.type):
                col = col.cast(pa.int64())
        except Exception:
            # Best-effort: if inspection/cast fails, fall back to raw values.
            pass
        out[name] = col.to_pylist()
    return out


def _load_day(symbol: str, day: str, data_dir: Path, tz_name: str, tf: str) -> LoadedDay:
    tf = tf.strip()
    if tf not in ("1s", "10s", "1m", "5m"):
        raise ValueError("tf must be one of: 1s, 10s, 1m, 5m")
    key = (symbol, day, str(data_dir), tf)
    if key in _CACHE:
        return _CACHE[key]

    # databento_out naming (multiple symbols inside parquet)
    mbp_path = data_dir / f"XNAS.ITCH.{day}.mbp-10.parquet"
    trd_path = data_dir / f"EQUS.MINI.{day}.trades.parquet"
    ohl_path = data_dir / f"EQUS.MINI.{day}.ohlcv-{tf}.parquet"

    if not mbp_path.exists():
        raise FileNotFoundError(f"Missing parquet: {mbp_path}")
    if not trd_path.exists():
        raise FileNotFoundError(f"Missing parquet: {trd_path}")
    if not ohl_path.exists():
        raise FileNotFoundError(f"Missing parquet: {ohl_path} (requested tf={tf})")

    bid_px_cols = [f"bid_px_{i:02d}" for i in range(10)]
    bid_sz_cols = [f"bid_sz_{i:02d}" for i in range(10)]
    ask_px_cols = [f"ask_px_{i:02d}" for i in range(10)]
    ask_sz_cols = [f"ask_sz_{i:02d}" for i in range(10)]

    mbp_cols = ["ts_event", "symbol"] + bid_px_cols + bid_sz_cols + ask_px_cols + ask_sz_cols
    mbp = _read_parquet_cols(mbp_path, mbp_cols)

    # Filter symbol (parquet does include symbol)
    mbp_sym = mbp["symbol"]
    keep = [i for i, s in enumerate(mbp_sym) if s == symbol]
    mbp_ts = [_ts_to_ns(mbp["ts_event"][i]) for i in keep]
    bid_px = [[mbp[c][i] for c in bid_px_cols] for i in keep]
    bid_sz = [[int(mbp[c][i]) for c in bid_sz_cols] for i in keep]
    ask_px = [[mbp[c][i] for c in ask_px_cols] for i in keep]
    ask_sz = [[int(mbp[c][i]) for c in ask_sz_cols] for i in keep]

    trd_cols = ["ts_event", "symbol", "price", "size"]
    trd = _read_parquet_cols(trd_path, trd_cols)
    trd_keep = [i for i, s in enumerate(trd["symbol"]) if s == symbol]
    trd_ts = [_ts_to_ns(trd["ts_event"][i]) for i in trd_keep]
    trd_px = [trd["price"][i] for i in trd_keep]
    trd_sz = [int(trd["size"][i]) for i in trd_keep]

    # OHLCV timestamp column name differs by dataset/timeframe:
    # - Some files use `ts_event` (common in Databento schemas)
    # - Others use `ts` (observed in generated/aggregated OHLCV parquet)
    ohl_pf = pq.ParquetFile(ohl_path)
    ohl_schema_names = ohl_pf.schema.names
    ohl_ts_col = "ts_event" if "ts_event" in ohl_schema_names else "ts"
    ohl_cols = [ohl_ts_col, "symbol", "open", "high", "low", "close", "volume"]
    ohl = _read_parquet_cols(ohl_path, ohl_cols)
    ohl_keep = [i for i, s in enumerate(ohl["symbol"]) if s == symbol]
    if not ohl_keep:
        try:
            nrows = int(getattr(getattr(ohl_pf, "metadata", None), "num_rows", 0) or 0)
        except Exception:
            nrows = 0
        if nrows <= 0:
            raise ValueError(
                f"OHLCV parquet is empty: {ohl_path} (tf={tf}). "
                f"This typically happens when higher-timeframe bars were built from sparse 1s input and all buckets were dropped. "
                f"Try tf=1s/10s or rebuild bars."
            )
        # Parquet has rows, but none matched this symbol.
        sample_syms: List[str] = []
        try:
            # Sample from the first row group to keep this lightweight.
            rg0 = ohl_pf.read_row_group(0, columns=["symbol"])
            sample_syms = sorted(set(rg0["symbol"].to_pylist()))[:20]
        except Exception:
            sample_syms = []
        raise ValueError(
            f"OHLCV parquet has {nrows} rows but 0 rows matched symbol={symbol!r}: {ohl_path} (tf={tf}). "
            + (f"Sample symbols: {sample_syms}" if sample_syms else "Could not sample symbols.")
        )
    ohl_ts = [_ts_to_ns(ohl[ohl_ts_col][i]) for i in ohl_keep]
    ohl_o = [ohl["open"][i] for i in ohl_keep]
    ohl_h = [ohl["high"][i] for i in ohl_keep]
    ohl_l = [ohl["low"][i] for i in ohl_keep]
    ohl_c = [ohl["close"][i] for i in ohl_keep]
    ohl_v = [int(ohl["volume"][i]) for i in ohl_keep]

    loaded = LoadedDay(
        symbol=symbol,
        day=day,
        data_dir=data_dir,
        tz_name=tz_name,
        tf=tf,
        mbp_ts=mbp_ts,
        bid_px=bid_px,
        bid_sz=bid_sz,
        ask_px=ask_px,
        ask_sz=ask_sz,
        trd_ts=trd_ts,
        trd_px=trd_px,
        trd_sz=trd_sz,
        ohl_ts=ohl_ts,
        ohl_o=ohl_o,
        ohl_h=ohl_h,
        ohl_l=ohl_l,
        ohl_c=ohl_c,
        ohl_v=ohl_v,
    )
    _CACHE[key] = loaded
    return loaded


def _bisect_right(a: List[int], x: int) -> int:
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if x < a[mid]:
            hi = mid
        else:
            lo = mid + 1
    return lo


def _bisect_left(a: List[int], x: int) -> int:
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _book_at_or_before(day: LoadedDay, ts_ns: int) -> Tuple[int, Dict[str, Any]]:
    if not day.mbp_ts:
        raise ValueError("No MBP10 data")
    i = _bisect_right(day.mbp_ts, ts_ns) - 1
    if i < 0:
        i = 0
    bids = [[_price_to_float(day.bid_px[i][j]), int(day.bid_sz[i][j])] for j in range(10)]
    asks = [[_price_to_float(day.ask_px[i][j]), int(day.ask_sz[i][j])] for j in range(10)]
    return day.mbp_ts[i], {"type": "book", "ts_event": int(day.mbp_ts[i]), "bids": bids, "asks": asks}


def _trades_before(day: LoadedDay, ts_ns: int, limit: int = 60) -> List[Dict[str, Any]]:
    if not day.trd_ts:
        return []
    j = _bisect_right(day.trd_ts, ts_ns)
    i0 = max(0, j - limit)
    out: List[Dict[str, Any]] = []
    for i in range(i0, j):
        out.append(
            {
                "type": "trade",
                "ts_event": int(day.trd_ts[i]),
                "price": _price_to_float(day.trd_px[i]),
                "size": int(day.trd_sz[i]),
            }
        )
    return out


def _candles_window(day: LoadedDay, start_ns: int, end_ns: int) -> List[Dict[str, Any]]:
    if not day.ohl_ts:
        return []
    i0 = max(0, _bisect_left(day.ohl_ts, start_ns))
    j = _bisect_right(day.ohl_ts, end_ns)
    out: List[Dict[str, Any]] = []
    for i in range(i0, j):
        out.append(
            {
                "type": "candle",
                "t": int(day.ohl_ts[i]),
                "o": _price_to_float(day.ohl_o[i]),
                "h": _price_to_float(day.ohl_h[i]),
                "l": _price_to_float(day.ohl_l[i]),
                "c": _price_to_float(day.ohl_c[i]),
                "v": int(day.ohl_v[i]),
            }
        )
    return out


def _resolve_effective_ts(day: LoadedDay, ts_ns: int) -> Tuple[int, Optional[str]]:
    """
    Resolve a requested timestamp to an effective chart timestamp.

    Databento OHLCV can have gaps. We:
    - Error if the requested time is outside the available OHLCV range.
    - Otherwise snap to the latest OHLCV bucket at or before the requested time.
    - Return a warning string if snapping occurred.
    """
    if not day.ohl_ts:
        raise ValueError("No OHLCV data available")
    if ts_ns < day.ohl_ts[0] or ts_ns > day.ohl_ts[-1]:
        raise ValueError("Selected time does not exist in data range.")
    i = _bisect_right(day.ohl_ts, ts_ns) - 1
    if i < 0:
        raise ValueError("Selected time does not exist in data range.")
    eff = int(day.ohl_ts[i])
    if eff != int(ts_ns):
        return eff, f"Exact chart bucket missing; snapped to {eff}."
    return eff, None


@APP.get("/")
def index():
    # Inject disk-backed config objects into the served HTML (so UI starts in a consistent state).
    def _read_cfg(name: str) -> Optional[Dict[str, Any]]:
        try:
            fname = CONFIGS_ALLOWED.get(name)
            if not fname:
                return None
            CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
            p = CONFIGS_DIR / fname
            if not p.exists():
                return None
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    layout = _read_cfg("layout")
    hotkeys = _read_cfg("hotkeys")
    commands = _read_cfg("commands")
    settings = _read_cfg("settings")
    html = INDEX_HTML
    html = html.replace("__DISK_LAYOUT__", json.dumps(layout) if layout is not None else "null")
    html = html.replace("__DISK_HOTKEYS__", json.dumps(hotkeys) if hotkeys is not None else "null")
    html = html.replace("__DISK_COMMANDS__", json.dumps(commands) if commands is not None else "null")
    html = html.replace("__DISK_SETTINGS__", json.dumps(settings) if settings is not None else "null")
    return HTMLResponse(html)


@APP.get("/api/config/load")
def config_load(name: str = Query(...)):
    fname = CONFIGS_ALLOWED.get(name)
    if not fname:
        raise HTTPException(status_code=400, detail="Unknown config name")
    try:
        CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
        p = CONFIGS_DIR / fname
        if not p.exists():
            return JSONResponse({"name": name, "data": None})
        return JSONResponse({"name": name, "data": json.loads(p.read_text(encoding="utf-8"))})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@APP.post("/api/config/save")
async def config_save(request: Request):
    try:
        payload = await request.json()
        name = str(payload.get("name") or "")
        data = payload.get("data")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}") from e
    fname = CONFIGS_ALLOWED.get(name)
    if not fname:
        raise HTTPException(status_code=400, detail="Unknown config name")
    try:
        CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
        p = CONFIGS_DIR / fname
        p.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        return JSONResponse({"ok": True, "name": name, "path": str(p)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@APP.post("/api/config/save_as")
async def config_save_as(request: Request):
    """
    Save a config JSON payload to a user-chosen filename under Configs/.
    This is used by the Settings UI "Save As…" actions.
    """
    try:
        payload = await request.json()
        kind = str(payload.get("kind") or "").strip().lower()
        filename = payload.get("filename")
        data = payload.get("data")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}") from e

    if kind not in ("layout", "hotkeys", "commands"):
        raise HTTPException(status_code=400, detail="Unknown config kind")
    try:
        safe = _sanitize_cfg_filename(str(filename or ""))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
        p = CONFIGS_DIR / safe
        p.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        return JSONResponse({"ok": True, "kind": kind, "filename": safe, "path": str(p)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@APP.post("/api/commands/validate")
async def commands_validate(request: Request):
    """
    Validate a DAS-like script string for syntax + allowed vars/commands.
    """
    try:
        payload = await request.json()
        script = str(payload.get("script") or "")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}") from e
    try:
        from Commands import validate_script  # Simulator/Commands.py (local module)

        return JSONResponse(validate_script(script))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@APP.get("/api/metadata")
def metadata(
    symbol: str = Query(...),
    day: str = Query(...),
    tf: str = Query("1s"),
    data_dir: str = Query(str(DATA_DIR_DEFAULT)),
    tz_name: str = Query(LOCAL_TZ_NAME_DEFAULT),
):
    try:
        loaded = _load_day(symbol, day, Path(data_dir), tz_name, tf=tf)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    lo, hi = loaded.bounds()
    return JSONResponse(
        {
            "symbol": symbol,
            "day": day,
            "tz_name": tz_name,
            "data_dir": str(Path(data_dir)),
            "tf": loaded.tf,
            "start_ns": lo,
            "end_ns": hi,
            "ohlcv_seconds": loaded.ohl_ts,
        }
    )


@APP.get("/api/catalog")
def catalog(
    data_dir: str = Query(str(DATA_DIR_DEFAULT)),
    tz_name: str = Query(LOCAL_TZ_NAME_DEFAULT),
    limit: int = Query(250, ge=1, le=2000),
):
    """
    Scan databento_out for applicable days/symbols and return a session list for the UI dropdown.
    Each entry includes the earliest bar timestamp (per symbol/day) in ET for quick "practice" loads.
    """
    key = (str(data_dir), str(tz_name), int(limit))
    now = time.time()
    cached = _CATALOG_CACHE.get(key)
    if cached and (now - float(cached[0])) < _CATALOG_CACHE_TTL_S:
        return JSONResponse(cached[1])

    try:
        from DataCatalog import scan_catalog  # Simulator/DataCatalog.py (local module)

        items = scan_catalog(Path(data_dir), tz_name=tz_name)
        if limit:
            items = items[: int(limit)]
        payload: Dict[str, Any] = {
            "data_dir": str(Path(data_dir)),
            "tz_name": tz_name,
            "items": [
                {
                    "symbol": it.symbol,
                    "day": it.day,
                    "start_ts_ns": int(it.start_ts_ns),
                    "start_et": it.start_et,
                    "label": it.label,
                }
                for it in items
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    _CATALOG_CACHE[key] = (now, payload)
    return JSONResponse(payload)


@APP.get("/api/snapshot")
def snapshot(
    symbol: str = Query(...),
    day: str = Query(...),
    ts: Optional[str] = Query(None, description="Datetime string interpreted in tz_name unless explicit offset/Z is provided."),
    ts_ns: Optional[int] = Query(None, description="UTC epoch ns. If provided, overrides ts."),
    tf: str = Query("1s"),
    data_dir: str = Query(str(DATA_DIR_DEFAULT)),
    tz_name: str = Query(LOCAL_TZ_NAME_DEFAULT),
):
    try:
        loaded = _load_day(symbol, day, Path(data_dir), tz_name, tf=tf)
        if ts_ns is None:
            if ts is None:
                raise ValueError("Provide either ts or ts_ns")
            ts_ns = _parse_ts_et_to_ns(ts, tz_name)
        else:
            ts_ns = int(ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        ts_eff, warn = _resolve_effective_ts(loaded, ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    ts_book, book = _book_at_or_before(loaded, ts_eff)
    trades = _trades_before(loaded, ts_eff, limit=60)

    # Chart context: show at least 20 bars prior (when available), regardless of TF.
    tf_ns = {
        "1s": int(1e9),
        "10s": int(10e9),
        "1m": int(60e9),
        "5m": int(300e9),
    }.get(loaded.tf, int(1e9))
    window_start = max(
        loaded.ohl_ts[0] if loaded.ohl_ts else ts_eff,
        ts_eff - int(20 * tf_ns),
    )
    candles = _candles_window(loaded, window_start, ts_eff)

    # effective = requested timestamp; book might be slightly before if no update at exact second
    return JSONResponse(
        {
            "ts_requested": int(ts_ns),
            "ts_effective": int(ts_eff),
            "book": book,
            "trades": trades,
            "candles": candles,
            "book_ts": int(ts_book),
            "tf": loaded.tf,
            "warning": warn,
        }
    )


@APP.get("/api/candles_window")
def candles_window(
    symbol: str = Query(...),
    day: str = Query(...),
    end_ts_ns: int = Query(..., description="UTC epoch ns. Window ends at the latest OHLCV bucket at or before this time."),
    tf: str = Query("1s"),
    bars: int = Query(500, ge=1, le=5000, description="Number of bars to include (lookback)."),
    data_dir: str = Query(str(DATA_DIR_DEFAULT)),
    tz_name: str = Query(LOCAL_TZ_NAME_DEFAULT),
):
    """
    Return a candles-only window ending at end_ts_ns (snapped to the latest OHLCV bucket at or before).
    Useful for zooming/panning back without loading book/trades.
    """
    try:
        loaded = _load_day(symbol, day, Path(data_dir), tz_name, tf=tf)
        end_ts_ns = int(end_ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        end_eff, warn = _resolve_effective_ts(loaded, end_ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    tf_ns = {
        "1s": int(1e9),
        "10s": int(10e9),
        "1m": int(60e9),
        "5m": int(300e9),
    }.get(loaded.tf, int(1e9))

    start_ns = max(loaded.ohl_ts[0] if loaded.ohl_ts else end_eff, int(end_eff) - int(bars) * tf_ns)
    candles = _candles_window(loaded, start_ns, int(end_eff))
    return JSONResponse(
        {
            "tf": loaded.tf,
            "end_effective": int(end_eff),
            "warning": warn,
            "candles": candles,
        }
    )


async def _aiter_stream(
    day: LoadedDay,
    start_ts_ns: int,
    speed: float,
    request: Request,
    what: str,
) -> Iterable[bytes]:
    """
    Server-sent event stream.
    what:
      - "all": book + trades + candles
      - "booktrades": book + trades only
      - "candles": candles only
    """
    what = (what or "all").strip().lower()
    if what not in ("all", "booktrades", "candles"):
        what = "all"
    # Start indices
    i_b = _bisect_left(day.mbp_ts, start_ts_ns) if what in ("all", "booktrades") else len(day.mbp_ts)
    i_t = _bisect_left(day.trd_ts, start_ts_ns) if (day.trd_ts and what in ("all", "booktrades")) else len(day.trd_ts)
    i_c = _bisect_left(day.ohl_ts, start_ts_ns) if (day.ohl_ts and what in ("all", "candles")) else len(day.ohl_ts)

    prev_ts = start_ts_ns

    def emit(obj: Dict[str, Any]) -> bytes:
        return f"data: {json.dumps(obj, separators=(',',':'))}\n\n".encode("utf-8")

    def _book_msg(i: int) -> Dict[str, Any]:
        bids = [[_price_to_float(day.bid_px[i][j]), int(day.bid_sz[i][j])] for j in range(10)]
        asks = [[_price_to_float(day.ask_px[i][j]), int(day.ask_sz[i][j])] for j in range(10)]
        return {"type": "book", "ts_event": int(day.mbp_ts[i]), "bids": bids, "asks": asks}

    def _trade_msg(i: int) -> Dict[str, Any]:
        return {
            "type": "trade",
            "ts_event": int(day.trd_ts[i]),
            "price": _price_to_float(day.trd_px[i]),
            "size": int(day.trd_sz[i]),
        }

    def _candle_msg(i: int) -> Dict[str, Any]:
        return {
            "type": "candle",
            "t": int(day.ohl_ts[i]),
            "o": _price_to_float(day.ohl_o[i]),
            "h": _price_to_float(day.ohl_h[i]),
            "l": _price_to_float(day.ohl_l[i]),
            "c": _price_to_float(day.ohl_c[i]),
            "v": int(day.ohl_v[i]),
        }

    while True:
        if await request.is_disconnected():
            return
        next_ts = None
        src = None
        if i_b < len(day.mbp_ts):
            next_ts = day.mbp_ts[i_b]
            src = "b"
        if i_t < len(day.trd_ts):
            ts = day.trd_ts[i_t]
            if next_ts is None or ts < next_ts:
                next_ts = ts
                src = "t"
        if i_c < len(day.ohl_ts):
            ts = day.ohl_ts[i_c]
            if next_ts is None or ts < next_ts:
                next_ts = ts
                src = "c"

        if next_ts is None or src is None:
            yield emit({"type": "eos"})
            return

        # Sleep scaled by speed, clamped to keep UI responsive
        dt_ns = max(0, int(next_ts) - int(prev_ts))
        prev_ts = int(next_ts)
        sleep_s = (dt_ns / 1e9) / max(0.0001, float(speed))
        # IMPORTANT: keep stream "real-time" relative to speed.
        # We chunk long sleeps to stay responsive, but we must pay back the full sleep.
        remaining = float(sleep_s)
        while remaining > 0:
            if await request.is_disconnected():
                return
            chunk = 0.25 if remaining > 0.25 else remaining
            await asyncio.sleep(chunk)
            remaining -= chunk

        # Performance: batch all events that share the same timestamp into a single SSE message.
        # Bursty moments often have many events with dt=0; emitting them one-by-one overwhelms the browser
        # event loop and causes visible "freezes" followed by catch-up jumps.
        items: List[Dict[str, Any]] = []
        ts0 = int(next_ts)
        # Preserve the same deterministic tie ordering as the single-event stream: book, then trade, then candle.
        if what in ("all", "booktrades"):
            while i_b < len(day.mbp_ts) and int(day.mbp_ts[i_b]) == ts0:
                items.append(_book_msg(i_b))
                i_b += 1
            while i_t < len(day.trd_ts) and int(day.trd_ts[i_t]) == ts0:
                items.append(_trade_msg(i_t))
                i_t += 1
        if what in ("all", "candles"):
            while i_c < len(day.ohl_ts) and int(day.ohl_ts[i_c]) == ts0:
                items.append(_candle_msg(i_c))
                i_c += 1

        if not items:
            # Shouldn't happen, but keep the stream moving.
            continue
        if len(items) == 1:
            yield emit(items[0])
        else:
            yield emit({"type": "batch", "ts_event": ts0, "items": items})


@APP.get("/api/stream")
async def stream(
    request: Request,
    symbol: str = Query(...),
    day: str = Query(...),
    ts: Optional[str] = Query(None, description="Datetime string interpreted in tz_name unless explicit offset/Z is provided."),
    ts_ns: Optional[int] = Query(None, description="UTC epoch ns. If provided, overrides ts."),
    speed: float = Query(1.0),
    tf: str = Query("1s"),
    what: str = Query("all", description="all | booktrades | candles"),
    data_dir: str = Query(str(DATA_DIR_DEFAULT)),
    tz_name: str = Query(LOCAL_TZ_NAME_DEFAULT),
):
    try:
        loaded = _load_day(symbol, day, Path(data_dir), tz_name, tf=tf)
        if ts_ns is None:
            if ts is None:
                raise ValueError("Provide either ts or ts_ns")
            ts_ns = _parse_ts_et_to_ns(ts, tz_name)
        else:
            ts_ns = int(ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        ts_eff, _ = _resolve_effective_ts(loaded, ts_ns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    async def gen() -> Iterable[bytes]:
        # If client disconnects, we stop yielding.
        yield b"retry: 1000\n\n"
        async for chunk in _aiter_stream(loaded, ts_eff, speed, request, what=what):
            yield chunk

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn

    import os

    # Override with env vars if desired:
    #   HOST=127.0.0.1 PORT=8000 python Simulator/Simulator.py
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(APP, host=host, port=port, reload=False)

