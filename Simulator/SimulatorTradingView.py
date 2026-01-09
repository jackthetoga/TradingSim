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
    table.l2 th, table.l2 td { font-size: 12px; padding: 3px 6px; border-bottom: 1px solid rgba(34,48,69,0.55); }
    table.l2 th { color: var(--muted); font-weight: 600; }
    table.l2 td { white-space: nowrap; }
    .l2row { height: 22px; }
    .bidx { text-align: right; }
    .askx { text-align: left; }
    .px { width: 74px; }
    .sz { width: 58px; }
    .lvl { width: 34px; color: var(--muted); }
    .center { text-align: center; }
    .meta { margin-top: 8px; color: var(--muted); font-size: 12px; }

    /* Tape */
    /* NOTE: keep tape window id as `tape` (for popout), but the scroll container is `tapeList`. */
    #tapeList { height: 360px; overflow: auto; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; }
    .tline { padding: 2px 6px; border-bottom: 1px solid rgba(34,48,69,0.35); display: grid; grid-template-columns: 140px 1fr 1fr; gap: 10px; }
    .tline .ts { color: #000; font-weight: 900; }
    .t-ask { color: #000; background: rgba(37,211,102,0.85); font-weight: 800; }
    .t-bid { color: #000; background: rgba(170,40,40,0.75); font-weight: 800; }
    .t-mid { color: #000; background: rgba(160,160,160,0.65); font-weight: 800; }

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
// ---------------- Windowing (drag/resize/popout) ----------------
function ensureWorkspaceStyles(){
  // convert panels into floating windows
}

// URL params
const QS = new URLSearchParams(location.search);
const POPOUT = QS.get('popout'); // l2 | tape | chart-<id>

function $(id){ return document.getElementById(id); }

let zTop = 10;
function bringToFront(win){
  zTop += 1;
  win.style.zIndex = String(zTop);
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
  win.innerHTML = `
    <div class="wtitle">
      <div class="wlabel">${title}</div>
      <div class="wbtns">
        <button class="wbtn" data-act="pop">Pop out</button>
        <button class="wbtn" data-act="close">×</button>
      </div>
    </div>
    <div class="wbody">${bodyHtml}</div>
    <div class="wresizer"></div>
  `;
  ws.appendChild(win);
  // drag
  const bar = win.querySelector('.wtitle');
  let dragging=false, sx=0, sy=0, ox=0, oy=0, dragPid=null;
  bar.addEventListener('pointerdown', (e)=>{
    if (e.target && e.target.closest('.wbtns')) return;
    dragging=true;
    bringToFront(win);
    dragPid = e.pointerId;
    win.setPointerCapture(e.pointerId);
    sx=e.clientX; sy=e.clientY;
    ox=parseInt(win.style.left,10); oy=parseInt(win.style.top,10);
  });
  // IMPORTANT: pointer capture is on `win`, so move/up/cancel handlers must be on `win`
  win.addEventListener('pointermove', (e)=>{
    if (!dragging) return;
    if (dragPid != null && e.pointerId !== dragPid) return;
    win.style.left = `${ox + (e.clientX - sx)}px`;
    win.style.top = `${oy + (e.clientY - sy)}px`;
  });
  const endDrag = (e)=>{
    if (!dragging) return;
    if (dragPid != null && e && e.pointerId !== dragPid) return;
    dragging=false;
    try { if (dragPid != null) win.releasePointerCapture(dragPid); } catch {}
    dragPid=null;
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
  });
  const endResize = (e)=>{
    if (!resizing) return;
    if (rzPid != null && e && e.pointerId !== rzPid) return;
    resizing=false;
    try { if (rzPid != null) win.releasePointerCapture(rzPid); } catch {}
    rzPid=null;
  };
  win.addEventListener('pointerup', endResize);
  win.addEventListener('pointercancel', endResize);
  win.addEventListener('lostpointercapture', ()=>{ resizing=false; rzPid=null; });

  // buttons
  win.querySelectorAll('.wbtn').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const act = btn.getAttribute('data-act');
      if (act === 'close') win.remove();
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

  win.addEventListener('mousedown', ()=>bringToFront(win));
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
  .wresizer { position: absolute; right: 0; bottom: 0; width: 16px; height: 16px; cursor: nwse-resize;
              background: linear-gradient(135deg, transparent 50%, rgba(230,237,243,0.18) 50%); }
  /* Multi-chart canvases must fill their wrapper; otherwise indicators/volume can be drawn "below" the visible area. */
  .chartWrap { position: relative; flex: 1 1 auto; min-height: 220px; }
  .tvChart { position: absolute; inset: 0; }
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
    setNow(`Now: ${nsToEt(playheadNs)} ET`);
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
function nsToEt(ns){
  const ms = Math.floor(ns / 1e6);
  const d = new Date(ms);
  const dtf = new Intl.DateTimeFormat('en-CA', {
    timeZone: ET,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  const parts = dtf.formatToParts(d);
  const get = (t)=>parts.find(p=>p.type===t)?.value ?? '';
  // en-CA gives YYYY-MM-DD ordering
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')}`;
}

function fmtPx(p){
  if (p === null || p === undefined) return '';
  return Number(p).toFixed(4);
}

function renderL2(book){
  const tb = $('l2body');
  tb.innerHTML = '';
  const bids = book.bids || [];
  const asks = book.asks || [];
  // Bright background tier colors: yellow, white, green, red (loops)
  const tierBgs = ['#ffd400', '#ffffff', '#31ff69', '#ff3b3b'];
  for (let i=0; i<10; i++){
    const b = bids[i] || [null, null];
    const a = asks[i] || [null, null];
    const tr = document.createElement('tr');
    tr.className = 'l2row';
    // bright tier coloring per level (loops)
    const tier = tierBgs[i % tierBgs.length];
    const bg = (hex, a)=> {
      const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
      return `rgba(${r},${g},${b},${a})`;
    };
    const bidBg = bg(tier, 0.92);
    const askBg = bg(tier, 0.92);
    tr.innerHTML = `
      <td class="sz bidx" style="background:${bidBg}; color:#000; font-weight:900">${b[1] ?? ''}</td>
      <td class="px bidx" style="background:${bidBg}; color:#000; font-weight:900">${fmtPx(b[0])}</td>
      <td class="center" style="color: var(--muted)">|</td>
      <td class="px askx" style="background:${askBg}; color:#000; font-weight:900">${fmtPx(a[0])}</td>
      <td class="sz askx" style="background:${askBg}; color:#000; font-weight:900">${a[1] ?? ''}</td>
    `;
    tb.appendChild(tr);
  }
  const bb = bids[0]?.[0], aa = asks[0]?.[0];
  const spr = (bb!=null && aa!=null) ? (aa - bb) : null;
  // Use the authoritative playhead time for display to avoid flicker/backwards jumps.
  const shownTs = (playheadNs != null) ? playheadNs : (book.ts_event ?? null);
  $('l2meta').textContent = `ts_event=${shownTs ? nsToEt(shownTs) : ''} ET   bid=${fmtPx(bb)}   ask=${fmtPx(aa)}   spread=${spr!=null ? spr.toFixed(4) : ''}`;
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
  lastTrade = tr;
  const line = document.createElement('div');
  const cls = classifyTrade(tr.price);
  line.className = 'tline ' + (cls === 'ask' ? 't-ask' : cls === 'bid' ? 't-bid' : 't-mid');
  line.innerHTML = `
    <div class="ts">${nsToEt(tr.ts_event).slice(11,19)}</div>
    <div>${fmtPx(tr.price)}</div>
    <div>${tr.size}</div>
  `;
  tape.prepend(line);
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
      rightPriceScale: { borderColor: 'rgba(34,48,69,0.8)' },
      timeScale: { borderColor: 'rgba(34,48,69,0.8)', timeVisible: true, secondsVisible: true },
      crosshair: (_tvCrosshairModeNormal() != null) ? { mode: _tvCrosshairModeNormal() } : undefined,
      localization: { timeFormatter: _tvTimeFormatter },
    });
    // Some versions use applyOptions for localization; do both, safely.
    try { chart.tv.applyOptions({ localization: { timeFormatter: _tvTimeFormatter } }); } catch {}
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
      const sec = Number(param.time);
      const tNs = Number.isFinite(sec) ? (sec * 1e9) : null;
      if (tNs == null) { chart.ohlcEl.textContent = ''; return; }
      chart.ohlcEl.textContent =
        `${nsToEt(tNs)}  O ${fmtPx(data.open)}  H ${fmtPx(data.high)}  L ${fmtPx(data.low)}  C ${fmtPx(data.close)}`;
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
  if (!chart?.tv || !chart?.wrap) return;
  const rect = chart.wrap.getBoundingClientRect();
  if (!rect || rect.width <= 2 || rect.height <= 2) return;
  chart.tv.applyOptions({ width: Math.floor(rect.width), height: Math.floor(rect.height) });
  // If MACD pane is enabled, resize it too.
  try { if (chart.macdTv && chart.macdWrap && chart.macdWrap.style.display !== 'none') {
    const r2 = chart.macdWrap.getBoundingClientRect();
    chart.macdTv.applyOptions({ width: Math.floor(r2.width), height: Math.floor(r2.height) });
  }} catch {}
}

function tvSetData(chart){
  if (!chart?.candleSeries) return;
  const candles = (chart.candles || []).filter(c=>c && Number.isFinite(c.t));
  const data = candles.map(_tvCandleFromInternal).filter(x=>x);
  const vols = candles.map(_tvVolFromInternal).filter(x=>x);
  try {
    console.debug(`[tv] setData ${chart.id} tf=${chart.tf} candles=${data.length} vols=${vols.length}`);
    chart.candleSeries.setData(data);
    if (chart.volSeries) chart.volSeries.setData(vols);
    try { chart.tv?.timeScale?.().fitContent(); } catch {}
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
  const smaOn = !!chart.indSmaEl?.checked;
  const vwapOn = !!chart.indVwapEl?.checked;
  const macdOn = !!chart.indMacdEl?.checked;

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
        chart.macdTv = LightweightCharts.createChart(chart.macdContainer, {
          layout: { background: { color: '#0b1220' }, textColor: 'rgba(230,237,243,0.85)' },
          grid: { vertLines: { color: 'rgba(34,48,69,0.55)' }, horzLines: { color: 'rgba(34,48,69,0.55)' } },
          rightPriceScale: { borderColor: 'rgba(34,48,69,0.8)' },
          timeScale: { borderColor: 'rgba(34,48,69,0.8)', timeVisible: true, secondsVisible: true },
          handleScroll: false,
          handleScale: false,
          localization: { timeFormatter: (t)=> (typeof t === 'number' ? (new Intl.DateTimeFormat('en-US', {timeZone: ET, hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false}).format(new Date(t*1000))) : String(t)) },
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
    try { chart.macdTv.timeScale().fitContent(); } catch {}
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

function makeChartHtml(chartId){
  return `
    <div class="chartCol" style="display:flex; flex-direction:column; gap:8px; height:100%; min-height:0;">
    <div class="controlsRow" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
      <span class="hint">TF (in-chart)</span>
    </div>
    <div class="ohlcHud" style="margin-top:6px; color: var(--muted); font-size: 12px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace; min-height: 16px;"></div>
    <div class="chartWrap" style="flex: 1 1 auto; min-height: 220px;">
      <div class="tfOverlay" style="position:absolute; left:10px; top:10px; z-index:5; display:flex; gap:6px; padding:6px; border:1px solid var(--grid); border-radius:10px; background: rgba(15,23,35,0.85);">
        <button class="wbtn tfBtn" data-tf="1s" style="padding:4px 8px;">1s</button>
        <button class="wbtn tfBtn" data-tf="10s" style="padding:4px 8px;">10s</button>
        <button class="wbtn tfBtn" data-tf="1m" style="padding:4px 8px;">1m</button>
        <button class="wbtn tfBtn" data-tf="5m" style="padding:4px 8px;">5m</button>
      </div>
      <div class="indOverlay" style="position:absolute; right:10px; top:10px; z-index:5; display:flex; gap:8px; align-items:center; padding:6px; border:1px solid var(--grid); border-radius:10px; background: rgba(15,23,35,0.85);">
        <label class="hint" style="display:flex; align-items:center; gap:6px;"><input type="checkbox" class="indSma" checked/> SMA</label>
        <label class="hint" style="display:flex; align-items:center; gap:6px;"><input type="checkbox" class="indVwap"/> VWAP</label>
        <label class="hint" style="display:flex; align-items:center; gap:6px;"><input type="checkbox" class="indMacd"/> MACD</label>
        <button class="wbtn indGear" title="Indicator settings" style="padding:4px 8px;">⚙</button>
      </div>
      <div class="indPanel" style="display:none; position:absolute; right:10px; top:52px; z-index:6; width:260px; padding:10px; border:1px solid var(--grid); border-radius:10px; background: rgba(15,23,35,0.95);">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px;">
          <div style="color:var(--muted); font-weight:700; font-size:12px;">Indicator settings</div>
          <button class="wbtn indClose" style="padding:4px 8px;">Close</button>
        </div>
        <div style="height:10px;"></div>
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
    indSmaEl: win.querySelector('.indSma'),
    indVwapEl: win.querySelector('.indVwap'),
    indMacdEl: win.querySelector('.indMacd'),
    indGearEl: win.querySelector('.indGear'),
    indPanelEl: win.querySelector('.indPanel'),
    smaPeriodEl: win.querySelector('.smaPeriod'),
    vwapPeriodEl: win.querySelector('.vwapPeriod'),
    macdFastEl: win.querySelector('.macdFast'),
    macdSlowEl: win.querySelector('.macdSlow'),
    macdSignalEl: win.querySelector('.macdSignal'),
    indCloseEl: win.querySelector('.indClose'),
    indApplyEl: win.querySelector('.indApply'),
    smaPeriod: 20,
    vwapPeriod: 0,
    macdFast: 12,
    macdSlow: 26,
    macdSignal: 9,
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
  };
  charts.set(id, chart);

  // init chart + resize observer
  tvInit(chart);
  const ro = new ResizeObserver(()=> tvResize(chart));
  ro.observe(wrap);
  function setTf(tfNext){
    chart.tf = tfNext;
    win.querySelector('.wlabel').textContent = `Chart (${chart.tf})`;
    // highlight selected
    win.querySelectorAll('.tfBtn').forEach(b=>{
      const on = (b.getAttribute('data-tf') === tfNext);
      b.style.background = on ? '#14304f' : '#0f1723';
      b.style.borderColor = on ? '#2f5a8a' : 'var(--grid)';
    });
  }
  setTf(tf);

  win.querySelectorAll('.tfBtn').forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const next = btn.getAttribute('data-tf') || '1s';
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
    chart.smaPeriodEl.value = String(chart.smaPeriod);
    chart.vwapPeriodEl.value = String(chart.vwapPeriod);
    chart.macdFastEl.value = String(chart.macdFast);
    chart.macdSlowEl.value = String(chart.macdSlow);
    chart.macdSignalEl.value = String(chart.macdSignal);
    openPanel();
  });
  chart.indCloseEl?.addEventListener('click', closePanel);
  chart.indApplyEl?.addEventListener('click', ()=>{
    const smaP = parseInt(chart.smaPeriodEl.value || '20', 10);
    const vwapP = parseInt(chart.vwapPeriodEl.value || '0', 10);
    const mf = parseInt(chart.macdFastEl.value || '12', 10);
    const ms = parseInt(chart.macdSlowEl.value || '26', 10);
    const msi = parseInt(chart.macdSignalEl.value || '9', 10);
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
  // Snapshot trades are in ascending time; prepend oldest->newest so newest ends up at top.
  for (const tr of (data.trades || [])) appendTape(tr);
  loadedStartNs = data.ts_effective;
  // Reset playhead + tape monotonic guard at snapshot time (prevents rewinds on resume).
  playheadNs = loadedStartNs;
  setNow(playheadNs ? `Now: ${nsToEt(playheadNs)} ET` : '');
  resetTapeMonotonic(playheadNs);
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
    if (msg.type === 'book'){
      currentBook = msg;
      renderL2(currentBook);
      updatePlayhead(msg.ts_event);
    } else if (msg.type === 'trade'){
      // Guard against rewinds (e.g., resume from an older playhead) which makes the tape "jump backwards".
      if (lastTapeTsSeen != null && msg.ts_event < lastTapeTsSeen) return;
      lastTapeTsSeen = msg.ts_event;
      appendTape(msg);
      updatePlayhead(msg.ts_event);
      // drive higher-TF charts' in-progress bar close/high/low/volume in real-time
      for (const ch of charts.values()) {
        updateChartFromTrade(ch, Number(msg.ts_event), Number(msg.price), Number(msg.size));
      }
    } else if (msg.type === 'eos'){
      setStatus('Paused (end of data)');
      stopStream();
      isPaused = true;
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
    if (msg.type === 'candle') upsertCandle(chart, msg);
    if (msg.type === 'eos') { chart.sse?.close(); chart.sse=null; }
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
    // fallback chart TF to 1s if missing parquet
    if (String(detail).toLowerCase().includes('missing parquet') && chart.tf !== '1s'){
      chart.tf = '1s';
      chart.win.querySelector('.wlabel').textContent = `Chart (1s)`;
      return await loadChartSnapshot(chart);
    }
    return;
  }
  chart.pendingCandles?.clear?.();
  chart.canLoadMore = true;
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

// Create windows
makeWindow({
  id: 'l2',
  title: 'LVL2 (Top 10)',
  x: 12, y: 12, w: 420, h: 380,
  bodyHtml: `
    <table class="l2">
      <thead>
        <tr>
          <th class="sz bidx">Bid Sz</th>
          <th class="px bidx">Bid Px</th>
          <th class="center" style="width:14px;"></th>
          <th class="px askx">Ask Px</th>
          <th class="sz askx">Ask Sz</th>
        </tr>
      </thead>
      <tbody id="l2body"></tbody>
    </table>
    <div class="meta" id="l2meta"></div>
  `
});

makeWindow({
  id: 'tape',
  title: 'Time and Sales',
  x: 444, y: 12, w: 420, h: 520,
  bodyHtml: `
    <div id="tapeList"></div>
  `
});

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

function _placeWin(win, x, y, w, h){
  win.style.left = `${x}px`;
  win.style.top = `${y}px`;
  win.style.width = `${w}px`;
  win.style.height = `${h}px`;
}

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

// Popout mode: show only the requested window and maximize it
if (POPOUT){
  document.querySelectorAll('.win').forEach(w=>{
    if (w.id !== POPOUT) w.style.display = 'none';
  });
  const w = document.getElementById(POPOUT);
  if (w){
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
    ohl_schema_names = pq.ParquetFile(ohl_path).schema.names
    ohl_ts_col = "ts_event" if "ts_event" in ohl_schema_names else "ts"
    ohl_cols = [ohl_ts_col, "symbol", "open", "high", "low", "close", "volume"]
    ohl = _read_parquet_cols(ohl_path, ohl_cols)
    ohl_keep = [i for i, s in enumerate(ohl["symbol"]) if s == symbol]
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
    return HTMLResponse(INDEX_HTML)


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

        if src == "b":
            bids = [[_price_to_float(day.bid_px[i_b][j]), int(day.bid_sz[i_b][j])] for j in range(10)]
            asks = [[_price_to_float(day.ask_px[i_b][j]), int(day.ask_sz[i_b][j])] for j in range(10)]
            yield emit({"type": "book", "ts_event": int(day.mbp_ts[i_b]), "bids": bids, "asks": asks})
            i_b += 1
        elif src == "t":
            yield emit(
                {
                    "type": "trade",
                    "ts_event": int(day.trd_ts[i_t]),
                    "price": _price_to_float(day.trd_px[i_t]),
                    "size": int(day.trd_sz[i_t]),
                }
            )
            i_t += 1
        else:  # "c"
            yield emit(
                {
                    "type": "candle",
                    "t": int(day.ohl_ts[i_c]),
                    "o": _price_to_float(day.ohl_o[i_c]),
                    "h": _price_to_float(day.ohl_h[i_c]),
                    "l": _price_to_float(day.ohl_l[i_c]),
                    "c": _price_to_float(day.ohl_c[i_c]),
                    "v": int(day.ohl_v[i_c]),
                }
            )
            i_c += 1


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

