### TradingProject — Day Trading Simulator (DAS-like UX + TradingView charts)

This repo is building a **realistic day trading simulator** for skill development. The target experience is **modeled after DAS Trader** (hotkeys + montage/Level2 + tape + order workflow), but with **charts emulated to match TradingView** (or a TradingView-equivalent charting surface).

The simulator’s purpose is training: **repeatable, high-fidelity practice** using real market microstructure (L2 + prints) so the user can learn to become a skilled day trader.

---

### Non-negotiables (agent rules)

- **Regression tests are off-limits**: the agent **CANNOT modify the regression test(s) to make them pass under any circumstances**. Fix production code instead.

---

### Business intent (what we’re building)

- **Primary goal**: A replayable (and eventually live) trading environment that feels like real trading—fast, constrained, and consistent with actual market behavior.
- **Modeled after DAS Trader**:
  - Montage / order entry (limit/market/stop, routes, share size, offsets)
  - Level 2 ladder + time & sales
  - Hotkeys and fast workflows (entry/exit, partials, flatten, reverse)
  - Risk controls (max loss, max position size, SSR, halts, short locate rules as applicable)
- **Charts from TradingView**:
  - Current code has a custom in-browser chart; future intent is TradingView (embed/widget or Lightweight Charts) while keeping playback synced to the simulator playhead.
- **User outcome**: build pattern recognition, execution skill, and risk discipline through realistic repetition.

---

### Current architecture (what exists today)

This project is currently a **local web app served by Python (FastAPI)**, reading **Databento Parquet** files for replay (and optionally streaming live via Databento Live in the simpler server).

- **Backend**: Python + FastAPI
  - Serves the HTML/JS UI
  - Exposes replay APIs (snapshot + streaming) over HTTP (SSE) in `Simulator.py`
  - Provides an alternate WebSocket-based demo (replay/live) in `server.py`
- **Frontend**: Plain HTML + CSS + vanilla JS (embedded in the Python files)
  - LVL2 ladder (top 10)
  - Time & Sales tape
  - Candle chart (custom canvas) + indicators
  - Window manager: draggable/resizable panels + popouts
- **Data**: Databento historical exports in Parquet, stored in `databento_out/` (and sample files in `databento_test/`)

---

### Key modules / files

- **`Data/download_day_databento.py`**
  - Downloads Databento historical data to parquet
  - Builds higher-timeframe bars from 1-second OHLCV (10s/1m/5m)
  - Output naming convention matches `databento_out/*.parquet`

- **`Simulator/Simulator.py`** (main replay app)
  - FastAPI server with a richer replay UI: **LVL2 + tape + chart**
  - Reads parquet efficiently via **PyArrow**, loads a day into an in-memory structure, then serves:
    - JSON snapshot endpoints
    - Real-time-ish playback via **Server-Sent Events (EventSource)**

- **`Simulator/server.py`** (simpler live/replay demo)
  - FastAPI server with embedded HTML UI + **WebSocket `/ws`**
  - Supports:
    - **Replay**: merges book+trade events and replays at variable speed with seeking
    - **Live**: subscribes to Databento Live (`XNAS.ITCH` mbp-10 + `EQUS.MINI` trades)

---

### Data model (parquet inputs)

The simulator’s replay is driven by three Databento schemas (per day), typically stored as:

- **Level 2 (Top 10) book**: `XNAS.ITCH.{YYYY-MM-DD}.mbp-10.parquet`
  - Expected columns include:
    - `ts_event` (event timestamp)
    - `symbol`
    - `bid_px_00..09`, `bid_sz_00..09`
    - `ask_px_00..09`, `ask_sz_00..09`

- **Time & Sales (prints)**: `EQUS.MINI.{YYYY-MM-DD}.trades.parquet`
  - Expected columns include:
    - `ts_event`
    - `symbol`
    - `price`
    - `size`
    - (sometimes `side`, depending on how parquet was produced / mapped)

- **Candles**: `EQUS.MINI.{YYYY-MM-DD}.ohlcv-{tf}.parquet` where `tf ∈ {1s,10s,1m,5m}`
  - Expected columns include:
    - timestamp column: `ts_event` **or** `ts` (code handles both)
    - `symbol`, `open`, `high`, `low`, `close`, `volume`

---

### In-memory structures (replay core)

`Simulator/Simulator.py` loads a day into a cached dataclass:

- **`LoadedDay`**
  - **MBP10 arrays**: `mbp_ts`, `bid_px[rows][10]`, `bid_sz[rows][10]`, `ask_px[rows][10]`, `ask_sz[rows][10]`
  - **Trades arrays**: `trd_ts`, `trd_px`, `trd_sz`
  - **OHLCV arrays**: `ohl_ts`, `ohl_o`, `ohl_h`, `ohl_l`, `ohl_c`, `ohl_v`
  - Provides time bounds and enables fast bisection lookups for snapshot/streaming.

This structure is designed for **fast sequential playback** and “at-or-before” queries (book snapshot at a given playhead).

---

### Backend APIs (Simulator.py)

`Simulator/Simulator.py` exposes:

- **`GET /`**
  - Serves the replay UI.

- **`GET /api/metadata`**
  - Returns available time bounds for the requested `symbol/day/tf/data_dir`.

- **`GET /api/snapshot`**
  - Given a requested time (ET string `ts` or UTC ns `ts_ns`), returns:
    - effective snapped timestamp (OHLCV bucket alignment)
    - current L2 book at-or-before
    - last N trades before playhead
    - a small candles window for chart context

- **`GET /api/candles_window`**
  - Fetches a candles-only lookback window ending at a playhead time (snapped).

- **`GET /api/stream`** (SSE / EventSource)
  - Streams JSON messages in timestamp order at the requested `speed`
  - `what` can be: `all | booktrades | candles`
  - Message shapes:
    - `{"type":"book","ts_event":...,"bids":[[px,sz]...],"asks":[[px,sz]...]}`
    - `{"type":"trade","ts_event":...,"price":...,"size":...}`
    - `{"type":"candle","t":...,"o":...,"h":...,"l":...,"c":...,"v":...}`
    - `{"type":"eos"}` end-of-stream

Important realism detail already handled:
- For higher timeframes (`10s/1m/5m`), the UI avoids “future leaking” by **building the in-progress candle from tape trades** rather than trusting precomputed OHLCV for the current bucket.

---

### User interface (current UI)

There are two UIs (both served at `http://127.0.0.1:8000` when their server is running):

- **Replay UI (`Simulator/Simulator.py`)**
  - Top bar controls:
    - symbol, day, timestamp (assumed ET by default), speed, Load/Play/Pause
  - Workspace:
    - **Level 2 ladder (Top 10)**: bids/asks table
    - **Time & Sales**: scrolling tape
    - **Chart(s)**: custom canvas candlesticks + volume + optional indicators (SMA/VWAP/MACD)
  - Windowing:
    - draggable/resizable windows
    - popout support (open panels in a separate window/tab)

- **L2 Ladder + Tape demo (`Simulator/server.py`)**
  - Minimal controls for mode (replay/live), symbol, day, speed, seek slider
  - WebSocket pushes `book` and `trade` updates

---

### How to run (local)

### Development environment

- We use the Conda environment **`TradingProject`** located at: `/Users/zizizink/miniforge3/envs/TradingProject`

- **Replay UI**:

```bash
python /Users/zizizink/Documents/TradingProject/Simulator/Simulator.py
```

- **WebSocket demo**:

```bash
uvicorn server:app --reload
```

Then open `http://127.0.0.1:8000`.

---

### Where TradingView fits (intended direction)

Today, charts are a custom canvas implementation in `Simulator/Simulator.py`. The end goal is to swap/augment that with a **TradingView charting surface** while keeping these constraints:

- **Single source of truth**: simulator playhead time (UTC ns) drives all panels
- **No future leakage** during replay (especially for higher TF candles)
- **Deterministic replay**: same inputs → same fills/PnL/outcomes
- **Low-latency UI**: hotkeys and order entry must feel instantaneous

Implementation options:
- Embed TradingView chart widget (visual-only) + keep simulator candles/tape separate
- Use TradingView Lightweight Charts and feed candles/trades from `/api/stream`

---

### North-star feature set (planned)

- **Order simulation**: realistic order book interaction, partial fills, queue position approximations, latency/slippage models
- **Account + risk**: buying power, margin, fees/commissions, max loss, max shares, hard stops
- **Replay tooling**: session presets, bookmarks, repeat scenarios, stats
- **Trader workflows**: DAS-like montage, hotkeys, layouts, symbol hot swap
- **Journal & review**: trades log, screenshots, metrics, play-by-play review
