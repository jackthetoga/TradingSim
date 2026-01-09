### TradingProject — Day Trading Simulator (DAS-like UX + TradingView charts)

This repo is building a **realistic day trading simulator** for skill development. The target experience is **modeled after DAS Trader** (hotkeys + montage/Level2 + tape + order workflow), with **charts implemented using TradingView Lightweight Charts** (TradingView-like charting UX).

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
  - The replay UI in `Simulator/Simulator.py` uses **TradingView Lightweight Charts** (standalone build) for candlesticks + volume + indicators, synced to the simulator playhead.
- **User outcome**: build pattern recognition, execution skill, and risk discipline through realistic repetition.

---

### Current architecture (what exists today)

This project is currently a **local web app served by Python (FastAPI)**, reading **Databento Parquet** files for deterministic replay.

- **Backend**: Python + FastAPI
  - Serves the HTML/JS UI
  - Exposes replay APIs (snapshot + streaming) over HTTP (SSE) in `Simulator.py`
  - Persists UI configs to disk under `Configs/` via `POST /api/config/save` (currently: `layout.json`, `hotkeys.json`)
- **Frontend**: Plain HTML + CSS + vanilla JS (embedded in the Python files)
  - LVL2 ladder (top 10)
  - Time & Sales tape
  - TradingView Lightweight Charts (candles + volume) + indicators (SMA/VWAP/MACD)
  - Window manager: draggable/resizable windows + popouts (with cross-tab control sync)
  - Event-driven trading simulator (orders → fills → positions/P&L) driven by incoming book/trade events
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

- **`POST /api/config/save`**
  - Persists allowed UI configs to `Configs/` (currently `layout` and `hotkeys`)

- **`GET /api/config/load`**
  - Loads a named config from `Configs/` (if present)

Important realism detail already handled:
- For higher timeframes (`10s/1m/5m`), the UI avoids “future leaking” by **building the in-progress candle from tape trades** rather than trusting precomputed OHLCV for the current bucket.

---

### User interface (current UI)

The replay UI is served at `http://127.0.0.1:8000` when `Simulator/Simulator.py` is running:

- **Replay UI (`Simulator/Simulator.py`)**
  - Top bar controls: symbol, day, time (ET), speed, Load/Play/Pause
  - Workspace (floating windows):
    - **Level 2 ladder (Top 10)**
    - **Time & Sales** (tape)
    - **Chart(s)**: TradingView Lightweight Charts (candles + volume) + optional indicators (SMA/VWAP/MACD)
    - **Order Entry**: basic market/limit order placement (DAS-like controls)
    - **Open Orders**: live view of working orders with cancel / cancel-all
    - **Trading History**: fills log with realized P/L per fill (when applicable)
    - **Positions**: shares, avg cost, open/total P&L (marks off last trade / book)
    - **Hotkeys**: view/edit/record keybinds by context
  - Window tools:
    - **Windows** picker (spawn/show windows in-page; charts can be spawned repeatedly)
    - **Add Chart** (spawn another chart window)
  - Windowing:
    - draggable/resizable windows
    - popout support (open a panel in a separate tab) with cross-tab control sync

---

### Event-based trading simulator (current)

Trading state is updated **event-by-event** as replay data arrives (book updates + trades). The browser keeps an in-memory trading ledger:

- **Orders**
  - Supported: **MKT** and **LMT** (basic)
  - Order status: `open | partial | filled | cancelled | rejected`
  - Marketable limits and markets sweep against current L2 using a deterministic participation model.
  - Passive limits can fill from prints at the order price with a rough FIFO-style `queueAhead` approximation.

- **Fills → Positions → P&L**
  - Each fill updates positions (signed shares + average cost) and tracks realized P&L on reductions/closures.
  - Positions are marked using last trade / book to show open + total P&L in the Positions window.

- **Replay correctness**
  - Seeking/snapshot resets trading state to the selected playhead time by pruning fills after the timestamp and rebuilding positions.

---

### Hotkeys + saved layouts (current)

- **Hotkeys**
  - Contexts: `global`, `entry`, `chart`, `tape`, `l2` (active context follows the focused window)
  - Editable in the **Hotkeys** window with a “record” flow; persisted to localStorage and to `Configs/hotkeys.json`.

- **Layout saving**
  - Window positions/sizes/z-order/hidden state and chart window layout are captured and persisted to localStorage and to `Configs/layout.json`.
  - On startup, layout/hotkeys are injected from disk configs when present for consistent defaults across sessions.

---

### How to run (local)

### Development environment

- We use the Conda environment **`TradingProject`** located at: `/Users/zizizink/miniforge3/envs/TradingProject`

- **Replay UI**:

```bash
python /Users/zizizink/Documents/TradingProject/Simulator/Simulator.py
```

Then open `http://127.0.0.1:8000`.

---

### TradingView charting (current)

Charts in `Simulator/Simulator.py` are implemented with **TradingView Lightweight Charts** while keeping these constraints:

- **Single source of truth**: simulator playhead time (UTC ns) drives all panels
- **No future leakage** during replay (especially for higher TF candles)
- **Deterministic replay**: same inputs → same fills/PnL/outcomes
- **Low-latency UI**: hotkeys and order entry must feel instantaneous

Implementation notes:
- The Lightweight Charts library is loaded via CDN (`unpkg`) inside the served HTML.
- The chart time axis is formatted in **America/New_York (ET)** for a trading-session-native display.

---

### North-star feature set (planned)

- **Order simulation**: realistic order book interaction, partial fills, queue position approximations, latency/slippage models
- **Account + risk**: buying power, margin, fees/commissions, max loss, max shares, hard stops
- **Replay tooling**: session presets, bookmarks, repeat scenarios, stats
- **Trader workflows**: DAS-like montage, hotkeys, layouts, symbol hot swap
- **Journal & review**: trades log, screenshots, metrics, play-by-play review
