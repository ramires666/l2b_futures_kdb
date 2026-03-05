#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import orjson
from dotenv import load_dotenv

EPOCH_2000_NS = 946_684_800_000_000_000


def unix_ms_to_q_ns(ts_ms: int) -> int:
    return ts_ms * 1_000_000 - EPOCH_2000_NS


def now_q_ns() -> int:
    return time.time_ns() - EPOCH_2000_NS


def norm_symbol(raw: str) -> str:
    return raw.strip().replace("/", "").replace("-", "").upper()


def parse_symbols(symbols_file: Path, symbols_env: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()

    if symbols_file.exists():
        with symbols_file.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                s = norm_symbol(row[0])
                if not s or s in seen:
                    continue
                seen.add(s)
                symbols.append(s)

    for s in [norm_symbol(x) for x in symbols_env.split(",") if x.strip()]:
        if s not in seen:
            seen.add(s)
            symbols.append(s)

    if not symbols:
        symbols = ["BTCUSDT", "ETHUSDT"]
    return symbols


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def depth_request_weight(limit: int) -> int:
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20


@dataclass(slots=True)
class Settings:
    symbols: list[str]
    flush_ms: int
    depth_levels: int
    stream_chunk_size: int
    ws_base: str
    rest_base: str
    depth_path: str
    q_port: int
    q_script: Path
    qhome: Path
    qlic: Path
    q_exec: Path
    persist_ms: int
    db_dir: Path
    reconnect_sec: float
    snapshot_concurrency: int
    snapshot_limit: int
    request_weight_limit_per_min: int
    resync_log_cooldown_sec: float
    q_process_logs: bool

    @classmethod
    def load(cls) -> "Settings":
        symbols_file = Path(os.getenv("SYMBOLS_FILE", "coinz.csv"))
        symbols_env = os.getenv("SYMBOLS", "")
        symbols = parse_symbols(symbols_file, symbols_env)
        return cls(
            symbols=symbols,
            flush_ms=int(os.getenv("FLUSH_MS", "100")),
            depth_levels=int(os.getenv("L2_LEVELS", "100")),
            stream_chunk_size=int(os.getenv("STREAM_CHUNK_SIZE", "40")),
            ws_base=os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/stream"),
            rest_base=os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com"),
            depth_path=os.getenv("BINANCE_DEPTH_PATH", "/fapi/v1/depth"),
            q_port=int(os.getenv("Q_PORT", "5010")),
            q_script=Path(os.getenv("Q_SCRIPT", "kdb/ingest.q")),
            qhome=Path(os.getenv("QHOME", "q")),
            qlic=Path(os.getenv("QLIC", "q")),
            q_exec=Path(os.getenv("PYKX_Q_EXECUTABLE", "q/l64/q")),
            persist_ms=int(os.getenv("L2B_PERSIST_MS", "5000")),
            db_dir=Path(os.getenv("L2B_DB_DIR", "/home/rut/l2b_kdb_db")),
            reconnect_sec=float(os.getenv("RECONNECT_SEC", "2.0")),
            snapshot_concurrency=int(os.getenv("SNAPSHOT_CONCURRENCY", "8")),
            snapshot_limit=int(os.getenv("SNAPSHOT_LIMIT", "1000")),
            request_weight_limit_per_min=int(os.getenv("REQUEST_WEIGHT_LIMIT_PER_MIN", "2400")),
            resync_log_cooldown_sec=float(os.getenv("RESYNC_LOG_COOLDOWN_SEC", "30")),
            q_process_logs=os.getenv("Q_PROCESS_LOGS", "0") == "1",
        )


@dataclass(slots=True)
class DepthBook:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    initialized: bool = False
    dirty: bool = False
    last_event_ms: int = 0
    buffer: list[dict[str, Any]] = field(default_factory=list)
    first_buffer_U: int | None = None
    sync_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def apply_updates(self, bids: list[list[str]], asks: list[list[str]]) -> None:
        for p_s, q_s in bids:
            p = float(p_s)
            q = float(q_s)
            if q == 0.0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for p_s, q_s in asks:
            p = float(p_s)
            q = float(q_s)
            if q == 0.0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

    def top_bids(self, n: int) -> list[tuple[float, float]]:
        return sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]

    def top_asks(self, n: int) -> list[tuple[float, float]]:
        return sorted(self.asks.items(), key=lambda x: x[0])[:n]


class Collector:
    def __init__(self, settings: Settings, http: aiohttp.ClientSession, qconn: Any) -> None:
        self.s = settings
        self.http = http
        self.qconn = qconn
        self.symbols = settings.symbols
        self.symbol_set = set(self.symbols)
        self.books = {sym: DepthBook() for sym in self.symbols}
        self.resyncing: set[str] = set()
        self.snapshot_sem = asyncio.Semaphore(max(1, settings.snapshot_concurrency))
        self.snapshot_backoff_until = 0.0
        self.snapshot_weight = depth_request_weight(settings.snapshot_limit)
        self.snapshot_min_interval = 60.0 * self.snapshot_weight / max(1, settings.request_weight_limit_per_min)
        self.snapshot_rate_lock = asyncio.Lock()
        self.next_snapshot_slot_monotonic = 0.0

        self.trade_buf: list[tuple[int, int, str, int, float, float, bool, int]] = []
        self.q_lock = asyncio.Lock()
        self.stop_evt = asyncio.Event()
        self.last_log_ns = time.time_ns()
        self.resync_events_since_log = 0
        self.last_resync_log_by_symbol: dict[str, float] = {}

    def _disable_symbol(self, symbol: str, reason: str) -> None:
        if symbol not in self.symbol_set:
            return
        self.symbol_set.discard(symbol)
        self.resyncing.discard(symbol)
        self.books.pop(symbol, None)
        self.symbols = [s for s in self.symbols if s != symbol]
        logging.error("Disabling symbol %s: %s", symbol, reason)

    def _set_snapshot_backoff(self, delay_sec: float) -> None:
        self.snapshot_backoff_until = max(self.snapshot_backoff_until, time.monotonic() + delay_sec)

    def _backoff_from_http_error(self, exc: aiohttp.ClientResponseError, default_delay: float) -> float:
        delay = max(default_delay, 0.0)
        retry_after = None
        if exc.headers:
            retry_after = exc.headers.get("retry-after")
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                pass

        if exc.message:
            m = re.search(r"banned until (\d{13})", exc.message)
            if m:
                ban_until_ms = int(m.group(1))
                delay = max(delay, (ban_until_ms / 1000) - time.time() + 1.0)
        return delay

    async def _wait_snapshot_backoff(self) -> None:
        while True:
            remaining = self.snapshot_backoff_until - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 1.0))

    async def _acquire_snapshot_rate_slot(self) -> None:
        if self.snapshot_min_interval <= 0:
            return
        while True:
            async with self.snapshot_rate_lock:
                now = time.monotonic()
                wait_sec = self.next_snapshot_slot_monotonic - now
                if wait_sec <= 0:
                    self.next_snapshot_slot_monotonic = max(now, self.next_snapshot_slot_monotonic) + self.snapshot_min_interval
                    return
            await asyncio.sleep(min(wait_sec, 0.25))

    async def _raise_for_status_with_body(self, resp: aiohttp.ClientResponse) -> None:
        if resp.status < 400:
            return
        body = await resp.text()
        msg = body.strip() or resp.reason or "HTTP error"
        raise aiohttp.ClientResponseError(
            resp.request_info,
            resp.history,
            status=resp.status,
            message=msg,
            headers=resp.headers,
        )

    async def init_books(self) -> None:
        startup_symbols = list(self.symbols)
        results = await asyncio.gather(
            *(self.sync_symbol(sym, reason="startup") for sym in startup_symbols),
            return_exceptions=True,
        )
        disabled = 0
        for sym, result in zip(startup_symbols, results):
            if isinstance(result, Exception):
                disabled += 1
                self._disable_symbol(sym, str(result))
        if not self.symbols:
            raise RuntimeError("No valid symbols available after snapshot initialization")
        logging.info("Initialized depth snapshots for %d symbols (%d disabled)", len(self.symbols), disabled)

    async def validate_symbols(self) -> None:
        url = f"{self.s.rest_base}/fapi/v1/exchangeInfo"
        try:
            async with self.http.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                await self._raise_for_status_with_body(resp)
                payload = await resp.json(loads=orjson.loads)
        except aiohttp.ClientResponseError as exc:
            if exc.status in (418, 429):
                delay = self._backoff_from_http_error(
                    exc,
                    60.0 if exc.status == 418 else 10.0,
                )
                self._set_snapshot_backoff(delay)
                logging.warning(
                    "exchangeInfo rate-limited with status %s; delaying snapshots for %.1fs",
                    exc.status,
                    delay,
                )
            else:
                logging.warning(
                    "exchangeInfo request failed with status %s; proceeding without filtering",
                    exc.status,
                )
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logging.warning("exchangeInfo request failed (%s); proceeding without filtering", exc)
            return

        for rl in payload.get("rateLimits", []):
            if (
                rl.get("rateLimitType") == "REQUEST_WEIGHT"
                and rl.get("interval") == "MINUTE"
                and int(rl.get("intervalNum", 0)) == 1
            ):
                limit = int(rl.get("limit", 0))
                if limit > 0:
                    self.s.request_weight_limit_per_min = limit
                    self.snapshot_min_interval = 60.0 * self.snapshot_weight / limit
                    logging.info(
                        "REQUEST_WEIGHT detected from exchangeInfo: %d/min (snapshot min_interval=%.3fs)",
                        limit,
                        self.snapshot_min_interval,
                    )
                break

        valid_symbols: set[str] = set()
        for item in payload.get("symbols", []):
            sym = item.get("symbol")
            if not sym:
                continue
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("status") != "TRADING":
                continue
            valid_symbols.add(sym)

        invalid_symbols = [sym for sym in self.symbols if sym not in valid_symbols]
        for sym in invalid_symbols:
            self._disable_symbol(sym, "missing from USD-M perpetual trading symbols")
        if invalid_symbols:
            logging.warning("Filtered %d invalid symbols before startup", len(invalid_symbols))
        if not self.symbols:
            raise RuntimeError("No valid symbols available after symbol validation")

    async def _fetch_snapshot(self, symbol: str) -> dict[str, Any]:
        url = f"{self.s.rest_base}{self.s.depth_path}"
        params = {"symbol": symbol, "limit": self.s.snapshot_limit}
        await self._wait_snapshot_backoff()
        await self._acquire_snapshot_rate_slot()
        async with self.snapshot_sem:
            await self._wait_snapshot_backoff()
            async with self.http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await self._raise_for_status_with_body(resp)
                return await resp.json(loads=orjson.loads)

    async def sync_symbol(self, symbol: str, reason: str) -> None:
        book = self.books[symbol]
        async with book.sync_lock:
            book.initialized = False
            while not self.stop_evt.is_set():
                try:
                    payload = await self._fetch_snapshot(symbol)
                except aiohttp.ClientResponseError as exc:
                    if exc.status in (400, 404):
                        raise RuntimeError(f"snapshot request rejected ({exc.status})") from exc
                    delay = self.s.reconnect_sec
                    if exc.status == 429:
                        delay = self._backoff_from_http_error(exc, max(5.0, self.s.reconnect_sec))
                    elif exc.status == 418:
                        delay = self._backoff_from_http_error(exc, max(30.0, self.s.reconnect_sec * 5))
                    self._set_snapshot_backoff(delay)
                    logging.warning(
                        "Snapshot fetch failed for %s with status %s; retrying in %.1fs",
                        symbol,
                        exc.status,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logging.warning("Snapshot fetch failed for %s: %s; retrying", symbol, exc)
                    await asyncio.sleep(self.s.reconnect_sec)
                    continue
                snapshot_u = int(payload["lastUpdateId"])
                first_u = book.first_buffer_U

                # Binance local book rule: snapshot lastUpdateId should catch buffered stream.
                if first_u is not None and snapshot_u + 1 < first_u:
                    await asyncio.sleep(0.05)
                    continue

                bids = {}
                asks = {}
                for p_s, q_s in payload.get("bids", []):
                    q = float(q_s)
                    if q > 0.0:
                        bids[float(p_s)] = q
                for p_s, q_s in payload.get("asks", []):
                    q = float(q_s)
                    if q > 0.0:
                        asks[float(p_s)] = q

                book.bids = bids
                book.asks = asks
                book.last_update_id = snapshot_u
                book.last_event_ms = int(time.time() * 1000)

                pending = list(book.buffer)

                bridged = False
                need_resync = False
                for ev in pending:
                    U = int(ev["U"])
                    u = int(ev["u"])
                    next_id = book.last_update_id + 1
                    if u < next_id:
                        continue

                    if not bridged:
                        if U <= next_id <= u:
                            bridged = True
                        elif U > next_id:
                            need_resync = True
                            break
                        else:
                            continue
                    else:
                        pu = ev.get("pu")
                        if pu is not None and int(pu) != book.last_update_id:
                            need_resync = True
                            break

                    book.apply_updates(ev.get("b", []), ev.get("a", []))
                    book.last_update_id = u
                    book.last_event_ms = int(ev.get("E", book.last_event_ms))

                if need_resync:
                    await asyncio.sleep(0.05)
                    continue

                book.buffer = []
                book.first_buffer_U = None
                book.initialized = True
                book.dirty = True
                if reason != "startup":
                    self.resync_events_since_log += 1
                    now_mono = time.monotonic()
                    last_mono = self.last_resync_log_by_symbol.get(symbol, 0.0)
                    if now_mono - last_mono >= self.s.resync_log_cooldown_sec:
                        self.last_resync_log_by_symbol[symbol] = now_mono
                        logging.warning("Resynced snapshot for %s after depth gap", symbol)
                    else:
                        logging.debug("Resynced snapshot for %s after depth gap", symbol)
                return

    async def run(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_running_loop().add_signal_handler(sig, self.stop_evt.set)
            except NotImplementedError:
                pass

        await self.validate_symbols()
        ws_urls = self.stream_urls()
        ws_tasks = [asyncio.create_task(self.ws_loop(url), name=f"ws:{url[-40:]}") for url in ws_urls]
        try:
            await self.init_books()
        except Exception:
            for t in ws_tasks:
                t.cancel()
            await asyncio.gather(*ws_tasks, return_exceptions=True)
            raise

        tasks: list[asyncio.Task[Any]] = list(ws_tasks)
        tasks.append(asyncio.create_task(self.flush_loop(), name="flush-loop"))

        await self.stop_evt.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await self.flush_once(force=True)
        except Exception:
            logging.exception("Final flush failed")

    def stream_urls(self) -> list[str]:
        streams: list[str] = []
        for sym in self.symbols:
            s = sym.lower()
            streams.append(f"{s}@trade")
            streams.append(f"{s}@depth@100ms")
        return [
            f"{self.s.ws_base}?streams={'/'.join(group)}"
            for group in chunked(streams, self.s.stream_chunk_size)
        ]

    async def ws_loop(self, url: str) -> None:
        while not self.stop_evt.is_set():
            try:
                async with self.http.ws_connect(url, heartbeat=20, autoclose=True, autoping=True) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = orjson.loads(msg.data)
                        data = payload.get("data", payload)
                        et = data.get("e")
                        if et == "trade":
                            self.on_trade(data)
                        elif et == "depthUpdate":
                            self.on_depth(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("WS loop error, reconnecting in %.1fs", self.s.reconnect_sec)
                await asyncio.sleep(self.s.reconnect_sec)
                await self.resync_all_books()

    async def resync_all_books(self) -> None:
        await asyncio.gather(*(self.resync_symbol(sym) for sym in self.symbols), return_exceptions=True)

    def on_trade(self, data: dict[str, Any]) -> None:
        sym = data["s"]
        if sym not in self.symbol_set:
            return
        t_ms = int(data["T"])
        e_ms = int(data["E"])
        win_ms = (t_ms // self.s.flush_ms) * self.s.flush_ms
        self.trade_buf.append(
            (
                unix_ms_to_q_ns(t_ms),
                unix_ms_to_q_ns(win_ms),
                sym,
                int(data["t"]),
                float(data["p"]),
                float(data["q"]),
                bool(data["m"]),
                unix_ms_to_q_ns(e_ms),
            )
        )

    def on_depth(self, data: dict[str, Any]) -> None:
        sym = data["s"]
        if sym not in self.symbol_set:
            return
        book = self.books[sym]
        if not book.initialized:
            if book.first_buffer_U is None:
                book.first_buffer_U = int(data["U"])
            # Keep bounded buffer in case of prolonged reconnect/sync.
            if len(book.buffer) >= 20000:
                book.buffer = book.buffer[-10000:]
            book.buffer.append(data)
            return

        u = int(data["u"])
        U = int(data["U"])
        pu = data.get("pu")
        if u <= book.last_update_id:
            return
        if pu is not None and int(pu) != book.last_update_id:
            asyncio.create_task(self.resync_symbol(sym))
            return

        book.apply_updates(data.get("b", []), data.get("a", []))
        book.last_update_id = u
        book.last_event_ms = int(data["E"])
        book.dirty = True

    async def resync_symbol(self, symbol: str) -> None:
        if symbol in self.resyncing:
            return
        self.resyncing.add(symbol)
        try:
            await self.sync_symbol(symbol, reason="resync")
        except Exception:
            logging.exception("Snapshot resync failed for %s", symbol)
        finally:
            self.resyncing.discard(symbol)

    async def flush_loop(self) -> None:
        while not self.stop_evt.is_set():
            await asyncio.sleep(self.s.flush_ms / 1000)
            await self.flush_once(force=False)

    async def flush_once(self, force: bool) -> None:
        trade_rows = self.trade_buf
        self.trade_buf = []

        now_ms = int(time.time() * 1000)
        win_ms = (now_ms // self.s.flush_ms) * self.s.flush_ms
        win_ns = unix_ms_to_q_ns(win_ms)
        ts_ns = now_q_ns()

        l2_rows: list[tuple[int, int, str, bool, int, float, float, int]] = []
        for sym, book in self.books.items():
            if not book.dirty and not force:
                continue
            evt_ns = unix_ms_to_q_ns(book.last_event_ms) if book.last_event_ms else ts_ns
            for lvl, (px, qty) in enumerate(book.top_bids(self.s.depth_levels)):
                l2_rows.append((ts_ns, win_ns, sym, True, lvl, px, qty, evt_ns))
            for lvl, (px, qty) in enumerate(book.top_asks(self.s.depth_levels)):
                l2_rows.append((ts_ns, win_ns, sym, False, lvl, px, qty, evt_ns))
            book.dirty = False

        if trade_rows:
            cols = [list(c) for c in zip(*trade_rows)]
            try:
                await self.q_call(".ingest.updTrades", *cols)
            except Exception:
                logging.exception("q insert failed for trades")
                self.stop_evt.set()
                return
        if l2_rows:
            cols = [list(c) for c in zip(*l2_rows)]
            try:
                await self.q_call(".ingest.updL2", *cols)
            except Exception:
                logging.exception("q insert failed for l2")
                self.stop_evt.set()
                return

        now_ns = time.time_ns()
        if now_ns - self.last_log_ns > 10_000_000_000:
            self.last_log_ns = now_ns
            resync_events = self.resync_events_since_log
            self.resync_events_since_log = 0
            logging.info(
                "flush trades=%d l2=%d symbols=%d resync=%d",
                len(trade_rows),
                len(l2_rows),
                len(self.symbols),
                resync_events,
            )

    async def q_call(self, query: str, *args: Any) -> Any:
        async with self.q_lock:
            return await asyncio.to_thread(self.qconn, query, *args)


async def async_main() -> None:
    load_dotenv()
    settings = Settings.load()

    settings.db_dir.mkdir(parents=True, exist_ok=True)
    os.environ["PYKX_UNLICENSED"] = "true"
    os.environ["QHOME"] = str(settings.qhome.resolve())
    os.environ["QLIC"] = str(settings.qlic.resolve())
    os.environ["PYKX_Q_EXECUTABLE"] = str(settings.q_exec.resolve())
    os.environ["L2B_DB_DIR"] = str(settings.db_dir)
    os.environ["L2B_PERSIST_MS"] = str(settings.persist_ms)

    import pykx as kx

    logging.info("Symbols: %s", ",".join(settings.symbols))
    logging.info("L2 levels: %d, flush=%dms, db=%s", settings.depth_levels, settings.flush_ms, settings.db_dir)
    logging.info(
        "Snapshot REST: limit=%d weight=%d request_weight_limit=%d/min min_interval=%.3fs",
        settings.snapshot_limit,
        depth_request_weight(settings.snapshot_limit),
        settings.request_weight_limit_per_min,
        60.0 * depth_request_weight(settings.snapshot_limit) / max(1, settings.request_weight_limit_per_min),
    )
    q_server = kx.util.start_q_subprocess(
        port=settings.q_port,
        load_file=str(settings.q_script.resolve()),
        process_logs=settings.q_process_logs,
    )
    await asyncio.sleep(0.4)
    q_conn = kx.QConnection("localhost", settings.q_port)

    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            collector = Collector(settings, http, q_conn)
            await collector.run()
    finally:
        try:
            await asyncio.to_thread(q_conn, ".ingest.persist[]")
        except Exception:
            logging.exception("Final persist call failed")
        try:
            q_conn.close()
        except Exception:
            pass
        try:
            q_server.terminate()
            q_server.wait(timeout=5)
        except Exception:
            logging.exception("Failed to terminate q server cleanly")


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
