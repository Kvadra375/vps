from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Set

import aiohttp  # websocket + REST helper
from binance import AsyncClient  # python-binance
from loguru import logger
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# Compatibility patch for PTB 20.x + Python 3.13
try:
    from telegram.ext._updater import Updater as _PTBUpdater

    if "_Updater__polling_cleanup_cb" not in _PTBUpdater.__slots__:
        _PTBUpdater.__slots__ = (
            *_PTBUpdater.__slots__,
            "_Updater__polling_cleanup_cb",
        )
except Exception as _patch_exc:
    import warnings

    warnings.warn(
        f"PTB compatibility patch failed — Telegram bot may not start: {_patch_exc}",
        RuntimeWarning,
    )

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7765268238:AAFfNYacUNK1KB93UWJgffa-vT2Q5ALA9x0")
SUBSCRIBERS_FILE = Path("subscribers.json")
LOG_FILE = Path("futures_trades.log")
NOTIFY_COOLDOWN = timedelta(minutes=15)
MIN_TRADES = 5  # Notify on 5 matching trades
MATCH_WINDOW = timedelta(seconds=60)  # 1-minute window
QUANTITY_TOLERANCE = 0.001  # 0.1% tolerance for matching quantities
MIN_NOTIONAL = 5000  # USDT
EXCLUDE_SYMBOLS = {"BTCUSDT", "ETHUSDT", "USDCUSDT", "BNBUSDT", "XRPUSDT", "AAVEUSDT", "FDUSDUSDT", "AVAXUSDT", "SOLUSDT", "1000PEPEUSDT"}

# Logging setup
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    diagnose=False,
    backtrace=False,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
)
logger.add(LOG_FILE, level="DEBUG", rotation="5 MB", compression="zip")

@dataclass
class Trade:
    ts: float  # unix seconds
    qty: float
    notional: float
    side: str  # "buy" | "sell"

    def nearly_equal(self, other: "Trade") -> bool:
        return math.isclose(self.qty, other.qty, rel_tol=QUANTITY_TOLERANCE)

class TelegramNotifier:
    def __init__(self, token: str) -> None:
        self.app: Application = (
            Application.builder().token(token).updater(None).build()
        )
        self._subs: Set[int] = set()
        self._load_subscribers()
        self._register_handlers()
        logger.info("Telegram bot initialized with {n} subscriber(s)", n=len(self._subs))

    def _register_handlers(self) -> None:
        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("stop", self._cmd_stop))

    def _load_subscribers(self) -> None:
        if SUBSCRIBERS_FILE.exists():
            try:
                self._subs = set(json.loads(SUBSCRIBERS_FILE.read_text()))
            except Exception as e:
                logger.error("Could not read {file}: {e}", file=SUBSCRIBERS_FILE, e=e)
                self._subs = set()

    def _save_subscribers(self) -> None:
        SUBSCRIBERS_FILE.write_text(json.dumps(list(self._subs)))

    async def _cmd_start(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        cid = update.effective_chat.id
        if cid in self._subs:
            await update.message.reply_text("⚠️ Вы уже подписаны.")
            return
        self._subs.add(cid)
        self._save_subscribers()
        await update.message.reply_text("✅ Подписка на сигналы успешно оформлена.")
        logger.info("Chat {id} subscribed", id=cid)

    async def _cmd_stop(self, update: Update, _: ContextTypes.DEFAULT_TYPE) -> None:
        cid = update.effective_chat.id
        if cid not in self._subs:
            await update.message.reply_text("⚠️ Вы ещё не подписаны.")
            return
        self._subs.remove(cid)
        self._save_subscribers()
        await update.message.reply_text("❌ Подписка отменена.")
        logger.info("Chat {id} unsubscribed", id=cid)

    async def broadcast(self, text: str) -> None:
        for cid in list(self._subs):
            try:
                await self.app.bot.send_message(cid, text)
            except Exception as e:
                logger.error("Telegram send to {id} failed: {e}", id=cid, e=e)

    async def run_forever(self) -> None:
        await self.app.initialize()
        await self.app.start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.app.stop()
            await self.app.shutdown()

class TradeMonitor:
    def __init__(self, notifier: TelegramNotifier) -> None:
        self.notifier = notifier
        self.buffers: Dict[str, deque[Trade]] = defaultdict(deque)
        self.last_notify: Dict[str, float] = defaultdict(float)
        self.sqlite_enabled = False
        self.sqlite_path: Path | None = None
        self.sqlite_queue: asyncio.Queue[Dict[str, Any]] | None = None
        self.valid_symbols: Set[str] = set()

    def enable_sqlite(self, path: str | os.PathLike) -> None:
        import aiosqlite
        self.sqlite_enabled = True
        self.sqlite_path = Path(path)
        self.sqlite_queue = asyncio.Queue()
        logger.info("SQLite persistence queued at {p}", p=path)
        asyncio.create_task(self._sqlite_writer())

    async def _sqlite_writer(self) -> None:
        if not self.sqlite_enabled or not self.sqlite_path:
            return
        import aiosqlite
        async with aiosqlite.connect(self.sqlite_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS matched_trades (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts       INTEGER,
                    symbol   TEXT,
                    qty      REAL,
                    notional REAL,
                    side     TEXT,
                    market   TEXT
                )
                """
            )
            await db.commit()
            while True:
                rec = await self.sqlite_queue.get()
                await db.execute(
                    "INSERT INTO matched_trades (ts, symbol, qty, notional, side, market) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        rec["ts"],
                        rec["symbol"],
                        rec["qty"],
                        rec["notional"],
                        rec["side"],
                        rec["market"],
                    ),
                )
                await db.commit()

    async def handle_trade(self, symbol: str, market: str, msg: Dict[str, Any]) -> None:
        if symbol not in self.valid_symbols:
            return

        ts = msg["T"] / 1000
        qty = float(msg["q"])
        price = float(msg["p"])
        notional = qty * price
        if notional < MIN_NOTIONAL:
            return

        side = "sell" if msg.get("m") else "buy"
        trade = Trade(ts, qty, notional, side)

        # Buffer trades and remove outdated ones
        buf = self.buffers[symbol]
        buf.append(trade)
        limit = ts - MATCH_WINDOW.total_seconds()
        while buf and buf[0].ts < limit:
            buf.popleft()

        # Count matching trades (same quantity)
        matching_trades = [t for t in buf if trade.nearly_equal(t)]
        match_count = len(matching_trades)

        # Debug log to track matching trades
        logger.debug(f"Symbol: {symbol} | Qty: {qty:.2f} | Match count: {match_count}")

        # Log trade to console with colored Qty
        qty_str = f"{trade.qty:.2f}"
        if match_count == 1:
            qty_str = f"<green>{qty_str}</green>"
        elif match_count == 2:
            qty_str = f"<yellow>{qty_str}</yellow>"
        else:  # match_count >= 3
            qty_str = f"<red>{qty_str}</red>"

        log_message = (
            f"Trade: {symbol} ({market}) | Side: {side} | Qty: {qty_str} | "
            f"Notional: {trade.notional:.2f} USDT | Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(trade.ts))}"
        )
        logger.info(log_message)

        # Send notification if MIN_TRADES or more matching trades
        if match_count >= MIN_TRADES:
            now = time.time()
            if now - self.last_notify[symbol] < NOTIFY_COOLDOWN.total_seconds():
                return
            self.last_notify[symbol] = now
            text = (
                f"\u26A0\ufe0f {match_count} сделки за {MATCH_WINDOW.total_seconds() / 60:.1f} минут\n"
                f"Объём: {trade.qty:.2f} контрактов ≈ {trade.notional:.2f} USDT\n"
                f"Пара: {symbol} ({market}), side: {trade.side}"
            )
            logger.info(text.replace("\n", " | "))
            await self.notifier.broadcast(text)

            if self.sqlite_enabled and self.sqlite_queue:
                await self.sqlite_queue.put(
                    {
                        "ts": int(ts * 1000),
                        "symbol": symbol,
                        "qty": trade.qty,
                        "notional": trade.notional,
                        "side": trade.side,
                        "market": market,
                    }
                )

    async def _filter_spot(self, client: AsyncClient) -> List[str]:
        info = await client.get_exchange_info()
        symbols: List[str] = []
        for s in info["symbols"]:
            symbol = s["symbol"]
            if s["quoteAsset"] != "USDT" or symbol in EXCLUDE_SYMBOLS:
                continue
            symbols.append(symbol)
            self.valid_symbols.add(symbol)
        logger.debug("Spot symbols: {s}", s=symbols)
        return symbols

    async def _filter_futures(self, client: AsyncClient) -> List[str]:
        info = await client.futures_exchange_info()
        symbols: List[str] = []
        for s in info["symbols"]:
            symbol = s["symbol"]
            if s["quoteAsset"] != "USDT" or symbol in EXCLUDE_SYMBOLS:
                continue
            symbols.append(symbol)
            self.valid_symbols.add(symbol)
        logger.debug("Futures symbols: {s}", s=symbols)
        return symbols

    async def _stream(self, base_url: str, symbols: List[str], market: str) -> None:
        if not symbols:
            logger.warning("No symbols to stream for {m}", m=market)
            return
        streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
        url = f"{base_url}/stream?streams={streams}"
        logger.info("Connecting {url}", url=url)
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=30) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                payload = data.get("data", {})
                                if "s" in payload:
                                    asyncio.create_task(self.handle_trade(payload["s"], market, payload))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("WS {url} error: {e}. Reconnecting in 5 s…", url=url, e=e)
                await asyncio.sleep(5)

    async def run(self) -> None:
        client = await AsyncClient.create()
        try:
            spot_syms = await self._filter_spot(client)
            fut_syms = await self._filter_futures(client)
        finally:
            await client.close_connection()

        logger.info("Spot symbols loaded: {n}", n=len(spot_syms))
        logger.info("Futures symbols loaded: {n}", n=len(fut_syms))

        tasks = [
            asyncio.create_task(
                self._stream(
                    "wss://stream.binance.com:9443", spot_syms, "spot"
                )
            ),
            asyncio.create_task(
                self._stream("wss://fstream.binance.com", fut_syms, "futures")
            ),
        ]
        await asyncio.gather(*tasks)

async def _telegram_runner(bot: TelegramNotifier):
    try:
        await bot.run_forever()
    except Exception as e:
        logger.error("Telegram runner crashed: {e}", e=e)
        await bot.app.stop()
        await bot.app.shutdown()
        os._exit(1)

async def main():
    notifier = TelegramNotifier(TELEGRAM_TOKEN)
    monitor = TradeMonitor(notifier)
    # monitor.enable_sqlite("trades.sqlite3")  # Uncomment to enable SQLite
    try:
        await asyncio.gather(
            _telegram_runner(notifier),
            monitor.run(),
        )
    finally:
        await notifier.app.stop()
        await notifier.app.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main(), debug=False)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down…")