"""
pump.fun sniper (standalone) with risk profiles.

  medium : default. Stricter filters, smaller size, tighter stops.
  high   : looser filters, bigger size, wider stops and bigger targets.

AUTO_SCALE moves between them based on recent results in the current mode
(paper results never promote live trading).

Data:    PumpPortal WebSocket (free)      wss://pumpportal.fun/api/data
Trading: PumpPortal Local Transaction API https://pumpportal.fun/api/trade-local
         -> returns an unsigned tx; we sign locally, so the key never leaves the droplet.
"""
import asyncio
import base64
import csv
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv()


def _b(k, d): return os.getenv(k, d).strip().lower() in ("1", "true", "yes")
def _f(k, d): return float(os.getenv(k, d))
def _i(k, d): return int(os.getenv(k, d))


# ---------- risk profiles (edit here to tune) ----------
PROFILES = {
    "medium": SimpleNamespace(
        buy_sol=_f("MEDIUM_BUY_SOL", "0.05"),
        daily_loss=_f("MEDIUM_DAILY_LOSS_SOL", "0.3"),
        slippage=25, priority_fee=0.0005,
        tp1=0.40, tp1_sell=0.50,        # at +40%, sell half
        trail=0.20,                     # then trail the rest 20% below its peak
        stop_loss=0.20, max_hold=150, max_positions=3,
        max_dev_pct=8, window=8, min_buyers=6, min_growth=0.10, require_socials=True,
    ),
    "high": SimpleNamespace(
        buy_sol=_f("HIGH_BUY_SOL", "0.10"),
        daily_loss=_f("HIGH_DAILY_LOSS_SOL", "0.75"),
        slippage=35, priority_fee=0.001,
        tp1=1.00, tp1_sell=0.30,        # at +100%, sell 30%
        trail=0.30,                     # let the rest run with a 30% trailing stop
        stop_loss=0.35, max_hold=300, max_positions=5,
        max_dev_pct=15, window=4, min_buyers=3, min_growth=0.05, require_socials=False,
    ),
}

PAPER = _b("PAPER_MODE", "true")
RPC_URL = os.getenv("RPC_URL", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
START_PROFILE = os.getenv("RISK_PROFILE", "medium").strip().lower()

AUTO_SCALE = _b("AUTO_SCALE", "true")
SCALE_WINDOW = _i("SCALE_WINDOW_TRADES", "20")       # judge on the last N closed trades
SCALE_MIN_TRADES = _i("SCALE_MIN_TRADES", "20")      # need this many before any switch
SCALE_UP_WINRATE = _f("SCALE_UP_WINRATE_PCT", "50") / 100
SCALE_DOWN_WINRATE = _f("SCALE_DOWN_WINRATE_PCT", "40") / 100
SCALE_DOWN_DRAWDOWN = _f("SCALE_DOWN_DRAWDOWN_SOL", "0.3")  # drop to medium if high loses this much

MAX_WATCH = _i("MAX_WATCH", "25")
FEE_PCT = _f("EST_ROUND_TRIP_FEE_PCT", "3") / 100

WS_URL = "wss://pumpportal.fun/api/data"
TRADE_URL = "https://pumpportal.fun/api/trade-local"
SUPPLY = 1_000_000_000
TRADES_CSV = Path("trades.csv")
CSV_HEADER = ["closed_utc", "mode", "profile", "symbol", "mint", "size_sol", "entry_mcap_sol",
              "exit_mcap_sol", "return_pct", "est_pnl_sol", "held_sec", "reason"]

if START_PROFILE not in PROFILES:
    raise SystemExit(f"RISK_PROFILE must be one of {list(PROFILES)}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("sniper.log")],
)
log = logging.getLogger("sniper")


@dataclass
class Watch:
    mint: str
    symbol: str
    dev: str
    created: float
    start_mcap: float
    buyers: set = field(default_factory=set)
    dev_sold: bool = False
    vetted: bool = False


@dataclass
class Position:
    mint: str
    symbol: str
    dev: str
    profile: str          # exits follow the profile the trade was opened under
    size_sol: float
    entry_mcap: float
    opened: float
    peak: float = 0.0
    remaining: float = 1.0
    realized: float = 0.0  # return contributed by portions already sold
    txs: int = 1
    tp1_done: bool = False
    busy: bool = False


class Sniper:
    def __init__(self):
        self.watch: dict[str, Watch] = {}
        self.pos: dict[str, Position] = {}
        self.last: dict[str, float] = {}
        self.pending = 0
        self.ws = None
        self.http = None
        self.day, self.day_pnl, self.halt_logged = date.today(), 0.0, False
        self.profile = START_PROFILE
        self.hist = deque(maxlen=SCALE_WINDOW)
        self.since_switch = 0.0
        self.kp = None
        if not PAPER:
            if not (RPC_URL and PRIVATE_KEY):
                raise SystemExit("LIVE mode needs RPC_URL and PRIVATE_KEY in .env")
            from solders.keypair import Keypair
            self.kp = Keypair.from_base58_string(PRIVATE_KEY)
        self.load_history()
        self.rescale()

    @property
    def cfg(self):
        return PROFILES[self.profile]

    # ---------- main loop ----------
    async def run(self):
        who = "PAPER" if PAPER else f"LIVE wallet {self.kp.pubkey()}"
        log.info("Starting in %s mode, profile %s, auto-scale %s", who, self.profile.upper(),
                 "on" if AUTO_SCALE else "off")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
            self.http = http
            self._ticker = asyncio.create_task(self.ticker())
            while True:
                try:
                    await self.stream()
                except Exception as e:
                    log.warning("Stream dropped (%s), reconnecting", e)
                self.ws = None
                await asyncio.sleep(2)

    async def stream(self):
        async with websockets.connect(WS_URL, ping_interval=20, max_size=2**20) as ws:
            self.ws = ws
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            keys = list(self.watch) + list(self.pos)
            if keys:
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": keys}))
            log.info("Connected to PumpPortal")
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                t = msg.get("txType")
                if t == "create":
                    await self.on_create(msg)
                elif t in ("buy", "sell"):
                    await self.on_trade(msg)

    async def sub(self, mint, on=True):
        if not self.ws:
            return
        method = "subscribeTokenTrade" if on else "unsubscribeTokenTrade"
        try:
            await self.ws.send(json.dumps({"method": method, "keys": [mint]}))
        except Exception as e:
            log.debug("sub error %s", e)

    # ---------- events ----------
    async def on_create(self, m):
        mint = m.get("mint")
        if not mint or len(self.watch) >= MAX_WATCH or not self.can_open():
            return
        dev_pct = float(m.get("initialBuy") or 0) / SUPPLY * 100
        if dev_pct > self.cfg.max_dev_pct:
            return
        mcap = float(m.get("marketCapSol") or 0)
        w = Watch(mint, m.get("symbol", "?"), m.get("traderPublicKey", ""), time.time(), mcap)
        self.watch[mint] = w
        self.last[mint] = mcap
        await self.sub(mint)
        log.info("Watching %s (%s..) dev %.1f%%, mcap %.1f SOL", w.symbol, mint[:6], dev_pct, mcap)
        asyncio.create_task(self.vet(w, m.get("uri")))

    async def vet(self, w, uri):
        if self.cfg.require_socials and not await self.has_socials(uri):
            await self.drop(w.mint, "no socials")
            return
        w.vetted = True
        if self.cfg.window <= 0 and w.mint in self.watch and self.can_open():
            del self.watch[w.mint]
            await self.buy(w)

    async def on_trade(self, m):
        mint = m.get("mint")
        mcap = float(m.get("marketCapSol") or 0)
        if mcap:
            self.last[mint] = mcap
        trader = m.get("traderPublicKey")
        w = self.watch.get(mint)
        if w:
            if m["txType"] == "buy" and trader != w.dev:
                w.buyers.add(trader)
            elif m["txType"] == "sell" and trader == w.dev:
                w.dev_sold = True
        p = self.pos.get(mint)
        if p:
            if m["txType"] == "sell" and trader == p.dev and not p.busy:
                p.busy = True
                asyncio.create_task(self.sell(p, 1.0, "dev sold"))
            else:
                self.check_exit(p)

    async def ticker(self):
        while True:
            await asyncio.sleep(1)
            c, now = self.cfg, time.time()
            for w in list(self.watch.values()):
                if w.dev_sold:
                    await self.drop(w.mint, "dev sold")
                    continue
                if not w.vetted or now - w.created < c.window:
                    continue
                mcap = self.last.get(w.mint, w.start_mcap)
                growth = mcap / w.start_mcap - 1 if w.start_mcap else 0
                if len(w.buyers) >= c.min_buyers and growth >= c.min_growth and self.can_open():
                    del self.watch[w.mint]
                    asyncio.create_task(self.buy(w))
                else:
                    await self.drop(w.mint, f"skip: {len(w.buyers)} buyers, {growth:+.0%}")
            for p in list(self.pos.values()):
                self.check_exit(p)

    # ---------- risk ----------
    def can_open(self):
        if date.today() != self.day:
            self.day, self.day_pnl, self.halt_logged = date.today(), 0.0, False
        if self.day_pnl <= -self.cfg.daily_loss:
            if not self.halt_logged:
                log.warning("Daily loss limit hit (%.3f SOL). No new buys until tomorrow.", self.day_pnl)
                self.halt_logged = True
            return False
        return len(self.pos) + self.pending < self.cfg.max_positions

    def check_exit(self, p):
        if p.busy or not p.entry_mcap:
            return
        c = PROFILES[p.profile]
        cur = self.last.get(p.mint, p.entry_mcap)
        p.peak = max(p.peak, cur)
        r = cur / p.entry_mcap - 1
        frac, reason = None, None
        if not p.tp1_done and r >= c.tp1:
            frac, reason = c.tp1_sell, f"take profit 1 ({c.tp1_sell:.0%})"
        elif r <= -c.stop_loss:
            frac, reason = 1.0, "stop loss"
        elif p.tp1_done and cur <= p.peak * (1 - c.trail):
            frac, reason = 1.0, "trailing stop"
        elif time.time() - p.opened >= c.max_hold:
            frac, reason = 1.0, "time exit"
        if frac:
            p.busy = True
            asyncio.create_task(self.sell(p, frac, reason))

    def rescale(self):
        if not AUTO_SCALE or len(self.hist) < SCALE_MIN_TRADES:
            if AUTO_SCALE and self.profile == "high" and self.since_switch <= -SCALE_DOWN_DRAWDOWN:
                self.switch("medium", f"high lost {self.since_switch:.3f} SOL since scaling up")
            return
        winrate = sum(1 for x in self.hist if x > 0) / len(self.hist)
        net = sum(self.hist)
        if self.profile == "medium" and winrate >= SCALE_UP_WINRATE and net > 0:
            self.switch("high", f"last {len(self.hist)} trades: {winrate:.0%} wins, {net:+.3f} SOL")
        elif self.profile == "high" and (winrate < SCALE_DOWN_WINRATE or net <= 0
                                         or self.since_switch <= -SCALE_DOWN_DRAWDOWN):
            self.switch("medium", f"last {len(self.hist)} trades: {winrate:.0%} wins, {net:+.3f} SOL")

    def switch(self, new, why):
        log.warning("PROFILE %s -> %s (%s)", self.profile.upper(), new.upper(), why)
        self.profile, self.since_switch = new, 0.0

    async def drop(self, mint, why):
        if self.watch.pop(mint, None):
            log.info("Drop %s..: %s", mint[:6], why)
            await self.sub(mint, False)
            self.last.pop(mint, None)

    # ---------- trading ----------
    async def buy(self, w):
        c, prof = self.cfg, self.profile
        self.pending += 1
        try:
            if not PAPER:
                sig = await self.trade("buy", w.mint, c.buy_sol, True, c)
                if not sig or not await self.confirmed(sig):
                    log.warning("Buy failed for %s", w.symbol)
                    await self.sub(w.mint, False)
                    self.last.pop(w.mint, None)
                    return
            entry = self.last.get(w.mint, w.start_mcap)
            self.pos[w.mint] = Position(w.mint, w.symbol, w.dev, prof, c.buy_sol, entry, time.time(), peak=entry)
            log.info("BUY [%s] %s at mcap %.1f SOL (%.3f SOL)", prof, w.symbol, entry, c.buy_sol)
        finally:
            self.pending -= 1

    async def sell(self, p, frac, reason):
        c = PROFILES[p.profile]
        if not PAPER:
            amount = "100%" if frac >= 1 else f"{round(frac * 100)}%"
            sig = await self.trade("sell", p.mint, amount, False, c)
            if not sig or not await self.confirmed(sig):
                log.error("SELL FAILED %s, will retry", p.symbol)
                p.busy = False
                return
        cur = self.last.get(p.mint, p.entry_mcap)
        r = cur / p.entry_mcap - 1
        sold = p.remaining * min(frac, 1.0)
        p.realized += sold * r
        p.remaining -= sold
        p.txs += 1
        if frac < 1:
            p.tp1_done, p.busy = True, False
            log.info("PARTIAL %s %s at %+.0f%%, %.0f%% left riding", p.symbol, reason, r * 100, p.remaining * 100)
            return
        await self.close(p, reason, cur)

    async def close(self, p, reason, exit_mcap):
        c = PROFILES[p.profile]
        pnl = p.size_sol * (p.realized - FEE_PCT) - p.txs * c.priority_fee
        self.day_pnl += pnl
        self.since_switch += pnl
        self.hist.append(pnl)
        log.info("CLOSE [%s] %s (%s) total %+.0f%%, est PnL %+.4f SOL, today %+.4f",
                 p.profile, p.symbol, reason, p.realized * 100, pnl, self.day_pnl)
        self.record(p, exit_mcap, pnl, reason)
        self.pos.pop(p.mint, None)
        self.last.pop(p.mint, None)
        await self.sub(p.mint, False)
        self.rescale()

    async def trade(self, action, mint, amount, in_sol, c):
        body = {
            "publicKey": str(self.kp.pubkey()),
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if in_sol else "false",
            "slippage": c.slippage,
            "priorityFee": c.priority_fee,
            "pool": "auto",
        }
        try:
            async with self.http.post(TRADE_URL, json=body) as r:
                if r.status != 200:
                    log.warning("PumpPortal %s -> %s: %s", action, r.status, (await r.text())[:200])
                    return None
                raw = await r.read()
            from solders.transaction import VersionedTransaction
            tx = VersionedTransaction.from_bytes(raw)
            signed = VersionedTransaction(tx.message, [self.kp])
            encoded = base64.b64encode(bytes(signed)).decode()
            return await self.rpc("sendTransaction",
                                  [encoded, {"encoding": "base64", "skipPreflight": True, "maxRetries": 3}])
        except Exception as e:
            log.warning("%s error: %s", action, e)
            return None

    async def rpc(self, method, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self.http.post(RPC_URL, json=payload) as r:
            data = await r.json(content_type=None)
        if "error" in data:
            log.warning("RPC %s error: %s", method, data["error"])
            return None
        return data.get("result")

    async def confirmed(self, sig, timeout=30):
        end = time.time() + timeout
        while time.time() < end:
            try:
                res = await self.rpc("getSignatureStatuses", [[sig]])
                st = ((res or {}).get("value") or [None])[0]
                if st:
                    if st.get("err"):
                        log.warning("Tx failed on-chain: %s", st["err"])
                        return False
                    if st.get("confirmationStatus") in ("confirmed", "finalized"):
                        return True
            except Exception:
                pass
            await asyncio.sleep(1)
        return False

    async def has_socials(self, uri):
        try:
            async with self.http.get(uri, timeout=aiohttp.ClientTimeout(total=3)) as r:
                meta = await r.json(content_type=None)
            return any(meta.get(k) for k in ("twitter", "telegram", "website"))
        except Exception:
            return False

    # ---------- history ----------
    def load_history(self):
        if not TRADES_CSV.exists():
            return
        with TRADES_CSV.open(newline="") as f:
            rows = csv.DictReader(f)
            if "profile" not in (rows.fieldnames or []):
                old = TRADES_CSV.with_name("trades_v1.csv")
                f.close()
                TRADES_CSV.rename(old)
                log.info("Old trades.csv moved to %s", old)
                return
            mode = "paper" if PAPER else "live"
            for row in rows:
                if row.get("mode") == mode:
                    try:
                        self.hist.append(float(row["est_pnl_sol"]))
                    except (KeyError, ValueError):
                        pass
        if self.hist:
            log.info("Loaded last %d %s trades for auto-scaling", len(self.hist), "paper" if PAPER else "live")

    def record(self, p, exit_mcap, pnl, reason):
        new = not TRADES_CSV.exists()
        with TRADES_CSV.open("a", newline="") as f:
            wr = csv.writer(f)
            if new:
                wr.writerow(CSV_HEADER)
            wr.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"),
                         "paper" if PAPER else "live", p.profile, p.symbol, p.mint, p.size_sol,
                         f"{p.entry_mcap:.2f}", f"{exit_mcap:.2f}", f"{p.realized * 100:.1f}",
                         f"{pnl:.5f}", int(time.time() - p.opened), reason])


if __name__ == "__main__":
    try:
        asyncio.run(Sniper().run())
    except KeyboardInterrupt:
        pass
