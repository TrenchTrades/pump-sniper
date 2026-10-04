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
import random
import time
from collections import deque
from dataclasses import asdict, dataclass, field
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
        slippage=25, priority_fee=0.0005, window=_f("MEDIUM_WINDOW_SEC", "8"),
        tp1=0.40, tp1_sell=0.50,        # at +40%, sell half
        trail=0.20,                     # then trail the rest 20% below its peak
        stop_loss=0.20, hard_stop=_f("MEDIUM_HARD_STOP_PCT", "35") / 100, max_hold=150, max_positions=3,
        max_growth=_f("MEDIUM_MAX_GROWTH_PCT", "100") / 100,   # skip if price more than doubled while watching
        max_dev_pct=8, min_buyers=6, min_growth=0.10, require_socials=_b("MEDIUM_REQUIRE_SOCIALS", "true"),
    ),
    "high": SimpleNamespace(
        buy_sol=_f("HIGH_BUY_SOL", "0.10"),
        daily_loss=_f("HIGH_DAILY_LOSS_SOL", "0.75"),
        slippage=35, priority_fee=0.001, window=_f("HIGH_WINDOW_SEC", "4"),
        tp1=1.00, tp1_sell=0.30,        # at +100%, sell 30%
        trail=0.30,                     # let the rest run with a 30% trailing stop
        stop_loss=0.35, hard_stop=_f("HIGH_HARD_STOP_PCT", "50") / 100, max_hold=300, max_positions=5,
        max_growth=_f("HIGH_MAX_GROWTH_PCT", "200") / 100,
        max_dev_pct=15, min_buyers=3, min_growth=0.05, require_socials=_b("HIGH_REQUIRE_SOCIALS", "false"),
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

# ---------- speed ----------
JITO = _b("JITO", "true")                      # also send every trade as a Jito bundle
JITO_URL = os.getenv("JITO_URL", "https://ny.mainnet.block-engine.jito.wtf").rstrip("/")
JITO_TIP = _f("JITO_TIP_SOL", "0.0003")       # tip on buys; sells use tip x SELL_URGENCY
SELL_URGENCY = _f("SELL_URGENCY", "2")        # multiplies priority fee and tip on sells
JITO_TIP_FALLBACK = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh", "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
]

IPFS_GATEWAYS = ["https://ipfs.io/ipfs/", "https://dweb.link/ipfs/", "https://gateway.pinata.cloud/ipfs/"]

STOP_CONFIRM_SEC = _f("STOP_CONFIRM_SEC", "1")   # price must stay below the stop this long

MAX_WATCH = _i("MAX_WATCH", "25")
FEE_PCT = _f("EST_ROUND_TRIP_FEE_PCT", "3") / 100

PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "").strip()
WS_URL = "wss://pumpportal.fun/api/data" + (f"?api-key={PUMPPORTAL_API_KEY}" if PUMPPORTAL_API_KEY else "")
TRADE_URL = "https://pumpportal.fun/api/trade-local"
SUPPLY = 1_000_000_000
TRADES_CSV = Path("trades.csv")
STATE_FILE = Path("state.json")   # open positions + daily PnL, so restarts are safe
CSV_HEADER = ["closed_utc", "mode", "profile", "symbol", "mint", "size_sol", "entry_mcap_sol",
              "exit_mcap_sol", "return_pct", "est_pnl_sol", "held_sec", "reason",
              "launch_to_buy_sec", "buy_sec", "sell_sec", "entry_slip_pct", "exit_slip_pct", "real_pnl_sol"]

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
    launch_to_buy: float = 0.0  # seconds from token launch to our buy landing
    buy_sec: float = 0.0        # seconds from buy decision to confirmed on-chain
    entry_slip: float = 0.0     # price move between buy decision and landing
    sell_sec: float = 0.0
    exit_slip: float = 0.0
    below_since: float = 0.0    # when price first dropped below the stop loss
    sigs: list = field(default_factory=list)  # on-chain buy/sell signatures


class Sniper:
    def __init__(self):
        self.watch: dict[str, Watch] = {}
        self.pos: dict[str, Position] = {}
        self.last: dict[str, float] = {}
        self.pending = 0
        self.stats = {"new": 0, "trades": 0, "buys": 0, "buy_secs": [], "sell_secs": []}
        self.last_beat = time.time()
        self.other_logged = 0
        self.dry_beats = 0
        self.trade_logged = False
        self.ws = None
        self.http = None
        self.day, self.day_pnl, self.halt_logged = date.today(), 0.0, False
        self.profile = START_PROFILE
        self.hist = deque(maxlen=SCALE_WINDOW)
        self.since_switch = 0.0
        self.kp = None
        self.tip_accounts = []
        self.start_balance = None
        if not PAPER:
            if not (RPC_URL and PRIVATE_KEY):
                raise SystemExit("LIVE mode needs RPC_URL and PRIVATE_KEY in .env")
            from solders.keypair import Keypair
            self.kp = Keypair.from_base58_string(PRIVATE_KEY)
        self.load_history()
        self.rescale()
        self.load_state()

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
            if not PAPER:
                await self.preflight()
            self._ticker = asyncio.create_task(self.ticker())
            while True:
                try:
                    await self.stream()
                except Exception as e:
                    log.warning("Stream dropped (%s), reconnecting", e)
                self.ws = None
                await asyncio.sleep(2)

    async def preflight(self):
        bal = await self.rpc("getBalance", [str(self.kp.pubkey())])
        sol = ((bal or {}).get("value") or 0) / 1e9
        need = max(p.buy_sol * p.max_positions for p in PROFILES.values()) + 0.05
        log.info("Wallet balance: %.4f SOL", sol)
        self.start_balance = sol
        if sol < self.cfg.buy_sol + 0.01:
            raise SystemExit(f"Balance {sol:.4f} SOL is too low to trade. Fund the wallet first.")
        if sol < need:
            log.warning("Balance is below the %.2f SOL needed for max positions; the bot will run but may skip buys.", need)
        if JITO:
            try:
                res = await self.jito("/api/v1/getTipAccounts", "getTipAccounts", [])
                self.tip_accounts = res or JITO_TIP_FALLBACK
            except Exception:
                self.tip_accounts = JITO_TIP_FALLBACK
            log.info("Jito bundles on via %s (%d tip accounts)", JITO_URL, len(self.tip_accounts))

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
                    self.stats["new"] += 1
                    await self.on_create(msg)
                elif t in ("buy", "sell"):
                    self.stats["trades"] += 1
                    if not self.trade_logged:
                        log.info("Trade feed working, first trade: %s", str(msg)[:200])
                        self.trade_logged = True
                    await self.on_trade(msg)
                elif "api key" in str(msg).lower() or "error" in str(msg).lower():
                    log.error("PumpPortal problem: %s", str(msg)[:300])   # always shown
                elif self.other_logged < 15:
                    # Subscription replies and errors from PumpPortal, kept for troubleshooting
                    log.info("PumpPortal says: %s", str(msg)[:300])
                    self.other_logged += 1

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
        if self.cfg.require_socials:
            ok, why = await self.has_socials(uri)
            if not ok:
                await self.drop(w.mint, f"no socials ({why})")
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
            await asyncio.sleep(0.25)
            c, now = self.cfg, time.time()
            if now - self.last_beat >= 60:
                bs, ss = self.stats["buy_secs"], self.stats["sell_secs"]
                speed = ""
                if bs or ss:
                    speed = " | avg buy %s, avg sell %s" % (f"{sum(bs)/len(bs):.2f}s" if bs else "-",
                                                         f"{sum(ss)/len(ss):.2f}s" if ss else "-")
                wallet = ""
                if not PAPER and self.start_balance is not None:
                    try:
                        bal = await self.rpc("getBalance", [str(self.kp.pubkey())])
                        sol = ((bal or {}).get("value") or 0) / 1e9
                        wallet = f" | wallet {sol:.4f} SOL ({sol - self.start_balance:+.4f} since start)"
                    except Exception:
                        pass
                log.info("STATUS [%s] last 60s: %d new tokens, %d trades seen, %d buys | watching %d, open %d, today %+.4f SOL%s%s",
                         self.profile, self.stats["new"], self.stats["trades"], self.stats["buys"],
                         len(self.watch), len(self.pos), self.day_pnl, speed, wallet)
                # Watchdog: new tokens arriving but no trade data means the trade feed broke
                if self.stats["new"] > 0 and self.stats["trades"] == 0:
                    self.dry_beats += 1
                    log.warning("No trade data for %d min. Check the PumpPortal API key wallet has 0.02+ SOL.", self.dry_beats)
                    if self.dry_beats % 3 == 0 and self.ws:
                        log.warning("Reconnecting to PumpPortal to recover the trade feed")
                        try:
                            await self.ws.close()
                        except Exception:
                            pass
                else:
                    self.dry_beats = 0
                self.stats = {"new": 0, "trades": 0, "buys": 0, "buy_secs": [], "sell_secs": []}
                self.last_beat = now
            for w in list(self.watch.values()):
                if w.dev_sold:
                    await self.drop(w.mint, "dev sold")
                    continue
                if not w.vetted or now - w.created < c.window:
                    continue
                mcap = self.last.get(w.mint, w.start_mcap)
                growth = mcap / w.start_mcap - 1 if w.start_mcap else 0
                if growth > c.max_growth:
                    await self.drop(w.mint, f"skip: ran up too fast ({growth:+.0%})")
                elif len(w.buyers) >= c.min_buyers and growth >= c.min_growth and self.can_open():
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
        now = time.time()
        if r > -c.stop_loss:
            p.below_since = 0.0             # price recovered, cancel any pending stop
        if not p.tp1_done and r >= c.tp1:
            frac, reason = c.tp1_sell, f"take profit 1 ({c.tp1_sell:.0%})"
        elif r <= -c.hard_stop:
            frac, reason = 1.0, "hard stop"  # real crash: sell immediately
        elif r <= -c.stop_loss:
            if not p.below_since:
                p.below_since = now
            elif now - p.below_since >= STOP_CONFIRM_SEC:
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
        self.save_state()

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
            t0 = time.time()
            decided = self.last.get(w.mint, w.start_mcap)
            t_sent = t0
            if not PAPER:
                sig = await self.trade("buy", w.mint, c.buy_sol, True, c)
                t_sent = time.time()
                if not sig or not await self.confirmed(sig):
                    log.warning("Buy failed for %s after %.2fs", w.symbol, time.time() - t0)
                    await self.sub(w.mint, False)
                    self.last.pop(w.mint, None)
                    return
                log.info("Buy landed: https://solscan.io/tx/%s", sig)
            now = time.time()
            entry = self.last.get(w.mint, w.start_mcap)
            slip = entry / decided - 1 if decided else 0.0
            self.stats["buys"] += 1
            self.stats["buy_secs"].append(now - t0)
            self.pos[w.mint] = Position(w.mint, w.symbol, w.dev, prof, c.buy_sol, entry, now, peak=entry,
                                        launch_to_buy=now - w.created, buy_sec=now - t0, entry_slip=slip)
            if not PAPER:
                self.pos[w.mint].sigs.append(sig)
            log.info("BUY [%s] %s at mcap %.1f SOL (%.3f SOL)", prof, w.symbol, entry, c.buy_sol)
            self.save_state()
            log.info("SPEED buy %s: build+send %.2fs, confirm %.2fs, total %.2fs | price moved %+.1f%% while buying | %.1fs after launch",
                     w.symbol, t_sent - t0, now - t_sent, now - t0, slip * 100, now - w.created)
        finally:
            self.pending -= 1

    async def sell(self, p, frac, reason):
        c = PROFILES[p.profile]
        t0 = time.time()
        trigger = self.last.get(p.mint, p.entry_mcap)
        t_sent = t0
        if not PAPER:
            amount = "100%" if frac >= 1 else f"{round(frac * 100)}%"
            sig = await self.trade("sell", p.mint, amount, False, c, SELL_URGENCY)
            t_sent = time.time()
            if not sig or not await self.confirmed(sig):
                log.error("SELL FAILED %s after %.2fs, will retry", p.symbol, time.time() - t0)
                p.busy = False
                return
            log.info("Sell landed: https://solscan.io/tx/%s", sig)
            p.sigs.append(sig)
        cur = self.last.get(p.mint, p.entry_mcap)
        now = time.time()
        p.sell_sec = now - t0
        p.exit_slip = cur / trigger - 1 if trigger else 0.0
        self.stats["sell_secs"].append(p.sell_sec)
        log.info("SPEED sell %s (%s): build+send %.2fs, confirm %.2fs, total %.2fs | price moved %+.1f%% while selling",
                 p.symbol, reason, t_sent - t0, now - t_sent, p.sell_sec, p.exit_slip * 100)
        r = cur / p.entry_mcap - 1
        sold = p.remaining * min(frac, 1.0)
        p.realized += sold * r
        p.remaining -= sold
        p.txs += 1
        if frac < 1:
            p.tp1_done, p.busy = True, False
            log.info("PARTIAL %s %s at %+.0f%%, %.0f%% left riding", p.symbol, reason, r * 100, p.remaining * 100)
            self.save_state()
            return
        await self.close(p, reason, cur)

    async def close(self, p, reason, exit_mcap):
        c = PROFILES[p.profile]
        est = p.size_sol * (p.realized - FEE_PCT) - p.txs * c.priority_fee
        real = await self.real_pnl(p.sigs) if p.sigs else None
        pnl = real if real is not None else est   # risk limits and auto-scale use real PnL when available
        self.day_pnl += pnl
        self.since_switch += pnl
        self.hist.append(pnl)
        log.info("CLOSE [%s] %s (%s) total %+.0f%%, est PnL %+.4f SOL, real PnL %s, today %+.4f",
                 p.profile, p.symbol, reason, p.realized * 100, est,
                 f"{real:+.4f} SOL" if real is not None else "n/a", self.day_pnl)
        self.record(p, exit_mcap, est, reason, real)
        self.pos.pop(p.mint, None)
        self.last.pop(p.mint, None)
        await self.sub(p.mint, False)
        self.rescale()
        self.save_state()

    async def real_pnl(self, sigs):
        """Actual SOL change across this trade's buy and sell transactions, read from the chain."""
        total = 0
        for sig in sigs:
            delta = None
            for _ in range(6):
                try:
                    tx = await self.rpc("getTransaction", [sig, {"encoding": "json", "commitment": "confirmed",
                                                                 "maxSupportedTransactionVersion": 0}])
                    meta = (tx or {}).get("meta")
                    if meta:
                        delta = meta["postBalances"][0] - meta["preBalances"][0]
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)
            if delta is None:
                log.warning("Could not read tx %s for real PnL", sig[:12])
                return None
            total += delta
        return total / 1e9

    async def trade(self, action, mint, amount, in_sol, c, urgency=1.0):
        body = {
            "publicKey": str(self.kp.pubkey()),
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if in_sol else "false",
            "slippage": c.slippage,
            "priorityFee": round(c.priority_fee * urgency, 6),
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
            sig = str(signed.signatures[0])
            enc = base64.b64encode(bytes(signed)).decode()
            # Race the same signed tx through the RPC and a Jito bundle; whichever lands first wins.
            sends = [self.rpc("sendTransaction", [enc, {"encoding": "base64", "skipPreflight": True, "maxRetries": 3}])]
            if JITO and self.tip_accounts:
                tip = self.tip_tx(signed.message.recent_blockhash, JITO_TIP * urgency)
                tip_enc = base64.b64encode(bytes(tip)).decode()
                sends.append(self.jito("/api/v1/bundles", "sendBundle", [[enc, tip_enc], {"encoding": "base64"}]))
            results = await asyncio.gather(*sends, return_exceptions=True)
            if all(r is None or isinstance(r, Exception) for r in results):
                log.warning("%s: every send path failed %s", action, results)
                return None
            return sig
        except Exception as e:
            log.warning("%s error: %s", action, e)
            return None

    def tip_tx(self, blockhash, tip_sol):
        from solders.message import MessageV0
        from solders.pubkey import Pubkey
        from solders.system_program import TransferParams, transfer
        from solders.transaction import VersionedTransaction
        ix = transfer(TransferParams(from_pubkey=self.kp.pubkey(),
                                     to_pubkey=Pubkey.from_string(random.choice(self.tip_accounts)),
                                     lamports=int(tip_sol * 1e9)))
        msg = MessageV0.try_compile(self.kp.pubkey(), [ix], [], blockhash)
        return VersionedTransaction(msg, [self.kp])

    async def jito(self, path, method, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self.http.post(JITO_URL + path, json=payload) as r:
            data = await r.json(content_type=None)
        if "error" in data:
            log.debug("Jito %s error: %s", method, data["error"])
            return None
        return data.get("result")

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
            await asyncio.sleep(0.4)
        return False

    async def has_socials(self, uri):
        """Returns (ok, reason). Tries several IPFS gateways because public ones rate-limit."""
        if not uri:
            return False, "no metadata link"
        cid = uri[7:] if uri.startswith("ipfs://") else uri.split("/ipfs/", 1)[1] if "/ipfs/" in uri else None
        urls = [] if uri.startswith("ipfs://") else [uri]
        if cid:
            urls += [g + cid for g in IPFS_GATEWAYS if g + cid != uri]
        last = "unknown"
        for u in urls:
            host = u.split("/")[2]
            try:
                async with self.http.get(u, timeout=aiohttp.ClientTimeout(total=2)) as r:
                    if r.status != 200:
                        last = f"HTTP {r.status} from {host}"
                        continue
                    meta = await r.json(content_type=None)
                if any(meta.get(k) for k in ("twitter", "telegram", "website")):
                    return True, ""
                return False, "none listed"
            except Exception as e:
                last = f"{type(e).__name__} from {host}"
        return False, f"metadata fetch failed: {last}"

    # ---------- restart safety ----------
    def save_state(self):
        state = {
            "mode": "paper" if PAPER else "live",
            "day": self.day.isoformat(), "day_pnl": self.day_pnl,
            "profile": self.profile, "since_switch": self.since_switch,
            "positions": [asdict(p) for p in self.pos.values()],
        }
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(STATE_FILE)

    def load_state(self):
        if not STATE_FILE.exists():
            return
        try:
            st = json.loads(STATE_FILE.read_text())
        except ValueError:
            log.warning("state.json unreadable, starting fresh")
            return
        if st.get("mode") != ("paper" if PAPER else "live"):
            return
        if st.get("day") == date.today().isoformat():
            self.day_pnl = float(st.get("day_pnl", 0.0))
        if AUTO_SCALE and st.get("profile") in PROFILES:
            self.profile = st["profile"]
            self.since_switch = float(st.get("since_switch", 0.0))
        for d in st.get("positions", []):
            d["busy"] = False
            p = Position(**d)
            self.pos[p.mint] = p
            self.last[p.mint] = p.entry_mcap
        if self.pos:
            log.warning("Restored %d open position(s) from before restart: %s",
                        len(self.pos), ", ".join(p.symbol for p in self.pos.values()))
        log.info("Restored today's PnL %+.4f SOL, profile %s", self.day_pnl, self.profile.upper())

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
            needs_header = rows.fieldnames != CSV_HEADER
            mode = "paper" if PAPER else "live"
            for row in rows:
                if row.get("mode") == mode:
                    try:
                        self.hist.append(float(row["est_pnl_sol"]))
                    except (KeyError, ValueError):
                        pass
        if needs_header:
            lines = TRADES_CSV.read_text().splitlines()
            lines[0] = ",".join(CSV_HEADER)
            TRADES_CSV.write_text("\n".join(lines) + "\n")
            log.info("trades.csv header updated with speed columns")
        if self.hist:
            log.info("Loaded last %d %s trades for auto-scaling", len(self.hist), "paper" if PAPER else "live")

    def record(self, p, exit_mcap, pnl, reason, real=None):
        new = not TRADES_CSV.exists()
        with TRADES_CSV.open("a", newline="") as f:
            wr = csv.writer(f)
            if new:
                wr.writerow(CSV_HEADER)
            wr.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"),
                         "paper" if PAPER else "live", p.profile, p.symbol, p.mint, p.size_sol,
                         f"{p.entry_mcap:.2f}", f"{exit_mcap:.2f}", f"{p.realized * 100:.1f}",
                         f"{pnl:.5f}", int(time.time() - p.opened), reason,
                         f"{p.launch_to_buy:.1f}", f"{p.buy_sec:.2f}", f"{p.sell_sec:.2f}",
                         f"{p.entry_slip * 100:.1f}", f"{p.exit_slip * 100:.1f}",
                         f"{real:.5f}" if real is not None else ""])


if __name__ == "__main__":
    try:
        asyncio.run(Sniper().run())
    except KeyboardInterrupt:
        pass
