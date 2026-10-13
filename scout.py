#!/usr/bin/env python3
"""
Wallet scout for copy mode.

  cd /opt/pump-sniper && sudo venv/bin/python scout.py          # interactive menu
  sudo venv/bin/python scout.py list                            # your wallets + how copying them went
  sudo venv/bin/python scout.py add ADDRESS [note]              # add one (e.g. found on GMGN or Cielo)
  sudo venv/bin/python scout.py remove ADDRESS

Finds candidates through Birdeye's official API (needs BIRDEYE_API_KEY in .env,
key from bds.birdeye.so). The bot reloads wallets.txt every 30 seconds, no restart needed.
"""
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
API = "https://public-api.birdeye.so"
WALLETS = Path("wallets.txt")
TRADES = Path("trades.csv")
B58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

MIN_TRADES = int(os.getenv("SCOUT_MIN_TRADES", "30"))      # too few trades = luck
MAX_TRADES = int(os.getenv("SCOUT_MAX_TRADES", "3000"))    # too many = bot/MEV you can't keep up with
MIN_WINRATE = float(os.getenv("SCOUT_MIN_WINRATE", "40"))
CHECK_TOP = int(os.getenv("SCOUT_CHECK_TOP", "25"))        # how many candidates to look up in detail
DELAY = float(os.getenv("SCOUT_DELAY_SEC", "1.2"))         # pause between Birdeye calls (free plans are rate-limited)
PAGES = int(os.getenv("SCOUT_PAGES", "3"))                 # pages of 100 top traders to scan


# ---------- Birdeye ----------
def birdeye(path, **params):
    if not KEY:
        raise RuntimeError("Add BIRDEYE_API_KEY to .env first (get a key at bds.birdeye.so)")
    url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers={"X-API-KEY": KEY, "x-chain": "solana", "accept": "application/json"})
    for attempt in range(5):
        time.sleep(DELAY)
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"  Birdeye rate limit, waiting {wait}s...")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Birdeye HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}")
    if data.get("success") is False:
        raise RuntimeError(f"Birdeye: {data.get('message')}")
    return data.get("data") or {}


def wallet_stats(addr):
    d = birdeye("/wallet/v2/pnl/summary", wallet=addr)
    s = d.get("summary", d)
    counts, pnl = s.get("counts", {}) or {}, s.get("pnl", {}) or {}
    wr = counts.get("win_rate")
    if wr is not None:
        wr = float(wr)
        wr = wr * 100 if wr <= 1 else wr
    return {
        "trades": int(counts.get("total_trade") or 0),
        "win_rate": wr,
        "realized": pnl.get("realized_profit_usd"),
        "total": pnl.get("total_usd"),
        "avg": pnl.get("avg_profit_per_trade_usd"),
        "tokens": s.get("unique_tokens"),
    }


# ---------- wallets.txt ----------
def read_list():
    out = {}
    if WALLETS.exists():
        for line in WALLETS.read_text().splitlines():
            addr, _, note = line.partition("#")
            addr = addr.strip()
            if B58.match(addr):
                out[addr] = note.strip()
    return out


def write_list(d):
    body = "# Copy-trade wallets: one address per line, notes after #\n"
    body += "".join(f"{a}  # {n}\n" if n else f"{a}\n" for a, n in d.items())
    WALLETS.write_text(body)
    os.chmod(WALLETS, 0o644)


def add(addr, note=""):
    addr = addr.strip()
    if not B58.match(addr):
        print(f"  Not a valid Solana address: {addr}")
        return False
    d = read_list()
    if addr in d:
        print(f"  Already on your list: {short(addr)}")
        return False
    d[addr] = note
    write_list(d)
    print(f"  Added {short(addr)}. The bot picks it up within 30 seconds.")
    return True


def remove(addr):
    d = read_list()
    if d.pop(addr.strip(), None) is None:
        print("  Not on your list.")
        return
    write_list(d)
    print(f"  Removed {short(addr)}.")


# ---------- how copying each wallet has gone ----------
def copy_results():
    res = {}
    if not TRADES.exists():
        return res
    with TRADES.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("profile") != "copy" or row.get("mode") != "live" or not row.get("leader"):
                continue
            try:
                pnl = float(row.get("real_pnl_sol") or row.get("est_pnl_sol") or 0)
            except ValueError:
                continue
            r = res.setdefault(row["leader"], [0, 0, 0.0])
            r[0] += 1
            r[1] += pnl > 0
            r[2] += pnl
    return res


# ---------- display helpers ----------
def short(a):
    return f"{a[:6]}..{a[-4:]}"


def usd(x):
    return "-" if x is None else f"${float(x):,.0f}"


def pct(x):
    return "-" if x is None else f"{x:.0f}%"


def ask(prompt, default=""):
    v = input(f"{prompt}{f' [{default}]' if default else ''}: ").strip()
    return v or default


# ---------- menu actions ----------
def show_list():
    d, res = read_list(), copy_results()
    if not d:
        print("\n  No wallets yet. Use option 1 or 2 to add some.")
        return []
    print(f"\n  {'#':>2}  {'Wallet':<14} {'Copied':>6} {'Wins':>5} {'Real PnL':>10}  Note")
    rows = list(d.items())
    for i, (a, note) in enumerate(rows, 1):
        n, w, p = res.get(a, [0, 0, 0.0])
        flag = "  <- losing, consider removing" if n >= 5 and p < 0 else ""
        pnl = f"{p:+.4f}" if n else "-"
        print(f"  {i:>2}  {short(a):<14} {n:>6} {w:>5} {pnl:>10}  {note}{flag}")
    return rows


def find():
    window = ask("Time window: 1W, 30d or 90d", "30d")
    print(f"\n  Getting top Solana traders by profit ({window}) from Birdeye...")
    items, cands, mine = [], [], read_list()
    for page in range(PAGES):
        batch = birdeye("/trader/gainers-losers", type=window, sort_by="PnL", sort_type="desc",
                        offset=page * 100, limit=100).get("items", [])
        items += batch
        cands += [x for x in batch if x.get("address") not in mine and (x.get("pnl") or 0) > 0
                  and MIN_TRADES <= (x.get("trade_count") or 0) <= MAX_TRADES]
        if len(batch) < 100 or len(cands) >= CHECK_TOP:
            break
    print(f"  {len(items)} traders returned, {len(cands)} pass the trade-count filter "
          f"({MIN_TRADES}-{MAX_TRADES} trades). Checking the top {min(CHECK_TOP, len(cands))} in detail...")
    rows = []
    for x in cands[:CHECK_TOP]:
        try:
            st = wallet_stats(x["address"])
        except RuntimeError as e:
            print(f"  skip {short(x['address'])}: {e}")
            continue
        if st["win_rate"] is not None and st["win_rate"] < MIN_WINRATE:
            continue
        rows.append((x, st))
    if not rows:
        print("  Nothing passed the filters. Try another window, or lower SCOUT_MIN_WINRATE in .env.")
        return
    rows.sort(key=lambda r: float(r[1]["avg"] or 0), reverse=True)
    print(f"\n  {'#':>2}  {'Wallet':<14} {window + ' PnL':>12} {'Trades':>7} {'Win rate':>9} {'Avg/trade':>10}")
    for i, (x, st) in enumerate(rows, 1):
        print(f"  {i:>2}  {short(x['address']):<14} {usd(x.get('pnl')):>12} {x.get('trade_count', 0):>7} "
              f"{pct(st['win_rate']):>9} {usd(st['avg']):>10}")
    print("\n  Review any wallet before adding: https://gmgn.ai/sol/address/<address>  or  https://solscan.io/account/<address>")
    print("  Look for pump.fun trades held minutes to hours, not seconds.")
    while True:
        pick = ask("Show full address (s 3), add (e.g. 1 4), or Enter to go back")
        if not pick:
            return
        if pick.startswith("s"):
            for n in pick[1:].split():
                if n.isdigit() and 1 <= int(n) <= len(rows):
                    print(f"  {n}: {rows[int(n) - 1][0]['address']}")
            continue
        for n in pick.replace(",", " ").split():
            if n.isdigit() and 1 <= int(n) <= len(rows):
                x, st = rows[int(n) - 1]
                add(x["address"], f"birdeye {window} {usd(x.get('pnl'))}, win {pct(st['win_rate'])}")
        return


def check():
    addr = ask("Paste a wallet address (from GMGN, Cielo, Solscan...)")
    if not B58.match(addr):
        print("  Not a valid Solana address.")
        return
    n, w, p = copy_results().get(addr, [0, 0, 0.0])
    if n:
        print(f"  Your copy results: {n} trades, {w} wins, real PnL {p:+.4f} SOL")
    try:
        st = wallet_stats(addr)
        print(f"  Birdeye: {st['trades']} trades, win rate {pct(st['win_rate'])}, realized {usd(st['realized'])}, "
              f"avg per trade {usd(st['avg'])}, {st['tokens'] or '-'} tokens")
    except RuntimeError as e:
        print(f"  Birdeye lookup failed: {e}")
    if addr in read_list():
        print("  Already on your list.")
    elif ask("Add to copy list? y/n", "n").lower().startswith("y"):
        add(addr, ask("Note (optional)"))


def remove_menu():
    rows = show_list()
    if not rows:
        return
    pick = ask("Numbers to remove (e.g. 2 5), or Enter to go back")
    for n in pick.replace(",", " ").split():
        if n.isdigit() and 1 <= int(n) <= len(rows):
            remove(rows[int(n) - 1][0])


def menu():
    actions = {"1": find, "2": check, "3": show_list, "4": remove_menu}
    while True:
        print("\n=== Copy-trade wallet scout ===")
        print("  1) Find top wallets (Birdeye)")
        print("  2) Check / add a wallet you found")
        print("  3) My wallets + copy results")
        print("  4) Remove wallets")
        print("  5) Exit")
        choice = ask("Choose", "3")
        if choice == "5" or choice.lower() in ("q", "exit"):
            return
        try:
            actions.get(choice, show_list)()
        except RuntimeError as e:
            print(f"  {e}")
        except (KeyboardInterrupt, EOFError):
            print()
            return


if __name__ == "__main__":
    args = sys.argv[1:]
    try:
        if not args:
            menu()
        elif args[0] == "list":
            show_list()
        elif args[0] == "add" and len(args) >= 2:
            add(args[1], " ".join(args[2:]))
        elif args[0] == "remove" and len(args) >= 2:
            remove(args[1])
        else:
            print(__doc__)
    except (KeyboardInterrupt, EOFError):
        print()
