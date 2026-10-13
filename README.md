# pump-sniper

Live pump.fun sniper with medium and high risk profiles, auto-scaling, and Jito bundle racing. Runs on the same droplet as solana-ai-bot with its own folder, Python environment, service, user and wallet.

## Files
- `sniper.py`: the bot
- `requirements.txt`: Python packages
- `env.example`: settings template, copied to `.env` on the droplet (keys only ever live there)
- `pump-sniper.service`: keeps the bot running 24/7

## Funding
- USDC: none. The bot only trades SOL.
- 0.5 SOL: medium only (set `AUTO_SCALE=false`)
- 1.5 SOL: room to scale up to high
- Each trade costs about 0.002-0.003 SOL in tips and priority fees.

## Install and go live (DigitalOcean droplet console)
```bash
sudo apt update && sudo apt install -y git python3-venv
sudo git clone https://github.com/TrenchTrades/pump-sniper.git /opt/pump-sniper
cd /opt/pump-sniper
sudo python3 -m venv venv
sudo venv/bin/pip install -r requirements.txt
sudo cp env.example .env
sudo useradd -r -s /usr/sbin/nologin sniper
sudo chown -R sniper: /opt/pump-sniper
sudo chmod 600 .env
sudo nano .env            # fill in RPC_URL and PRIVATE_KEY, save: Ctrl+O, Enter, Ctrl+X
sudo cp pump-sniper.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pump-sniper
journalctl -u pump-sniper -f
```
Live startup shows: `LIVE wallet ...`, `Wallet balance: ...`, `Jito bundles on via ...`, `Connected to PumpPortal`. Each trade prints a Solscan link.

## Everyday commands
| What | Command |
|---|---|
| Live logs | `journalctl -u pump-sniper -f` |
| Status | `systemctl status pump-sniper` |
| Stop / start | `sudo systemctl stop pump-sniper` / `start` |
| Trades | `column -s, -t /opt/pump-sniper/trades.csv \| tail -20` |
| Update code | `cd /opt/pump-sniper && sudo git pull && sudo chown -R sniper: . && sudo systemctl restart pump-sniper` |

## Risk profiles
| | Medium (default) | High |
|---|---|---|
| Size per trade | 0.05 SOL | 0.10 SOL |
| Max open trades | 3 | 5 |
| Dev holding allowed | up to 8% | up to 15% |
| Watch before buying | 8 s | 4 s |
| Non-dev buyers needed | 6 | 3 |
| Socials required | yes | no |
| Stop loss | -20% | -35% |
| First take profit | +40%, sell half | +100%, sell 30% |
| Trailing stop on the rest | 20% below peak | 30% below peak |
| Daily loss cap | 0.3 SOL | 0.75 SOL |

With `AUTO_SCALE=true`, after 20 closed trades it moves to high when win rate is 50%+ and net profitable, and back to medium when win rate drops under 40%, net PnL turns negative, or high loses 0.3 SOL.

## Copy-trading mode
Set `STRATEGY=copy` and list wallets in `COPY_WALLETS` (comma-separated). Needs `PUMPPORTAL_API_KEY`.
- Buys `COPY_BUY_SOL` when a tracked wallet buys at least `COPY_MIN_LEADER_SOL` on the pump.fun bonding curve
- Sells everything when that wallet sells, with stop loss, hard stop, take profit and max hold as backups
- Never buys the same token twice per run; set `COPY_MIN_WALLETS=2` to require two tracked wallets to agree
- Daily loss cap `COPY_DAILY_LOSS_SOL` uses real on-chain PnL

### Managing wallets with scout.py
```bash
cd /opt/pump-sniper && sudo venv/bin/python scout.py
```
1) Find top wallets through Birdeye, 2) check/add a wallet from GMGN or Cielo, 3) see how copying each wallet has gone, 4) remove wallets.
Wallets live in `wallets.txt`; the bot reloads it every 30 seconds. Needs `BIRDEYE_API_KEY` in `.env` for options 1 and 2.
Shortcuts: `scout.py list`, `scout.py add ADDRESS note`, `scout.py remove ADDRESS`.

## Speed settings (.env)
- `JITO=true`: each trade is raced through the RPC and a Jito bundle
- `JITO_TIP_SOL`: higher lands faster, costs more
- `SELL_URGENCY`: multiplier on sell fees and tips (default 2)
- `MEDIUM_WINDOW_SEC` / `HIGH_WINDOW_SEC`: seconds watched before buying. Lower is faster but filters less.

## Known limits
- PnL is estimated from market cap, not actual fills. Check the wallet for real results.
- Open positions aren't saved across restarts. After a restart, check the wallet for leftover tokens.
