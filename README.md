# pump-sniper

Standalone pump.fun sniper with medium and high risk profiles plus auto-scaling. Runs on the same droplet as solana-ai-bot with its own folder, Python environment, service, user and wallet.

## Files
- `sniper.py`: the bot
- `requirements.txt`: Python packages
- `env.example`: settings template (copied to `.env` on the droplet; keys only ever live there)
- `pump-sniper.service`: keeps the bot running 24/7

## Install on the droplet
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
sudo cp pump-sniper.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pump-sniper
journalctl -u pump-sniper -f
```

## Update after changing files on GitHub
```bash
cd /opt/pump-sniper && sudo git pull && sudo chown -R sniper: . && sudo systemctl restart pump-sniper
```

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

With `AUTO_SCALE=true`, after 20 closed trades it moves to high when win rate is 50%+ and net profitable, and back to medium when win rate drops under 40%, net PnL turns negative, or high loses 0.3 SOL. Paper results never scale up live trading.

## Known limits
- PnL is estimated from market cap, not actual fills. Check the wallet for real results.
- Open positions aren't saved across restarts. After a restart, check the wallet for leftover tokens.
