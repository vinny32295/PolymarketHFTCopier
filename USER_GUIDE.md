# Polymarket Martingale Bot — User Guide

> Version 1.2.0

## Table of Contents

1. [Quick Start](#quick-start)
2. [Tab Reference](#tab-reference)
   - [Dashboard](#dashboard-tab)
   - [Configuration](#configuration-tab)
   - [Martingale](#martingale-tab)
   - [Trade History](#trade-history-tab)
   - [Equity](#equity-tab)
   - [Log](#log-tab)
3. [Bottom Control Bar](#bottom-control-bar)
4. [What Is Martingale Betting?](#what-is-martingale-betting)
5. [Step-by-Step Setup](#step-by-step-setup)
6. [Telegram Notifications](#telegram-notifications)
7. [FAQ & Troubleshooting](#faq--troubleshooting)

---

## Quick Start

1. **Install Python 3.8+** from [python.org](https://python.org/downloads/) (check "Add to PATH" on Windows).
2. **Clone or extract** the bot to a folder on your Desktop.
3. **Double-click** `install_and_run.bat` (Windows) or run `./install_and_run.sh` (Mac/Linux).
4. The GUI opens. Go to the **Configuration** tab and enter your **Private Key** and **HTTP RPC URL**.
5. Go to the **Martingale** tab, click **Add** to create a strategy, configure it, and check **Enable Martingale Mode**.
6. Click **Start** at the bottom of the window.

---

## Tab Reference

### Dashboard Tab

Your live overview of account status and open positions.

| Field | Description |
|-------|-------------|
| **USDC Balance** | Your current USDC balance on the Polygon network. This is the capital available for betting. |
| **MATIC Balance** | Your MATIC (POL) balance, used to pay small gas fees on Polygon. Keep at least 1–2 MATIC. |
| **Open Positions** | Number of markets where you currently hold outcome tokens. |
| **Floating P/L** | Unrealized profit or loss across all open positions at current market prices. |
| **Session P/L** | Profit or loss from martingale bets placed during this session (since you last started the bot). |
| **Lifetime P/L** | Total accumulated profit or loss across all sessions. |
| **W/L** | Win/loss record (e.g. "12/5" means 12 wins and 5 losses). |

**Positions Table:**

| Column | Description |
|--------|-------------|
| **Direction** | Whether you hold the UP or DOWN outcome token. |
| **Market** | The market name or slug for this position. |
| **Shares** | Number of outcome tokens you hold. |
| **Avg Price** | Your average entry price per token (in USDC). |
| **Cur Price** | The current market price of the token. |
| **Cost Basis** | Total USDC spent to acquire this position. |
| **Cur Value** | What the position is worth right now at market price. |
| **Float P/L** | Unrealized profit/loss in dollars (Cur Value − Cost Basis). |
| **P/L %** | Unrealized profit/loss as a percentage. |
| **Time Held** | How long you have held this position (e.g. "2h 15m"). |

- Click **Refresh Now** to manually update balances and positions.
- The dashboard auto-refreshes every ~10 seconds while the bot is running.

---

### Configuration Tab

Set up your wallet, RPC connection, safety limits, API credentials, and notifications.

#### Connection Settings

| Field | Description |
|-------|-------------|
| **HTTP RPC URL** | The Polygon network HTTP RPC endpoint. Get a free one from [Alchemy](https://www.alchemy.com/), [Infura](https://infura.io/), or [QuickNode](https://www.quicknode.com/). Example: `https://polygon-rpc.com` |
| **WebSocket RPC URL** | Optional Polygon WebSocket endpoint (starts with `wss://`) for real-time event streaming. The bot falls back to HTTP polling if not provided. |

#### Wallet

| Field | Description |
|-------|-------------|
| **Private Key** | Your Polygon wallet private key (hex string starting with `0x`). Used to sign all transactions. **Never share this with anyone.** It is stored locally in a restricted config file on your machine. |
| **Show/Hide** | Toggles visibility of the private key field. |

#### Safety Limits

| Field | Description |
|-------|-------------|
| **Resume Threshold (USDC)** | Minimum wallet balance required before the bot resumes betting after an automatic pause. Prevents betting with too little capital. |
| **Max Loss Kill Switch (USDC)** | Emergency stop — the bot shuts down entirely after cumulative losses reach this amount. Set to `0` to disable. |

#### Mode Checkboxes

| Field | Description |
|-------|-------------|
| **Use Polymarket CLOB API** | Routes orders through the Polymarket Central Limit Order Book for faster, cheaper execution. Recommended — leave this checked. |
| **Dry Run Mode** | Simulates everything without spending real money. The bot logs what it *would* do but places no actual orders. Use this to test your configuration safely. |
| **Auto-redeem settled positions** | Automatically converts winning outcome tokens back to USDC when a market settles. Keeps your balance liquid for the next bet. |

#### CLOB API Credentials

| Field | Description |
|-------|-------------|
| **API Key** | Your Polymarket CLOB API key. Leave blank to auto-derive from your private key (recommended). |
| **API Secret** | Your CLOB API secret. Leave blank to auto-derive. |
| **API Passphrase** | Your CLOB API passphrase. Leave blank to auto-derive. |
| **Derive Credentials** | Click this button to generate all three credentials automatically from your private key. This also happens automatically on first bot start if the fields are left blank. |

#### Telegram Notifications

| Field | Description |
|-------|-------------|
| **Bot Token** | Your Telegram bot token from @BotFather. Enables mobile push notifications for wins, losses, and status changes. |
| **Chat ID** | Your personal Telegram Chat ID (a number). See [Telegram Notifications](#telegram-notifications) below for setup instructions. |
| **Test** | Sends a test message to verify your Telegram setup works. |

---

### Martingale Tab

Configure one or more independent martingale betting strategies.

#### Top Controls

| Control | Description |
|---------|-------------|
| **Enable Martingale Mode** | Master switch — turns martingale betting on or off globally. When unchecked, no martingale bets are placed regardless of strategy settings. |

#### Strategy List (Left Panel)

A list of all your configured strategies. Each entry shows the strategy name, starting bet, and market slug.

| Button | Description |
|--------|-------------|
| **Add** | Create a new strategy with default settings. |
| **Remove** | Delete the currently selected strategy. |
| **Duplicate** | Copy the selected strategy to create a new one with the same settings. |

#### Strategy Settings (Right Panel)

Edit the selected strategy's parameters. Click **Apply to Selected** after making changes.

| Field | Description |
|-------|-------------|
| **Name** | A friendly label for this strategy (e.g. "BTC 5m Up"). Shown in the strategy list, logs, and notifications. |
| **Slug Base** | The Polymarket market slug prefix (e.g. `btc-updown-5m`). The bot appends the current window timestamp to form the full market slug each round. |
| **Window (seconds)** | Duration of each betting window. For a 5-minute market, use `300`. The bot aligns bets to these windows automatically. |
| **Direction (Up/Down)** | Which side to bet on every window — `Up` or `Down`. The bot always buys this outcome. |
| **Starting Bet (USDC)** | Your initial wager in USDC. After a loss the bet doubles; after a win it resets to this amount. |
| **Max Bet (0=no limit)** | Cap on the maximum single bet in USDC. If the doubled amount would exceed this, it is clamped. Set `0` for no limit (use with caution). |
| **Max Streak (0=no limit)** | Maximum number of consecutive losses before the strategy pauses automatically. Useful as a safety valve. Set `0` for unlimited. |
| **Poll Interval (seconds)** | How often (in seconds) the bot checks prices and places bets. Lower = more responsive but more API calls. Recommended: `10`. |
| **Buy Price Min** | Only buy when the token price is at or above this value (range 0.00–1.00). Filters out unfavorable odds. |
| **Buy Price Max** | Only buy when the token price is at or below this value (range 0.00–1.00). Prevents buying when odds are too expensive. |
| **Max Entry (sec into window)** | Latest point (in seconds after window start) at which a bet can still be placed. Prevents entering too late when the outcome is nearly decided. |

#### Bottom Controls

| Control | Description |
|---------|-------------|
| **Status Line** | Shows the current martingale state: idle, running, current streak count, bet size, and last outcome. |
| **Reset All State** | Resets all strategies back to their starting bet and clears the loss streak counter. Does **not** delete strategies — only resets the running state. |

---

### Trade History Tab

Review all completed (closed) trades.

| Column | Description |
|--------|-------------|
| **Closed At** | Timestamp when the trade was settled. |
| **Market / Token** | Market name or strategy label. |
| **Shares** | Number of tokens traded. |
| **Entry** | Price at which you bought the tokens. |
| **Exit** | Settlement price (1.00 for a win, 0.00 for a loss). |
| **P&L ($)** | Profit or loss in dollars for this trade. |
| **Slip ($)** | Slippage — difference between expected and actual fill price. |
| **Result** | Outcome: WON or LOST. |
| **Reason** | Why the trade closed (e.g. "settled", "expired"). |

**Summary Line:** Shows total trades, capital deployed, capital returned, total P&L, and win rate.

| Button | Description |
|--------|-------------|
| **Refresh** | Reload trade history from the saved file. |
| **Export CSV** | Export the full trade history table to a CSV file for spreadsheet analysis. |
| **Clear History** | Delete all saved trade history (cannot be undone). |
| **Current Session Only** | Toggle to show only trades from the current session. |

---

### Equity Tab

A visual chart of your cumulative profit/loss over time.

- **Green area/line** = you are in profit.
- **Red area/line** = you are in loss.
- **Dots** on the line represent individual trades (green = winning trade, red = losing trade).
- **Hover** over any dot to see the exact timestamp and cumulative P&L at that point.
- The **zero line** marks the break-even point.

| Control | Description |
|---------|-------------|
| **Refresh** | Redraw the chart with the latest data. |
| **Current Session Only** | Toggle to show only the current session's equity curve. |
| **Time Range** | Displays the time span of the data shown on the chart. |

---

### Log Tab

A real-time scrolling log of all bot activity.

- Shows every action the bot takes: market checks, bet placements, wins, losses, errors, and status changes.
- Auto-scrolls to the latest message. You can scroll up to view history.
- Click **Clear Log** to erase the display (does not affect saved trade history).

---

## Bottom Control Bar

Always visible at the bottom of every tab.

| Control | Description |
|---------|-------------|
| **Start** | Begin running the bot with your current configuration. The bot starts monitoring markets and placing bets (if martingale is enabled). |
| **Stop** | Gracefully stop the bot. Open positions are not closed — they will settle naturally. |
| **Status Bar** | Shows the current bot state (Stopped / Running / Error) and brief status messages. |

---

## What Is Martingale Betting?

The **martingale strategy** is a betting system where you double your bet after every loss. When you eventually win, you recover all previous losses plus a profit equal to your original bet.

**Example** with a $5 starting bet:

| Round | Bet | Outcome | Running P/L |
|-------|-----|---------|-------------|
| 1 | $5 | Loss | −$5 |
| 2 | $10 | Loss | −$15 |
| 3 | $20 | Win (+$20) | +$5 |

After the win, the bet resets to $5 and the cycle begins again.

**Risks:** A long losing streak can require very large bets. Use **Max Bet** and **Max Streak** limits to protect yourself. The **Max Loss Kill Switch** on the Configuration tab provides an additional safety net.

---

## Step-by-Step Setup

### 1. Get a Polygon Wallet

You need a wallet with USDC and a small amount of MATIC on the **Polygon** network.

- Use MetaMask or any Ethereum-compatible wallet.
- Bridge USDC to Polygon from Ethereum mainnet, or buy USDC directly on Polygon via an exchange.
- Export your private key from MetaMask: Account Details → Export Private Key.

### 2. Get an RPC URL

A free RPC endpoint lets the bot communicate with the Polygon blockchain.

- Sign up at [Alchemy](https://www.alchemy.com/) (recommended) or [Infura](https://infura.io/).
- Create a new app, select "Polygon" network.
- Copy the HTTP URL (and optionally the WebSocket URL).

### 3. Configure the Bot

1. Open the bot and go to the **Configuration** tab.
2. Paste your **HTTP RPC URL** and optionally the **WebSocket RPC URL**.
3. Paste your **Private Key**.
4. Set a **Resume Threshold** (e.g. `10`) and **Max Loss Kill Switch** (e.g. `50`).
5. Leave **Use Polymarket CLOB API** checked.
6. (Optional) Enable **Dry Run Mode** first to test without real money.

### 4. Create a Martingale Strategy

1. Go to the **Martingale** tab.
2. Click **Add** to create a new strategy.
3. Fill in the fields:
   - **Name:** A label like "BTC 5m Up"
   - **Slug Base:** The market slug, e.g. `btc-updown-5m`
   - **Window:** `300` (for 5-minute markets)
   - **Direction:** `Up` or `Down`
   - **Starting Bet:** e.g. `5` (USDC)
   - **Max Bet:** e.g. `160` (limits downside)
   - **Max Streak:** e.g. `6` (pauses after 6 consecutive losses)
   - **Poll Interval:** `10`
   - **Buy Price Min / Max:** e.g. `0.40` / `0.55`
   - **Max Entry:** e.g. `60`
4. Click **Apply to Selected**.
5. Check **Enable Martingale Mode** at the top.

### 5. Start the Bot

Click **Start** at the bottom of the window. Monitor progress on the **Dashboard** and **Log** tabs.

---

## Telegram Notifications

Get mobile alerts for every bet, win, loss, and bot status change.

### Setup Steps

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` and follow the prompts to create a bot. Copy the **Bot Token**.
3. Open a chat with your new bot and send `/start`.
4. Search for **@userinfobot** on Telegram and send it any message. It will reply with your **Chat ID** (a number).
5. In the bot's **Configuration** tab, paste the **Bot Token** and **Chat ID**.
6. Click **Test** to verify — you should receive a test message on Telegram.

---

## FAQ & Troubleshooting

**Q: The bot says "Insufficient USDC balance"**
A: Make sure your wallet has enough USDC on the Polygon network (not Ethereum mainnet). The bot needs enough to cover the current bet size.

**Q: I see "API key derivation failed"**
A: Ensure your private key is correct and starts with `0x`. Click "Derive Credentials from Private Key" to regenerate the CLOB API credentials.

**Q: The bot is running but not placing bets**
A: Check that: (1) Martingale Mode is enabled, (2) you have at least one strategy configured, (3) the current market price is within your Buy Price Min/Max range, and (4) you are not in Dry Run Mode.

**Q: How do I run the bot 24/7?**
A: You can deploy it to a cloud server (e.g. a small VPS). Run `python polymarket_martingale.py --headless` for a no-GUI mode suitable for servers. A `Procfile` is included for platforms like Railway or Heroku.

**Q: What happens if my internet drops?**
A: The bot will automatically retry connections. Open positions settle on-chain regardless of whether the bot is running — you won't lose tokens.

**Q: Can I run multiple strategies at the same time?**
A: Yes. Each strategy in the Martingale tab operates independently with its own bet size, streak counter, and market slug. You can run as many as you want simultaneously.

**Q: Where is my configuration stored?**
A: In a `config.json` file in the same folder as the bot. Trade history is stored in `trade_history.json`. Both files are created automatically.
