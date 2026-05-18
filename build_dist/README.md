# 💎 Universe Magnet — Personal Wealth Operating System

> Your complete personal finance dashboard — track money, investments, loans,
> goals, and documents — all stored privately on your own computer.
> No internet required after setup. No subscriptions. No data sharing.

---

## 🔑 First Login Credentials

```
Username :  admin
Password :  Universe
```

> After logging in, go to **App Settings → Security** to set your own
> username and password. There is also a **"?"** hint button on the login
> page if you ever forget the default credentials.

---

## 📦 What Is Inside This Folder

When you extract the zip you will see:

```
UniverseMagnet/
│
├── launch.py          ← START HERE on Windows  (double-click this)
├── launch.command     ← START HERE on macOS    (double-click this)
├── README.md          ← This guide
│
└── source/            ← App files — do NOT move, rename, or delete anything here
    ├── app.py
    ├── requirements.txt
    ├── money_tracker.db   ← Your data lives here (SQLite database)
    ├── templates/
    └── static/
```

You only ever need to interact with `launch.py` (Windows) or
`launch.command` (macOS). Everything else is automatic.

---

## 🚀 How to Start the App

### ▶ Windows Users

1. **Extract** the zip file (right-click → Extract All)
2. Open the extracted `UniverseMagnet` folder
3. **Double-click `launch.py`**
   - If nothing happens: right-click `launch.py` → **Open with → Python**
4. A black console window opens — **leave it open** while using the app
5. Your browser opens automatically at `http://localhost:9876`
6. Log in with `admin` / `Universe`

**Python not installed?**
- Download from [python.org/downloads](https://www.python.org/downloads/)
- During install, tick **"Add Python to PATH"** ← this step is important
- Then double-click `launch.py` again

---

### ▶ macOS Users

1. **Extract** the zip (double-click it)
2. Open the extracted `UniverseMagnet` folder
3. **Double-click `launch.command`**
4. A Terminal window opens — **leave it open** while using the app
5. Your browser opens automatically at `http://localhost:9876`
6. Log in with `admin` / `Universe`

**Mac says "cannot be opened because developer cannot be verified"?**
- Right-click `launch.command` → click **Open** → click **Open** in the popup
- You only need to do this once — Mac remembers your choice

**Python not installed on Mac?**
- The launcher will tell you and open the download page automatically
- Install Python 3 from [python.org](https://www.python.org/downloads/) and try again

---

## ⏱ First Launch Takes ~2 Minutes — That Is Normal

The very first time you start the app, the launcher:

1. Creates a private Python environment on your computer (like a sandbox)
2. Downloads and installs the required packages (Flask, pandas, etc.)
3. Starts the app and opens your browser

This only happens **once**. Every launch after the first one starts in
a few seconds.

---

## 🧭 First-Time Setup Wizard

After logging in for the first time, a **Setup Wizard** appears automatically.
It walks you through 5 quick steps — each can be skipped and filled in later:

| Step | What it asks |
|------|-------------|
| 1 | Your name and a new password |
| 2 | Your monthly income and regular expenses |
| 3 | Any active loans or EMIs |
| 4 | Your existing investments (stocks, mutual funds, gold, FD, etc.) |
| 5 | Spending and savings alert rules |

You can always revisit any section from the main dashboard after setup.

---

## 🛑 How to Stop the App

- **Close the Terminal / console window** that opened when you launched the app
- Or click inside that window and press **Ctrl + C**

The browser tab will stop working once the app is stopped. That is expected.

---

## 💾 Your Data — Private & Local

- Everything is stored in **`source/money_tracker.db`** on your own computer
- Nothing is uploaded to any server or cloud — fully offline after setup
- To **back up** your data: App Settings → Backup & Restore → Download Backup
- To **restore** a backup: App Settings → Backup & Restore → Restore

**Tip:** Copy the entire `UniverseMagnet` folder to a USB drive or cloud storage
folder as a backup. That is all you need.

---

## ❓ Common Problems & Fixes

| Problem | What to do |
|---------|-----------|
| Browser does not open automatically | Open it yourself and go to `http://localhost:9876` |
| App opens but shows blank / error | Close the console and relaunch — wait for "App is running" message |
| "Permission denied" on Mac | Right-click `launch.command` → Open → Open |
| Login credentials not working | Use `admin` and `Universe` exactly as shown (case-sensitive) |
| App crashes on startup | Open `launcher.log` (in the same folder) — it shows the error |
| Packages fail to install | Make sure you have an internet connection on first launch |
| Port already in use | Another app is using port 9876 — restart your computer and try again |
| Python version error | Install Python 3.8 or newer from python.org |

---

## 🗂 App Modules at a Glance

| Module | What it does |
|--------|-------------|
| 💰 Money Tracker | Log income, expenses, and transactions by month |
| 📊 Reports | Monthly summaries, trends, category breakdowns |
| 💹 Wealth Engine | Track investments — stocks, mutual funds, gold, FD, NPS, etc. |
| 📈 Stock Desk | Live NSE prices, GTT signals, P&L, 52-week analysis |
| 🎯 Vision Board | Set life goals across Money, Health, Career, Relationships |
| 🏦 Loans | Track EMIs, outstanding balances, repayment progress |
| 🔔 Alerts | Budget limits, savings targets, investment signals |
| 📂 Docs Wallet | Store important document references (insurance, property, etc.) |
| ⚙️ App Settings | Password, theme, backup, reset options |

---

## 🔁 Keeping the App Up to Date

When you receive a newer version of `UniverseMagnet.zip`:

1. **Back up your data first** — App Settings → Backup & Restore → Download Backup
2. Extract the new zip alongside the old folder (do not overwrite yet)
3. Start the new version and use **Restore** to bring your data back
4. Once confirmed working, you can delete the old folder

---

*Universe Magnet — Built by Krishna Mishra*
*All data stays on your device. Always.*
