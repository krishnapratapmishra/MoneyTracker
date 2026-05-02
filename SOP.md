# Universe Magnet — Standard Operating Procedures (SOP)

**Version:** 1.0  
**Last Updated:** 2026-05-02  
**Author:** Krishna Mishra

---

## Table of Contents
1. [Daily Operations](#1-daily-operations)
2. [Starting & Stopping the App](#2-starting--stopping-the-app)
3. [Adding Financial Data](#3-adding-financial-data)
4. [Portfolio & Investment Management](#4-portfolio--investment-management)
5. [Importing Data from Excel](#5-importing-data-from-excel)
6. [Importing Broker Data (Groww / Zerodha)](#6-importing-broker-data-groww--zerodha)
7. [Live Market Sync](#7-live-market-sync)
8. [Universe Magnet — Life Goals](#8-universe-magnet--life-goals)
9. [Alerts & Budget Rules](#9-alerts--budget-rules)
10. [Backup & Recovery](#10-backup--recovery)
11. [Development Workflow](#11-development-workflow)
12. [Adding a New Feature — Checklist](#12-adding-a-new-feature--checklist)
13. [Adding a New DB Column or Table](#13-adding-a-new-db-column-or-table)
14. [Adding a New API Route](#14-adding-a-new-api-route)
15. [Adding a New Frontend Page](#15-adding-a-new-frontend-page)
16. [Updating This Document](#16-updating-this-document)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Daily Operations

### Recommended Daily Workflow
1. Open app → **Money Tracker → Dashboard** — review current month summary
2. Add any new transactions (income, expenses, investments) via ＋ Add button
3. If market open day → sync live prices: **Wealth Engine → Live Market → Sync All**
4. Review alerts if budget limits are approaching

### Monthly Workflow (1st of each month)
1. Record salary entry as income (type: `income`, category: `Salary`)
2. Record EPF/NPS deductions (auto-added if configured in Salary Deductions)
3. Log SIP/investment transactions
4. Update loan EMI entry for the month
5. Navigate to **Money → Vision** — review Financial Health Score
6. Update Universe Magnet status cards for Health, Relationship, Career

---

## 2. Starting & Stopping the App

### Start
```bash
# Windows — double-click or run:
start.bat

# Or manually:
cd C:\Users\DevOps\OneDrive\MyProject\MoneyTracker
python app.py
```
App runs at: **http://127.0.0.1:5000**

### Stop
Press `Ctrl+C` in the terminal window running the app.

### First-Time Setup
```bash
pip install flask openpyxl pandas yfinance
python app.py
```
Database (`money_tracker.db`) is auto-created on first run via `init_schema()`.

---

## 3. Adding Financial Data

### Adding a Transaction
1. Navigate to any page — click **＋ Add** button (top-right)
2. Select tab: Income / Expense / Investment / Loan
3. Fill in: Category, Sub-category, Amount, Date, Note (optional)
4. Click **Save**

### Transaction Types & Categories

| Type | Common Categories |
|------|------------------|
| `income` | Salary, Freelance, Rental, Interest, Dividend |
| `expense` | Food, Transport, Utilities, Healthcare, Shopping, Entertainment |
| `investment` | SIP, Stocks, Gold, ETF, PPF, NPS |
| `loan` | Home Loan EMI, Car Loan EMI, Personal Loan |

### Salary with Auto-Deductions
1. Go to **Dashboard** → expand **Salary Deductions** section
2. Enter EPF amount and NPS amount
3. Click **＋ Add deduction** for any other deductions (name + amount)
4. Click **Save Deductions**
5. When next salary is recorded as income, EPF → Fixed Return and NPS → Retirement are auto-posted as investments

### Editing / Deleting Transactions
- Go to **Transactions** page → find the entry → click ✕ to delete
- There is no inline edit — delete and re-add if correction needed

---

## 4. Portfolio & Investment Management

### Adding an Asset (WCE / Portfolio page)
1. Navigate to **Wealth Engine → Portfolio**
2. Click **＋ Add Asset** button
3. Fill in:
   - **Asset Name**: e.g. NIFTY 50 ETF
   - **Asset Class**: Equity / Debt / Alternative / Real Estate / Cash / Commodity
   - **Asset Category**: e.g. Large Cap, Index Fund
   - **Asset Type**: Equity / Mutual Fund / ETF / Fixed Return / Retirement / Gold / Real Estate
   - **Purpose**: Link to a wealth goal
   - **Symbol**: NSE symbol for live price sync
   - **Qty / Avg Price / Current LTP**
4. Click **Save Asset**

### Updating Asset Price Manually
1. Portfolio page → find the asset card
2. Edit the current value inline → save

### Syncing Live Prices
- **Stocks**: Wealth Engine → Live Market → Shares tab → **Sync NSE**
- **Mutual Funds**: Live Market → Mutual Funds tab → **Fetch All NAV**
- **Gold**: Live Market → Gold tab → **Fetch Gold Price** → **Apply**
- **All at once**: Live Market → **Sync All Market** button

### Managing Wealth Goals
1. Wealth Engine → Overview (WCE page)
2. Use the Wealth Purpose cards to add/edit goals
3. Assign assets to goals via **Assign Purpose** button in Portfolio

### Dashboard Navigation
- The Dashboard calendar **always opens on the current month** (no manual selection needed)
- Use ‹ › arrows to navigate to prior months for historical review

### Salary Deduction Setup
1. Go to **Money Tracker → Dashboard → ⚙️ Salary Deductions** (collapsible section)
2. Set **EPF** and **NPS** amounts (always post as investment entries)
3. Click **＋ Add Deduction** for any extra deductions:
   - **Name**: label for this deduction (e.g. "HDFC Home Loan EMI", "Spotify")
   - **Type**: choose one of —
     - `Investment` → linked to an asset type from InvestMapping (posts as investment transaction)
     - `Loan` → linked to an active loan name (posts as Loan EMI expense)
     - `Choice Pay Expense` → linked to an expense category (posts as expense transaction)
   - **Linked To**: dropdown auto-populates based on selected type
   - **Amount**: deduction value per salary cycle
4. Click **💾 Save Deductions**
5. Next time you add a **Salary** income entry via ＋ Add Entry → Income → Salary, all configured deductions auto-post as the correct transaction types

### Setting Allocation Targets (WCE Overview)
1. Navigate to **Wealth Engine → Overview**
2. Click **📊 Targets** in the top-right action bar
3. An inline panel opens with a numeric input per asset class
4. Enter target allocation % for each class (e.g. Growth 60%, Stability 15%)
5. The **Sum** badge shows the running total — must be ≤ 100% to save
6. Click **💾 Save Targets** — targets persist in browser storage and update the allocation rings on each card immediately
7. Click **↺ Reset** to clear all targets back to 0

### Syncing Live Prices (WCE Overview)
1. Click **⚡ Sync All LTP** in the top-right action bar
2. A results modal shows per-asset sync status (synced / failed / skipped)
3. Equity assets use NSE ticker (.NS via yfinance); MF uses MFAPI.in AMFI codes; Gold/Silver use XAU/XAG spot × USD/INR; Rate-based assets (PPF/FD) accrue at their interest rate

### Configuring Fetch Mode (Fetch Config modal)
1. Click **⚙️ Fetch Config** in the top-right action bar
2. The modal lists all asset types with their PriceFetchMode, Symbol, etc.
3. Edit inline → changes save to DB immediately via PATCH /api/invest_mapping
4. Use **⛶** to maximize the modal; **✕** to close

---

## 5. Importing Data from Excel

### File Format
Use the standard `MoneyTracker.xlsx` format (see sample file for structure).

### Steps
1. Navigate to **Config → Import Excel**
2. Choose **Import Mode**:
   - **Replace All**: Clears existing data, imports fresh — use for initial setup or full refresh
   - **Append**: Adds rows — use for incremental monthly updates
3. Drag & drop file or click to browse
4. Click **📤 Upload & Import**
5. Review results — rows imported per category shown

### What Gets Imported
- Income transactions
- Expense transactions
- Investment transactions
- Loan EMI records
- Portfolio holdings (pre-Nov 2025 snapshot)
- Investment transaction history

---

## 6. Importing Broker Data (Groww / Zerodha)

### Overview
Raw broker files are uploaded to a staging table (`raw_upload_data`). This data is stored for future processing — it does **not** automatically update portfolio/assets yet (future feature).

### Supported Sources

| Source | How to Export from Broker |
|--------|--------------------------|
| Groww MF Holdings | Groww App → Portfolio → Mutual Funds → Download |
| Groww Stock Holdings | Groww App → Portfolio → Stocks → Download |
| Groww MF Order History | Groww → Orders → Mutual Funds → Export |
| Groww Stock Orders | Groww → Orders → Stocks → Export |
| Zerodha Stock Holdings | Zerodha Console → Portfolio → Holdings → Download CSV |
| Zerodha Trade History | Zerodha Console → Reports → Tradebook → Download |

### Upload Steps
1. Navigate to **Config → Import Excel** → scroll to **📂 Broker Data Import**
2. Find the card for the source you want to upload
3. Click **📤 Upload File** → select the downloaded file (.xlsx or .csv)
4. Wait for the success toast showing row count
5. Click **👁 Preview** to verify data was parsed correctly
6. To replace with a newer file — just upload again (previous data is cleared automatically)

### File Parsing Notes
- **Groww MF Orders**: System auto-detects the header row by scanning for "Scheme Name"
- **Groww Stock Orders**: Header detected by "Stock name" keyword
- **Zerodha Tradebook**: Header at row 15 — blank first column is skipped automatically
- **Holdings files** (all 3): Generic parsing — all columns stored in `raw_data` JSON

### Viewing Uploaded Data (SQL)
```sql
-- All uploads summary
SELECT source_type, file_name, row_count, uploaded_at FROM raw_upload_meta;

-- Preview Zerodha trades
SELECT name, symbol, trade_type, trade_date, quantity, price
FROM raw_upload_data
WHERE source_type = 'zerodha_stock_trades'
LIMIT 20;

-- All Groww MF purchases
SELECT name, trade_type, quantity, price, amount, trade_date
FROM raw_upload_data
WHERE source_type = 'groww_mf_orders' AND trade_type = 'PURCHASE';
```

---

## 7. Live Market Sync

### Stocks (NSE)
- Uses `yfinance` library to fetch prices for symbols in `nse_master`
- Auto-add button discovers stocks from portfolio and adds to tracking
- Sync frequency: Manual (click Sync button) or on-demand

### Mutual Funds
- Uses MFAPI.in (free, no API key required)
- Fuzzy match by fund name to find scheme code
- NAV is fetched and applied to holdings quantity

### Gold
- Fetches XAU/USD from gold-api.com
- Fetches USD/INR from frankfurter.app
- Calculates ₹/gram for 24K gold

### Best Practice
- Sync prices after market close (3:30 PM IST for equity)
- MF NAV updates by 9–10 PM IST — sync after that
- Gold price is live (24/7)

---

## 8. Universe Magnet — Life Goals

### Structure
4 Magnets, each with:
- **Vision Cards** — photo + goal description (what you want to achieve)
- **Status Cards** — quantitative metrics tracked over time (e.g., "Body Weight: 75 kg → Target: 70 kg")
- **Trend Charts** — historical trend of logged status values

### Adding a Vision Card
1. Go to the magnet page (Health / Relationship / Career / Vision)
2. Click Vision tab (default)
3. Click **＋ Add Vision**
4. Fill title, description, optional photo
5. Use crop tool if needed → Save

### Logging a Status Update
1. Go to the magnet page → click **Status** tab
2. Click any status card to update it
3. Enter current value, target, and optional note
4. Click **Save** — value is logged with today's date

### Viewing Trends
1. Click **Trend** tab on any magnet page
2. Select/deselect metrics via checkboxes
3. Chart.js line charts show historical progression

### Financial Health (Vision / Money Magnet)
The Money Magnet (Vision page) auto-logs 5 key financial metrics monthly:
- Financial Health Score
- Investment Discipline Score
- Income Sources count
- Emergency Fund coverage (months)
- Loan Burden %

These are logged automatically when you visit the Money tab.

---

## 9. Alerts & Budget Rules

### Creating an Alert
1. Navigate to **Config → Alerts**
2. Click **＋ Add Alert**
3. Configure:
   - **Name**: Descriptive name (e.g., "Food Budget")
   - **Condition**: `category_exceeds` / `savings_below` / `total_below`
   - **Threshold**: Amount trigger
   - **Category**: For `category_exceeds` — which category to watch
4. Save

### Alert Types
| Condition | Trigger |
|-----------|---------|
| `category_exceeds` | A category's monthly total exceeds threshold |
| `savings_below` | Monthly savings falls below threshold |
| `total_below` | Total balance/income below threshold |

### Checking Alerts
- Alerts are evaluated for the selected month
- Active alerts with triggered conditions are highlighted
- Navigate months to check historical alert status

---

## 10. Backup & Recovery

### Database Backup
```bash
# Manual backup — run from project folder
copy money_tracker.db money_tracker_backup_%date%.db

# Or on Linux/Mac
cp money_tracker.db money_tracker_backup_$(date +%Y%m%d).db
```

### Recommended Backup Schedule
- **Daily**: Automated copy to OneDrive (already in OneDrive folder)
- **Before major imports**: Manual backup before Replace All import
- **Before schema changes**: Always backup before running migrations

### Recovery
```bash
# Replace corrupted DB with backup
copy money_tracker_backup_YYYYMMDD.db money_tracker.db
```

### Database Integrity Check
```bash
python -c "
import sqlite3
conn = sqlite3.connect('money_tracker.db')
result = conn.execute('PRAGMA integrity_check').fetchone()
print('DB integrity:', result[0])
conn.close()
"
```

---

## 11. Development Workflow

### Local Development
```bash
cd C:\Users\DevOps\OneDrive\MyProject\MoneyTracker

# Start in debug mode
python app.py
# Flask runs with debug=True — auto-reloads on app.py changes
# index.html changes require manual page refresh in browser
```

### Git Workflow
```bash
git status                    # check what changed
git add app.py templates/     # stage specific files
git commit -m "feat: describe change"
```

### Branch Strategy
Currently single-branch (`main`). All development happens on main.

### Code Style
- **Python**: PEP 8, single-line DB queries where possible, `conn.close()` after every request
- **JavaScript**: `camelCase` functions, `const`/`let` (no `var`), async/await for all fetches
- **CSS**: BEM-style class names, CSS custom properties for all colors, no hardcoded hex in HTML (use `var(--...)`)
- **HTML IDs**: kebab-case (`page-dashboard`, `val-income`)

---

## 12. Adding a New Feature — Checklist

When implementing any significant new feature:

- [ ] **DB**: Add table/columns via migration in `init_schema()` migration list (not in CREATE TABLE)
- [ ] **Backend**: Add API route(s) in `app.py`, follow existing pattern (get_db → query → conn.close → jsonify)
- [ ] **CSS**: Add styles near related existing styles in `<style>` block, use CSS variables for colors
- [ ] **HTML**: Add new page as `<div id="page-{name}" class="page">` — keep inactive by default
- [ ] **JS**: Add `if (name === '{name}') loadFunctionName();` in `showPage()` switch
- [ ] **Navigation**: Add to appropriate hub constant (`_HUB_UNIVERSE`, `_HUB_WCE`, etc.)
- [ ] **Tab Config**: If new hub tab, add to appropriate `_*_TABS` array
- [ ] **Test**: Manually test the golden path + edge cases
- [ ] **Docs**: Update `PROJECT_DOCUMENT.md` (schema, API, pages table, changelog)
- [ ] **SOP**: Update `SOP.md` if the feature changes user workflows
- [ ] **Git**: Commit with descriptive message

---

## 13. Adding a New DB Column or Table

### Adding a Column to an Existing Table
**Never modify the `CREATE TABLE` block** — the table already exists in production.

Add a migration to the list in `init_schema()`:
```python
for migration in [
    # ... existing migrations ...
    "ALTER TABLE tablename ADD COLUMN new_col TEXT DEFAULT ''",
]:
    try:
        conn.execute(migration)
        conn.commit()
    except Exception:
        pass   # Column already exists — safe to ignore
```

Apply immediately:
```bash
python -c "from app import init_schema; init_schema(); print('OK')"
```

### Adding a New Table
Add `CREATE TABLE IF NOT EXISTS ...` to the main schema string in `init_schema()`. The `IF NOT EXISTS` guard makes it safe to run multiple times.

### Updating `raw_upload_data` Parser
When adding a new broker source type:
1. Add entry to `_BROKER_PARSERS` dict in `app.py`
2. Add card definition to `BI_SOURCES` array in `index.html`
3. Test with a real sample file
4. Document in **Section 6** of this SOP and the broker file format table in `PROJECT_DOCUMENT.md`

---

## 14. Adding a New API Route

```python
@app.route('/api/my_feature', methods=['GET'])
def api_my_feature():
    conn = get_db()
    rows = conn.execute("SELECT ...").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/my_feature', methods=['POST'])
def api_my_feature_create():
    d = request.json or {}
    conn = get_db()
    conn.execute("INSERT INTO ...", (...))
    conn.commit()
    conn.close()
    return jsonify({'success': True})
```

**Rules:**
- Always call `conn.close()` — no connection pooling
- Always return `jsonify(...)` — never raw strings except for error cases
- Use `request.json or {}` to safely handle missing body
- Use parameterized queries (`?` placeholders) — never string interpolation

---

## 15. Adding a New Frontend Page

### 1. HTML — add page div (before closing `</body>` or after last page):
```html
<div id="page-myfeature" class="page">
  <div class="mod-band"> ... </div>
  <div class="header"> ... </div>
  <!-- content -->
</div>
```

### 2. JavaScript — register load function in `showPage()`:
```javascript
if (name === 'myfeature') loadMyFeature();
```

### 3. Add to hub routing (if part of existing hub):
```javascript
const _HUB_MONEY = new Set(['dashboard','transactions','loans','myfeature']);
```

### 4. Add to tab config array (if it gets a tab):
```javascript
const _MONEY_TABS = [
  // ... existing tabs ...
  { name:'myfeature', label:'🆕 My Feature', color:'#14b8a6' },
];
```

### 5. Add load function:
```javascript
async function loadMyFeature() {
  currentPage = 'myfeature';
  try {
    const data = await fetch('/api/my_feature').then(r => r.json());
    // render...
  } catch(e) { toast('Failed to load', 'error'); }
}
```

---

## 16. Updating This Document

### When to Update
Update **both** `PROJECT_DOCUMENT.md` AND `SOP.md` whenever:

| Change Type | Update Required |
|-------------|-----------------|
| New DB table or column | PROJECT_DOCUMENT.md — Schema section |
| New API route | PROJECT_DOCUMENT.md — API Reference section |
| New frontend page | PROJECT_DOCUMENT.md — Frontend Pages table |
| New broker source type | PROJECT_DOCUMENT.md — Broker File Format + API tables; SOP.md — Section 6 |
| Changed user workflow | SOP.md — relevant workflow section |
| New JS state variable | PROJECT_DOCUMENT.md — Key Variables section |
| New localStorage key | PROJECT_DOCUMENT.md — localStorage section |
| Any significant change | PROJECT_DOCUMENT.md — Changelog table |

### Changelog Format (PROJECT_DOCUMENT.md)
```markdown
| YYYY-MM-DD | version | One-line description of change |
```

### Version Numbering
- **Minor fix / content update**: keep same version (e.g., 1.0)
- **New feature**: bump minor version (e.g., 1.0 → 1.1)
- **Major architecture change**: bump major version (e.g., 1.x → 2.0)

---

## 17. Troubleshooting

### App Won't Start
```
Error: Address already in use
```
→ Another instance is running. Find and kill it:
```bash
# Windows
netstat -ano | findstr :5000
taskkill /PID <pid> /F
```

### Database Locked
```
sqlite3.OperationalError: database is locked
```
→ Another process has the DB open. Close any DB browser tools or stop the other app instance.

### Missing Module
```
ModuleNotFoundError: No module named 'openpyxl'
```
→ Install missing dependency:
```bash
pip install openpyxl flask pandas yfinance
```

### Broker File Parse Error
- **"Unknown source type"**: Check source_type string matches exactly one of the 6 keys in `_BROKER_PARSERS`
- **0 rows parsed**: The header hint text may not match — check the actual file for the column header name
- **Values showing as None**: The column name in the file may differ from what's in `col_map` — use Preview to inspect raw_data JSON

### Chart Not Showing
- Open browser DevTools → Console — look for Chart.js errors
- Common cause: `canvas` element has `display:none` parent — Chart.js can't determine size
- Fix: initialize chart after section becomes visible

### TDZ (Temporal Dead Zone) Error
```
ReferenceError: Cannot access '_HUB_UNIVERSE' before initialization
```
→ A function is calling code that references `const` variables before they are declared in script execution order. Wrap in `setTimeout(() => ..., 0)` to defer past all declarations.

### Data Not Updating After Navigation
- Check if `showPage()` calls the load function for that page
- Check if `dataset.loaded` guard is preventing re-render (trend section uses this — clear by setting `wrap.dataset.loaded = ''`)

### Salary Deductions Not Working
- Dashboard form uses context `'dash'` — buttons must call `vbAddDedExtra('dash')` and `saveSalaryDed('dash')`
- Vision Board form uses no context (defaults to `'vb'`)
- If mixing up contexts, the wrong container ID is queried and silently returns null

---

*End of SOP Document*
