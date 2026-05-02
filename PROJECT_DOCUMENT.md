# Universe Magnet — Project Document

**Version:** 2.4  
**Last Updated:** 2026-05-03  
**Author:** Krishna Mishra  
**Status:** Active Development

---

## 1. Project Overview

**Universe Magnet** is a personal finance and life-goals web application built for individual use. It combines financial tracking (income, expenses, investments, loans) with a holistic "life magnet" system that tracks progress across Health, Relationship, Career, and Wealth dimensions.

### Core Purpose
- Track all personal financial activity in one place
- Monitor investment portfolio across multiple asset classes
- Set and track life goals via Vision Boards and Magnet status cards
- Get live market prices for stocks, mutual funds, and gold
- Import data from Groww and Zerodha brokerage accounts

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.14 · Flask 2.x |
| Database | SQLite (`money_tracker.db`) |
| Frontend | Vanilla HTML/CSS/JavaScript (single-page app) |
| Charts | Chart.js 4.4.0 |
| Excel | SheetJS (XLSX) 0.18.5 · openpyxl (server-side) |
| Image Crop | Cropper.js 1.6.2 |
| Fonts | Google Fonts — Plus Jakarta Sans, Inter |
| Data APIs | MFAPI.in · gold-api.com · frankfurter.app · yfinance |

---

## 2. Architecture

### High-Level Architecture

```
Browser (index.html)
  │  Vanilla JS + Chart.js
  │  CSS custom properties (light/dark theme)
  │
  ▼ REST API (JSON)
Flask App (app.py)
  │  66+ routes
  │  openpyxl for Excel parsing
  │  yfinance for market data
  │
  ▼ SQLite
money_tracker.db
  │  15 tables
  │  No ORM — raw sqlite3 row_factory
```

### Single-Page Application (SPA) Pattern
The entire frontend lives in `templates/index.html`. Navigation works by toggling `.active` CSS classes on `<div id="page-*">` elements. There are no page reloads — all data fetches use the `fetch()` API.

### Hub/Tab Navigation Model

Pages are grouped into 4 hubs. Each hub shows a tab bar when active:

| Hub Constant | Pages | Sidebar Item |
|---|---|---|
| `_HUB_UNIVERSE` | universe, health, relationship, career, vision | si-universe |
| `_HUB_WCE` | wce, portfolio, nse, invtx, trading | si-wce |
| `_HUB_MONEY` | dashboard, transactions, loans | si-money |
| `_HUB_CONFIG` | alerts, import | si-config |

### Database Connection
```python
def get_db():
    conn = sqlite3.connect('money_tracker.db')
    conn.row_factory = sqlite3.Row   # dict-like access by column name
    return conn
```
Connection is opened per request and closed after — no connection pooling.

---

## 3. File & Folder Structure

```
MoneyTracker/
├── app.py                     Main Flask application (~2200 lines)
├── requirements.txt           Python dependencies
├── import_excel.py            Legacy Excel import utility
├── money_tracker.db           SQLite database (production data)
├── PROJECT_DOCUMENT.md        This file
├── SOP.md                     Standard Operating Procedures
├── UniverseMagnetPlayBook.xlsx Reference spreadsheet
│
├── templates/
│   ├── index.html             Entire frontend (~9000 lines HTML+CSS+JS)
│   ├── palette.html           Design system reference page
│   └── *.jpg                  Sample image assets
│
├── static/                    Static assets (currently empty)
│
└── SampleFiles/               Broker export samples
    ├── Mutual_Funds_Order_History_Sample-Groww.xlsx
    ├── Stocks_Order_History_Sample-Groww.xlsx
    └── tradebook-sample-zerodha.xlsx
```

---

## 4. Database Schema

### 4.1 `transactions` — Core Financial Ledger
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| type | TEXT | `income` / `expense` / `investment` |
| category | TEXT | e.g. Salary, Food, SIP |
| sub_category | TEXT | Detail (e.g. Salary Credit, Bonus) |
| amount | REAL | Transaction amount in ₹ |
| date | TEXT | YYYY-MM-DD |
| note | TEXT | Optional description |
| created_at | TEXT | Timestamp |

### 4.2 `loans` — Monthly Loan EMI Tracking
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| month | TEXT | YYYY-MM |
| loan_type | TEXT | Home Loan, Car Loan, etc. |
| amount | REAL | EMI amount for the month |

### 4.3 `loan_master` — Loan Definitions
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| loan_name | TEXT | Loan identifier |
| loan_type | TEXT | Type of loan |
| loan_amount | REAL | Original principal |
| total_repayment | REAL | Total to repay |
| start_date | TEXT | Loan start date |
| target_close_date | TEXT | Expected payoff date |
| status | TEXT | `active` / `closed` |

### 4.4 `portfolio` — Asset Holdings Summary
| Column | Type | Description |
|--------|------|-------------|
| asset | TEXT | Asset name |
| asset_type | TEXT | Equity, Gold, MF, Fixed Return, etc. |
| invested_pre_nov25 | REAL | Capital before Nov 2025 |
| value_pre_nov25 | REAL | Value as of Oct 2025 |
| invested_since_nov25 | REAL | Capital after Nov 2025 |
| current_value | REAL | Current market value |
| total_invested | REAL | Total capital deployed |
| total_return | REAL | Absolute P&L |
| return_pct | REAL | Return % |
| purpose | TEXT | Investment goal/purpose |
| asset_class | TEXT | Equity / Debt / Commodity / etc. |
| asset_category | TEXT | Large Cap, Index Fund, etc. |
| updated_at | TEXT | Last sync timestamp |

### 4.5 `assets` — Actual Holdings (My Portfolio)
Each row = one holding entry linked to an `AssetMapping` record. Asset name, type, class, and symbol come via JOIN — not stored directly in this table.

| Column | Type | Description |
|--------|------|-------------|
| AssetEntryID | INTEGER PK | Auto-increment primary key |
| MappingID | INTEGER NOT NULL FK | → `AssetMapping(MappingID)` |
| purpose | TEXT | Investment goal/purpose |
| qty | REAL | Quantity held |
| avgprice | REAL | Average cost per unit |
| ltp | REAL | Last traded price |
| investedvalue | REAL | qty × avgprice |
| currentvalue | REAL | qty × ltp |
| pnl | REAL | currentvalue − investedvalue |
| pnlpct | REAL | P&L as % of invested |
| lastsynced | TEXT | Last price sync timestamp |
| updatedat | TEXT | Record last updated |
| targetpct | REAL | Target allocation % (default 25) |

**Read queries** (all GET routes): JOIN `AssetMapping` + `InvestMapping` and return old-style aliases (`id`, `asset`, `asset_type`, `avg_price`, etc.) for frontend compatibility.

### 4.6 `invest_transactions` — Individual Trade Records
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| entry_date | TEXT | Trade date |
| stock_name | TEXT | Security name |
| asset_type | TEXT | Asset classification |
| quantity | REAL | Units traded |
| action | TEXT | `BUY` / `SELL` |
| price | REAL | Price per unit |
| invested_value | REAL | Total value |
| current_value | REAL | Current market value |
| profit | REAL | Unrealized P&L |
| profit_pct | REAL | P&L % |
| rationale | TEXT | Investment reason |
| month | TEXT | YYYY-MM (normalized) |

### 4.7 `wealth` — Wealth Goal Targets
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| purpose | TEXT UNIQUE | Goal name |
| target | REAL | Target corpus amount |
| target_date | TEXT | Goal date |

### 4.8 `alerts` — Budget Alert Rules
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| name | TEXT | Alert name |
| type | TEXT | Alert type |
| condition | TEXT | `category_exceeds` / `savings_below` / `total_below` |
| threshold | REAL | Trigger value |
| category | TEXT | Category to watch |
| period | TEXT | `monthly` (default) |
| is_active | INTEGER | 0 / 1 |

### 4.9 `nse_master` — NSE Stock Reference
| Column | Type | Description |
|--------|------|-------------|
| symbol | TEXT UNIQUE | NSE symbol |
| company_name | TEXT | Company name |
| ltp | REAL | Last traded price |
| prev_close | REAL | Previous close |
| change_pct | REAL | % daily change |
| high_52w / low_52w | REAL | 52-week range |
| from_52w_high_pct | REAL | % below 52w high |
| volume | INTEGER | Trading volume |
| sector | TEXT | Industry sector |
| category | TEXT | `Shares` default |

### 4.10 `magnet_status` — Life Goal Status History
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| magnet | TEXT | `health` / `relationship` / `career` / `vision` |
| metric_name | TEXT | e.g. "Body Weight", "Sleep Hours" |
| emoji | TEXT | Associated emoji |
| current_value | TEXT | Current reading |
| target_value | TEXT | Goal value |
| note | TEXT | Context note |
| recorded_date | TEXT | Date recorded |

### 4.11 `um_vision_cards` — Vision Board Cards
| Column | Type | Description |
|--------|------|-------------|
| id | TEXT PK | UUID |
| magnet | TEXT | Associated magnet |
| title | TEXT | Card title |
| description | TEXT | Card description |
| photo_data | TEXT | Base64 image data |

### 4.12 `monthly_investment_calc` — Monthly Aggregation Cache
| Column | Type | Description |
|--------|------|-------------|
| month | TEXT | YYYY-MM |
| symbol | TEXT | Security name |
| asset_type | TEXT | Asset classification |
| qty_bought / qty_sold / net_qty | REAL | Quantity breakdown |
| avg_buy_price | REAL | Average cost |
| total_invested | REAL | Capital deployed |
| current_price / current_value | REAL | Live values |
| unrealized_pnl / unrealized_pnl_pct | REAL | P&L |
| UNIQUE | — | (month, symbol) |

### 4.13 `raw_upload_meta` — Broker Upload Metadata
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| source_type | TEXT | e.g. `groww_mf_orders`, `zerodha_stock_trades` |
| file_name | TEXT | Original filename |
| row_count | INTEGER | Parsed row count |
| uploaded_at | TEXT | Upload timestamp |

### 4.14 `InvestMapping` — Investment Classification Reference
Static lookup table. Defines the valid AssetClass → AssetCategory → AssetType hierarchy. Seeded on `init_schema()` using `INSERT OR IGNORE`.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment integer (internal) |
| AssetID | TEXT UNIQUE | Formatted key: `Asset01`, `Asset02`, … auto-set by trigger `trg_investmapping_assetid` |
| AssetClass | TEXT | Top-level class (Commodities, Growth, Real State, Retirement, Stability) |
| AssetCategory | TEXT | Sub-class (Gold, Silver, Equity, Real Estate, Hybrid, Fixed Income) |
| AssetType | TEXT | Instrument type (ETF, Stocks, PPF, etc.) |
| UNIQUE | — | (AssetClass, AssetCategory, AssetType) |

**Seed data (15 rows):**

| AssetClass | AssetCategory | AssetType |
|---|---|---|
| Commodities | Gold | Bonds |
| Commodities | Gold | Physical Gold |
| Commodities | Gold | Digital Gold |
| Commodities | Gold | Mutual Fund |
| Commodities | Gold | ETF |
| Commodities | Silver | ETF |
| Growth | Equity | Stocks |
| Growth | Equity | Mutual Fund |
| Growth | Equity | ETF |
| Real State | Real Estate | Plot |
| Real State | Real Estate | Flat |
| Retirement | Hybrid | Pension Scheme |
| Stability | Fixed Income | PPF |
| Stability | Fixed Income | EPF |
| Stability | Fixed Income | Government Bond/Yojana |

### 4.15 `AssetMapping` — Asset Identity Lookup
Specific asset registrations (e.g. "SBI Bluechip Fund", "RELIANCE"). Each row ties one named instrument to its InvestMapping classification.

| Column | Type | Description |
|--------|------|-------------|
| MappingID | INTEGER PK AUTOINCREMENT | Surrogate primary key |
| AssetName | TEXT NOT NULL | Human-readable name (e.g. "Reliance Industries") |
| AssetSymbol | TEXT | Ticker / ISIN / fund code (for live sync) |
| AssetId | TEXT NOT NULL | FK → `InvestMapping.AssetID` (e.g. `Asset07`) |

**Role in data flow:** InvestMapping → classification → AssetMapping → identity → Assets → my holdings

### 4.16 `raw_upload_data` — Broker Raw Trade Staging Table
Normalized staging table for broker imports. Each row = one trade/holding record. `raw_data` stores the full original JSON for any additional columns.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| upload_id | INTEGER FK | → raw_upload_meta.id (CASCADE DELETE) |
| source_type | TEXT | Broker source type |
| broker | TEXT | `groww` / `zerodha` |
| instrument | TEXT | `mutual_fund` / `stocks` |
| data_type | TEXT | `holdings` / `orders` / `trades` |
| name | TEXT | Fund/stock name |
| symbol | TEXT | Market symbol |
| isin | TEXT | ISIN code |
| trade_type | TEXT | BUY / SELL / PURCHASE / REDEMPTION |
| trade_date | TEXT | Trade date |
| quantity | REAL | Units / qty |
| price | REAL | NAV / price per unit |
| amount | REAL | Total value |
| exchange | TEXT | NSE / BSE |
| order_id | TEXT | Broker order ID |
| status | TEXT | Executed / Pending / timestamp |
| raw_data | TEXT | Full original row as JSON |

---

## 5. API Reference

### Financial Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/summary` | Monthly snapshot. Params: `?month=YYYY-MM` |
| GET | `/api/monthly_trend` | 6–12 month trend data |
| GET | `/api/category_breakdown` | Category totals. Params: `?month=YYYY-MM` (empty = all-time) |

### Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/transactions` | List. Params: `?month=&type=` |
| POST | `/api/transactions` | Add new transaction |
| DELETE | `/api/transactions/<tid>` | Delete by ID |
| GET | `/api/transactions/last_amounts` | Last known amounts per category |

### Portfolio

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/portfolio` | All holdings (total_invested > 0) |
| GET | `/api/portfolio/summary` | Aggregated by asset_type |
| GET | `/api/portfolio/asset_types` | Distinct asset types |
| PATCH | `/api/portfolio/<pid>` | Update current_value |
| POST | `/api/portfolio/update` | Bulk update current_value |

### Assets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/assets` | List assets. Params: `?type=` |
| PATCH | `/api/assets/<aid>` | Update asset |
| POST | `/api/assets/rebuild` | Rebuild from invest_transactions |
| POST | `/api/assets/sync_stocks` | Sync NSE stock prices |
| POST | `/api/assets/sync_mf` | Sync mutual fund NAVs |
| POST | `/api/assets/sync_gold` | Sync gold price |

### Wealth Goals (WCE Assets)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/wt/assets` | List assets by purpose |
| POST | `/api/wt/assets` | Add asset. Body: asset, asset_type, asset_class, asset_category, symbol, qty, avg_price, ltp, purpose |
| PATCH | `/api/wt/assets/<aid>` | Update asset |
| DELETE | `/api/wt/assets/<aid>` | Delete asset |
| POST | `/api/wt/assets/csv_upload` | Bulk CSV upload |
| GET | `/api/wt/assets/sample_csv` | Download sample CSV |
| GET | `/api/wealth` | List wealth goals |
| POST | `/api/wealth` | Add goal |
| PUT | `/api/wealth/<wid>` | Update goal |
| DELETE | `/api/wealth/<wid>` | Delete goal |

### Investment Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/invest_transactions` | List. Params: `?type=&stock=` |
| GET | `/api/invest_transactions/summary` | Aggregated by type + monthly |

### Loans

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/loans/summary` | Monthly loan summary |
| GET | `/api/loan_master` | Loan master list |
| POST | `/api/loan_master` | Add loan |
| DELETE | `/api/loan_master/<lid>` | Delete loan |
| POST | `/api/loan_master/<lid>/close` | Mark closed |

### Universe Magnet (Life Goals)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/magnet_status/<magnet>` | Get status + history |
| POST | `/api/magnet_status` | Log status entry |
| DELETE | `/api/magnet_status/<sid>` | Delete entry |
| GET | `/api/um_vision/<magnet>` | Get vision cards |
| POST | `/api/um_vision` | Save vision card |
| DELETE | `/api/um_vision/<vid>` | Delete vision card |

### NSE / Live Market

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/nse/list` | All tracked NSE stocks |
| POST | `/api/nse/add` | Add stock |
| DELETE | `/api/nse/<symbol>` | Remove stock |
| POST | `/api/nse/auto_add` | Auto-add from portfolio |
| POST | `/api/nse/auto_add_etf` | Auto-add ETFs |
| POST | `/api/nse/sync` | Sync prices (yfinance) |
| GET | `/api/mf_nav` | Search MF NAV |
| GET | `/api/gold_price` | Fetch gold price |

### Alerts

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/alerts` | List alerts. Params: `?month=` |
| POST | `/api/alerts` | Add alert rule |
| DELETE | `/api/alerts/<aid>` | Delete alert |

### Help / Documentation

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/docs/sop` | Returns SOP.md as plain text |
| GET | `/api/docs/project` | Returns PROJECT_DOCUMENT.md as plain text |
| GET | `/docs/SOP.md` | Download SOP.md file |
| GET | `/docs/PROJECT_DOCUMENT.md` | Download PROJECT_DOCUMENT.md file |

### Import / Broker Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/upload_excel` | Import MoneyTracker.xlsx |
| POST | `/api/broker_upload/<source_type>` | Upload broker file |
| GET | `/api/broker_uploads` | List all upload metadata |
| GET | `/api/broker_uploads/<source_type>` | Preview rows (max 50) |
| DELETE | `/api/broker_uploads/<source_type>` | Clear source data |

**Broker `source_type` values:**

| source_type | Broker | Instrument | Data Type |
|---|---|---|---|
| `groww_mf_holdings` | Groww | Mutual Fund | Holdings |
| `groww_stock_holdings` | Groww | Stocks | Holdings |
| `groww_mf_orders` | Groww | Mutual Fund | Orders |
| `groww_stock_orders` | Groww | Stocks | Orders |
| `zerodha_stock_holdings` | Zerodha | Stocks | Holdings |
| `zerodha_stock_trades` | Zerodha | Stocks | Trades |

---

## 6. Frontend Pages

| Page ID | Hub | Description |
|---------|-----|-------------|
| `page-universe` | Universe | Home overview — magnet summaries, life-area stats |
| `page-health` | Universe | Health magnet — vision cards, status, trends |
| `page-relationship` | Universe | Relationship magnet |
| `page-career` | Universe | Career magnet |
| `page-vision` | Universe | Money magnet — financial score cards, vision board |
| `page-dashboard` | Money Tracker | Monthly summary, charts, income sources, deductions |
| `page-transactions` | Money Tracker | Transaction ledger with add/delete |
| `page-loans` | Money Tracker | Loan master + EMI tracking |
| `page-wce` | Wealth Engine | Wealth Creation Engine overview |
| `page-portfolio` | Wealth Engine | Portfolio holdings, donut chart, by-type breakdown |
| `page-nse` | Wealth Engine | Live market — stocks, MF, gold, ETF sync |
| `page-invtx` | Wealth Engine | Investment transactions with monthly aggregation |
| `page-trading` | Wealth Engine | Trading strategy tracker, NSE watchlist |
| `page-alerts` | Config | Budget alert rules |
| `page-import` | Config | Excel import + broker data upload (6 sources) |
| `page-help`   | Config | In-app Help Docs — renders SOP.md + PROJECT_DOCUMENT.md with search |

---

## 7. Key JavaScript State Variables

| Variable | Purpose |
|----------|---------|
| `currentPage` | Active page name |
| `dashMonthValue` | Selected month for Dashboard (YYYY-MM) |
| `txMonthValue` | Selected month for Transactions |
| `importMode` | `replace` or `append` for Excel import |
| `selectedFile` | File selected for Excel import |
| `trendChart` | Chart.js instance for trend chart |
| `donutChart` | Chart.js instance for donut chart |
| `portfolioDonut` | Chart.js instance for portfolio donut |
| `returnBar` | Chart.js instance for return bar |
| `_wtGoals` | Cached wealth goals array |
| `_biStatus` | Cached broker upload status per source_type |
| `_umVisions` | Cached vision cards for Universe Home |
| `_umStatuses` | Cached status metrics for Universe Home |
| `_trendData` | Cached monthly trend data |

---

## 8. Theme System

All colors are CSS custom properties defined in `:root` (light) and `[data-theme=dark]`. Key variables:

```css
--bg, --surface, --surface2, --surface3, --border
--text, --muted, --muted2
--income-p, --expense-p, --invest-p, --savings-p, --gold-p, --teal-p, --loan-p
--sb-bg, --sb-text  (sidebar)
--chart-grid, --chart-tick, --chart-bg
```

Theme is persisted in `localStorage` under key `theme`. Toggle via `toggleTheme()`.

---

## 9. localStorage Keys

| Key | Purpose |
|-----|---------|
| `theme` | `light` / `dark` |
| `vb_config` | Vision Board configuration (targets, allocation %) |
| `mt_salary_deductions` | EPF, NPS, and extra deductions `{epf, nps, others[]}` |
| `wce_circle_style` | WCE circle gradient/image setting |

---

## 10. Broker File Format Reference

### Groww MF Order History (`.xlsx`)
- Sheet: `Transactions`
- Personal details header block (rows 0–9), then data
- Header row: `Scheme Name, Transaction Type, Units, NAV, Amount, Date`
- Data starts after two blank rows below header

### Groww Stock Order History (`.xlsx`)
- Sheet: `Sheet1`
- Client info block (rows 0–4)
- Header row: `Stock name, Symbol, ISIN, Type, Quantity, Value, Exchange, Exchange Order Id, Execution date and time, Order status`
- Data starts immediately after header

### Zerodha Trade Book (`.xlsx`)
- Sheet: `Equity`
- Advisory/client info block (rows 0–13)
- Header row: `(blank), Symbol, ISIN, Trade Date, Exchange, Segment, Series, Trade Type, Auction, Quantity, Price, Trade ID, Order ID, Order Execution Time`
- Data starts immediately after header
- Column[0] is always blank (skip)

---

## 11. Financial Score Calculations

### Financial Health Score (Vision Board)
Six equally-weighted components (each max ~16.67 points):
1. Income Sources (actual vs target count)
2. Investment % (actual vs target min %)
3. Expense ratio (actual vs max %)
4. Loan burden (EMI as % of income)
5. Emergency fund (months covered vs target)
6. Charity % (actual vs target min %)

### Investment Discipline Score
Four equally-weighted components (each max 25 points):
1. Equity allocation vs target %
2. Gold allocation vs target %
3. Fixed Return allocation vs target %
4. Retirement allocation vs target %

---

## 12. Change Log

| Date | Version | Change Summary |
|------|---------|----------------|
| 2026-05-03 | 2.3 | Dashboard defaults to current month on load; Salary Deduction rows enhanced with Type (Investment/Loan/Choice Pay Expense) + Linked dropdown (active loans / InvestMapping AssetTypes / expense categories); submitTransaction() routes each deduction type to correct transaction record (Loan→Loan EMI expense, Choice Pay→expense with linked category, Investment→investment with linked AssetType); Money Tracker tabs reordered (Dashboard→Loans→Transactions) and all hub-tab inactive buttons now solid color + white text; Loans page new EMI card: Expected vs Paid this month with progress bar, deficit %, Next-to-Close loan; /api/expense_categories and /api/loans/emi_month Flask endpoints added |
| 2026-05-03 | 2.5 | WCE category cards redesigned: INV SHARE + RET SHARE converted to SVG circle rings; all 4 rings (ALLOC, RETURN, INV SHARE, RET SHARE) arranged in 2×2 grid on card right side; INVESTED/CURRENT stacked vertically with 18px font; unique solid card background per asset class (cardBg in WCE_CLASS_META); card min-width increased to 300px; ring track stroke changed to rgba for compatibility with colored card backgrounds |
| 2026-05-03 | 2.4 | WCE Total Portfolio circle: inner invested-amount circle added (`.wce-inv-circle`); size proportional to `totalInv/totalCur` ratio (clamp 0.4–0.99) set via CSS custom property `--inv-ratio`; shows profit gap visually — larger gap = bigger difference between outer current-value circle and inner invested circle; all text labels set to `z-index:1` to render above inner circle; Loans page: `#loanTopCards` overrides `cards-grid` with `repeat(4,1fr)` so all 4 summary cards fill complete row width |
| 2026-05-03 | 2.3 | Money Tracker: dashboard calendar defaults to current month; salary deduction redesigned as card system (ded-cards-grid, type badges, edit/delete per card, inline add form); auto-migration from legacy `mt_salary_deductions` to `mt_salary_deductions_v2`; deduction routing in `submitTransaction()` (Investment→invest, Loan→Loan EMI expense, Choice Pay→linked category expense); Loans/Transactions tabs swapped; hub tab buttons solid-fill inactive state; Loans page EMI card (expected vs paid, deficit%, next-to-close info); `/api/expense_categories` + `/api/loans/emi_month` endpoints added |
| 2026-05-03 | 2.2 | WCE Overview: Investment Allocation Targets panel (📊 Targets toggle button); per-AssetClass % inputs fetched dynamically from _wceCats; real-time sum badge with ≤100% validation; Save/Reset persisted to localStorage `mt_alloc_targets`; one-time migration from legacy `mt_vb_config` keys; loadWCE() now reads `getAllocTargets()` instead of hardcoded VB config; WCE category cards redesigned with dual SVG progress rings (Alloc % vs target + Return % of investment); removed noisy desc/target text; cleaner 2-stat grid + return+count row |
| 2026-05-02 | 2.1 | Fixed FK deadlock: added UNIQUE INDEX on InvestMapping.AssetID; DELETE /api/invest_mapping now cascades assets → AssetMapping → portfolio → InvestMapping; frontend warns how many assets will be removed before confirm; portfolio recalculated after delete |
| 2026-05-02 | 2.0 | Fetch Config modal rebuilt as full-panel: ⛶ maximize/restore, ✕ close top-right, sticky table header, add-row form with auto-complete datalists, delete row with asset-count guard; POST + DELETE /api/invest_mapping endpoints; .modal-header/.modal-close CSS applied globally to all modals |
| 2026-05-02 | 1.9 | InvestMapping enriched with PriceFetchMode (EQUITY/MF/COMMODITY_GOLD/COMMODITY_SILVER/RATE_BASED/MANUAL), Symbol, WeightGrams, Purity, InterestRate columns; api_wt_sync fully rewritten to route by mode; COMMODITY_SILVER support via XAG/USD; RATE_BASED calculates PPF/EPF/SSY/NPS accrual; /api/invest_mapping PATCH + /api/market/prices + /api/wt/last_sync endpoints added; WCE Overview gets "⚡ Sync All LTP" button + ⚙️ Fetch Config modal with inline editing |
| 2026-05-02 | 1.8 | Portfolio table redesigned: new schema (AssetID PK → FK InvestMapping, InvestedValue, CurrentValue, ReturnValue, Purpose, ReturnPCT, UpdateAt); all values derived via JOIN from assets→AssetMapping→InvestMapping; new /api/portfolio/summary returns by_class + by_type; WCE_CLASS_META replaces hardcoded WCE_CATS; _wceCats built dynamically from DB; donut chart groups by AssetClass; drilldown filters by asset_class; _populatePortAllocCard now fully dynamic (no hardcoded class names); openWtAssign + wtAssignPurpose updated to use asset_id; /api/wt/assets supports asset_class filter |
| 2026-05-02 | 1.7 | Sidebar score cards added for Health, Relationship, and Career magnets; populated by loadMagnetPage() engagement score; sidebar-bottom made scrollable |
| 2026-05-02 | 1.6 | Wealth Goals → circular SVG ring cards in single row; Add Asset button in dashboard header; portfolioByType grouped by asset_class from InvestMapping; CSV format updated to invest_id+asset_name schema |
| 2026-05-02 | 1.5 | Dropped old assets table; new schema (AssetEntryID PK, MappingID FK, camelCase columns); all routes updated to JOIN AssetMapping+InvestMapping; old-style aliases preserved for frontend |
| 2026-05-02 | 1.4 | Rebuilt AssetMapping with MappingID PK; added mapping_id FK to assets; new /api/invest_mapping route; redesigned Add Asset modal |
| 2026-05-02 | 1.3 | AssetMapping v1 (superseded) |
| 2026-05-02 | 1.2 | Added AssetID column to InvestMapping (format Asset01…) with auto-set trigger |
| 2026-05-02 | 1.2 | Added InvestMapping table — 15-row reference seed (AssetClass/Category/Type hierarchy) |
| 2026-05-02 | 1.1 | Added Help Docs page (Config hub) — renders SOP.md + PROJECT_DOCUMENT.md in-app with search |
| 2026-05-02 | 1.0 | Initial project document created |
| 2026-05-02 | 1.0 | Added `asset_class`, `asset_category` columns to assets + portfolio tables |
| 2026-05-02 | 1.0 | Added broker data import (raw_upload_meta, raw_upload_data) with 6 source types |
| 2026-05-02 | 1.0 | Added Broker Import UI section under Config → Import |
| 2026-05-02 | 1.0 | Fixed Dashboard salary deductions context bug (`'dash'` context added) |
| 2026-05-02 | 1.0 | Added standalone `_loadDashWidgets()` — income sources load without visiting Money tab |
| 2026-05-02 | 1.0 | Added `_populatePortAllocCard()` — allocation card loads from Portfolio page directly |
