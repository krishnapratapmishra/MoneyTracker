from flask import Flask, render_template, request, jsonify
import sqlite3, os, tempfile, urllib.request, urllib.parse, json as _json
from datetime import datetime
import pandas as pd
from werkzeug.utils import secure_filename

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), 'money_tracker.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_schema():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK(type IN ('income','expense','investment')),
            category TEXT NOT NULL, sub_category TEXT, amount REAL NOT NULL,
            date TEXT NOT NULL, note TEXT, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS loans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL, loan_type TEXT NOT NULL, amount REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT, asset TEXT NOT NULL, asset_type TEXT NOT NULL,
            invested_pre_nov25 REAL DEFAULT 0, value_pre_nov25 REAL DEFAULT 0,
            invested_since_nov25 REAL DEFAULT 0, current_value REAL DEFAULT 0,
            total_invested REAL DEFAULT 0, total_return REAL DEFAULT 0,
            return_pct REAL DEFAULT 0, updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS invest_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT, stock_name TEXT,
            asset_type TEXT, quantity REAL, action TEXT, price REAL,
            invested_value REAL, current_value REAL, profit REAL,
            profit_pct REAL, rationale TEXT, month TEXT
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, type TEXT NOT NULL,
            condition TEXT NOT NULL, threshold REAL NOT NULL, category TEXT,
            period TEXT DEFAULT 'monthly', is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS nse_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            company_name TEXT DEFAULT '',
            ltp REAL DEFAULT 0,
            prev_close REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            high_52w REAL DEFAULT 0,
            low_52w REAL DEFAULT 0,
            from_52w_high_pct REAL DEFAULT 0,
            volume INTEGER DEFAULT 0,
            sector TEXT DEFAULT '',
            category TEXT DEFAULT 'Shares',
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS loan_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loan_name TEXT NOT NULL,
            loan_type TEXT NOT NULL,
            loan_amount REAL NOT NULL DEFAULT 0,
            total_repayment REAL NOT NULL DEFAULT 0,
            start_date TEXT NOT NULL,
            target_close_date TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS monthly_investment_calc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT DEFAULT '',
            qty_bought REAL DEFAULT 0,
            qty_sold REAL DEFAULT 0,
            net_qty REAL DEFAULT 0,
            avg_buy_price REAL DEFAULT 0,
            total_invested REAL DEFAULT 0,
            current_price REAL DEFAULT 0,
            current_value REAL DEFAULT 0,
            unrealized_pnl REAL DEFAULT 0,
            unrealized_pnl_pct REAL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(month, symbol)
        );
        CREATE TABLE IF NOT EXISTS magnet_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            magnet TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            emoji TEXT DEFAULT '📌',
            current_value TEXT,
            target_value TEXT,
            note TEXT,
            recorded_date TEXT DEFAULT (date('now')),
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS um_vision_cards (
            id TEXT PRIMARY KEY,
            magnet TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            photo_data TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            symbol TEXT DEFAULT '',
            qty REAL DEFAULT 0,
            avg_price REAL DEFAULT 0,
            ltp REAL DEFAULT 0,
            invested_value REAL DEFAULT 0,
            current_value REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            last_synced TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS wealth (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purpose TEXT UNIQUE NOT NULL,
            target REAL NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    # Migrate: add category column to nse_master if it doesn't exist yet
    for migration in [
        "ALTER TABLE nse_master ADD COLUMN category TEXT DEFAULT 'Shares'",
        "ALTER TABLE portfolio ADD COLUMN purpose TEXT",
        "ALTER TABLE portfolio ADD COLUMN return_pct REAL DEFAULT 0",
        "ALTER TABLE portfolio ADD COLUMN updated_at TEXT",
        "ALTER TABLE assets ADD COLUMN purpose TEXT",
        "ALTER TABLE wealth ADD COLUMN target_date TEXT",
        "ALTER TABLE assets ADD COLUMN target_pct REAL DEFAULT 25",
    ]:
        try:
            conn.execute(migration)
            conn.commit()
        except Exception:
            pass

    # Migrate: replace UNIQUE(asset, asset_type) with expression index on
    # (asset, asset_type, purpose, symbol) so each holding can differ by purpose/symbol.
    idx_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_assets_key'"
    ).fetchone()
    if not idx_exists:
        try:
            conn.execute("ALTER TABLE assets RENAME TO _assets_old")
            conn.execute("""
                CREATE TABLE assets (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset        TEXT    NOT NULL,
                    asset_type   TEXT    NOT NULL,
                    purpose      TEXT    DEFAULT NULL,
                    symbol       TEXT    DEFAULT '',
                    qty          REAL    DEFAULT 0,
                    avg_price    REAL    DEFAULT 0,
                    ltp          REAL    DEFAULT 0,
                    invested_value REAL  DEFAULT 0,
                    current_value  REAL  DEFAULT 0,
                    pnl          REAL    DEFAULT 0,
                    pnl_pct      REAL    DEFAULT 0,
                    last_synced  TEXT,
                    updated_at   TEXT    DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX idx_assets_key
                ON assets(asset, asset_type, COALESCE(purpose,''), COALESCE(symbol,''))
            """)
            conn.execute("""
                INSERT OR IGNORE INTO assets
                    (id, asset, asset_type, purpose, symbol, qty, avg_price, ltp,
                     invested_value, current_value, pnl, pnl_pct, last_synced, updated_at)
                SELECT id, asset, asset_type, purpose, symbol, qty, avg_price, ltp,
                       invested_value, current_value, pnl, pnl_pct, last_synced, updated_at
                FROM _assets_old
            """)
            conn.execute("DROP TABLE _assets_old")
            conn.commit()
        except Exception:
            pass

    conn.close()

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/palette')
def palette():
    return render_template('palette.html')

@app.route('/api/summary')
def api_summary():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db(); c = conn.cursor()
    def q(sql, *p): return c.execute(sql, p).fetchone()[0]
    income     = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?",     f'{month}%')
    loan_emi   = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND category='Loan EMI' AND date LIKE ?", f'{month}%')
    expense    = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND category!='Loan EMI' AND date LIKE ?", f'{month}%')
    investment = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?", f'{month}%')
    savings    = income - expense - loan_emi - investment
    savings_rate = round(savings / income * 100, 1) if income > 0 else 0
    y, m = map(int, month.split('-'))
    pm = m - 1 if m > 1 else 12; py = y if m > 1 else y - 1
    prev = f'{py}-{pm:02d}'
    pi  = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?",     f'{prev}%')
    pl  = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND category='Loan EMI' AND date LIKE ?", f'{prev}%')
    pe  = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND category!='Loan EMI' AND date LIKE ?", f'{prev}%')
    pv  = q("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?", f'{prev}%')
    ps  = pi - pe - pl - pv
    def delta(cur, prv): return round(((cur - prv) / prv) * 100, 1) if prv != 0 else 0
    conn.close()
    return jsonify({
        'income': income, 'expense': expense, 'loan_emi': loan_emi, 'investment': investment,
        'savings': savings, 'savings_rate': savings_rate,
        'delta_income': delta(income, pi), 'delta_expense': delta(expense, pe),
        'delta_loan': delta(loan_emi, pl), 'delta_investment': delta(investment, pv),
        'delta_savings': delta(savings, ps),
    })

@app.route('/api/monthly_trend')
def api_monthly_trend():
    conn = get_db(); c = conn.cursor()
    # All expense rows (to split loan vs non-loan)
    rows = c.execute("""SELECT strftime('%Y-%m',date) as month, type, category,
                               COALESCE(SUM(amount),0) as total
                        FROM transactions GROUP BY month,type,category ORDER BY month""").fetchall()
    conn.close()
    months = sorted(set(r['month'] for r in rows))
    data = {m: {'income':0,'expense':0,'loan_emi':0,'investment':0} for m in months}
    for r in rows:
        mo = r['month']
        if r['type'] == 'income':      data[mo]['income']     += r['total']
        elif r['type'] == 'investment': data[mo]['investment'] += r['total']
        elif r['type'] == 'expense':
            if r['category'] == 'Loan EMI': data[mo]['loan_emi'] += r['total']
            else:                           data[mo]['expense']  += r['total']
    for m in months:
        data[m]['savings'] = data[m]['income'] - data[m]['expense'] - data[m]['loan_emi'] - data[m]['investment']
    return jsonify([{'month': m, **data[m]} for m in months])

@app.route('/api/category_breakdown')
def api_category_breakdown():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT type, category, COALESCE(SUM(amount),0) as total
                        FROM transactions WHERE date LIKE ?
                        GROUP BY type,category ORDER BY type, total DESC""", (f'{month}%',)).fetchall()
    # Loan EMI breakdown by sub_category (individual loan types)
    loan_rows = c.execute("""SELECT COALESCE(sub_category,'Unknown') as category,
                                    COALESCE(SUM(amount),0) as total
                             FROM transactions
                             WHERE type='expense' AND category='Loan EMI' AND date LIKE ?
                             GROUP BY sub_category ORDER BY total DESC""", (f'{month}%',)).fetchall()
    conn.close()
    result = {}
    for r in rows: result.setdefault(r['type'], []).append({'category': r['category'], 'total': r['total']})
    result['loan_emi'] = [dict(r) for r in loan_rows]
    return jsonify(result)

@app.route('/api/transactions')
def api_transactions():
    month = request.args.get('month', ''); ttype = request.args.get('type', '')
    conn = get_db(); c = conn.cursor()
    q = "SELECT * FROM transactions WHERE 1=1"; p = []
    if month: q += " AND date LIKE ?"; p.append(f'{month}%')
    if ttype: q += " AND type=?"; p.append(ttype)
    q += " ORDER BY date DESC"
    rows = c.execute(q, p).fetchall(); conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/mf_nav')
def get_mf_nav():
    """Search MFAPI.in for a mutual fund's latest NAV by name.
    Returns top 5 matches plus the best match's current NAV."""
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'error': 'No query'}), 400
    try:
        # 1. Search for matching schemes
        search_url = 'https://api.mfapi.in/mf/search?q=' + urllib.parse.quote(query)
        req = urllib.request.Request(search_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            results = _json.loads(r.read())
        if not results:
            return jsonify({'error': f'No fund found for: {query}'}), 404
        # 2. Fetch NAV for the best match
        best = results[0]
        nav_url = f'https://api.mfapi.in/mf/{best["schemeCode"]}'
        req2 = urllib.request.Request(nav_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req2, timeout=8) as r:
            nav_data = _json.loads(r.read())
        latest = nav_data['data'][0]
        return jsonify({
            'scheme_code': best['schemeCode'],
            'scheme_name': best['schemeName'],
            'nav':  float(latest['nav']),
            'date': latest['date'],
            'matches': [{'code': x['schemeCode'], 'name': x['schemeName']} for x in results[:6]],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/portfolio_units')
def get_portfolio_units():
    """Return net units held per fund/stock from invest_transactions (BUY − SELL)."""
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""
        SELECT stock_name, asset_type,
               SUM(CASE WHEN UPPER(action)='BUY' THEN quantity ELSE -quantity END) AS net_units,
               SUM(CASE WHEN UPPER(action)='BUY' THEN invested_value ELSE -invested_value END) AS net_invested
        FROM invest_transactions
        WHERE stock_name IS NOT NULL AND stock_name != ''
        GROUP BY stock_name, asset_type
        HAVING net_units > 0
        ORDER BY LOWER(asset_type), stock_name
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/gold_price')
def get_gold_price():
    """Fetch live 24K gold price in INR per gram.
    Combines XAU/USD from gold-api.com with USD/INR from frankfurter.app.
    1 troy oz = 31.1035 grams."""
    try:
        # XAU spot price in USD per troy oz
        req1 = urllib.request.Request(
            'https://api.gold-api.com/price/XAU',
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req1, timeout=6) as r:
            xau = _json.loads(r.read())
        usd_per_oz = float(xau['price'])

        # USD → INR exchange rate
        req2 = urllib.request.Request(
            'https://api.frankfurter.app/latest?from=USD&to=INR',
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req2, timeout=6) as r:
            fx = _json.loads(r.read())
        inr_per_usd = float(fx['rates']['INR'])

        inr_per_gram = round((usd_per_oz * inr_per_usd) / 31.1035)
        return jsonify({
            'price_inr_per_gram': inr_per_gram,
            'usd_per_oz': round(usd_per_oz, 2),
            'inr_per_usd': round(inr_per_usd, 2),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transactions/last_amounts')
def api_last_amounts():
    """Return the most-recently-entered amount per category for a given type.
    Excludes auto-posted deduction entries (sub_category='Choice Pay Deduction').
    Used by the Add Transaction modal to pre-fill fields on first open."""
    ttype = request.args.get('type', '')
    if not ttype:
        return jsonify({})
    conn = get_db(); c = conn.cursor()
    rows = c.execute(
        """SELECT category, amount FROM transactions
           WHERE type=? AND (sub_category IS NULL OR sub_category != 'Choice Pay Deduction')
           ORDER BY date DESC, id DESC""",
        (ttype,)
    ).fetchall()
    conn.close()
    seen = {}
    for r in rows:
        if r['category'] not in seen:
            seen[r['category']] = r['amount']
    return jsonify(seen)

def _tx_to_portfolio_asset(category, sub_category):
    """Map a transaction category/sub_category to portfolio (asset, asset_type)."""
    cat = (category or '').strip()
    sub = (sub_category or '').strip()
    mapping = {
        ('Stocks',        ''               ): ('Stocks',             'Equity'),
        ('Stocks',        'Direct Equity'  ): ('Stocks',             'Equity'),
        ('Mutual Fund',   'MF SIP'         ): ('Mutual Fund',        'Equity'),
        ('Mutual Fund',   ''               ): ('Mutual Fund',        'Equity'),
        ('Gold',          'Gold Mutual Fund'): ('Gold MF',           'Gold'),
        ('Gold',          'Gold ETF'       ): ('Gold ETF',           'Gold'),
        ('Gold',          'SGB'            ): ('SGB',                'Gold'),
        ('Gold',          ''               ): ('Gold',               'Gold'),
        ('Fixed Return',  'EPF Contribution'): ('EPF/PPF',           'Fixed Return'),
        ('Fixed Return',  'Sukanya Samriddhi'): ('Sukanya Samriddhi','Fixed Return'),
        ('Fixed Return',  'PPF'            ): ('EPF/PPF',            'Fixed Return'),
        ('Fixed Return',  ''               ): ('EPF/PPF',            'Fixed Return'),
        ('Sukanya Samriddhi', ''           ): ('Sukanya Samriddhi',  'Fixed Return'),
        ('EPF',           ''               ): ('EPF/PPF',            'Fixed Return'),
        ('Retirement',    'NPS Contribution'): ('NPS',               'Retirement'),
        ('Retirement',    ''               ): ('NPS',                'Retirement'),
        ('NPS',           ''               ): ('NPS',                'Retirement'),
        ('Real Estate',   ''               ): ('Flat',               'Real State'),
    }
    # Exact match first
    result = mapping.get((cat, sub))
    if result:
        return result
    # Category-only fallback
    result = mapping.get((cat, ''))
    if result:
        return result
    return None

@app.route('/api/transactions', methods=['POST'])
def add_transaction():
    d = request.json; conn = get_db()
    conn.execute("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
                 (d['type'], d['category'], d.get('sub_category',''), float(d['amount']), d['date'], d.get('note','')))
    conn.commit(); nid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Auto-update portfolio when an investment transaction is posted
    if d['type'] == 'investment':
        portfolio_map = _tx_to_portfolio_asset(d.get('category',''), d.get('sub_category',''))
        if portfolio_map:
            asset, asset_type = portfolio_map
            amt = float(d['amount'])
            existing = conn.execute("SELECT id FROM portfolio WHERE asset=?", (asset,)).fetchone()
            if existing:
                conn.execute("""UPDATE portfolio
                                SET total_invested = total_invested + ?,
                                    current_value  = current_value  + ?,
                                    total_return   = current_value + ? - total_invested,
                                    return_pct     = CASE WHEN (total_invested + ?) > 0
                                                     THEN (current_value + ? - (total_invested + ?)) / (total_invested + ?) * 100
                                                     ELSE 0 END,
                                    updated_at     = datetime('now')
                                WHERE asset=?""",
                             (amt, amt, amt, amt, amt, amt, amt, asset))
            else:
                conn.execute("""INSERT INTO portfolio (asset,asset_type,total_invested,current_value,total_return,return_pct)
                                VALUES (?,?,?,?,0,0)""", (asset, asset_type, amt, amt))
            conn.commit()

    conn.close()
    return jsonify({'success': True, 'id': nid})

@app.route('/api/transactions/<int:tid>', methods=['DELETE'])
def delete_transaction(tid):
    conn = get_db(); conn.execute("DELETE FROM transactions WHERE id=?", (tid,)); conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/portfolio')
def api_portfolio():
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT * FROM portfolio WHERE total_invested > 0
                        ORDER BY asset_type, current_value DESC""").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/portfolio/asset_types')
def api_portfolio_asset_types():
    """Distinct asset_type values from portfolio (drives Live Market tabs)."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT asset_type FROM portfolio WHERE asset_type IS NOT NULL ORDER BY asset_type"
    ).fetchall()
    conn.close()
    return jsonify([r['asset_type'] for r in rows])

@app.route('/api/portfolio/summary')
def api_portfolio_summary():
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT asset_type,
                               COALESCE(SUM(total_invested), 0)  AS invested,
                               COALESCE(SUM(current_value),  0)  AS current_val,
                               COALESCE(SUM(total_return),   0)  AS total_ret
                        FROM portfolio WHERE total_invested > 0
                        GROUP BY asset_type ORDER BY current_val DESC""").fetchall()
    rows = [dict(r) for r in rows]
    total_inv = sum(r['invested']    for r in rows)
    total_val = sum(r['current_val'] for r in rows)
    conn.close()
    return jsonify({
        'by_type': rows, 'total_invested': total_inv, 'total_value': total_val,
        'total_return': total_val - total_inv,
        'return_pct': round((total_val - total_inv) / total_inv * 100, 2) if total_inv else 0,
    })

@app.route('/api/portfolio/<int:pid>', methods=['PATCH'])
def patch_portfolio(pid):
    """Update current_value (and recalc total_return) for a single portfolio row by id."""
    d = request.json; conn = get_db()
    cv = float(d.get('current_value', 0))
    conn.execute("""UPDATE portfolio SET current_value=?,
                    total_return=?-total_invested,
                    updated_at=datetime('now') WHERE id=?""", (cv, cv, pid))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/portfolio/update', methods=['POST'])
def update_portfolio():
    d = request.json; conn = get_db()
    cv = float(d['current_value'])
    conn.execute("""UPDATE portfolio SET current_value=?,
                    total_return=current_value-total_invested,
                    updated_at=datetime('now') WHERE asset=?""", (cv, d['asset']))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/invest_transactions')
def api_invest_tx():
    asset_type = request.args.get('type',  '')
    stock      = request.args.get('stock', '')
    month      = request.args.get('month', '')
    conn = get_db(); c = conn.cursor()
    q = "SELECT * FROM invest_transactions WHERE 1=1"; p = []
    if asset_type: q += " AND asset_type=?"; p.append(asset_type)
    if stock:      q += " AND stock_name=?"; p.append(stock)
    if month:      q += " AND (month LIKE ? OR entry_date LIKE ?)"; p += [f'{month}%', f'{month}%']
    q += " ORDER BY entry_date DESC LIMIT 500"
    rows = c.execute(q, p).fetchall(); conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/invest_transactions/summary')
def api_invest_tx_summary():
    """Aggregate investment transactions by asset_type.
    Returns total invested per type, monthly breakdown, and distinct months list."""
    month = request.args.get('month', '')
    conn = get_db(); c = conn.cursor()
    where = "WHERE action IN ('BUY','Buy','buy')"
    params = []
    if month:
        where += " AND (month LIKE ? OR entry_date LIKE ?)"
        params += [f'{month}%', f'{month}%']
    # Per asset type totals
    by_type = c.execute(f"""
        SELECT asset_type,
               COUNT(*) AS tx_count,
               SUM(invested_value) AS total_invested,
               SUM(CASE WHEN action IN ('BUY','Buy','buy') THEN invested_value ELSE 0 END) AS inflow,
               SUM(CASE WHEN action IN ('SELL','Sell','sell') THEN invested_value ELSE 0 END) AS outflow
        FROM invest_transactions {where}
        GROUP BY asset_type ORDER BY total_invested DESC
    """, params).fetchall()
    # Monthly breakdown per type
    monthly = c.execute(f"""
        SELECT COALESCE(month, substr(entry_date,1,7)) AS mo,
               asset_type,
               SUM(invested_value) AS invested
        FROM invest_transactions {where}
        GROUP BY mo, asset_type ORDER BY mo
    """, params).fetchall()
    # All distinct months
    months = c.execute("""
        SELECT DISTINCT COALESCE(month, substr(entry_date,1,7)) AS mo
        FROM invest_transactions WHERE mo IS NOT NULL AND mo != ''
        ORDER BY mo DESC
    """).fetchall()
    conn.close()
    return jsonify({
        'by_type':  [dict(r) for r in by_type],
        'monthly':  [dict(r) for r in monthly],
        'months':   [r['mo'] for r in months if r['mo']],
    })

@app.route('/api/loans/summary')
def api_loans_summary():
    conn = get_db(); c = conn.cursor()
    latest_month = c.execute("SELECT MAX(month) FROM loans").fetchone()[0] or ''
    rows = c.execute("""
        SELECT l.loan_type,
               SUM(l.amount)                     AS total_paid,
               COALESCE(lc.amount, 0)            AS current_emi
        FROM loans l
        LEFT JOIN loans lc
               ON lc.loan_type = l.loan_type AND lc.month = ?
        GROUP BY l.loan_type
        ORDER BY COALESCE(lc.amount, 0) DESC, SUM(l.amount) DESC
    """, (latest_month,)).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    for r in result:
        r['is_active'] = r['current_emi'] > 0
    return jsonify(result)

@app.route('/api/loan_master')
def api_loan_master_list():
    conn = get_db(); c = conn.cursor()
    loans = [dict(r) for r in c.execute(
        "SELECT * FROM loan_master ORDER BY loan_type, start_date"
    ).fetchall()]
    today_dt = datetime.now()
    updated = False
    for loan in loans:
        total_paid = c.execute("""
            SELECT COALESCE(SUM(amount), 0) FROM transactions
            WHERE type='expense' AND category='Loan EMI' AND sub_category=?
        """, (loan['loan_name'],)).fetchone()[0]
        loan['total_paid'] = round(total_paid, 2)
        loan['balance'] = round(max(0.0, loan['total_repayment'] - total_paid), 2)
        loan['repayment_pct'] = round(total_paid / loan['total_repayment'] * 100, 1) if loan['total_repayment'] > 0 else 0.0
        loan['remaining_pct'] = round(100.0 - loan['repayment_pct'], 1)
        try:
            target_dt = datetime.strptime(loan['target_close_date'], '%Y-%m-%d')
        except Exception:
            target_dt = today_dt
        months_remaining = max(1, (target_dt.year - today_dt.year) * 12 + (target_dt.month - today_dt.month))
        loan['months_remaining'] = months_remaining
        loan['expected_emi'] = round(loan['balance'] / months_remaining, 0) if loan['balance'] > 0 else 0.0
        if loan['balance'] <= 0 and loan['status'] == 'active':
            c.execute("UPDATE loan_master SET status='closed' WHERE id=?", (loan['id'],))
            loan['status'] = 'closed'
            updated = True
    if updated:
        conn.commit()
    conn.close()
    return jsonify(loans)

@app.route('/api/loan_master', methods=['POST'])
def api_loan_master_add():
    d = request.json
    if not d or not d.get('loan_name'):
        return jsonify({'error': 'loan_name required'}), 400
    conn = get_db()
    conn.execute("""INSERT INTO loan_master
        (loan_name, loan_type, loan_amount, total_repayment, start_date, target_close_date, status)
        VALUES (?,?,?,?,?,?,?)""",
        (d['loan_name'].strip(), d['loan_type'], float(d.get('loan_amount', 0)),
         float(d.get('total_repayment', 0)), d['start_date'], d['target_close_date'], 'active'))
    conn.commit()
    nid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return jsonify({'success': True, 'id': nid})

@app.route('/api/loan_master/<int:lid>', methods=['DELETE'])
def api_loan_master_delete(lid):
    conn = get_db()
    conn.execute("DELETE FROM loan_master WHERE id=?", (lid,))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/loan_master/<int:lid>/close', methods=['POST'])
def api_loan_master_close(lid):
    conn = get_db()
    conn.execute("UPDATE loan_master SET status='closed' WHERE id=?", (lid,))
    conn.commit(); conn.close()
    return jsonify({'success': True})

# ── Universe Magnet: Status entries (DB-backed, date-tracked for trend charts) ──

@app.route('/api/magnet_status/<magnet>', methods=['GET'])
def get_magnet_status(magnet):
    conn = get_db(); c = conn.cursor()
    # Latest entry per metric_name for display
    latest = c.execute("""
        SELECT ms.* FROM magnet_status ms
        INNER JOIN (
            SELECT metric_name, MAX(recorded_date) as max_date
            FROM magnet_status WHERE magnet=? GROUP BY metric_name
        ) mx ON ms.metric_name=mx.metric_name AND ms.recorded_date=mx.max_date
        WHERE ms.magnet=? ORDER BY ms.metric_name
    """, (magnet, magnet)).fetchall()
    # History for all metrics (for future trend charts)
    history = c.execute("""
        SELECT * FROM magnet_status WHERE magnet=?
        ORDER BY metric_name, recorded_date
    """, (magnet,)).fetchall()
    conn.close()
    return jsonify({
        'latest':  [dict(r) for r in latest],
        'history': [dict(r) for r in history],
    })

@app.route('/api/magnet_status', methods=['POST'])
def save_magnet_status():
    d = request.json
    magnet      = d.get('magnet','').strip()
    metric_name = d.get('metric_name','').strip()
    if not magnet or not metric_name:
        return jsonify({'error': 'magnet and metric_name required'}), 400
    conn = get_db()
    conn.execute("""
        INSERT INTO magnet_status (magnet, metric_name, emoji, current_value, target_value, note, recorded_date)
        VALUES (?,?,?,?,?,?,?)
    """, (
        magnet, metric_name,
        d.get('emoji','📌'),
        d.get('current_value',''),
        d.get('target_value',''),
        d.get('note',''),
        d.get('recorded_date', datetime.now().strftime('%Y-%m-%d')),
    ))
    conn.commit()
    new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return jsonify({'success': True, 'id': new_id})

@app.route('/api/magnet_status/<int:sid>', methods=['DELETE'])
def delete_magnet_status(sid):
    conn = get_db()
    conn.execute("DELETE FROM magnet_status WHERE id=?", (sid,))
    conn.commit(); conn.close()
    return jsonify({'success': True})

# ── Universe Magnet: Vision cards (DB-backed) ────────────────────────────────

@app.route('/api/um_vision/<magnet>', methods=['GET'])
def get_um_vision(magnet):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM um_vision_cards WHERE magnet=? ORDER BY created_at", (magnet,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/um_vision', methods=['POST'])
def save_um_vision():
    d = request.json
    vid    = d.get('id')
    magnet = d.get('magnet','').strip()
    title  = d.get('title','').strip()
    if not magnet or not title:
        return jsonify({'error': 'magnet and title required'}), 400
    conn = get_db()
    if vid:
        conn.execute("""
            UPDATE um_vision_cards SET title=?, description=?, photo_data=?, updated_at=datetime('now')
            WHERE id=?
        """, (title, d.get('description',''), d.get('photo_data',''), vid))
    else:
        import uuid
        new_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO um_vision_cards (id, magnet, title, description, photo_data)
            VALUES (?,?,?,?,?)
        """, (new_id, magnet, title, d.get('description',''), d.get('photo_data','')))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/um_vision/<vid>', methods=['DELETE'])
def delete_um_vision(vid):
    conn = get_db()
    conn.execute("DELETE FROM um_vision_cards WHERE id=?", (vid,))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/alerts')
def api_alerts():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db(); c = conn.cursor()
    alerts = [dict(a) for a in c.execute("SELECT * FROM alerts WHERE is_active=1").fetchall()]
    for a in alerts:
        actual = 0
        if a['condition'] == 'category_exceeds':
            actual = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND category=? AND date LIKE ?",
                               (a['type'], a['category'], f'{month}%')).fetchone()[0]
            a['triggered'] = actual > a['threshold']
        elif a['condition'] == 'savings_below':
            inc = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?", (f'{month}%',)).fetchone()[0]
            exp = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND date LIKE ?", (f'{month}%',)).fetchone()[0]
            inv = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?", (f'{month}%',)).fetchone()[0]
            actual = inc - exp - inv; a['triggered'] = actual < a['threshold']
        elif a['condition'] == 'total_below':
            actual = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND date LIKE ?",
                               (a['type'], f'{month}%')).fetchone()[0]
            a['triggered'] = actual < a['threshold']
        a['actual'] = actual
    conn.close()
    return jsonify(alerts)

@app.route('/api/alerts', methods=['POST'])
def add_alert():
    d = request.json; conn = get_db()
    conn.execute("INSERT INTO alerts (name,type,condition,threshold,category,period) VALUES (?,?,?,?,?,?)",
                 (d['name'], d['type'], d['condition'], float(d['threshold']), d.get('category'), d.get('period','monthly')))
    conn.commit(); aid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]; conn.close()
    return jsonify({'success': True, 'id': aid})

@app.route('/api/alerts/<int:aid>', methods=['DELETE'])
def delete_alert(aid):
    conn = get_db(); conn.execute("DELETE FROM alerts WHERE id=?", (aid,)); conn.commit(); conn.close()
    return jsonify({'success': True})

# ── EXCEL IMPORT HELPERS ──────────────────────────────────────────────────────
def _xl_safe(v):
    try: return float(v) if pd.notna(v) else 0.0
    except: return 0.0

def _xl_income(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Income', header=2)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Salary Credit'].notna()].copy()
    rows = []
    cats = [
        ('Salary Credit','Salary','Salary Credit'),
        ('Incentive Bonus','Salary','Incentive / Bonus'),
        ('Rati','Income','Rati'),
        ('Family','Income','Family'),
        ('Investment Return','Investment Return','Returns'),
        ('Loan Return','Income','Loan Return'),
    ]
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        for col, cat, sub in cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('income', cat, sub, float(val), dt, 'Excel Upload'))
    conn.executemany("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

def _xl_expenses(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Expenses', header=3)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Actual  Expense'].notna() & (df['Actual  Expense'] > 0)].copy()
    cats = [
        ('Kid','Family',"Kids' Expenses"),
        ('Payments','Bills','Subscriptions & Payments'),
        ('HouseHold','Household','Household Expenses'),
        ('Transport','Transport','Transport'),
        ('Family Care','Family','Family Care'),
        ('Shopping','Shopping','Shopping'),
        ('Luxary','Lifestyle','Luxury & Entertainment'),
        ('Support','Family','Family Support'),
    ]
    rows = []
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        for col, cat, sub in cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('expense', cat, sub, float(val), dt, 'Excel Upload'))
    conn.executemany("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

def _xl_loans(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Loan', header=3)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Total Loan'].notna() & (df['Total Loan'] > 0)].copy()
    loan_types = ['Home Loan','Personal Loan','Car Loan','NBFC Loan (LIC)',
                  'EPF Loan','Education Loan','Gadaget EMI','Credit Card EMI',
                  'Family loan','Credit Purchase','Secret Partner']
    rows, tx_rows = [], []
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        for lt in loan_types:
            val = row.get(lt, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append((dt[:7], lt, float(val)))
                tx_rows.append(('expense','Loan EMI',lt,float(val),dt,'Excel Upload'))
    conn.executemany("INSERT INTO loans (month,loan_type,amount) VALUES (?,?,?)", rows)
    conn.executemany("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)", tx_rows)
    conn.commit()
    return len(rows)

def _xl_investments(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Investment', header=5)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Entry Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Total Investment'].notna()].copy()
    cats = [
        ('Stocks','Stocks','Direct Equity'),
        ('Mutual Fund','Mutual Fund','MF SIP'),
        ('Gold MF','Gold','Gold Mutual Fund'),
        ('Gold ETF','Gold','Gold ETF'),
        ('Gold','Gold','Physical Gold'),
        ('SGB','Gold','Sovereign Gold Bond'),
        ('EPF','Fixed Return','EPF Contribution'),
        ('NPS','Retirement','NPS Contribution'),
        ('Sukanya Samriddhi','Fixed Return','Sukanya Samriddhi'),
        ('Land','Real Estate','Land'),
        ('Flat','Real Estate','Flat'),
    ]
    rows = []
    for _, row in df.iterrows():
        dt = str(row['Entry Month'])[:10]
        for col, cat, sub in cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('investment', cat, sub, float(val), dt, 'Excel Upload'))
    conn.executemany("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

def _xl_portfolio(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Corpus', header=2)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Asset'].notna() & df['Type'].notna()].copy()
    df = df[~df['Asset'].astype(str).str.lower().isin(['total','nan','asset'])].copy()
    rows = []
    for _, row in df.iterrows():
        asset = str(row.get('Asset','')).strip()
        atype = str(row.get('Type','')).strip()
        if not asset or asset == 'nan': continue
        rows.append((
            asset, atype,
            _xl_safe(row.get('Invested Till Oct 25',0)),
            _xl_safe(row.get('Actual Till Oct-25',0)),
            _xl_safe(row.get('Invested Since Nov-25',0)),
            _xl_safe(row.get('Current Value',0)),
            _xl_safe(row.get('Invested',0)),
            _xl_safe(row.get('Return',0)),
            _xl_safe(row.get('Return%',0)) if str(row.get('Return%','')).replace('.','').replace('-','').isdigit() else 0.0,
        ))
    conn.executemany("""INSERT INTO portfolio
       (asset,asset_type,invested_pre_nov25,value_pre_nov25,invested_since_nov25,
        current_value,total_invested,total_return,return_pct)
       VALUES (?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    return len(rows)

def _xl_invest_tx(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Investment Transactions', header=1)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Entry Date'].apply(lambda x: hasattr(x, 'year'))].copy()
    rows = []
    for _, row in df.iterrows():
        # Normalise month to YYYY-MM
        raw_month = row.get('Month', '')
        if hasattr(raw_month, 'year'):          # pandas Timestamp
            month_str = raw_month.strftime('%Y-%m')
        else:
            s = str(raw_month).strip()
            month_str = s[:7] if len(s) >= 7 else s   # take first 7 chars → YYYY-MM
        rows.append((
            str(row['Entry Date'])[:10],
            str(row.get('Stock Name','')).strip(),
            str(row.get('Type','')).strip(),
            _xl_safe(row.get('Quantity')),
            str(row.get('Buy/Sell','')).strip(),
            _xl_safe(row.get('Price')),
            _xl_safe(row.get('Invested Value')),
            _xl_safe(row.get('Current Value')),
            _xl_safe(row.get('Profit')),
            _xl_safe(row.get('Profit %')),
            str(row.get('Rationale','')).strip(),
            month_str,
        ))
    conn.executemany("""INSERT INTO invest_transactions
       (entry_date,stock_name,asset_type,quantity,action,price,invested_value,
        current_value,profit,profit_pct,rationale,month)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    return len(rows)

def _refresh_monthly_calc(conn):
    """Recompute monthly_investment_calc from invest_transactions.
    Normalises the month field to YYYY-MM regardless of how it was stored
    (handles 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', plain 'YYYY-MM', etc.)."""
    conn.execute("DELETE FROM monthly_investment_calc")
    conn.execute("""
        INSERT INTO monthly_investment_calc
            (month, symbol, asset_type, qty_bought, qty_sold, net_qty,
             avg_buy_price, total_invested, updated_at)
        SELECT
            -- Normalise to YYYY-MM: use month col if it looks like a date, else fall back to entry_date
            substr(
                COALESCE(
                    CASE WHEN month GLOB '????-??*' THEN month ELSE NULL END,
                    CASE WHEN entry_date GLOB '????-??*' THEN entry_date ELSE NULL END,
                    month
                ), 1, 7
            ) AS norm_month,
            stock_name,
            asset_type,
            SUM(CASE WHEN UPPER(action) = 'BUY'  THEN quantity ELSE 0 END),
            SUM(CASE WHEN UPPER(action) = 'SELL' THEN quantity ELSE 0 END),
            SUM(CASE WHEN UPPER(action) = 'BUY'  THEN quantity ELSE -quantity END),
            CASE WHEN SUM(CASE WHEN UPPER(action)='BUY' THEN quantity ELSE 0 END) > 0
                 THEN SUM(CASE WHEN UPPER(action)='BUY' THEN invested_value ELSE 0 END) /
                      SUM(CASE WHEN UPPER(action)='BUY' THEN quantity ELSE 0 END)
                 ELSE 0 END,
            SUM(CASE WHEN UPPER(action) = 'BUY' THEN invested_value ELSE 0 END),
            datetime('now')
        FROM invest_transactions
        WHERE stock_name IS NOT NULL AND stock_name != ''
          AND (month IS NOT NULL OR entry_date IS NOT NULL)
        GROUP BY norm_month, stock_name, asset_type
        HAVING norm_month IS NOT NULL AND norm_month != ''
    """)
    conn.commit()

# ── ASSETS TABLE HELPERS ─────────────────────────────────────────────────────
def _get_portfolio_asset_key(asset_type):
    """Map assets.asset_type → portfolio.asset for current_value aggregation."""
    at = (asset_type or '').strip().lower()
    if at in ('stocks', 'equity', 'direct equity'):
        return 'Stocks'
    if 'mutual' in at or at == 'mf':
        return 'Mutual Fund'
    if at == 'etf':
        return 'ETF'
    if at == 'gold etf':
        return 'Gold ETF'
    if at == 'gold mf':
        return 'Gold MF'
    if at in ('physical gold', 'gold', 'gold physical'):
        return 'Gold'
    if at == 'sgb':
        return 'SGB'
    if at in ('fixed return', 'epf', 'ppf', 'epf/ppf'):
        return 'EPF/PPF'
    if at in ('nps', 'retirement'):
        return 'NPS'
    return None

def _update_portfolio_from_assets(conn):
    """Update portfolio.current_value from assets table.

    Two-pass strategy
    -----------------
    Pass 1 – SQL correlated subquery:
        Updates every portfolio row whose `asset` column directly matches an
        `assets.asset_type` value  (e.g. portfolio.asset='Mutual Fund' ← SUM
        of all assets WHERE asset_type='Mutual Fund').
        Join key:  portfolio.asset = assets.asset_type
        This is safe because each portfolio.asset name is unique, so even
        though multiple portfolio rows share the same asset_type (e.g. several
        Gold rows), only the row whose .asset literally equals the asset_type
        string gets updated.

    Pass 2 – Python mapping fallback:
        Handles asset_types whose label differs from the target portfolio bucket
        (e.g. assets.asset_type='Equity' → portfolio.asset='Shares').
        Only asset_types that have NO matching portfolio.asset row are processed
        here, so there is no double-counting with Pass 1.
    """
    # ── Pass 1: SQL – direct match: portfolio.asset = assets.asset_type ─────
    conn.execute("""
        UPDATE portfolio
        SET current_value = COALESCE((
                SELECT SUM(a.current_value) FROM assets a
                WHERE a.asset_type = portfolio.asset
            ), 0),
            total_return  = COALESCE((
                SELECT SUM(a.current_value) FROM assets a
                WHERE a.asset_type = portfolio.asset
            ), 0) - total_invested,
            return_pct    = CASE WHEN total_invested > 0
                            THEN (
                                COALESCE((
                                    SELECT SUM(a.current_value) FROM assets a
                                    WHERE a.asset_type = portfolio.asset
                                ), 0) - total_invested
                            ) / total_invested * 100
                            ELSE 0 END,
            updated_at    = datetime('now')
        WHERE EXISTS (
            SELECT 1 FROM assets a WHERE a.asset_type = portfolio.asset
        )
    """)

    # ── Pass 2: Python mapping – asset_types with no direct portfolio.asset ──
    # Only fetch types not already handled by Pass 1 (avoids redundant updates)
    rows = conn.execute("""
        SELECT asset_type, SUM(current_value) AS total_current
        FROM assets
        WHERE current_value > 0
          AND asset_type NOT IN (SELECT asset FROM portfolio)
        GROUP BY asset_type
    """).fetchall()
    for r in rows:
        portfolio_asset = _get_portfolio_asset_key(r['asset_type'])
        if not portfolio_asset:
            continue
        cv = float(r['total_current'])
        conn.execute("""
            UPDATE portfolio
            SET current_value = ?,
                total_return  = ? - total_invested,
                return_pct    = CASE WHEN total_invested > 0
                                THEN (? - total_invested) / total_invested * 100
                                ELSE 0 END,
                updated_at    = datetime('now')
            WHERE asset = ?
        """, (cv, cv, cv, portfolio_asset))
    conn.commit()

# ── ASSETS ENDPOINTS ──────────────────────────────────────────────────────────
@app.route('/api/assets')
def api_assets_list():
    asset_type = request.args.get('type', '')
    conn = get_db()
    q = "SELECT * FROM assets WHERE 1=1"; p = []
    if asset_type:
        q += " AND LOWER(asset_type) LIKE ?"; p.append(f'%{asset_type.lower()}%')
    q += " ORDER BY asset_type, asset"
    rows = conn.execute(q, p).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/assets/rebuild', methods=['POST'])
def api_assets_rebuild():
    """Rebuild assets from invest_transactions net holdings (BUY − SELL)."""
    conn = get_db()
    holdings = conn.execute("""
        SELECT stock_name AS asset, asset_type,
               SUM(CASE WHEN UPPER(action)='BUY' THEN quantity     ELSE -quantity     END) AS net_units,
               SUM(CASE WHEN UPPER(action)='BUY' THEN invested_value ELSE -invested_value END) AS net_invested
        FROM invest_transactions
        WHERE stock_name IS NOT NULL AND stock_name != ''
        GROUP BY stock_name, asset_type
        HAVING net_units > 0
        ORDER BY asset_type, stock_name
    """).fetchall()
    conn.execute("DELETE FROM assets")
    count = 0
    for h in holdings:
        qty      = float(h['net_units'])
        invested = float(h['net_invested'])
        avg_price = invested / qty if qty > 0 else 0
        conn.execute("""
            INSERT INTO assets
                (asset, asset_type, qty, avg_price, ltp, invested_value, current_value, pnl, pnl_pct, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, datetime('now'))
        """, (h['asset'], h['asset_type'], qty, avg_price, avg_price, invested, invested))
        count += 1
    conn.commit(); conn.close()
    return jsonify({'success': True, 'count': count})

@app.route('/api/assets/<int:aid>', methods=['PATCH'])
def api_assets_patch(aid):
    d = request.json; conn = get_db()
    row = conn.execute("SELECT * FROM assets WHERE id=?", (aid,)).fetchone()
    if not row:
        conn.close(); return jsonify({'error': 'Not found'}), 404
    ltp        = float(d.get('ltp',        row['ltp']       or 0))
    qty        = float(d.get('qty',        row['qty']       or 0))
    avg_price  = float(d.get('avg_price',  row['avg_price'] or 0))
    symbol     = d.get('symbol', row['symbol'] or '')
    target_pct = float(d.get('target_pct', row['target_pct'] if row['target_pct'] is not None else 25))
    invested   = qty * avg_price
    current    = qty * ltp
    pnl        = current - invested
    pnl_pct    = (pnl / invested * 100) if invested > 0 else 0
    conn.execute("""
        UPDATE assets SET ltp=?, qty=?, avg_price=?, symbol=?, target_pct=?,
            invested_value=?, current_value=?, pnl=?, pnl_pct=?,
            last_synced=datetime('now'), updated_at=datetime('now')
        WHERE id=?
    """, (ltp, qty, avg_price, symbol, target_pct, invested, current, pnl, pnl_pct, aid))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/assets/sync_stocks', methods=['POST'])
def api_assets_sync_stocks():
    """Sync LTP for Stocks and ETF assets via yfinance, then update portfolio."""
    if not YF_AVAILABLE:
        return jsonify({'error': 'yfinance not installed — run: pip install yfinance'}), 503
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM assets
        WHERE UPPER(asset_type) IN ('STOCKS','EQUITY','DIRECT EQUITY','ETF')
           OR UPPER(asset_type) LIKE '%ETF%'
           OR UPPER(asset_type) LIKE '%STOCK%'
    """).fetchall()
    if not rows:
        conn.close()
        return jsonify({'success': True, 'synced': 0, 'message': 'No stock/ETF assets found'})
    results = []
    for r in rows:
        sym = (r['symbol'] or r['asset']).strip().upper()
        try:
            ticker = yf.Ticker(sym + '.NS')
            info   = ticker.info
            ltp    = float(info.get('currentPrice') or info.get('regularMarketPrice') or 0)
            if ltp > 0:
                qty      = float(r['qty'])
                invested = float(r['invested_value'])
                current  = qty * ltp
                pnl      = current - invested
                pnl_pct  = (pnl / invested * 100) if invested > 0 else 0
                conn.execute("""
                    UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                        last_synced=datetime('now'), updated_at=datetime('now')
                    WHERE id=?
                """, (ltp, current, pnl, pnl_pct, r['id']))
                results.append({'asset': r['asset'], 'ltp': ltp, 'status': 'ok'})
            else:
                results.append({'asset': r['asset'], 'status': 'no_price'})
        except Exception as e:
            results.append({'asset': r['asset'], 'error': str(e), 'status': 'error'})
    conn.commit()
    _update_portfolio_from_assets(conn)
    conn.close()
    ok = sum(1 for r in results if r['status'] == 'ok')
    return jsonify({'success': True, 'synced': ok, 'failed': len(results) - ok, 'results': results})

@app.route('/api/assets/sync_mf', methods=['POST'])
def api_assets_sync_mf():
    """Sync NAV for Mutual Fund assets via MFAPI.in, then update portfolio."""
    import time
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM assets
        WHERE LOWER(asset_type) LIKE '%mutual%' OR LOWER(asset_type) = 'mf'
    """).fetchall()
    if not rows:
        conn.close()
        return jsonify({'success': True, 'synced': 0, 'message': 'No mutual fund assets found'})
    results = []
    for r in rows:
        try:
            scheme_code = (r['symbol'] or '').strip() or None
            nav = None
            if not scheme_code:
                search_url = 'https://api.mfapi.in/mf/search?q=' + urllib.parse.quote(r['asset'])
                req = urllib.request.Request(search_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    matches = _json.loads(resp.read())
                if matches:
                    scheme_code = str(matches[0]['schemeCode'])
                    conn.execute("UPDATE assets SET symbol=? WHERE id=?", (scheme_code, r['id']))
            if scheme_code:
                req2 = urllib.request.Request(
                    f'https://api.mfapi.in/mf/{scheme_code}',
                    headers={'User-Agent': 'Mozilla/5.0'}
                )
                with urllib.request.urlopen(req2, timeout=8) as resp2:
                    nav_data = _json.loads(resp2.read())
                nav = float(nav_data['data'][0]['nav'])
            if nav and nav > 0:
                qty      = float(r['qty'])
                invested = float(r['invested_value'])
                current  = qty * nav
                pnl      = current - invested
                pnl_pct  = (pnl / invested * 100) if invested > 0 else 0
                conn.execute("""
                    UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                        last_synced=datetime('now'), updated_at=datetime('now')
                    WHERE id=?
                """, (nav, current, pnl, pnl_pct, r['id']))
                results.append({'asset': r['asset'], 'nav': nav, 'status': 'ok'})
            else:
                results.append({'asset': r['asset'], 'status': 'no_nav'})
        except Exception as e:
            results.append({'asset': r['asset'], 'error': str(e), 'status': 'error'})
        time.sleep(0.3)
    conn.commit()
    _update_portfolio_from_assets(conn)
    conn.close()
    ok = sum(1 for r in results if r['status'] == 'ok')
    return jsonify({'success': True, 'synced': ok, 'failed': len(results) - ok, 'results': results})

@app.route('/api/assets/sync_gold', methods=['POST'])
def api_assets_sync_gold():
    """Sync gold price for Physical Gold assets, then update portfolio."""
    try:
        req1 = urllib.request.Request(
            'https://api.gold-api.com/price/XAU', headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req1, timeout=6) as r:
            xau = _json.loads(r.read())
        usd_per_oz = float(xau['price'])
        req2 = urllib.request.Request(
            'https://api.frankfurter.app/latest?from=USD&to=INR', headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req2, timeout=6) as r:
            fx = _json.loads(r.read())
        inr_per_gram = round((usd_per_oz * float(fx['rates']['INR'])) / 31.1035)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch gold price: {e}'}), 500

    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM assets
        WHERE LOWER(asset_type) LIKE '%gold%'
          AND LOWER(asset_type) NOT LIKE '%etf%'
          AND LOWER(asset_type) NOT LIKE '%mf%'
          AND LOWER(asset_type) NOT LIKE '%mutual%'
    """).fetchall()
    count = 0
    for r in rows:
        qty      = float(r['qty'])
        invested = float(r['invested_value'])
        current  = qty * inr_per_gram
        pnl      = current - invested
        pnl_pct  = (pnl / invested * 100) if invested > 0 else 0
        conn.execute("""
            UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                last_synced=datetime('now'), updated_at=datetime('now')
            WHERE id=?
        """, (inr_per_gram, current, pnl, pnl_pct, r['id']))
        count += 1
    conn.commit()
    _update_portfolio_from_assets(conn)
    conn.close()
    return jsonify({'success': True, 'synced': count, 'price_inr_per_gram': inr_per_gram})

# ── EXCEL UPLOAD ──────────────────────────────────────────────────────────────
@app.route('/api/upload_excel', methods=['POST'])
def upload_excel():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    if not f.filename or not f.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Only .xlsx / .xls files accepted'}), 400
    mode = request.form.get('mode', 'replace')
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    try:
        f.save(tmp.name); tmp.close()
        conn = get_db()
        if mode == 'replace':
            for tbl in ('transactions', 'loans', 'portfolio', 'invest_transactions'):
                conn.execute(f'DELETE FROM {tbl}')
            conn.commit()
        results, errors = {}, {}
        for key, fn in [
            ('income',               _xl_income),
            ('expenses',             _xl_expenses),
            ('loans',                _xl_loans),
            ('investments',          _xl_investments),
            ('portfolio',            _xl_portfolio),
            ('invest_transactions',  _xl_invest_tx),
        ]:
            try:
                results[key] = fn(conn, tmp.name)
            except Exception as e:
                errors[key] = str(e)
        _refresh_monthly_calc(conn)
        # Auto-rebuild assets table from invest_transactions after upload
        try:
            holdings = conn.execute("""
                SELECT stock_name AS asset, asset_type,
                       SUM(CASE WHEN UPPER(action)='BUY' THEN quantity      ELSE -quantity      END) AS net_units,
                       SUM(CASE WHEN UPPER(action)='BUY' THEN invested_value ELSE -invested_value END) AS net_invested
                FROM invest_transactions
                WHERE stock_name IS NOT NULL AND stock_name != ''
                GROUP BY stock_name, asset_type
                HAVING net_units > 0
                ORDER BY asset_type, stock_name
            """).fetchall()
            conn.execute("DELETE FROM assets")
            for h in holdings:
                qty       = float(h['net_units'])
                invested  = float(h['net_invested'])
                avg_price = invested / qty if qty > 0 else 0
                conn.execute("""
                    INSERT INTO assets
                        (asset, asset_type, qty, avg_price, ltp, invested_value, current_value, pnl, pnl_pct, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, datetime('now'))
                """, (h['asset'], h['asset_type'], qty, avg_price, avg_price, invested, invested))
            conn.commit()
            results['assets_rebuilt'] = len(holdings)
        except Exception as e:
            errors['assets_rebuild'] = str(e)
        conn.close()
        return jsonify({'success': True, 'mode': mode, 'results': results, 'errors': errors})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try: os.unlink(tmp.name)
        except: pass

# ── NSE MASTER ────────────────────────────────────────────────────────────────
@app.route('/api/nse')
def api_nse_list():
    conn = get_db()
    rows = conn.execute("SELECT * FROM nse_master ORDER BY symbol").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/nse', methods=['POST'])
def api_nse_add():
    d = request.json
    if not d or not d.get('symbol'):
        return jsonify({'error': 'symbol required'}), 400
    sym = d['symbol'].strip().upper()
    cat = d.get('category', 'Shares')
    conn = get_db()
    conn.execute(
        "INSERT OR IGNORE INTO nse_master (symbol, company_name, sector, category) VALUES (?,?,?,?)",
        (sym, d.get('company_name', ''), d.get('sector', ''), cat)
    )
    conn.commit(); conn.close()
    return jsonify({'success': True, 'symbol': sym})

@app.route('/api/nse/<symbol>', methods=['DELETE'])
def api_nse_delete(symbol):
    conn = get_db()
    conn.execute("DELETE FROM nse_master WHERE symbol=?", (symbol.upper(),))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/nse/auto_add', methods=['POST'])
def api_nse_auto_add():
    """Add distinct symbols from assets table WHERE asset LIKE %tab% → nse_master.
    Body: {tab: "Shares"}  (defaults to "Shares" if omitted)."""
    d   = request.get_json(silent=True) or {}
    tab = (d.get('tab') or 'Shares').strip()
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM assets WHERE LOWER(asset) LIKE LOWER(?) AND symbol IS NOT NULL AND symbol != ''",
        (f'%{tab}%',)
    ).fetchall()
    added = 0
    for r in rows:
        sym = r['symbol'].strip().upper()
        res = conn.execute(
            "INSERT OR IGNORE INTO nse_master (symbol, category) VALUES (?, ?)", (sym, tab)
        )
        if res.rowcount:
            added += 1
    conn.commit(); conn.close()
    return jsonify({'success': True, 'added': added, 'total': len(rows), 'tab': tab})

@app.route('/api/nse/auto_add_etf', methods=['POST'])
def api_nse_auto_add_etf():
    """Add distinct ETF names from invest_transactions → nse_master (category=ETF)."""
    conn = get_db()
    etfs = conn.execute("""SELECT DISTINCT stock_name FROM invest_transactions
                           WHERE (UPPER(asset_type) LIKE '%ETF%' OR asset_type = 'ETF')
                           AND stock_name IS NOT NULL AND stock_name != ''""").fetchall()
    added = 0
    for r in etfs:
        sym = r['stock_name'].strip().upper()
        res = conn.execute(
            "INSERT OR IGNORE INTO nse_master (symbol, category) VALUES (?, 'ETF')", (sym,)
        )
        if res.rowcount:
            added += 1
        else:
            # If it already exists, make sure category is set to ETF
            conn.execute(
                "UPDATE nse_master SET category='ETF' WHERE symbol=? AND (category IS NULL OR category='Shares')",
                (sym,)
            )
    conn.commit(); conn.close()
    return jsonify({'success': True, 'added': added, 'total': len(etfs)})

@app.route('/api/nse/sync', methods=['POST'])
def api_nse_sync():
    if not YF_AVAILABLE:
        return jsonify({'error': 'yfinance not installed — run: pip install yfinance'}), 503
    conn = get_db()
    rows = conn.execute("SELECT symbol FROM nse_master ORDER BY symbol").fetchall()
    symbols = [r['symbol'] for r in rows]
    if not symbols:
        conn.close()
        return jsonify({'success': True, 'message': 'No stocks to sync', 'results': []})
    results = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym + '.NS')
            info   = ticker.info
            ltp    = float(info.get('currentPrice') or info.get('regularMarketPrice') or 0)
            prev   = float(info.get('previousClose') or 0)
            h52    = float(info.get('fiftyTwoWeekHigh') or 0)
            l52    = float(info.get('fiftyTwoWeekLow')  or 0)
            chg    = round((ltp - prev) / prev * 100, 2) if prev else 0
            frm52  = round((h52 - ltp) / h52 * 100, 2)  if h52  else 0
            name   = str(info.get('longName') or info.get('shortName') or sym)
            sect   = str(info.get('sector') or '')
            vol    = int(info.get('volume') or 0)
            conn.execute("""UPDATE nse_master SET ltp=?,prev_close=?,change_pct=?,
                            high_52w=?,low_52w=?,from_52w_high_pct=?,company_name=?,
                            sector=?,volume=?,updated_at=datetime('now') WHERE symbol=?""",
                         (ltp, prev, chg, h52, l52, frm52, name, sect, vol, sym))
            results.append({'symbol': sym, 'ltp': ltp, 'high_52w': h52, 'change_pct': chg, 'status': 'ok'})
        except Exception as e:
            results.append({'symbol': sym, 'error': str(e), 'status': 'error'})
    # Enrich monthly_investment_calc with live prices
    for r in results:
        if r['status'] == 'ok' and r['ltp'] > 0:
            conn.execute("""UPDATE monthly_investment_calc
                SET current_price=?,
                    current_value=net_qty*?,
                    unrealized_pnl=net_qty*?-total_invested,
                    unrealized_pnl_pct=CASE WHEN total_invested>0
                        THEN (net_qty*?-total_invested)/total_invested*100 ELSE 0 END,
                    updated_at=datetime('now')
                WHERE symbol=?""",
                (r['ltp'], r['ltp'], r['ltp'], r['ltp'], r['symbol']))
    conn.commit(); conn.close()
    ok = sum(1 for r in results if r['status'] == 'ok')
    return jsonify({'success': True, 'synced': ok, 'failed': len(results) - ok, 'results': results})

# ── WEALTH TRACKER ────────────────────────────────────────────────────────────

def _wt_cascade(conn, asset_type, purpose=None):
    """After a single asset changes, update the affected portfolio row.

    Uses the same two-pass strategy as _update_portfolio_from_assets but
    scoped to the single asset_type that changed:
      Pass 1 – SQL: portfolio.asset = asset_type  (exact name match)
      Pass 2 – Python mapping fallback (e.g. 'Equity' → 'Shares')
    """
    if not asset_type:
        return

    # Aggregate current total for this asset_type from assets table
    cv_row = conn.execute(
        "SELECT COALESCE(SUM(current_value), 0) AS cv FROM assets WHERE LOWER(asset_type) = LOWER(?)",
        (asset_type,)
    ).fetchone()
    cv = float(cv_row['cv']) if cv_row else 0.0

    _sql_update_portfolio = """
        UPDATE portfolio
        SET current_value = ?,
            total_return  = ? - total_invested,
            return_pct    = CASE WHEN total_invested > 0
                            THEN (? - total_invested) / total_invested * 100
                            ELSE 0 END,
            updated_at    = datetime('now')
        WHERE LOWER(asset) = LOWER(?)
    """

    # Pass 1: direct match — portfolio.asset = assets.asset_type
    conn.execute(_sql_update_portfolio, (cv, cv, cv, asset_type))

    # Pass 2: Python mapping fallback — only if no direct match exists
    direct_hit = conn.execute(
        "SELECT 1 FROM portfolio WHERE LOWER(asset) = LOWER(?)", (asset_type,)
    ).fetchone()
    if not direct_hit:
        portfolio_asset = _get_portfolio_asset_key(asset_type)
        if portfolio_asset:
            conn.execute(_sql_update_portfolio, (cv, cv, cv, portfolio_asset))

@app.route('/api/wealth')
def api_wealth_list():
    """All wealth goals with computed current_value / achieved_pct from portfolio."""
    from datetime import date as _date, datetime as _datetime
    conn = get_db(); c = conn.cursor()
    goals = [dict(r) for r in c.execute("SELECT * FROM wealth ORDER BY id").fetchall()]
    today = _date.today()
    for g in goals:
        row = c.execute("""
            SELECT COALESCE(SUM(current_value),0) cv,
                   COALESCE(SUM(total_invested),0) ti
            FROM portfolio WHERE purpose=?
        """, (g['purpose'],)).fetchone()
        cv = float(row['cv']); ti = float(row['ti'])
        g['current_value']  = round(cv, 2)
        g['total_invested'] = round(ti, 2)
        g['total_return']   = round(cv - ti, 2)
        g['achieved_pct']   = round(cv / g['target'] * 100, 2) if g['target'] > 0 else 0
        g['remaining']      = round(max(0.0, g['target'] - cv), 2)
        # Time remaining fields
        td = g.get('target_date')
        if td:
            try:
                target_d = _date.fromisoformat(td[:10])
                days_left = (target_d - today).days
                g['days_remaining'] = days_left
                # Created date for elapsed % calculation
                created_raw = g.get('created_at', '')
                if created_raw:
                    created_d = _datetime.fromisoformat(created_raw[:10]).date()
                    total_days = (target_d - created_d).days
                    elapsed    = (today - created_d).days
                    g['time_elapsed_pct'] = round(min(100, max(0, elapsed / total_days * 100)), 1) if total_days > 0 else 0
                else:
                    g['time_elapsed_pct'] = 0
            except Exception:
                g['days_remaining']   = None
                g['time_elapsed_pct'] = 0
        else:
            g['days_remaining']   = None
            g['time_elapsed_pct'] = 0
    conn.close()
    return jsonify(goals)

@app.route('/api/wealth', methods=['POST'])
def api_wealth_add():
    d = request.json or {}
    purpose = (d.get('purpose') or '').strip()
    if not purpose:
        return jsonify({'error': 'purpose required'}), 400
    target      = float(d.get('target', 0))
    target_date = (d.get('target_date') or '').strip() or None
    conn = get_db()
    try:
        conn.execute("INSERT INTO wealth (purpose, target, target_date) VALUES (?,?,?)",
                     (purpose, target, target_date))
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 400
    nid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return jsonify({'success': True, 'id': nid})

@app.route('/api/wealth/<int:wid>', methods=['PUT'])
def api_wealth_update(wid):
    d = request.json or {}; conn = get_db()
    fields = []; params = []
    if 'purpose'     in d: fields.append("purpose=?");     params.append(d['purpose'].strip())
    if 'target'      in d: fields.append("target=?");      params.append(float(d['target']))
    if 'target_date' in d: fields.append("target_date=?"); params.append((d['target_date'] or '').strip() or None)
    if fields:
        params.append(wid)
        conn.execute(f"UPDATE wealth SET {','.join(fields)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/wealth/<int:wid>', methods=['DELETE'])
def api_wealth_delete(wid):
    conn = get_db()
    conn.execute("DELETE FROM wealth WHERE id=?", (wid,))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/wt/portfolio')
def api_wt_portfolio_list():
    conn = get_db()
    purpose = request.args.get('purpose', '')
    q = "SELECT * FROM portfolio WHERE 1=1"; p = []
    if purpose: q += " AND purpose=?"; p.append(purpose)
    q += " ORDER BY purpose, asset_type, asset"
    rows = conn.execute(q, p).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/wt/portfolio/assign', methods=['POST'])
def api_wt_portfolio_assign():
    """Assign a purpose to a portfolio row (by id or asset name)."""
    d = request.json or {}; conn = get_db()
    purpose = d.get('purpose', '') or None
    if 'id' in d:
        conn.execute("UPDATE portfolio SET purpose=? WHERE id=?", (purpose, d['id']))
    elif 'asset' in d:
        conn.execute("UPDATE portfolio SET purpose=? WHERE asset=?", (purpose, d['asset']))
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/wt/assets')
def api_wt_assets_list():
    conn = get_db()
    purpose    = request.args.get('purpose', '')
    asset_type = request.args.get('asset_type', '')
    q = "SELECT * FROM assets WHERE 1=1"; p = []
    if purpose:    q += " AND purpose=?";                 p.append(purpose)
    if asset_type: q += " AND LOWER(asset_type) LIKE ?";  p.append(f'%{asset_type.lower()}%')
    q += " ORDER BY asset_type, asset"
    rows = conn.execute(q, p).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/wt/assets', methods=['POST'])
def api_wt_assets_add():
    d = request.json or {}; conn = get_db()
    qty       = float(d.get('qty', 0))
    avg_price = float(d.get('avg_price', 0))
    ltp       = float(d.get('ltp', avg_price))
    invested  = qty * avg_price
    current   = qty * ltp
    pnl       = current - invested
    pnl_pct   = (pnl / invested * 100) if invested > 0 else 0
    purpose   = (d.get('purpose') or '').strip() or None
    conn.execute("""
        INSERT INTO assets (asset, asset_type, purpose, symbol, qty, avg_price, ltp,
            invested_value, current_value, pnl, pnl_pct, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(asset, asset_type) DO UPDATE SET
            purpose=excluded.purpose, symbol=excluded.symbol,
            qty=excluded.qty, avg_price=excluded.avg_price, ltp=excluded.ltp,
            invested_value=excluded.invested_value, current_value=excluded.current_value,
            pnl=excluded.pnl, pnl_pct=excluded.pnl_pct, updated_at=datetime('now')
    """, (d.get('asset',''), d.get('asset_type',''), purpose, d.get('symbol',''),
          qty, avg_price, ltp, invested, current, pnl, pnl_pct))
    conn.commit()
    _wt_cascade(conn, d.get('asset_type',''), purpose)
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/wt/assets/<int:aid>', methods=['PATCH'])
def api_wt_assets_patch(aid):
    d = request.json or {}; conn = get_db()
    row = conn.execute("SELECT * FROM assets WHERE id=?", (aid,)).fetchone()
    if not row:
        conn.close(); return jsonify({'error': 'Not found'}), 404
    ltp       = float(d.get('ltp',       row['ltp']))
    qty       = float(d.get('qty',       row['qty']))
    avg_price = float(d.get('avg_price', row['avg_price']))
    symbol    = d.get('symbol',  row['symbol'] or '')
    purpose   = (d.get('purpose', row['purpose'] or '') or '') or None
    invested  = qty * avg_price
    current   = qty * ltp
    pnl       = current - invested
    pnl_pct   = (pnl / invested * 100) if invested > 0 else 0
    conn.execute("""
        UPDATE assets SET ltp=?, qty=?, avg_price=?, symbol=?, purpose=?,
            invested_value=?, current_value=?, pnl=?, pnl_pct=?,
            last_synced=datetime('now'), updated_at=datetime('now')
        WHERE id=?
    """, (ltp, qty, avg_price, symbol, purpose, invested, current, pnl, pnl_pct, aid))
    conn.commit()
    _wt_cascade(conn, row['asset_type'], purpose)
    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/wt/assets/<int:aid>', methods=['DELETE'])
def api_wt_assets_delete(aid):
    conn = get_db()
    row = conn.execute("SELECT asset_type, purpose FROM assets WHERE id=?", (aid,)).fetchone()
    conn.execute("DELETE FROM assets WHERE id=?", (aid,))
    conn.commit()
    if row:
        _wt_cascade(conn, row['asset_type'], row['purpose'])
        conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/wt/assets/csv_upload', methods=['POST'])
def api_wt_assets_csv_upload():
    """Bulk-import assets from a CSV file.
    Required columns : asset, asset_type, qty, avg_price
    Optional columns : purpose, symbol, ltp
    Each row is inserted as an individual holding.
    Rows where (asset, asset_type, purpose, symbol) all match an existing record
    are replaced (qty/price/ltp updated).
    After all rows are saved, portfolio current_values are refreshed."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    if not f.filename or not f.filename.lower().endswith('.csv'):
        return jsonify({'error': 'Only .csv files accepted'}), 400
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    try:
        f.save(tmp.name); tmp.close()
        df = pd.read_csv(tmp.name)
        df.columns = [c.strip().lower() for c in df.columns]
        required = {'asset', 'asset_type', 'qty', 'avg_price'}
        missing  = required - set(df.columns)
        if missing:
            return jsonify({'error': f'Missing columns: {", ".join(missing)}'}), 400

        conn = get_db()
        ok = 0; errors = []
        for i, row in df.iterrows():
            try:
                asset     = str(row.get('asset', '')).strip()
                atype     = str(row.get('asset_type', '')).strip()
                if not asset or not atype or asset.lower() == 'nan' or atype.lower() == 'nan':
                    errors.append(f'Row {i+2}: asset and asset_type required'); continue
                purpose   = str(row.get('purpose', '')).strip() or None
                symbol    = str(row.get('symbol',  '')).strip()
                qty       = float(row.get('qty',       0) or 0)
                avg_price = float(row.get('avg_price', 0) or 0)
                ltp_raw   = str(row.get('ltp', '')).strip()
                ltp       = float(ltp_raw) if ltp_raw not in ('', 'nan', '0') and float(ltp_raw or 0) > 0 else avg_price
                invested  = qty * avg_price
                current   = qty * ltp
                pnl       = current - invested
                pnl_pct   = (pnl / invested * 100) if invested > 0 else 0.0

                # If symbol is provided, try to update existing row with that symbol first
                updated = 0
                if symbol:
                    updated = conn.execute("""
                        UPDATE assets SET
                            asset=?, asset_type=?, purpose=?, qty=?, avg_price=?, ltp=?,
                            invested_value=?, current_value=?, pnl=?, pnl_pct=?,
                            updated_at=datetime('now')
                        WHERE symbol=?
                    """, (asset, atype, purpose, qty, avg_price, ltp,
                          invested, current, pnl, pnl_pct, symbol)).rowcount

                if not updated:
                    conn.execute("""
                        INSERT INTO assets
                            (asset, asset_type, purpose, symbol, qty, avg_price, ltp,
                             invested_value, current_value, pnl, pnl_pct, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                    """, (asset, atype, purpose, symbol, qty, avg_price, ltp,
                          invested, current, pnl, pnl_pct))
                ok += 1
            except Exception as e:
                errors.append(f'Row {i+2}: {e}')

        conn.commit()

        # Refresh portfolio current_values from assets
        conn.execute("""
            UPDATE portfolio AS B
            SET current_value = (
                SELECT SUM(A.current_value)
                FROM assets A
                WHERE A.asset = B.asset
                  AND A.asset_type = B.asset_type
            )
            WHERE EXISTS (
                SELECT 1
                FROM assets A
                WHERE A.asset = B.asset
                  AND A.asset_type = B.asset_type
            )
        """)
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'imported': ok, 'errors': errors})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try: os.unlink(tmp.name)
        except: pass

@app.route('/api/wt/assets/sample_csv')
def api_wt_assets_sample_csv():
    """Download a sample CSV template for bulk asset import."""
    sample = (
        "asset,asset_type,purpose,symbol,qty,avg_price,ltp\n"
        "NIFTY 50 ETF,ETF,Ira Corpus,NIFTYBEES,500,200,250\n"
        "Axis ELSS Tax Saver Direct Plan Growth,Mutual Fund,Corpus,147070,303.371,75.07,\n"
        "Physical Gold,Gold,Gold Corpus,,100,5500,\n"
        "Sukanya Samriddhi,Fixed Return,Ira Corpus,,1,921405,921405\n"
        "Employer PF,Fixed Return,Corpus,,1,500000,550000\n"
        "NPS,Retirement,Corpus,,1,400000,450000\n"
        "ADANIGREEN,Equity,Corpus,ADANIGREEN,50,800,950\n"
    )
    from flask import Response
    return Response(
        sample,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=wealthflow_assets_sample.csv'}
    )

@app.route('/api/wt/sync', methods=['POST'])
def api_wt_sync():
    """Sync LTP for stocks/ETF/MF/gold assets, cascade to portfolio + wealth."""
    import time as _time
    results = []
    conn = get_db()

    # 1. Stocks + ETF via yfinance
    if YF_AVAILABLE:
        rows = conn.execute("""
            SELECT * FROM assets
            WHERE UPPER(asset_type) IN ('STOCKS','EQUITY','DIRECT EQUITY','ETF')
               OR UPPER(asset_type) LIKE '%ETF%' OR UPPER(asset_type) LIKE '%STOCK%'
        """).fetchall()
        for r in rows:
            sym = (r['symbol'] or r['asset']).strip().upper()
            try:
                info = yf.Ticker(sym + '.NS').info
                ltp  = float(info.get('currentPrice') or info.get('regularMarketPrice') or 0)
                if ltp > 0:
                    qty = float(r['qty']); invested = float(r['invested_value'])
                    current = qty * ltp; pnl = current - invested
                    pnl_pct = (pnl / invested * 100) if invested > 0 else 0
                    conn.execute("""UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                        last_synced=datetime('now'), updated_at=datetime('now') WHERE id=?""",
                        (ltp, current, pnl, pnl_pct, r['id']))
                    results.append({'asset': r['asset'], 'ltp': ltp, 'status': 'ok'})
                else:
                    results.append({'asset': r['asset'], 'status': 'no_price'})
            except Exception as e:
                results.append({'asset': r['asset'], 'status': 'error', 'error': str(e)})

    # 2. Mutual Funds via MFAPI
    mf_rows = conn.execute("""
        SELECT * FROM assets WHERE LOWER(asset_type) LIKE '%mutual%' OR LOWER(asset_type)='mf'
    """).fetchall()
    for r in mf_rows:
        try:
            sc = (r['symbol'] or '').strip()
            if not sc:
                req = urllib.request.Request(
                    'https://api.mfapi.in/mf/search?q=' + urllib.parse.quote(r['asset']),
                    headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=6) as resp:
                    matches = _json.loads(resp.read())
                if matches:
                    sc = str(matches[0]['schemeCode'])
                    conn.execute("UPDATE assets SET symbol=? WHERE id=?", (sc, r['id']))
            if sc:
                req2 = urllib.request.Request(f'https://api.mfapi.in/mf/{sc}',
                    headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req2, timeout=6) as resp2:
                    nav_data = _json.loads(resp2.read())
                nav = float(nav_data['data'][0]['nav'])
                if nav > 0:
                    qty = float(r['qty']); invested = float(r['invested_value'])
                    current = qty * nav; pnl = current - invested
                    pnl_pct = (pnl / invested * 100) if invested > 0 else 0
                    conn.execute("""UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                        last_synced=datetime('now'), updated_at=datetime('now') WHERE id=?""",
                        (nav, current, pnl, pnl_pct, r['id']))
                    results.append({'asset': r['asset'], 'ltp': nav, 'status': 'ok'})
                else:
                    results.append({'asset': r['asset'], 'status': 'no_nav'})
            _time.sleep(0.2)
        except Exception as e:
            results.append({'asset': r['asset'], 'status': 'error', 'error': str(e)})

    # 3. Gold (physical / non-ETF / non-MF)
    try:
        req_xau = urllib.request.Request('https://api.gold-api.com/price/XAU',
            headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req_xau, timeout=6) as r: xau = _json.loads(r.read())
        req_fx = urllib.request.Request('https://api.frankfurter.app/latest?from=USD&to=INR',
            headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req_fx, timeout=6) as r: fx = _json.loads(r.read())
        gold_inr = round((float(xau['price']) * float(fx['rates']['INR'])) / 31.1035)
        gold_rows = conn.execute("""
            SELECT * FROM assets
            WHERE LOWER(asset_type) LIKE '%gold%'
              AND LOWER(asset_type) NOT LIKE '%etf%'
              AND LOWER(asset_type) NOT LIKE '%mf%'
              AND LOWER(asset_type) NOT LIKE '%mutual%'
        """).fetchall()
        for r in gold_rows:
            qty = float(r['qty']); invested = float(r['invested_value'])
            current = qty * gold_inr; pnl = current - invested
            pnl_pct = (pnl / invested * 100) if invested > 0 else 0
            conn.execute("""UPDATE assets SET ltp=?, current_value=?, pnl=?, pnl_pct=?,
                last_synced=datetime('now'), updated_at=datetime('now') WHERE id=?""",
                (gold_inr, current, pnl, pnl_pct, r['id']))
            results.append({'asset': r['asset'], 'ltp': gold_inr, 'status': 'ok'})
    except Exception:
        pass

    conn.commit()
    _update_portfolio_from_assets(conn)
    conn.commit(); conn.close()
    ok = sum(1 for r in results if r['status'] == 'ok')
    return jsonify({'success': True, 'synced': ok, 'failed': len(results) - ok, 'results': results})

# ── MONTHLY INVESTMENT CALC ───────────────────────────────────────────────────
@app.route('/api/monthly_investment_calc')
def api_monthly_calc():
    month = request.args.get('month', '')
    conn  = get_db()
    q = "SELECT * FROM monthly_investment_calc WHERE 1=1"; p = []
    if month: q += " AND month LIKE ?"; p.append(f'{month}%')
    q += " ORDER BY month DESC, total_invested DESC"
    rows = conn.execute(q, p).fetchall(); conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/monthly_investment_calc/refresh', methods=['POST'])
def api_monthly_calc_refresh():
    conn = get_db()
    _refresh_monthly_calc(conn)
    cnt = conn.execute("SELECT COUNT(*) FROM monthly_investment_calc").fetchone()[0]
    conn.close()
    return jsonify({'success': True, 'rows': cnt})

# ══════════════════════════════════════════════════════════════════════════════
# TRADING STRATEGY
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/trading_strategy')
def api_trading_strategy():
    """Return shares from assets table enriched with NSE live data + last tx."""
    conn = get_db()
    # Shares assets joined with NSE live data
    assets = conn.execute("""
        SELECT a.id, a.asset, a.asset_type, a.symbol,
               a.qty, a.avg_price, a.ltp AS db_ltp,
               COALESCE(a.target_pct, 25) AS target_pct,
               a.invested_value, a.current_value,
               COALESCE(n.ltp,        a.ltp, 0)            AS ltp,
               COALESCE(n.high_52w,   0)                    AS high_52w,
               COALESCE(n.low_52w,    0)                    AS low_52w,
               COALESCE(n.from_52w_high_pct, 0)            AS from_52w_high_pct,
               COALESCE(n.change_pct, 0)                    AS day_change_pct,
               COALESCE(n.company_name, a.asset)            AS company_name,
               n.updated_at AS nse_updated
        FROM assets a
        LEFT JOIN nse_master n ON UPPER(TRIM(a.symbol)) = UPPER(TRIM(n.symbol))
        WHERE LOWER(a.asset_type) LIKE '%share%'
           OR LOWER(a.asset_type) = 'equity'
        ORDER BY a.symbol
    """).fetchall()

    # Last buy per symbol from invest_transactions
    last_buys = conn.execute("""
        SELECT t.stock_name, t.price, t.entry_date, t.quantity
        FROM invest_transactions t
        INNER JOIN (
            SELECT stock_name, MAX(entry_date) AS max_date
            FROM invest_transactions WHERE LOWER(action)='buy'
            GROUP BY stock_name
        ) m ON t.stock_name = m.stock_name AND t.entry_date = m.max_date
        WHERE LOWER(t.action) = 'buy'
    """).fetchall()
    last_buy_map = {r['stock_name']: dict(r) for r in last_buys}

    # Last sell per symbol
    last_sells = conn.execute("""
        SELECT t.stock_name, t.price, t.entry_date, t.quantity
        FROM invest_transactions t
        INNER JOIN (
            SELECT stock_name, MAX(entry_date) AS max_date
            FROM invest_transactions WHERE LOWER(action)='sell'
            GROUP BY stock_name
        ) m ON t.stock_name = m.stock_name AND t.entry_date = m.max_date
        WHERE LOWER(t.action) = 'sell'
    """).fetchall()
    last_sell_map = {r['stock_name']: dict(r) for r in last_sells}

    rows = []
    for a in assets:
        sym  = (a['symbol'] or '').strip().upper()
        name = (a['asset'] or '').strip()
        lb   = last_buy_map.get(sym) or last_buy_map.get(name) or {}
        ls   = last_sell_map.get(sym) or last_sell_map.get(name) or {}
        rows.append({
            'id':                a['id'],
            'symbol':            sym,
            'asset':             name,
            'company_name':      a['company_name'] or name,
            'qty':               float(a['qty'] or 0),
            'avg_price':         float(a['avg_price'] or 0),
            'ltp':               float(a['ltp'] or 0),
            'invested_value':    float(a['invested_value'] or 0),
            'current_value':     float(a['current_value'] or 0),
            'high_52w':          float(a['high_52w'] or 0),
            'low_52w':           float(a['low_52w'] or 0),
            'from_52w_high_pct': float(a['from_52w_high_pct'] or 0),
            'day_change_pct':    float(a['day_change_pct'] or 0),
            'target_pct':        float(a['target_pct'] or 25),
            'nse_updated':       a['nse_updated'] or '',
            'last_buy':  {'price': float(lb.get('price') or 0),
                          'date':  lb.get('entry_date',''),
                          'qty':   float(lb.get('quantity') or 0)},
            'last_sell': {'price': float(ls.get('price') or 0),
                          'date':  ls.get('entry_date',''),
                          'qty':   float(ls.get('quantity') or 0)},
        })
    conn.close()
    return jsonify(rows)


if __name__ == '__main__':
    # Auto-install yfinance if missing (YF_AVAILABLE is module-level, no global needed here)
    if not YF_AVAILABLE:
        try:
            import subprocess, sys as _sys
            print("📦 Installing yfinance (required for NSE sync)…")
            subprocess.check_call(
                [_sys.executable, '-m', 'pip', 'install', 'yfinance', '-q'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            import yfinance as yf
            YF_AVAILABLE = True
            print("✅ yfinance installed successfully")
        except Exception as _e:
            print(f"⚠️  Could not auto-install yfinance: {_e}")
            print("   Run manually: pip install yfinance")
    init_schema()
    app.run(debug=True, port=5000)
