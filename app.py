from flask import Flask, render_template, request, jsonify
import sqlite3, os, tempfile
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
    """)
    conn.commit(); conn.close()

@app.route('/')
def dashboard():
    return render_template('index.html')

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

@app.route('/api/portfolio/summary')
def api_portfolio_summary():
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT asset_type, SUM(total_invested) as invested,
                               SUM(current_value) as current_val, SUM(total_return) as total_ret
                        FROM portfolio WHERE total_invested > 0
                        GROUP BY asset_type ORDER BY current_val DESC""").fetchall()
    rows = [dict(r) for r in rows]
    total_inv = sum(r['invested'] for r in rows)
    total_val = sum(r['current_val'] for r in rows)
    conn.close()
    return jsonify({
        'by_type': rows, 'total_invested': total_inv, 'total_value': total_val,
        'total_return': total_val - total_inv,
        'return_pct': round((total_val - total_inv) / total_inv * 100, 2) if total_inv else 0,
    })

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
    conn = get_db(); c = conn.cursor()
    q = "SELECT * FROM invest_transactions WHERE 1=1"; p = []
    if asset_type: q += " AND asset_type=?"; p.append(asset_type)
    if stock:      q += " AND stock_name=?"; p.append(stock)
    q += " ORDER BY entry_date DESC LIMIT 500"
    rows = c.execute(q, p).fetchall(); conn.close()
    return jsonify([dict(r) for r in rows])

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
            str(row.get('Month','')).strip(),
        ))
    conn.executemany("""INSERT INTO invest_transactions
       (entry_date,stock_name,asset_type,quantity,action,price,invested_value,
        current_value,profit,profit_pct,rationale,month)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
    conn.commit()
    return len(rows)

def _refresh_monthly_calc(conn):
    """Recompute monthly_investment_calc from invest_transactions."""
    conn.execute("DELETE FROM monthly_investment_calc")
    conn.execute("""
        INSERT INTO monthly_investment_calc
            (month, symbol, asset_type, qty_bought, qty_sold, net_qty,
             avg_buy_price, total_invested, updated_at)
        SELECT
            month,
            stock_name,
            asset_type,
            SUM(CASE WHEN action = 'Buy' THEN quantity ELSE 0 END),
            SUM(CASE WHEN action = 'Sell' THEN quantity ELSE 0 END),
            SUM(CASE WHEN action = 'Buy' THEN quantity ELSE -quantity END),
            CASE WHEN SUM(CASE WHEN action = 'Buy' THEN quantity ELSE 0 END) > 0
                 THEN SUM(CASE WHEN action = 'Buy' THEN invested_value ELSE 0 END) /
                      SUM(CASE WHEN action = 'Buy' THEN quantity ELSE 0 END)
                 ELSE 0 END,
            SUM(CASE WHEN action = 'Buy' THEN invested_value ELSE 0 END),
            datetime('now')
        FROM invest_transactions
        WHERE month IS NOT NULL AND month != ''
          AND stock_name IS NOT NULL AND stock_name != ''
        GROUP BY month, stock_name, asset_type
    """)
    conn.commit()

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
    conn = get_db()
    conn.execute("INSERT OR IGNORE INTO nse_master (symbol, company_name, sector) VALUES (?,?,?)",
                 (sym, d.get('company_name', ''), d.get('sector', '')))
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
    conn = get_db()
    stocks = conn.execute("""SELECT DISTINCT stock_name FROM invest_transactions
                             WHERE asset_type IN ('Stocks','Equity','Stocks ')
                             AND stock_name IS NOT NULL AND stock_name != ''""").fetchall()
    added = 0
    for r in stocks:
        sym = r['stock_name'].strip().upper()
        res = conn.execute("INSERT OR IGNORE INTO nse_master (symbol) VALUES (?)", (sym,))
        if res.rowcount: added += 1
    conn.commit(); conn.close()
    return jsonify({'success': True, 'added': added, 'total': len(stocks)})

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
