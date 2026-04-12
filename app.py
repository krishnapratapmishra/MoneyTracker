from flask import Flask, render_template, request, jsonify
import sqlite3, os
from datetime import datetime

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
    """)
    conn.commit(); conn.close()

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db(); c = conn.cursor()
    def mt(t): return c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND date LIKE ?", (t, f'{month}%')).fetchone()[0]
    income = mt('income'); expense = mt('expense'); investment = mt('investment')
    savings = income - expense - investment
    savings_rate = round(savings / income * 100, 1) if income > 0 else 0
    y, m = map(int, month.split('-'))
    pm = m - 1 if m > 1 else 12; py = y if m > 1 else y - 1
    prev = f'{py}-{pm:02d}'
    pi = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?", (f'{prev}%',)).fetchone()[0]
    pe = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND date LIKE ?", (f'{prev}%',)).fetchone()[0]
    pv = c.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?", (f'{prev}%',)).fetchone()[0]
    ps = pi - pe - pv
    def delta(cur, prv): return round(((cur - prv) / prv) * 100, 1) if prv != 0 else 0
    conn.close()
    return jsonify({
        'income': income, 'expense': expense, 'investment': investment,
        'savings': savings, 'savings_rate': savings_rate,
        'delta_income': delta(income, pi), 'delta_expense': delta(expense, pe),
        'delta_investment': delta(investment, pv), 'delta_savings': delta(savings, ps),
    })

@app.route('/api/monthly_trend')
def api_monthly_trend():
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT strftime('%Y-%m',date) as month, type, COALESCE(SUM(amount),0) as total
                        FROM transactions GROUP BY month,type ORDER BY month""").fetchall()
    conn.close()
    months = sorted(set(r['month'] for r in rows))
    data = {m: {'income':0,'expense':0,'investment':0} for m in months}
    for r in rows: data[r['month']][r['type']] = r['total']
    for m in months: data[m]['savings'] = data[m]['income'] - data[m]['expense'] - data[m]['investment']
    return jsonify([{'month': m, **data[m]} for m in months])

@app.route('/api/category_breakdown')
def api_category_breakdown():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT type, category, COALESCE(SUM(amount),0) as total
                        FROM transactions WHERE date LIKE ?
                        GROUP BY type,category ORDER BY type, total DESC""", (f'{month}%',)).fetchall()
    conn.close()
    result = {}
    for r in rows: result.setdefault(r['type'], []).append({'category': r['category'], 'total': r['total']})
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

@app.route('/api/transactions', methods=['POST'])
def add_transaction():
    d = request.json; conn = get_db()
    conn.execute("INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
                 (d['type'], d['category'], d.get('sub_category',''), float(d['amount']), d['date'], d.get('note','')))
    conn.commit(); nid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]; conn.close()
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
    asset_type = request.args.get('type', '')
    conn = get_db(); c = conn.cursor()
    q = "SELECT * FROM invest_transactions WHERE 1=1"; p = []
    if asset_type: q += " AND asset_type=?"; p.append(asset_type)
    q += " ORDER BY entry_date DESC LIMIT 200"
    rows = c.execute(q, p).fetchall(); conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/loans/summary')
def api_loans_summary():
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""SELECT loan_type, SUM(amount) as total_paid
                        FROM loans GROUP BY loan_type ORDER BY total_paid DESC""").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

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

if __name__ == '__main__':
    init_schema()
    app.run(debug=True, port=5000)
