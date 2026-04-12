from flask import Flask, render_template, request, jsonify, redirect, url_for
import sqlite3
import os
from datetime import datetime, date
import calendar

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), 'money_tracker.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK(type IN ('income', 'expense', 'investment')),
            category TEXT NOT NULL,
            sub_category TEXT,
            amount REAL NOT NULL,
            date TEXT NOT NULL,
            note TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            type TEXT NOT NULL,
            monthly_limit REAL NOT NULL,
            month TEXT NOT NULL,
            UNIQUE(category, month)
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            condition TEXT NOT NULL,
            threshold REAL NOT NULL,
            category TEXT,
            period TEXT DEFAULT 'monthly',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)

    # Seed with sample data if empty
    count = c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    if count == 0:
        today = date.today()
        samples = [
            # Income
            ('income', 'Salary', 'Primary Job', 85000, f'{today.year}-{today.month:02d}-01', 'Monthly salary'),
            ('income', 'Freelance', 'Consulting', 15000, f'{today.year}-{today.month:02d}-10', 'Project payment'),
            ('income', 'Rental', 'Property', 12000, f'{today.year}-{today.month:02d}-05', 'Apartment rent'),
            ('income', 'Dividends', 'Stocks', 3200, f'{today.year}-{today.month:02d}-15', 'Quarterly dividend'),
            # Expenses
            ('expense', 'Rent', 'Home Loan EMI', 25000, f'{today.year}-{today.month:02d}-01', 'Home EMI'),
            ('expense', 'Food', 'Groceries', 6500, f'{today.year}-{today.month:02d}-12', 'Monthly groceries'),
            ('expense', 'Food', 'Dining Out', 3200, f'{today.year}-{today.month:02d}-18', 'Restaurant bills'),
            ('expense', 'Transport', 'Fuel', 2800, f'{today.year}-{today.month:02d}-20', 'Fuel costs'),
            ('expense', 'Utilities', 'Electricity', 1800, f'{today.year}-{today.month:02d}-08', 'Electricity bill'),
            ('expense', 'Utilities', 'Internet', 999, f'{today.year}-{today.month:02d}-08', 'Broadband'),
            ('expense', 'Entertainment', 'OTT', 1200, f'{today.year}-{today.month:02d}-03', 'Netflix + Hotstar'),
            ('expense', 'Health', 'Insurance', 3500, f'{today.year}-{today.month:02d}-01', 'Health premium'),
            ('expense', 'Shopping', 'Clothes', 4500, f'{today.year}-{today.month:02d}-22', 'New wardrobe'),
            # Investments
            ('investment', 'Mutual Funds', 'SIP', 10000, f'{today.year}-{today.month:02d}-05', 'Monthly SIP'),
            ('investment', 'Stocks', 'Direct Equity', 5000, f'{today.year}-{today.month:02d}-14', 'HDFC Bank shares'),
            ('investment', 'PPF', 'PPF Account', 8000, f'{today.year}-{today.month:02d}-10', 'Annual PPF deposit'),
            ('investment', 'Gold', 'Digital Gold', 2000, f'{today.year}-{today.month:02d}-20', 'Sovereign Gold Bond'),
            ('investment', 'FD', 'Fixed Deposit', 15000, f'{today.year}-{today.month:02d}-25', 'Bank FD'),
        ]
        c.executemany(
            "INSERT INTO transactions (type, category, sub_category, amount, date, note) VALUES (?,?,?,?,?,?)",
            samples
        )

        # Sample alerts
        alerts = [
            ('Monthly Food Budget', 'expense', 'category_exceeds', 12000, 'Food', 'monthly'),
            ('Low Savings Warning', 'savings', 'savings_below', 20000, None, 'monthly'),
            ('Investment Target', 'investment', 'total_below', 30000, None, 'monthly'),
        ]
        c.executemany(
            "INSERT INTO alerts (name, type, condition, threshold, category, period) VALUES (?,?,?,?,?,?)",
            alerts
        )

    conn.commit()
    conn.close()


# ─── Dashboard ───────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    return render_template('index.html')


@app.route('/api/summary')
def api_summary():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db()
    c = conn.cursor()

    def month_total(ttype):
        row = c.execute(
            "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND date LIKE ?",
            (ttype, f'{month}%')
        ).fetchone()
        return row[0]

    income = month_total('income')
    expense = month_total('expense')
    investment = month_total('investment')
    savings = income - expense - investment
    savings_rate = round((savings / income * 100), 1) if income > 0 else 0

    # Previous month
    y, m = map(int, month.split('-'))
    pm = m - 1 if m > 1 else 12
    py = y if m > 1 else y - 1
    prev_month = f'{py}-{pm:02d}'

    prev_income = c.execute(
        "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?",
        (f'{prev_month}%',)
    ).fetchone()[0]
    prev_expense = c.execute(
        "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND date LIKE ?",
        (f'{prev_month}%',)
    ).fetchone()[0]
    prev_investment = c.execute(
        "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?",
        (f'{prev_month}%',)
    ).fetchone()[0]
    prev_savings = prev_income - prev_expense - prev_investment

    conn.close()

    def delta(curr, prev):
        if prev == 0:
            return 0
        return round(((curr - prev) / prev) * 100, 1)

    return jsonify({
        'income': income, 'expense': expense, 'investment': investment,
        'savings': savings, 'savings_rate': savings_rate,
        'delta_income': delta(income, prev_income),
        'delta_expense': delta(expense, prev_expense),
        'delta_investment': delta(investment, prev_investment),
        'delta_savings': delta(savings, prev_savings),
    })


@app.route('/api/category_breakdown')
def api_category_breakdown():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db()
    c = conn.cursor()
    rows = c.execute(
        """SELECT type, category, COALESCE(SUM(amount),0) as total
           FROM transactions WHERE date LIKE ?
           GROUP BY type, category ORDER BY type, total DESC""",
        (f'{month}%',)
    ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        result.setdefault(r['type'], []).append({'category': r['category'], 'total': r['total']})
    return jsonify(result)


@app.route('/api/monthly_trend')
def api_monthly_trend():
    conn = get_db()
    c = conn.cursor()
    rows = c.execute(
        """SELECT strftime('%Y-%m', date) as month,
                  type, COALESCE(SUM(amount),0) as total
           FROM transactions
           GROUP BY month, type
           ORDER BY month"""
    ).fetchall()
    conn.close()
    months = sorted(set(r['month'] for r in rows))
    data = {m: {'income': 0, 'expense': 0, 'investment': 0, 'savings': 0} for m in months}
    for r in rows:
        data[r['month']][r['type']] = r['total']
    for m in months:
        d = data[m]
        d['savings'] = d['income'] - d['expense'] - d['investment']
    return jsonify([{'month': m, **data[m]} for m in months])


@app.route('/api/transactions')
def api_transactions():
    month = request.args.get('month', '')
    ttype = request.args.get('type', '')
    conn = get_db()
    c = conn.cursor()
    query = "SELECT * FROM transactions WHERE 1=1"
    params = []
    if month:
        query += " AND date LIKE ?"
        params.append(f'{month}%')
    if ttype:
        query += " AND type=?"
        params.append(ttype)
    query += " ORDER BY date DESC"
    rows = c.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/transactions', methods=['POST'])
def add_transaction():
    data = request.json
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO transactions (type, category, sub_category, amount, date, note) VALUES (?,?,?,?,?,?)",
        (data['type'], data['category'], data.get('sub_category', ''),
         float(data['amount']), data['date'], data.get('note', ''))
    )
    conn.commit()
    new_id = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': new_id})


@app.route('/api/transactions/<int:tid>', methods=['DELETE'])
def delete_transaction(tid):
    conn = get_db()
    conn.execute("DELETE FROM transactions WHERE id=?", (tid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/alerts')
def api_alerts():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    conn = get_db()
    c = conn.cursor()
    alerts = c.execute("SELECT * FROM alerts WHERE is_active=1").fetchall()
    triggered = []
    for a in alerts:
        a = dict(a)
        actual = 0
        if a['condition'] == 'category_exceeds':
            row = c.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND category=? AND date LIKE ?",
                (a['type'], a['category'], f'{month}%')
            ).fetchone()
            actual = row[0]
            a['triggered'] = actual > a['threshold']
        elif a['condition'] == 'savings_below':
            income = c.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='income' AND date LIKE ?",
                (f'{month}%',)
            ).fetchone()[0]
            expense = c.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='expense' AND date LIKE ?",
                (f'{month}%',)
            ).fetchone()[0]
            invest = c.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='investment' AND date LIKE ?",
                (f'{month}%',)
            ).fetchone()[0]
            actual = income - expense - invest
            a['triggered'] = actual < a['threshold']
        elif a['condition'] == 'total_below':
            row = c.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type=? AND date LIKE ?",
                (a['type'], f'{month}%')
            ).fetchone()
            actual = row[0]
            a['triggered'] = actual < a['threshold']
        a['actual'] = actual
        triggered.append(a)
    conn.close()
    return jsonify(triggered)


@app.route('/api/alerts', methods=['POST'])
def add_alert():
    data = request.json
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO alerts (name, type, condition, threshold, category, period) VALUES (?,?,?,?,?,?)",
        (data['name'], data['type'], data['condition'],
         float(data['threshold']), data.get('category'), data.get('period', 'monthly'))
    )
    conn.commit()
    aid = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': aid})


@app.route('/api/alerts/<int:aid>', methods=['DELETE'])
def delete_alert(aid):
    conn = get_db()
    conn.execute("DELETE FROM alerts WHERE id=?", (aid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)
