"""
import_excel.py — Import MoneyTracker.xlsx into the SQLite database.
Run once: python import_excel.py <path_to_xlsx>
"""
import sqlite3, sys, os
import pandas as pd

XLSX = sys.argv[1] if len(sys.argv) > 1 else 'MoneyTracker.xlsx'
DB   = os.path.join(os.path.dirname(__file__), 'money_tracker.db')

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK(type IN ('income','expense','investment')),
            category TEXT NOT NULL,
            sub_category TEXT,
            amount REAL NOT NULL,
            date TEXT NOT NULL,
            note TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS loans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL,
            loan_type TEXT NOT NULL,
            amount REAL NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            invested_pre_nov25 REAL DEFAULT 0,
            value_pre_nov25 REAL DEFAULT 0,
            invested_since_nov25 REAL DEFAULT 0,
            current_value REAL DEFAULT 0,
            total_invested REAL DEFAULT 0,
            total_return REAL DEFAULT 0,
            return_pct REAL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS invest_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT,
            stock_name TEXT,
            asset_type TEXT,
            quantity REAL,
            action TEXT,
            price REAL,
            invested_value REAL,
            current_value REAL,
            profit REAL,
            profit_pct REAL,
            rationale TEXT,
            month TEXT
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
    conn.commit()

def import_income(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Income', header=2)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Salary Credit'].notna()].copy()

    rows = []
    income_cats = [
        ('Salary Credit',       'Salary',       'Salary Credit'),
        ('Incentive Bonus',     'Salary',       'Incentive / Bonus'),
        ('Rati',                'Income',       'Rati'),
        ('Family',              'Income',       'Family'),
        ('Investment Return',   'Investment Return', 'Returns'),
        ('Loan Return',         'Income',       'Loan Return'),
    ]
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        for col, cat, sub in income_cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('income', cat, sub, float(val), dt, f'Imported from Excel'))

    conn.executemany(
        "INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    print(f"✅ Income: {len(rows)} rows imported")

def import_expenses(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Expenses', header=3)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Actual  Expense'].notna() & (df['Actual  Expense'] > 0)].copy()

    exp_cats = [
        ('Kid',          'Family',       "Kids' Expenses"),
        ('Payments',     'Bills',        'Subscriptions & Payments'),
        ('HouseHold',    'Household',    'Household Expenses'),
        ('Transport',    'Transport',    'Transport'),
        ('Family Care',  'Family',       'Family Care'),
        ('Shopping',     'Shopping',     'Shopping'),
        ('Luxary',       'Lifestyle',    'Luxury & Entertainment'),
        ('Support',      'Family',       'Family Support'),
    ]
    rows = []
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        for col, cat, sub in exp_cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('expense', cat, sub, float(val), dt, 'Imported from Excel'))

    conn.executemany(
        "INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    print(f"✅ Expenses: {len(rows)} rows imported")

def import_loans(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Loan', header=3)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Total Loan'].notna() & (df['Total Loan'] > 0)].copy()

    loan_types = ['Home Loan','Personal Loan','Car Loan','NBFC Loan (LIC)',
                  'EPF Loan','Education Loan','Gadaget EMI','Credit Card EMI',
                  'Family loan','Credit Purchase','Secret Partner']

    rows = []
    tx_rows = []
    for _, row in df.iterrows():
        dt = str(row['Month'])[:10]
        month_str = dt[:7]
        for lt in loan_types:
            val = row.get(lt, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append((month_str, lt, float(val)))
                tx_rows.append(('expense', 'Loan EMI', lt, float(val), dt, 'Imported from Excel'))

    conn.executemany("INSERT INTO loans (month, loan_type, amount) VALUES (?,?,?)", rows)
    conn.executemany(
        "INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
        tx_rows
    )
    conn.commit()
    print(f"✅ Loans: {len(rows)} entries, {len(tx_rows)} transactions imported")

def import_investments(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Investment', header=5)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Entry Month'].apply(lambda x: hasattr(x, 'year'))].copy()
    df = df[df['Total Investment'].notna()].copy()

    inv_cats = [
        ('Stocks',             'Stocks',            'Direct Equity'),
        ('Mutual Fund',        'Mutual Fund',       'MF SIP'),
        ('Gold MF',            'Gold',              'Gold Mutual Fund'),
        ('Gold ETF',           'Gold',              'Gold ETF'),
        ('Gold',               'Gold',              'Physical Gold'),
        ('SGB',                'Gold',              'Sovereign Gold Bond'),
        ('EPF',                'Fixed Return',      'EPF Contribution'),
        ('NPS',                'Retirement',        'NPS Contribution'),
        ('Sukanya Samriddhi',  'Fixed Return',      'Sukanya Samriddhi'),
        ('Land',               'Real Estate',       'Land'),
        ('Flat',               'Real Estate',       'Flat'),
    ]
    rows = []
    for _, row in df.iterrows():
        dt = str(row['Entry Month'])[:10]
        for col, cat, sub in inv_cats:
            val = row.get(col, 0)
            if pd.notna(val) and float(val) > 0:
                rows.append(('investment', cat, sub, float(val), dt, 'Imported from Excel'))

    conn.executemany(
        "INSERT INTO transactions (type,category,sub_category,amount,date,note) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    print(f"✅ Investments: {len(rows)} rows imported")

def import_portfolio(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Corpus', header=2)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Asset'].notna() & df['Type'].notna()].copy()
    df = df[~df['Asset'].astype(str).str.lower().isin(['total','nan','asset'])].copy()

    rows = []
    for _, row in df.iterrows():
        asset = str(row.get('Asset','')).strip()
        atype = str(row.get('Type','')).strip()
        if not asset or asset == 'nan': continue
        def safe(v):
            try: return float(v) if pd.notna(v) else 0.0
            except: return 0.0
        rows.append((
            asset, atype,
            safe(row.get('Invested Till Oct 25', 0)),
            safe(row.get('Actual Till Oct-25', 0)),
            safe(row.get('Invested Since Nov-25', 0)),
            safe(row.get('Current Value', 0)),
            safe(row.get('Invested', 0)),
            safe(row.get('Return', 0)),
            safe(row.get('Return%', 0)) if str(row.get('Return%','')).replace('.','').replace('-','').isdigit() else 0.0,
        ))

    conn.executemany(
        """INSERT INTO portfolio
           (asset,asset_type,invested_pre_nov25,value_pre_nov25,invested_since_nov25,
            current_value,total_invested,total_return,return_pct)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows
    )
    conn.commit()
    print(f"✅ Portfolio: {len(rows)} assets imported")

def import_invest_transactions(conn, xlsx):
    df = pd.read_excel(xlsx, sheet_name='Investment Transactions', header=1)
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df['Entry Date'].apply(lambda x: hasattr(x, 'year'))].copy()

    rows = []
    for _, row in df.iterrows():
        def safe(v):
            try: return float(v) if pd.notna(v) else 0.0
            except: return 0.0
        rows.append((
            str(row['Entry Date'])[:10],
            str(row.get('Stock Name','')).strip(),
            str(row.get('Type','')).strip(),
            safe(row.get('Quantity')),
            str(row.get('Buy/Sell','')).strip(),
            safe(row.get('Price')),
            safe(row.get('Invested Value')),
            safe(row.get('Current Value')),
            safe(row.get('Profit')),
            safe(row.get('Profit %')),
            str(row.get('Rationale','')).strip(),
            str(row.get('Month','')).strip(),
        ))

    conn.executemany(
        """INSERT INTO invest_transactions
           (entry_date,stock_name,asset_type,quantity,action,price,invested_value,
            current_value,profit,profit_pct,rationale,month)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows
    )
    conn.commit()
    print(f"✅ Investment Transactions: {len(rows)} rows imported")

def seed_alerts(conn):
    alerts = [
        ('Monthly Household Budget', 'expense', 'category_exceeds', 50000, 'Household', 'monthly'),
        ('Monthly Transport Budget', 'expense', 'category_exceeds', 20000, 'Transport', 'monthly'),
        ('Monthly Shopping Budget',  'expense', 'category_exceeds', 15000, 'Shopping',  'monthly'),
        ('Net Savings Floor',        'savings', 'savings_below',    50000, None,         'monthly'),
        ('Monthly Investment Target','investment','total_below',     80000, None,         'monthly'),
        ('Loan EMI Alert',           'expense', 'category_exceeds', 200000,'Loan EMI',  'monthly'),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO alerts (name,type,condition,threshold,category,period) VALUES (?,?,?,?,?,?)",
        alerts
    )
    conn.commit()
    print(f"✅ Alerts: {len(alerts)} default alerts created")

if __name__ == '__main__':
    print(f"📂 Reading: {XLSX}")
    print(f"🗃  Database: {DB}")
    conn = get_db()
    init_schema(conn)

    # Clear existing sample data
    conn.execute("DELETE FROM transactions")
    conn.execute("DELETE FROM loans")
    conn.execute("DELETE FROM portfolio")
    conn.execute("DELETE FROM invest_transactions")
    conn.execute("DELETE FROM alerts")
    conn.commit()
    print("🗑  Cleared old data\n")

    import_income(conn, XLSX)
    import_expenses(conn, XLSX)
    import_loans(conn, XLSX)
    import_investments(conn, XLSX)
    import_portfolio(conn, XLSX)
    import_invest_transactions(conn, XLSX)
    seed_alerts(conn)

    total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    print(f"\n🎉 Done! {total} total transactions in DB")
    conn.close()
