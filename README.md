# 💰 Money Tracker

A personal finance tracker with a beautiful dark dashboard — tracking Income, Expenses, and Investments.

## Features
- 📊 **Interactive Dashboard** — Summary cards with month-over-month deltas, trend charts, and category breakdown
- 📋 **Transactions Table** — Add/delete/filter income, expense, investment entries
- 🔔 **Smart Alerts** — Configure threshold alerts (e.g., food budget > ₹12,000; savings < ₹20,000)
- 🗃 **SQLite Database** — Zero-config local storage
- 📅 **Month Picker** — View any historical month

## Setup

```bash
# 1. Install dependencies
pip install flask

# 2. Run the app
python app.py

# 3. Open in browser
# http://localhost:5000
```

## Data Structure

### Transactions
| Field | Type | Description |
|-------|------|-------------|
| type | TEXT | income / expense / investment |
| category | TEXT | Salary, Food, SIP, etc. |
| sub_category | TEXT | Optional detail |
| amount | REAL | Amount in ₹ |
| date | TEXT | YYYY-MM-DD format |
| note | TEXT | Optional description |

### Alerts
| Condition | Description |
|-----------|-------------|
| category_exceeds | Triggers when a category's spending exceeds limit |
| savings_below | Triggers when net savings < threshold |
| total_below | Triggers when total investment < threshold |

## Importing from Excel
You can run this one-time script to import from your Excel file:

```python
import pandas as pd, sqlite3

df = pd.read_excel('your_file.xlsx')
# Map columns to: type, category, sub_category, amount, date, note
conn = sqlite3.connect('money_tracker.db')
df.to_sql('transactions', conn, if_exists='append', index=False)
conn.close()
```
