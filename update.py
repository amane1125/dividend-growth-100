import requests
import sqlite3
from datetime import datetime, timedelta

DB = "database.db"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS documents(
        doc_id TEXT PRIMARY KEY,
        sec_code TEXT,
        period_end TEXT,
        processed INTEGER DEFAULT 0
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS financials(
        sec_code TEXT,
        year INTEGER,
        revenue REAL,
        operating REAL,
        net_income REAL,
        PRIMARY KEY(sec_code, year)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS dividends(
        sec_code TEXT,
        year INTEGER,
        dividend REAL,
        PRIMARY KEY(sec_code, year)
    )
    """)

    conn.commit()
    conn.close()


def fetch_documents(date_str):
    url = "https://disclosure.edinet-fsa.go.jp/api/v1/documents.json"
    params = {"date": date_str, "type": 2}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def save_new_documents(results):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    for r in results:
        # 120 = 有価証券報告書
        if r.get("docTypeCode") == "120":
            c.execute("""
            INSERT OR IGNORE INTO documents(doc_id, sec_code, period_end)
            VALUES(?,?,?)
            """, (
                r.get("docID"),
                r.get("secCode"),
                r.get("periodEnd")
            ))

    conn.commit()
    conn.close()


def main():
    init_db()

    # 直近30日分をスキャン（差分吸収）
    today = datetime.today()
    for i in range(30):
        d = today - timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        try:
            results = fetch_documents(date_str)
            save_new_documents(results)
            print(f"OK: {date_str}")
        except Exception as e:
            print(f"SKIP: {date_str} {e}")

if __name__ == "__main__":
    main()
