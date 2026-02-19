import requests
import sqlite3
import zipfile
import io
from datetime import datetime, timedelta
from lxml import etree

DB = "database.db"

# ========= DB =========

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


# ========= EDINET 一覧 =========

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
        if r.get("docTypeCode") == "120":  # 有価証券報告書
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


def get_unprocessed_docs():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT doc_id, sec_code, period_end FROM documents WHERE processed = 0")
    rows = c.fetchall()
    conn.close()
    return rows


# ========= XBRL取得 =========

def download_xbrl_zip(doc_id):
    url = f"https://disclosure.edinet-fsa.go.jp/api/v1/documents/{doc_id}"
    params = {"type": 1}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.content


def extract_main_xbrl(zip_bytes):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    for name in z.namelist():
        if name.endswith(".xbrl"):
            return z.read(name)
    return None


# ========= 財務抽出（軽量版） =========

def find_value(tree, keywords):
    for elem in tree.iter():
        tag = elem.tag.lower()
        for k in keywords:
            if k in tag:
                try:
                    return float(elem.text.replace(",", ""))
                except:
                    continue
    return None


def parse_financials(xbrl_bytes):
    tree = etree.fromstring(xbrl_bytes)

    revenue = find_value(tree, ["revenue", "netsales"])
    operating = find_value(tree, ["operatingincome"])
    net_income = find_value(tree, ["profitloss", "netincome"])
    dividend = find_value(tree, ["dividend"])

    return revenue, operating, net_income, dividend


# ========= DB保存 =========

def save_financials(sec_code, period_end, revenue, operating, net_income, dividend):
    year = int(period_end[:4])

    conn = sqlite3.connect(DB)
    c = conn.cursor()

    if revenue is not None:
        c.execute("""
        INSERT OR REPLACE INTO financials
        VALUES(?,?,?,?,?)
        """, (sec_code, year, revenue, operating, net_income))

    if dividend is not None:
        c.execute("""
        INSERT OR REPLACE INTO dividends
        VALUES(?,?,?)
        """, (sec_code, year, dividend))

    c.execute("""
    UPDATE documents SET processed = 1 WHERE sec_code=? AND period_end=?
    """, (sec_code, period_end))

    conn.commit()
    conn.close()


# ========= メイン =========

def main():
    init_db()

    # 直近30日スキャン
    today = datetime.today()
    for i in range(365 * 5):
        d = today - timedelta(days=i)
        try:
            results = fetch_documents(d.strftime("%Y-%m-%d"))
            save_new_documents(results)
        except:
            pass

    # 未処理だけ処理
    docs = get_unprocessed_docs()

    for doc_id, sec_code, period_end in docs:
        try:
            zip_bytes = download_xbrl_zip(doc_id)
            xbrl = extract_main_xbrl(zip_bytes)
            if xbrl:
                revenue, operating, net_income, dividend = parse_financials(xbrl)
                save_financials(sec_code, period_end, revenue, operating, net_income, dividend)
                print(f"Processed: {sec_code} {period_end}")
        except Exception as e:
            print(f"Error: {doc_id} {e}")


if __name__ == "__main__":
    main()
