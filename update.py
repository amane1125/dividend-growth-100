import requests
import sqlite3
import zipfile
import io
from datetime import datetime, timedelta
from lxml import etree

DB = "database.db"


# =========================
# DB初期化
# =========================

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


# =========================
# EDINET API取得
# =========================

def fetch_documents(date_str):
    url = "https://disclosure.edinet-fsa.go.jp/api/v1/documents.json"
    params = {"date": date_str, "type": 2}

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    data = r.json()
    results = data.get("results", [])

    print(f"[{date_str}] total results: {len(results)}")

    return results


# =========================
# 有報保存
# =========================

def save_new_documents(results):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    inserted = 0

    for r in results:
        doc_type = str(r.get("docTypeCode", ""))
        sec_code = r.get("secCode")
        period_end = r.get("periodEnd")

        # デバッグ表示（最初の数件だけ）
        # print("docType:", doc_type)

        # 有価証券報告書判定
        if doc_type.startswith("120"):

            if not sec_code:
                continue

            c.execute("""
            INSERT OR IGNORE INTO documents(doc_id, sec_code, period_end)
            VALUES(?,?,?)
            """, (
                r.get("docID"),
                sec_code,
                period_end
            ))

            inserted += 1

    conn.commit()
    conn.close()

    print(f"Inserted documents: {inserted}")


def get_unprocessed_docs():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT doc_id, sec_code, period_end FROM documents WHERE processed = 0")
    rows = c.fetchall()
    conn.close()
    return rows


# =========================
# XBRL取得
# =========================

def download_xbrl_zip(doc_id):
    url = f"https://disclosure.edinet-fsa.go.jp/api/v1/documents/{doc_id}"
    params = {"type": 1}

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def extract_main_xbrl(zip_bytes):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    for name in z.namelist():
        if name.endswith(".xbrl"):
            return z.read(name)
    return None


# =========================
# 財務抽出（簡易）
# =========================

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


# =========================
# DB保存
# =========================

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


# =========================
# メイン処理
# =========================

def main():
    init_db()

    print("===== Fetching documents =====")

    today = datetime.today()

    # 🔥 3年分スキャン
    for i in range(365 * 3):
        d = today - timedelta(days=i)

        try:
            results = fetch_documents(d.strftime("%Y-%m-%d"))
            save_new_documents(results)
        except Exception as e:
            print("Fetch error:", e)

    print("===== Processing XBRL =====")

    docs = get_unprocessed_docs()
    print("Unprocessed count:", len(docs))

    for doc_id, sec_code, period_end in docs:
        try:
            zip_bytes = download_xbrl_zip(doc_id)
            xbrl = extract_main_xbrl(zip_bytes)

            if xbrl:
                revenue, operating, net_income, dividend = parse_financials(xbrl)
                save_financials(sec_code, period_end, revenue, operating, net_income, dividend)
                print(f"Processed: {sec_code} {period_end}")
        except Exception as e:
            print(f"XBRL error: {doc_id} {e}")


if __name__ == "__main__":
    main()
