import streamlit as st
import sqlite3
import pandas as pd

st.set_page_config(page_title="Dividend Growth 100", layout="wide")
st.title("🇯🇵 Dividend Growth 100")

DB = "database.db"

def load_docs():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM documents", conn)
    conn.close()
    return df

df = load_docs()

st.subheader("登録済み有報数")
st.metric("Documents", len(df))

st.dataframe(df.tail(20))
