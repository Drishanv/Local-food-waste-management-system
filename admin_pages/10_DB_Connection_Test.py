import streamlit as st
import mysql.connector

st.title("🧪 DB Connection Test")

@st.cache_resource
def get_conn():
    cfg = st.secrets["mysql"]
    return mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        connection_timeout=10,
    )

if st.button("Run test"):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        st.success("DB connection OK")
    except Exception as e:
        st.error(f"DB connection failed: {e}")
