# db.py
import streamlit as st
import mysql.connector

@st.cache_resource
def get_conn():
    cfg = st.secrets["mysql"]
    conn = mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        connection_timeout=10,
        charset="utf8mb4",
    )
    conn.ping(reconnect=True, attempts=3, delay=2)
    return conn

def run_q(sql, params=None):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close()
    return rows

def run_exec(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit()
    cur.close()
