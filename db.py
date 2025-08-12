# db.py  — robust pooling; fresh connection per call
import streamlit as st
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

def _cfg():
    s = st.secrets["mysql"]
    return {
        "host": s["host"],
        "port": int(s["port"]),
        "user": s["user"],
        "password": s["password"],
        "database": s["database"],
        "connection_timeout": 10,
        "charset": "utf8mb4",
    }

@st.cache_resource
def _pool():
    # Create once per session; connections drawn on demand
    return MySQLConnectionPool(pool_name="app_pool", pool_size=6, **_cfg())

def get_conn():
    conn = _pool().get_connection()
    # ensure the socket is alive; auto-reconnect if needed
    try:
        conn.ping(reconnect=True, attempts=3, delay=2)
    except Exception:
        # if ping failed, let connector re-open next time
        raise
    return conn

def run_q(sql, params=None):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()

def run_exec(sql, params=None):
    """
    params: tuple -> execute
            list[tuple] -> executemany
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        if isinstance(params, list):
            cur.executemany(sql, params)
        else:
            cur.execute(sql, params or ())
        conn.commit()
        cur.close()
    finally:
        conn.close()

# ---------- optional: schema helpers ----------
SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS providers (
      Provider_ID INT PRIMARY KEY,
      Name VARCHAR(100) NOT NULL,
      Type VARCHAR(50),
      Address VARCHAR(255),
      City VARCHAR(100),
      Contact VARCHAR(100)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS receivers (
      Receiver_ID INT PRIMARY KEY,
      Name VARCHAR(100) NOT NULL,
      Type VARCHAR(50),
      City VARCHAR(100),
      Contact VARCHAR(100)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS food_listings (
      Food_ID INT PRIMARY KEY,
      Food_Name VARCHAR(120) NOT NULL,
      Quantity INT DEFAULT 0,
      Expiry_Date DATE,
      Provider_ID INT NOT NULL,
      Provider_Type VARCHAR(50),
      Location VARCHAR(100),
      Food_Type VARCHAR(50),
      Meal_Type VARCHAR(50)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS claims (
      Claim_ID INT PRIMARY KEY,
      Food_ID INT NOT NULL,
      Receiver_ID INT NOT NULL,
      Status VARCHAR(20) DEFAULT 'Pending',
      Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    """
)

def ensure_schema():
    for stmt in SCHEMA:
        run_exec(stmt)
