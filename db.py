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

# -------- One-time schema (create if not exists) --------
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
