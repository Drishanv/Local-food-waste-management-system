import streamlit as st
from db import run_exec, run_q

st.title("🔧 One-time DB Setup")

DDL = [
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
]

if st.button("Create/ensure tables"):
    for sql in DDL:
        run_exec(sql)
    st.success("Tables are ready.")

st.divider()
st.subheader("Optional: insert a few sample rows")
if st.button("Insert sample data"):
    run_exec("INSERT IGNORE INTO providers VALUES (1,'Food Bank','NGO','123 Main','Delhi','+911234567890')")
    run_exec("INSERT IGNORE INTO receivers VALUES (1,'Shelter A','NGO','Delhi','+911111111111')")
    run_exec("""
        INSERT IGNORE INTO food_listings
        (Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
        VALUES (1,'Veg Meals',20,DATE_ADD(CURDATE(), INTERVAL 2 DAY),1,'NGO','Delhi','Cooked','Lunch')
    """)
    run_exec("INSERT IGNORE INTO claims (Claim_ID, Food_ID, Receiver_ID, Status) VALUES (1,1,1,'Pending')")
    st.success("Sample rows inserted.")

st.divider()
st.write("Current tables:")
for r in run_q("SHOW TABLES"):
    st.write(list(r.values())[0])
import streamlit as st
from db import run_exec, run_q

st.title("🔧 One-time DB Setup")

DDL = [
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
]

if st.button("Create/ensure tables"):
    for sql in DDL:
        run_exec(sql)
    st.success("Tables are ready.")

st.divider()
st.subheader("Optional: insert a few sample rows")
if st.button("Insert sample data"):
    run_exec("INSERT IGNORE INTO providers VALUES (1,'Food Bank','NGO','123 Main','Delhi','+911234567890')")
    run_exec("INSERT IGNORE INTO receivers VALUES (1,'Shelter A','NGO','Delhi','+911111111111')")
    run_exec("""
        INSERT IGNORE INTO food_listings
        (Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
        VALUES (1,'Veg Meals',20,DATE_ADD(CURDATE(), INTERVAL 2 DAY),1,'NGO','Delhi','Cooked','Lunch')
    """)
    run_exec("INSERT IGNORE INTO claims (Claim_ID, Food_ID, Receiver_ID, Status) VALUES (1,1,1,'Pending')")
    st.success("Sample rows inserted.")

st.divider()
st.write("Current tables:")
for r in run_q("SHOW TABLES"):
    st.write(list(r.values())[0])
