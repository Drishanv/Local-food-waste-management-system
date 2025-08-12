import streamlit as st
import pandas as pd
from db import run_exec

st.title("📥 Bulk Import CSVs")

MAP = {
    "providers": ["Provider_ID","Name","Type","Address","City","Contact"],
    "receivers": ["Receiver_ID","Name","Type","City","Contact"],
    "food_listings": ["Food_ID","Food_Name","Quantity","Expiry_Date","Provider_ID","Provider_Type","Location","Food_Type","Meal_Type"],
    "claims": ["Claim_ID","Food_ID","Receiver_ID","Status","Timestamp"],
}

table = st.selectbox("Target table", list(MAP.keys()))
file = st.file_uploader("Upload CSV for " + table, type=["csv"])

if file:
    df = pd.read_csv(file)
    required = MAP[table]
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"Missing columns: {missing}")
    else:
        # basic cleanup
        if "Expiry_Date" in df.columns:
            df["Expiry_Date"] = pd.to_datetime(df["Expiry_Date"], errors="coerce").dt.date
        if "Timestamp" in df.columns:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

        rows = df.to_dict(orient="records")
        with st.spinner("Inserting rows..."):
            inserted = 0
            if table == "providers":
                for r in rows:
                    run_exec("""
                        INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                        Address=VALUES(Address),City=VALUES(City),Contact=VALUES(Contact)
                    """, (int(r["Provider_ID"]), r["Name"], r.get("Type"), r.get("Address"), r.get("City"), str(r.get("Contact"))))
                    inserted += 1
            elif table == "receivers":
                for r in rows:
                    run_exec("""
                        INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                        VALUES (%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),City=VALUES(City),Contact=VALUES(Contact)
                    """, (int(r["Receiver_ID"]), r["Name"], r.get("Type"), r.get("City"), str(r.get("Contact"))))
                    inserted += 1
            elif table == "food_listings":
                for r in rows:
                    run_exec("""
                        INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name),Quantity=VALUES(Quantity),
                        Expiry_Date=VALUES(Expiry_Date),Provider_ID=VALUES(Provider_ID),Provider_Type=VALUES(Provider_Type),
                        Location=VALUES(Location),Food_Type=VALUES(Food_Type),Meal_Type=VALUES(Meal_Type)
                    """, (
                        int(r["Food_ID"]), r["Food_Name"], int(r.get("Quantity",0)), r.get("Expiry_Date"),
                        int(r["Provider_ID"]), r.get("Provider_Type"), r.get("Location"), r.get("Food_Type"), r.get("Meal_Type")
                    ))
                    inserted += 1
            elif table == "claims":
                for r in rows:
                    run_exec("""
                        INSERT INTO claims(Claim_ID,Food_ID,Receiver_ID,Status,Timestamp)
                        VALUES (%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Food_ID=VALUES(Food_ID),Receiver_ID=VALUES(Receiver_ID),
                        Status=VALUES(Status),Timestamp=VALUES(Timestamp)
                    """, (
                        int(r["Claim_ID"]), int(r["Food_ID"]), int(r["Receiver_ID"]),
                        r.get("Status","Pending"), r.get("Timestamp")
                    ))
                    inserted += 1
        st.success(f"Inserted/updated {inserted} rows into {table}.")
