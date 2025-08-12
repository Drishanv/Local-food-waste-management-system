import streamlit as st
import pandas as pd
from db import run_exec

st.title("📥 Bulk Import CSVs")

# ----- canonical schemas (DB column names) -----
CANON = {
    "providers": {
        "Provider_ID": ["provider_id","providerid","id"],
        "Name":        ["name","providername"],
        "Type":        ["type","providertype"],
        "Address":     ["address","addr","street"],
        "City":        ["city","locationcity"],
        "Contact":     ["contact","phone","phone_number","phonenumber","contactnumber","email"],
    },
    "receivers": {
        "Receiver_ID": ["receiver_id","receiverid","id"],
        "Name":        ["name","receivername"],
        "Type":        ["type"],
        "City":        ["city"],
        "Contact":     ["contact","phone","phone_number","phonenumber","contactnumber","email"],
    },
    "food_listings": {
        "Food_ID":       ["food_id","foodid","id"],
        "Food_Name":     ["food_name","foodname","name"],
        "Quantity":      ["quantity","qty","count"],
        "Expiry_Date":   ["expiry_date","expiredate","expdate","expiry"],
        "Provider_ID":   ["provider_id","providerid"],
        "Provider_Type": ["provider_type","providertype","type"],
        "Location":      ["location","city","area"],
        "Food_Type":     ["food_type","foodtype","category"],
        "Meal_Type":     ["meal_type","mealtype"],
    },
    "claims": {
        "Claim_ID":    ["claim_id","claimid","id"],
        "Food_ID":     ["food_id","foodid"],
        "Receiver_ID": ["receiver_id","receiverid"],
        "Status":      ["status"],
        "Timestamp":   ["timestamp","created_at","createdat","time"],
    },
}

def norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())

def make_mapper(table: str, cols):
    rev = {}
    for canon, alts in CANON[table].items():
        for a in [canon] + alts:
            rev[norm(a)] = canon
    mapping = {}
    for c in cols:
        mapping[c] = rev.get(norm(c))
    return mapping

def apply_mapping(df: pd.DataFrame, table: str) -> pd.DataFrame:
    mapping = make_mapper(table, df.columns)
    mapped = {src: dst for src, dst in mapping.items() if dst}
    df = df.rename(columns=mapped)
    required = set(CANON[table].keys())
    have = set(df.columns) & required
    missing = list(required - have)
    return df, mapped, missing

TABLE = st.selectbox("Target table", list(CANON.keys()), key="import_table")
file  = st.file_uploader(f"Upload CSV for {TABLE}", type=["csv"], key=f"upload_{TABLE}")

if file:
    df = pd.read_csv(file)
    df, mapped, missing = apply_mapping(df, TABLE)

    with st.expander("Column mapping preview", expanded=True):
        st.write("**Detected mapping (source → canonical):**")
        st.write({k: v for k, v in mapped.items()} if mapped else "(No columns mapped)")

        if missing:
            st.error(f"Missing required columns for `{TABLE}`: {missing}")
            st.info("Headers can be any case; underscores/spaces are ignored. Common synonyms like phone/contact_number are accepted.")
        else:
            st.success("All required columns present.")

        st.write("**First 5 rows (after renaming):**")
        st.dataframe(df.head(), use_container_width=True, key=f"preview_{TABLE}")

    if not missing and st.button("Insert / Upsert rows", key=f"import_btn_{TABLE}"):
        total = len(df)
        inserted = 0
        ph = st.empty()
        prog = ph.progress(0, text="Inserting...")

        def tick(i):
            prog.progress(min(i/total, 1.0), text=f"Inserting... {i}/{total}")

        if TABLE == "providers":
            rows = []
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                rows.append((
                    int(r["Provider_ID"]), str(r["Name"]), r.get("Type"),
                    r.get("Address"), r.get("City"), str(r.get("Contact"))
                ))
                inserted += 1
                if i % 25 == 0 or i == total: tick(i)
                run_exec("""
                    INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                    Address=VALUES(Address),City=VALUES(City),Contact=VALUES(Contact)
                """, rows.pop() )  # simple row-by-row; feel free to batch

        elif TABLE == "receivers":
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                run_exec("""
                    INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                    City=VALUES(City),Contact=VALUES(Contact)
                """, (int(r["Receiver_ID"]), str(r["Name"]), r.get("Type"),
                      r.get("City"), str(r.get("Contact"))))
                inserted += 1
                if i % 25 == 0 or i == total: tick(i)

        elif TABLE == "food_listings":
            if "Expiry_Date" in df.columns:
                df["Expiry_Date"] = pd.to_datetime(df["Expiry_Date"], errors="coerce").dt.date
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                run_exec("""
                    INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name),Quantity=VALUES(Quantity),
                    Expiry_Date=VALUES(Expiry_Date),Provider_ID=VALUES(Provider_ID),Provider_Type=VALUES(Provider_Type),
                    Location=VALUES(Location),Food_Type=VALUES(Food_Type),Meal_Type=VALUES(Meal_Type)
                """, (int(r["Food_ID"]), str(r["Food_Name"]), int(r.get("Quantity", 0)),
                      r.get("Expiry_Date"), int(r["Provider_ID"]), r.get("Provider_Type"),
                      r.get("Location"), r.get("Food_Type"), r.get("Meal_Type")))
                inserted += 1
                if i % 25 == 0 or i == total: tick(i)

        elif TABLE == "claims":
            if "Timestamp" in df.columns:
                df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                run_exec("""
                    INSERT INTO claims(Claim_ID,Food_ID,Receiver_ID,Status,Timestamp)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Food_ID=VALUES(Food_ID),Receiver_ID=VALUES(Receiver_ID),
                    Status=VALUES(Status),Timestamp=VALUES(Timestamp)
                """, (int(r["Claim_ID"]), int(r["Food_ID"]), int(r["Receiver_ID"]),
                      r.get("Status","Pending"), r.get("Timestamp")))
                inserted += 1
                if i % 25 == 0 or i == total: tick(i)

        ph.empty()
        st.success(f"Inserted/updated {inserted:,} rows into `{TABLE}`.")
        st.toast("Done!", icon="✅")
