import streamlit as st
import pandas as pd
from db import run_exec

st.title("📥 Bulk Import CSVs")

# -------- batching + tiny helpers --------
BATCH = 100  # lower to 50 if your DB proxy is flaky; raise to 200 if stable

def _truncate(v, n):
    if v is None:
        return None
    s = str(v)
    return s[:n]

def _clean(v):
    # Turn NaN/NaT into None so MySQL accepts them
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v

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
    # Build reverse lookup {normalized_source: canonical}
    rev = {}
    for canon, alts in CANON[table].items():
        for a in [canon] + alts:
            rev[norm(a)] = canon
    mapping = {}
    for c in cols:
        nc = norm(c)
        mapping[c] = rev.get(nc)  # None if unknown
    return mapping

def apply_mapping(df: pd.DataFrame, table: str) -> pd.DataFrame:
    mapping = make_mapper(table, df.columns)
    # columns we successfully mapped
    mapped = {src: dst for src, dst in mapping.items() if dst}
    df = df.rename(columns=mapped)
    required = set(CANON[table].keys())
    have = set(df.columns) & required
    missing = list(required - have)
    return df, mapped, missing

TABLE = st.selectbox("Target table", list(CANON.keys()))
file = st.file_uploader(f"Upload CSV for {TABLE}", type=["csv"])

if file:
    df = pd.read_csv(file)
    df, mapped, missing = apply_mapping(df, TABLE)

    with st.expander("Column mapping preview", expanded=True):
        st.write("**Detected mapping (source → canonical):**")
        if mapped:
            st.write({k: v for k, v in mapped.items()})
        else:
            st.write("(No columns mapped)")

        if missing:
            st.error(f"Missing required columns for `{TABLE}`: {missing}")
            st.info(
                "Tip: headers can be any case; underscores/spaces are ignored. "
                "Common synonyms like phone/contact_number are accepted."
            )
        else:
            st.success("All required columns present.")

        st.write("**First 5 rows (after renaming):**")
        st.dataframe(df.head())

    if not missing and st.button("Insert / Upsert rows"):
        total = len(df)
        prog = st.progress(0)
        status = st.empty()

        # ---------- PROVIDERS ----------
        if TABLE == "providers":
            rows = []
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                rows.append((
                    int(r["Provider_ID"]),
                    _truncate(_clean(r.get("Name")), 100),
                    _truncate(_clean(r.get("Type")), 50),
                    _truncate(_clean(r.get("Address")), 255),
                    _truncate(_clean(r.get("City")), 100),
                    _truncate(_clean(r.get("Contact")), 100),
                ))
                if len(rows) >= BATCH:
                    run_exec("""
                        INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                        Address=VALUES(Address),City=VALUES(City),Contact=VALUES(Contact)
                    """, rows)
                    rows.clear()
                if i % BATCH == 0 or i == total:
                    prog.progress(i/total)
                    status.write(f"Inserting providers… {i:,}/{total:,}")
            if rows:
                run_exec("""
                    INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                    Address=VALUES(Address),City=VALUES(City),Contact=VALUES(Contact)
                """, rows)

        # ---------- RECEIVERS ----------
        elif TABLE == "receivers":
            rows = []
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                rows.append((
                    int(r["Receiver_ID"]),
                    _truncate(_clean(r.get("Name")), 100),
                    _truncate(_clean(r.get("Type")), 50),
                    _truncate(_clean(r.get("City")), 100),
                    _truncate(_clean(r.get("Contact")), 100),
                ))
                if len(rows) >= BATCH:
                    run_exec("""
                        INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                        VALUES (%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                        City=VALUES(City),Contact=VALUES(Contact)
                    """, rows)
                    rows.clear()
                if i % BATCH == 0 or i == total:
                    prog.progress(i/total)
                    status.write(f"Inserting receivers… {i:,}/{total:,}")
            if rows:
                run_exec("""
                    INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Name=VALUES(Name),Type=VALUES(Type),
                    City=VALUES(City),Contact=VALUES(Contact)
                """, rows)

        # ---------- FOOD_LISTINGS ----------
        elif TABLE == "food_listings":
            if "Expiry_Date" in df.columns:
                df["Expiry_Date"] = pd.to_datetime(df["Expiry_Date"], errors="coerce").dt.date
            rows = []
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                rows.append((
                    int(r["Food_ID"]),
                    _truncate(_clean(r.get("Food_Name")), 120),
                    int(_clean(r.get("Quantity", 0)) or 0),
                    _clean(r.get("Expiry_Date")),
                    int(r["Provider_ID"]),
                    _truncate(_clean(r.get("Provider_Type")), 50),
                    _truncate(_clean(r.get("Location")), 100),
                    _truncate(_clean(r.get("Food_Type")), 50),
                    _truncate(_clean(r.get("Meal_Type")), 50),
                ))
                if len(rows) >= BATCH:
                    run_exec("""
                        INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name),Quantity=VALUES(Quantity),
                        Expiry_Date=VALUES(Expiry_Date),Provider_ID=VALUES(Provider_ID),Provider_Type=VALUES(Provider_Type),
                        Location=VALUES(Location),Food_Type=VALUES(Food_Type),Meal_Type=VALUES(Meal_Type)
                    """, rows)
                    rows.clear()
                if i % BATCH == 0 or i == total:
                    prog.progress(i/total)
                    status.write(f"Inserting food_listings… {i:,}/{total:,}")
            if rows:
                run_exec("""
                    INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name),Quantity=VALUES(Quantity),
                    Expiry_Date=VALUES(Expiry_Date),Provider_ID=VALUES(Provider_ID),Provider_Type=VALUES(Provider_Type),
                    Location=VALUES(Location),Food_Type=VALUES(Food_Type),Meal_Type=VALUES(Meal_Type)
                """, rows)

        # ---------- CLAIMS ----------
        elif TABLE == "claims":
            if "Timestamp" in df.columns:
                df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            rows = []
            for i, r in enumerate(df.to_dict(orient="records"), 1):
                rows.append((
                    int(r["Claim_ID"]),
                    int(r["Food_ID"]),
                    int(r["Receiver_ID"]),
                    _truncate(_clean(r.get("Status", "Pending")), 20),
                    _clean(r.get("Timestamp")),
                ))
                if len(rows) >= BATCH:
                    run_exec("""
                        INSERT INTO claims(Claim_ID,Food_ID,Receiver_ID,Status,Timestamp)
                        VALUES (%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE Food_ID=VALUES(Food_ID),Receiver_ID=VALUES(Receiver_ID),
                        Status=VALUES(Status),Timestamp=VALUES(Timestamp)
                    """, rows)
                    rows.clear()
                if i % BATCH == 0 or i == total:
                    prog.progress(i/total)
                    status.write(f"Inserting claims… {i:,}/{total:,}")
            if rows:
                run_exec("""
                    INSERT INTO claims(Claim_ID,Food_ID,Receiver_ID,Status,Timestamp)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE Food_ID=VALUES(Food_ID),Receiver_ID=VALUES(Receiver_ID),
                    Status=VALUES(Status),Timestamp=VALUES(Timestamp)
                """, rows)

        status.empty()
        prog.progress(1.0)
        st.success(f"Inserted/updated {total:,} rows into `{TABLE}`.")
        st.toast("Done!", icon="✅")
