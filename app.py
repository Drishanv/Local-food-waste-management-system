# app.py
import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error, InterfaceError
from urllib.parse import quote

st.set_page_config(page_title="Local Food Wastage Management", layout="wide")

# ---------- DB: connection via Streamlit Secrets (Railway) ----------
# Make sure your Streamlit Cloud Secrets contain:
# MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE
@st.cache_resource(show_spinner=False)
def get_conn():
    return mysql.connector.connect(
        host=st.secrets["MYSQLHOST"],
        port=int(st.secrets.get("MYSQLPORT", 3306)),
        user=st.secrets["MYSQLUSER"],
        password=st.secrets["MYSQLPASSWORD"],
        database=st.secrets["MYSQLDATABASE"],
        autocommit=False,
    )

def run_q(sql, params=None, many=False, commit=False):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if many and isinstance(params, list):
            cur.executemany(sql, params)
        else:
            cur.execute(sql, params or ())
        rows = None
        try:
            rows = cur.fetchall()
        except InterfaceError:
            # no result set (e.g., INSERT/UPDATE)
            pass
        if commit:
            conn.commit()
        return pd.DataFrame(rows) if rows is not None else None
    except Error as e:
        if commit:
            conn.rollback()
        st.error(f"SQL error: {e}")
        return pd.DataFrame()
    finally:
        cur.close()

# ---------- Small utilities ----------
def tel_link(num):      return f"tel:{num}"
def mailto_link(addr, subject="Food Donation", body="Hi, I’m interested."):
    return f"mailto:{addr}?subject={quote(subject)}&body={quote(body)}"
def wa_link(num, text="Hello, I’m interested in this food listing."):
    num = ''.join(ch for ch in str(num) if ch.isdigit())
    return f"https://wa.me/{num}?text={quote(text)}"

# ---------- Helper to get next Claim_ID (works even without AUTO_INCREMENT) ----------
def next_claim_id():
    df = run_q("SELECT COALESCE(MAX(Claim_ID)+1,1) AS next_id FROM claims")
    return int(df.iloc[0]["next_id"]) if df is not None and not df.empty else 1

# ---------- UI ----------
st.title("Local Food Wastage Management System")
page = st.sidebar.radio("Go to", ["Browse & Filter", "CRUD", "Reports & Insights"])

# ==================== BROWSE & FILTER ====================
if page == "Browse & Filter":
    st.subheader("Filter food donations")

    has_fl = run_q("SELECT COUNT(*) AS c FROM food_listings")
    locs = run_q("SELECT DISTINCT Location FROM food_listings ORDER BY 1")["Location"].tolist() if not has_fl.empty else []
    provs = run_q("SELECT provider_id, name FROM providers ORDER BY name")
    food_types = run_q("SELECT DISTINCT Food_Type FROM food_listings ORDER BY 1")["Food_Type"].tolist() if not has_fl.empty else []

    col1, col2, col3 = st.columns(3)
    sel_loc = col1.selectbox("Location", ["All"] + locs)
    sel_prov = col2.selectbox("Provider", ["All"] + (provs["name"].tolist() if not provs.empty else []))
    sel_food = col3.selectbox("Food Type", ["All"] + food_types)

    where, args = [], []
    if sel_loc != "All":  where.append("fl.Location = %s");     args.append(sel_loc)
    if sel_prov != "All": where.append("p.Name = %s");          args.append(sel_prov)
    if sel_food != "All": where.append("fl.Food_Type = %s");    args.append(sel_food)

    sql = """
        SELECT fl.Food_ID, fl.Food_Name, fl.Quantity, fl.Expiry_Date,
               fl.Provider_ID, p.Name AS Provider_Name, p.Type AS Provider_Type,
               fl.Location, fl.Food_Type, fl.Meal_Type, p.Contact
        FROM food_listings fl
        JOIN providers p ON p.Provider_ID = fl.Provider_ID
    """
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY fl.Expiry_Date ASC"

    results = run_q(sql, args)
    st.success(f"{len(results)} matching listings found.")
    st.dataframe(results, use_container_width=True)

    st.markdown("### Contact provider for a selected Food_ID")
    selected_id = st.number_input("Enter Food_ID", step=1, min_value=0)
    if st.button("Show Contact Options"):
        row = results[results["Food_ID"] == selected_id]
        if row.empty:
            st.warning("Food_ID not in the filtered table above.")
        else:
            r = row.iloc[0]
            st.markdown(f"**Provider:** {r['Provider_Name']} ({r['Provider_Type']})")
            contact = str(r["Contact"])
            c1, c2, c3 = st.columns(3)
            c1.link_button("Call", tel_link(contact))
            c2.link_button("Email", mailto_link(contact))
            c3.link_button("WhatsApp", wa_link(contact))

    st.divider()
    st.subheader("Quick claim (demo)")
    fid = st.number_input("Food_ID to claim", step=1, min_value=0, key="claim_fid")
    rid = st.number_input("Receiver_ID", step=1, min_value=0, key="claim_rid")
    if st.button("Create Claim"):
        try:
            new_id = next_claim_id()
            run_q(
                "INSERT INTO claims(Claim_ID, Food_ID, Receiver_ID, Status, Timestamp) VALUES (%s,%s,%s,%s,NOW())",
                params=(new_id, fid, rid, "Pending"),
                commit=True
            )
            st.success(f"Claim created (ID: {new_id}, status: Pending).")
        except mysql.connector.Error as e:
            st.error(f"Could not create claim: {e}")

# ==================== CRUD ====================
elif page == "CRUD":
    st.subheader("Create / Update / Delete records")
    table = st.selectbox("Choose table", ["providers", "receivers", "food_listings", "claims"])

    if table == "providers":
        st.markdown("**Add / Update Provider**")
        pid = st.number_input("Provider_ID", step=1, min_value=0)
        name = st.text_input("Name")
        typ = st.text_input("Type")
        addr = st.text_input("Address")
        city = st.text_input("City")
        contact = st.text_input("Contact")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Provider"):
            run_q("""
                INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                VALUES(%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Name=VALUES(Name), Type=VALUES(Type),
                                         Address=VALUES(Address), City=VALUES(City), Contact=VALUES(Contact)
            """, (pid, name, typ, addr, city, contact), commit=True)
            st.success("Upserted.")
        if c2.button("Delete Provider"):
            run_q("DELETE FROM providers WHERE Provider_ID=%s", (pid,), commit=True)
            st.warning("Deleted (if existed).")

    if table == "receivers":
        st.markdown("**Add / Update Receiver**")
        rid = st.number_input("Receiver_ID", step=1, min_value=0)
        name = st.text_input("Name", key="rname")
        typ = st.text_input("Type", key="rtype")
        city = st.text_input("City", key="rcity")
        contact = st.text_input("Contact", key="rcontact")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Receiver"):
            run_q("""
                INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                VALUES(%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Name=VALUES(Name), Type=VALUES(Type),
                                         City=VALUES(City), Contact=VALUES(Contact)
            """, (rid, name, typ, city, contact), commit=True)
            st.success("Upserted.")
        if c2.button("Delete Receiver"):
            run_q("DELETE FROM receivers WHERE Receiver_ID=%s", (rid,), commit=True)
            st.warning("Deleted (if existed).")

    if table == "food_listings":
        st.markdown("**Add / Update Food Listing**")
        fid = st.number_input("Food_ID", step=1, min_value=0)
        fname = st.text_input("Food_Name")
        qty = st.number_input("Quantity", step=1, min_value=0)
        exp = st.date_input("Expiry_Date")
        pid = st.number_input("Provider_ID", step=1, min_value=0, key="fl_pid")
        ptype = st.text_input("Provider_Type")
        loc = st.text_input("Location")
        ftype = st.text_input("Food_Type")
        mtype = st.text_input("Meal_Type")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Food"):
            run_q("""
                INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name), Quantity=VALUES(Quantity),
                    Expiry_Date=VALUES(Expiry_Date), Provider_ID=VALUES(Provider_ID), Provider_Type=VALUES(Provider_Type),
                    Location=VALUES(Location), Food_Type=VALUES(Food_Type), Meal_Type=VALUES(Meal_Type)
            """, (fid, fname, qty, exp, pid, ptype, loc, ftype, mtype), commit=True)
            st.success("Upserted.")
        if c2.button("Delete Food"):
            run_q("DELETE FROM food_listings WHERE Food_ID=%s", (fid,), commit=True)
            st.warning("Deleted (if existed).")

    if table == "claims":
        st.markdown("**Add / Update Claim**")
        st.caption("Tip: leave Claim_ID as 0 to create a NEW claim. Any positive Claim_ID will UPDATE that claim.")
        cid = st.number_input("Claim_ID (0 = create new)", step=1, min_value=0, value=0)
        fid = st.number_input("Food_ID", step=1, min_value=0, key="c_fid")
        rid = st.number_input("Receiver_ID", step=1, min_value=0, key="c_rid")
        status = st.selectbox("Status", ["Pending", "Completed", "Cancelled"])
        c1, c2 = st.columns(2)
        if c1.button("Save Claim"):
            try:
                if cid == 0:
                    new_id = next_claim_id()
                    run_q(
                        "INSERT INTO claims(Claim_ID, Food_ID, Receiver_ID, Status, Timestamp) VALUES (%s,%s,%s,%s,NOW())",
                        (new_id, fid, rid, status),
                        commit=True
                    )
                    st.success(f"New claim created with Claim_ID {new_id}.")
                else:
                    run_q(
                        "UPDATE claims SET Food_ID=%s, Receiver_ID=%s, Status=%s, Timestamp=NOW() WHERE Claim_ID=%s",
                        (fid, rid, status, cid),
                        commit=True
                    )
                    st.success(f"Claim {cid} updated.")
            except mysql.connector.Error as e:
                st.error(f"Could not save claim: {e}")
        if c2.button("Delete Claim"):
            if cid == 0:
                st.warning("Enter a Claim_ID > 0 to delete.")
            else:
                run_q("DELETE FROM claims WHERE Claim_ID=%s", (cid,), commit=True)
                st.warning(f"Claim {cid} deleted (if it existed).")

    st.divider()
    st.markdown("**Preview Table**")
    st.dataframe(run_q(f"SELECT * FROM {table} LIMIT 200"), use_container_width=True)

# ==================== REPORTS & INSIGHTS ====================
elif page == "Reports & Insights":
    st.subheader("SQL-powered insights (15 queries)")

    QUERIES = {
        "1. Providers per city":
            "SELECT city, COUNT(provider_id) AS no_of_food_providers FROM providers GROUP BY city ORDER BY 2 DESC;",
        "2. Receivers per city":
            "SELECT city, COUNT(receiver_id) AS no_of_receivers FROM receivers GROUP BY city ORDER BY 2 DESC;",
        "3. Top provider types by quantity":
            "SELECT provider_type, SUM(quantity) AS total_quantity FROM food_listings GROUP BY 1 ORDER BY 2 DESC;",
        "4. Provider contacts in a city (enter city below)":
            "SELECT name, type, address, city, contact FROM providers WHERE city = %s ORDER BY name;",
        "5. Top receivers by successful claims":
            "SELECT r.receiver_id, r.name, COUNT(*) AS successful_claims FROM claims c INNER JOIN receivers r ON r.receiver_id=c.receiver_id WHERE status='Completed' GROUP BY 1,2 ORDER BY 3 DESC;",
        "6. Total quantity available":
            "SELECT SUM(quantity) AS total_quantity_available FROM food_listings;",
        "7. Listings count by city":
            "SELECT location AS city, COUNT(*) AS listings_count FROM food_listings GROUP BY 1 ORDER BY 2 DESC;",
        "8. Most common food types":
            "SELECT food_type, COUNT(*) AS occurrences FROM food_listings GROUP BY 1 ORDER BY 2 DESC;",
        "9. Claims per food item":
            "SELECT fl.food_id, fl.food_name, COUNT(c.claim_id) AS claims_count FROM food_listings fl LEFT JOIN claims c ON c.food_id=fl.food_id GROUP BY 1,2 ORDER BY 3 DESC;",
        "10. Provider with most completed claims":
            "SELECT p.provider_id, p.name, COUNT(*) AS successful_claims FROM claims c JOIN food_listings fl ON fl.food_id=c.food_id JOIN providers p ON p.provider_id=fl.provider_id WHERE status='Completed' GROUP BY 1,2 ORDER BY 3 DESC;",
        "11. Claims status % split":
            "SELECT status, ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM claims),2) AS percentage FROM claims GROUP BY 1;",
        "12. Avg quantity claimed per receiver":
            """
            WITH successful_claims AS (
              SELECT receiver_id, food_id FROM claims WHERE status='Completed'
            )
            SELECT r.receiver_id, r.name, ROUND(AVG(fl.quantity),2) AS avg_quantity_claimed
            FROM successful_claims sc
            JOIN receivers r ON r.receiver_id=sc.receiver_id
            JOIN food_listings fl ON fl.food_id=sc.food_id
            GROUP BY 1,2 ORDER BY 3 DESC;
            """,
        "13. Most-claimed meal types":
            "SELECT fl.meal_type, COUNT(*) AS successful_claims FROM claims c JOIN food_listings fl ON fl.food_id=c.food_id WHERE status='Completed' GROUP BY 1 ORDER BY 2 DESC;",
        "14. Total quantity donated by provider":
            "SELECT p.provider_id, p.name, SUM(fl.quantity) AS total_qty_donated FROM food_listings fl INNER JOIN providers p ON p.provider_id=fl.provider_id GROUP BY 1,2 ORDER BY 3 DESC;",
        "15. Highest demand locations (completed claims)":
            "SELECT fl.Location, COUNT(*) AS completed_claims FROM claims c JOIN food_listings fl ON fl.food_id=c.food_id WHERE c.Status='Completed' GROUP BY fl.Location ORDER BY 2 DESC;",
        "16. Monthly claim trend (count)":
            "SELECT DATE_FORMAT(Timestamp, '%Y-%m') AS month, COUNT(*) AS claims FROM claims GROUP BY 1 ORDER BY 1;"
    }

    city_for_q4 = st.text_input("For Query 4 (contacts by city), type a city exactly as in DB:", "")

    for title, sql in QUERIES.items():
        st.markdown(f"**{title}**")
        if title.startswith("4.") and city_for_q4:
            df = run_q(sql, (city_for_q4,))
        elif title.startswith("4.") and not city_for_q4:
            st.info("Enter a city above to run this query.")
            continue
        else:
            df = run_q(sql)
        st.dataframe(df, use_container_width=True)
        st.caption(f"SQL: `{sql.strip()[:200]}{'...' if len(sql.strip()) > 200 else ''}`")
