# app.py — Streamlit + MySQL (Railway) using db.py helpers

import streamlit as st
import pandas as pd
import mysql.connector   # only for catching mysql errors in try/except
from urllib.parse import quote

from db import run_q as db_run_q, run_exec, ensure_schema

# ------------------------------------------------------------
# App setup
# ------------------------------------------------------------
st.set_page_config(page_title="Local Food Wastage Management", layout="wide")

# Run the schema creation safely (won't crash UI if DB had a transient hiccup)
try:
    ensure_schema()  # safe to run every start
except Exception as e:
    st.warning(f"Could not verify DB schema right now. If this persists, check DB: {e}")

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def run_q_df(sql, params=None):
    """Always return a pandas DataFrame, whether db_run_q returns list or DF."""
    rows = db_run_q(sql, params)
    return rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)

# Safer link builders (avoid throwing if contact is empty/odd)
def tel_link(num: str | int | None):
    """Return tel: link or None if no digits found."""
    if num is None:
        return None
    digits = "".join(ch for ch in str(num) if ch.isdigit())
    return f"tel:{digits}" if digits else None

def mailto_link(addr: str | None, subject="Food Donation", body="Hi, I’m interested."):
    """Return mailto: link or None if empty."""
    if not addr:
        return None
    return f"mailto:{addr}?subject={quote(subject)}&body={quote(body)}"

def wa_link(num: str | int | None, text="Hello, I’m interested in this food listing."):
    """Return WhatsApp link or None if no digits found."""
    if num is None:
        return None
    digits = "".join(ch for ch in str(num) if ch.isdigit())
    return f"https://wa.me/{digits}?text={quote(text)}" if digits else None

def next_claim_id():
    """Get the next integer Claim_ID even without AUTO_INCREMENT."""
    df = run_q_df("SELECT COALESCE(MAX(Claim_ID)+1,1) AS next_id FROM claims")
    return int(df.iloc[0]["next_id"]) if df is not None and not df.empty else 1

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("Local Food Wastage Management System")
page = st.sidebar.radio("Go to", ["Browse & Filter", "CRUD", "Reports & Insights"], key="nav_radio")

# ==================== BROWSE & FILTER ====================
if page == "Browse & Filter":
    st.subheader("Filter food donations")

    has_fl = run_q_df("SELECT COUNT(*) AS c FROM food_listings")
    locs = run_q_df("SELECT DISTINCT Location FROM food_listings ORDER BY 1")["Location"].tolist() if not has_fl.empty else []
    provs = run_q_df("SELECT provider_id, name FROM providers ORDER BY name")
    food_types = run_q_df("SELECT DISTINCT Food_Type FROM food_listings ORDER BY 1")["Food_Type"].tolist() if not has_fl.empty else []

    col1, col2, col3 = st.columns(3)
    sel_loc  = col1.selectbox("Location", ["All"] + locs, key="filter_loc")
    sel_prov = col2.selectbox("Provider", ["All"] + (provs["name"].tolist() if not provs.empty else []), key="filter_prov")
    sel_food = col3.selectbox("Food Type", ["All"] + food_types, key="filter_food")

    where, args = [], []
    if sel_loc  != "All": where.append("fl.Location = %s");  args.append(sel_loc)
    if sel_prov != "All": where.append("p.Name = %s");       args.append(sel_prov)
    if sel_food != "All": where.append("fl.Food_Type = %s"); args.append(sel_food)

    sql = """
        SELECT fl.Food_ID, fl.Food_Name, fl.Quantity, fl.Expiry_Date,
               fl.Provider_ID, p.Name AS Provider_Name, p.Type AS Provider_Type,
               fl.Location, fl.Food_Type, fl.Meal_Type, p.Contact
        FROM food_listings fl
        JOIN providers p ON p.Provider_ID = fl.Provider_ID
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY fl.Expiry_Date ASC"

    results = run_q_df(sql, args)
    st.success(f"{len(results)} matching listings found.")
    st.dataframe(results, use_container_width=True, key="df_results")

    # ---------- Contact Provider ----------
    st.markdown("### Contact provider for a selected Food_ID")
    selected_id = st.number_input("Enter Food_ID", step=1, min_value=0, key="bf_selected_id")

    if st.button("Show Contact Options", key="btn_show_contact"):
        row = results[results["Food_ID"] == selected_id]
        if row.empty:
            st.warning("Food_ID not in the filtered table above.")
        else:
            r = row.iloc[0]
            st.markdown(f"**Provider:** {r['Provider_Name']} ({r['Provider_Type']})")

            # contact can be phone or email; build safe links
            raw_contact = r.get("Contact")
            contact_str = "" if pd.isna(raw_contact) else str(raw_contact)

            tel  = tel_link(contact_str)
            mail = mailto_link(contact_str)
            wa   = wa_link(contact_str)

            c1, c2, c3 = st.columns(3)
            if tel:  c1.link_button("Call", tel)
            else:    c1.button("Call", disabled=True)

            if mail: c2.link_button("Email", mail)
            else:    c2.button("Email", disabled=True)

            if wa:   c3.link_button("WhatsApp", wa)
            else:    c3.button("WhatsApp", disabled=True)

    st.divider()
    st.subheader("Quick claim (demo)")
    fid = st.number_input("Food_ID to claim", step=1, min_value=0, key="claim_fid")
    rid = st.number_input("Receiver_ID", step=1, min_value=0, key="claim_rid")
    if st.button("Create Claim", key="btn_create_claim"):
        try:
            new_id = next_claim_id()
            run_exec(
                "INSERT INTO claims(Claim_ID, Food_ID, Receiver_ID, Status, Timestamp) VALUES (%s,%s,%s,%s,NOW())",
                (new_id, fid, rid, "Pending"),
            )
            st.success(f"Claim created (ID: {new_id}, status: Pending).")
        except mysql.connector.Error as e:
            st.error(f"Could not create claim: {e}")

# ==================== CRUD ====================
elif page == "CRUD":
    st.subheader("Create / Update / Delete records")
    table = st.selectbox("Choose table", ["providers", "receivers", "food_listings", "claims"], key="crud_table")

    if table == "providers":
        st.markdown("**Add / Update Provider**")
        pid     = st.number_input("Provider_ID", step=1, min_value=0, key="prov_pid")
        name    = st.text_input("Name", key="prov_name")
        typ     = st.text_input("Type", key="prov_type")
        addr    = st.text_input("Address", key="prov_addr")
        city    = st.text_input("City", key="prov_city")
        contact = st.text_input("Contact", key="prov_contact")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Provider", key="btn_upsert_provider"):
            run_exec("""
                INSERT INTO providers(Provider_ID,Name,Type,Address,City,Contact)
                VALUES(%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Name=VALUES(Name), Type=VALUES(Type),
                                         Address=VALUES(Address), City=VALUES(City), Contact=VALUES(Contact)
            """, (pid, name, typ, addr, city, contact))
            st.success("Upserted.")
        if c2.button("Delete Provider", key="btn_delete_provider"):
            run_exec("DELETE FROM providers WHERE Provider_ID=%s", (pid,))
            st.warning("Deleted (if existed).")

    if table == "receivers":
        st.markdown("**Add / Update Receiver**")
        rid     = st.number_input("Receiver_ID", step=1, min_value=0, key="rec_rid")
        name    = st.text_input("Name", key="rec_name")
        typ     = st.text_input("Type", key="rec_type")
        city    = st.text_input("City", key="rec_city")
        contact = st.text_input("Contact", key="rec_contact")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Receiver", key="btn_upsert_receiver"):
            run_exec("""
                INSERT INTO receivers(Receiver_ID,Name,Type,City,Contact)
                VALUES(%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Name=VALUES(Name), Type=VALUES(Type),
                                         City=VALUES(City), Contact=VALUES(Contact)
            """, (rid, name, typ, city, contact))
            st.success("Upserted.")
        if c2.button("Delete Receiver", key="btn_delete_receiver"):
            run_exec("DELETE FROM receivers WHERE Receiver_ID=%s", (rid,))
            st.warning("Deleted (if existed).")

    if table == "food_listings":
        st.markdown("**Add / Update Food Listing**")
        fid   = st.number_input("Food_ID", step=1, min_value=0, key="fl_fid")
        fname = st.text_input("Food_Name", key="fl_fname")
        qty   = st.number_input("Quantity", step=1, min_value=0, key="fl_qty")
        exp   = st.date_input("Expiry_Date", key="fl_exp")
        pid   = st.number_input("Provider_ID", step=1, min_value=0, key="fl_pid")
        ptype = st.text_input("Provider_Type", key="fl_ptype")
        loc   = st.text_input("Location", key="fl_loc")
        ftype = st.text_input("Food_Type", key="fl_ftype")
        mtype = st.text_input("Meal_Type", key="fl_mtype")
        c1, c2 = st.columns(2)
        if c1.button("Upsert Food", key="btn_upsert_food"):
            run_exec("""
                INSERT INTO food_listings(Food_ID,Food_Name,Quantity,Expiry_Date,Provider_ID,Provider_Type,Location,Food_Type,Meal_Type)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE Food_Name=VALUES(Food_Name), Quantity=VALUES(Quantity),
                    Expiry_Date=VALUES(Expiry_Date), Provider_ID=VALUES(Provider_ID), Provider_Type=VALUES(Provider_Type),
                    Location=VALUES(Location), Food_Type=VALUES(Food_Type), Meal_Type=VALUES(Meal_Type)
            """, (fid, fname, qty, exp, pid, ptype, loc, ftype, mtype))
            st.success("Upserted.")
        if c2.button("Delete Food", key="btn_delete_food"):
            run_exec("DELETE FROM food_listings WHERE Food_ID=%s", (fid,))
            st.warning("Deleted (if existed).")

    if table == "claims":
        st.markdown("**Add / Update Claim**")
        st.caption("Tip: leave Claim_ID as 0 to create a NEW claim. Any positive Claim_ID will UPDATE that claim.")
        cid    = st.number_input("Claim_ID (0 = create new)", step=1, min_value=0, value=0, key="cl_cid")
        fid    = st.number_input("Food_ID", step=1, min_value=0, key="cl_fid")
        rid    = st.number_input("Receiver_ID", step=1, min_value=0, key="cl_rid")
        status = st.selectbox("Status", ["Pending", "Completed", "Cancelled"], key="cl_status")
        c1, c2 = st.columns(2)
        if c1.button("Save Claim", key="btn_save_claim"):
            try:
                if cid == 0:
                    new_id = next_claim_id()
                    run_exec(
                        "INSERT INTO claims(Claim_ID, Food_ID, Receiver_ID, Status, Timestamp) VALUES (%s,%s,%s,%s,NOW())",
                        (new_id, fid, rid, status),
                    )
                    st.success(f"New claim created with Claim_ID {new_id}.")
                else:
                    run_exec(
                        "UPDATE claims SET Food_ID=%s, Receiver_ID=%s, Status=%s, Timestamp=NOW() WHERE Claim_ID=%s",
                        (fid, rid, status, cid),
                    )
                    st.success(f"Claim {cid} updated.")
            except mysql.connector.Error as e:
                st.error(f"Could not save claim: {e}")
        if c2.button("Delete Claim", key="btn_delete_claim"):
            if cid == 0:
                st.warning("Enter a Claim_ID > 0 to delete.")
            else:
                run_exec("DELETE FROM claims WHERE Claim_ID=%s", (cid,))
                st.warning(f"Claim {cid} deleted (if it existed).")

    st.divider()
    st.markdown("**Preview Table**")
    st.dataframe(run_q_df(f"SELECT * FROM {table} LIMIT 200"), use_container_width=True, key=f"crud_preview_{table}")

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

    city_for_q4 = st.text_input("For Query 4 (contacts by city), type a city exactly as in DB:", "", key="q4_city")

    for title, sql in QUERIES.items():
        st.markdown(f"**{title}**")
        if title.startswith("4.") and city_for_q4:
            df = run_q_df(sql, (city_for_q4,))
        elif title.startswith("4.") and not city_for_q4:
            st.info("Enter a city above to run this query.")
            continue
        else:
            df = run_q_df(sql)
        st.dataframe(df, use_container_width=True, key=f"report_{title[:2]}")
        st.caption(f"SQL: {sql.strip()[:200]}{'...' if len(sql.strip())>200 else ''}")
