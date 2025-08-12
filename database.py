import re
import pandas as pd
import mysql.connector

def to_null(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN with None for MySQL NULLs."""
    return df.where(pd.notnull(df), None)

def canonize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names from CSVs:
    - Strip non-alphanumerics to underscores
    - Lowercase
    Examples: 'Provider_ID' -> 'provider_id', 'Food Name' -> 'food_name'
    """
    out = df.copy()
    out.columns = [re.sub(r'[^A-Za-z0-9]+', '_', c).strip('_').lower() for c in out.columns]
    return out

def rows(df: pd.DataFrame, cols: list):
    """Return list of tuples in the given column order, with a helpful error if missing."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns {missing}. CSV columns found: {list(df.columns)}")
    return list(df[cols].itertuples(index=False, name=None))

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Drishvig997@",  # consider env var instead
    database="food_waste_db"
)
cursor = conn.cursor()

try:
    # -------------------- PROVIDERS --------------------
    providers = pd.read_csv(r"C:\Users\Drishan\Downloads\providers_data.csv")
    print("Providers CSV:", providers.shape, list(providers.columns)[:10])
    print(providers.head(2))

    providers = canonize_cols(providers)         # <= makes 'Provider_ID' -> 'provider_id'
    providers = to_null(providers)

    prov_sql = """
        INSERT INTO providers (Provider_ID, Name, Type, Address, City, Contact)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          Name=VALUES(Name),
          Type=VALUES(Type),
          Address=VALUES(Address),
          City=VALUES(City),
          Contact=VALUES(Contact)
    """
    cursor.executemany(
        prov_sql,
        rows(providers, ['provider_id','name','type','address','city','contact'])
    )
    print("Providers upserted (including updates):", cursor.rowcount)

    # -------------------- RECEIVERS --------------------
    receivers = pd.read_csv(r"C:\Users\Drishan\Downloads\receivers_data.csv")
    print("Receivers CSV:", receivers.shape, list(receivers.columns)[:10])
    print(receivers.head(2))
    receivers = canonize_cols(receivers)         # 'Receiver_ID' -> 'receiver_id'
    receivers = to_null(receivers)

    recv_sql = """
        INSERT INTO receivers (Receiver_ID, Name, Type, City, Contact)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          Name=VALUES(Name),
          Type=VALUES(Type),
          City=VALUES(City),
          Contact=VALUES(Contact)
    """
    cursor.executemany(
        recv_sql,
        rows(receivers, ['receiver_id','name','type','city','contact'])
    )
    print("Receivers upserted (including updates):", cursor.rowcount)

    # -------------------- FOOD LISTINGS --------------------
    food = pd.read_csv(r"C:\Users\Drishan\Downloads\food_listings_data.csv")
    print("Food CSV:", food.shape, list(food.columns)[:12])
    print(food.head(2))
    food = canonize_cols(food)                   # 'Food_ID' -> 'food_id', 'Expiry_Date' -> 'expiry_date', etc.
    if 'expiry_date' in food.columns:
        food['expiry_date'] = pd.to_datetime(food['expiry_date']).dt.date
    food = to_null(food)

    food_sql = """
        INSERT INTO food_listings
          (Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          Food_Name=VALUES(Food_Name),
          Quantity=VALUES(Quantity),
          Expiry_Date=VALUES(Expiry_Date),
          Provider_ID=VALUES(Provider_ID),
          Provider_Type=VALUES(Provider_Type),
          Location=VALUES(Location),
          Food_Type=VALUES(Food_Type),
          Meal_Type=VALUES(Meal_Type)
    """
    cursor.executemany(
        food_sql,
        rows(food, ['food_id','food_name','quantity','expiry_date','provider_id','provider_type','location','food_type','meal_type'])
    )
    print("Food listings upserted (including updates):", cursor.rowcount)

    # -------------------- CLAIMS --------------------
    claims = pd.read_csv(r"C:\Users\Drishan\Downloads\claims_data.csv")
    claims = canonize_cols(claims)               # 'Claim_ID' -> 'claim_id', 'Timestamp' -> 'timestamp'
    print("Claims CSV:", claims.shape, list(claims.columns)[:10])
    print(claims.head(2))
    if 'timestamp' in claims.columns:
        claims['timestamp'] = pd.to_datetime(claims['timestamp'])
    claims = to_null(claims)

    claims_sql = """
        INSERT INTO claims (Claim_ID, Food_ID, Receiver_ID, Status, `Timestamp`)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          Food_ID=VALUES(Food_ID),
          Receiver_ID=VALUES(Receiver_ID),
          Status=VALUES(Status),
          `Timestamp`=VALUES(`Timestamp`)
    """
    cursor.executemany(
        claims_sql,
        rows(claims, ['claim_id','food_id','receiver_id','status','timestamp'])
    )
    print("Claims upserted (including updates):", cursor.rowcount)

    conn.commit()
    print("All done ✅")

except Exception as e:
    conn.rollback()
    print("Error:", e)
    raise
finally:
    cursor.close()
    conn.close()