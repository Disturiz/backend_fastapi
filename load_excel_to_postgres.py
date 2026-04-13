import pandas as pd
from sqlalchemy import create_engine, text

EXCEL_FILE = r"C:\Users\distu\OneDrive\Documentos\Proyecto_Douglas\AnalisisDeVentas\VENTASPF.xlsx"
DB_USER = "postgres"
DB_PASSWORD = 1234
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ventaspf"

TABLE_NAME = "sales_transactions"

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(DATABASE_URL)

# Leer Excel
df = pd.read_excel(EXCEL_FILE)

print("Columnas detectadas:")
print(df.columns.tolist())

# Renombrar para que coincida con la tabla REAL de PostgreSQL
df = df.rename(
    columns={
        "ORDERID": "order_id",
        "FIRSTNAME": "first_name",
        "LASTNAME": "last_name",
        "PRODCODE": "prod_code",
        "PRODUCT": "product",
        "QTY": "qty",
        "COSTUNIT": "cost_unit",
        "PRICEUNIT": "price_unit",
        "TOTALCOST": "total_cost",
        "TOTALREV": "total_rev",
        "COUNTRY": "country",
        "SALESZONE": "sales_zone",
        "ORDERDATE": "order_date",
        "CUSTEMAIL": "cust_email",
        "CITY": "city",
        "PTYPCODE": "ptyp_code",
        "PTYPDESC": "ptyp_desc",
    }
)

required_columns = [
    "order_id",
    "first_name",
    "last_name",
    "prod_code",
    "product",
    "qty",
    "cost_unit",
    "price_unit",
    "total_cost",
    "total_rev",
    "country",
    "sales_zone",
    "order_date",
    "cust_email",
    "city",
    "ptyp_code",
    "ptyp_desc",
]

missing = [col for col in required_columns if col not in df.columns]
if missing:
    raise ValueError(f"Faltan columnas: {missing}")

df = df[required_columns].copy()

# Limpieza de tipos
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
df["cost_unit"] = pd.to_numeric(df["cost_unit"], errors="coerce")
df["price_unit"] = pd.to_numeric(df["price_unit"], errors="coerce")
df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce")
df["total_rev"] = pd.to_numeric(df["total_rev"], errors="coerce")

for col in [
    "order_id",
    "first_name",
    "last_name",
    "prod_code",
    "product",
    "country",
    "sales_zone",
    "cust_email",
    "city",
    "ptyp_code",
    "ptyp_desc",
]:
    df[col] = df[col].astype(str).str.strip()

df = df.dropna(
    subset=[
        "order_id",
        "product",
        "qty",
        "price_unit",
        "total_rev",
        "order_date",
        "cust_email",
    ]
)

print(f"Registros a insertar: {len(df)}")

# Vaciar tabla
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME};"))

# Insertar
df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)

print("Datos cargados correctamente.")

# Validación
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};"))
    print("Total registros en sales_transactions:", result.scalar())
