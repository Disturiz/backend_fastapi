import os
import pandas as pd
from psycopg import connect

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/customer_analytics")
XLSX_PATH = os.getenv("XLSX_PATH", "VENTASPF.xlsx")

RENAME = {
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

def main():
    df = pd.read_excel(XLSX_PATH)
    df = df.rename(columns=RENAME)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date

    with connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE sales_transactions;")
        for row in df.to_dict(orient="records"):
            cur.execute(
                '''
                INSERT INTO sales_transactions (
                    order_id, first_name, last_name, prod_code, product, qty,
                    cost_unit, price_unit, total_cost, total_rev, country,
                    sales_zone, order_date, cust_email, city, ptyp_code, ptyp_desc
                )
                VALUES (
                    %(order_id)s, %(first_name)s, %(last_name)s, %(prod_code)s, %(product)s, %(qty)s,
                    %(cost_unit)s, %(price_unit)s, %(total_cost)s, %(total_rev)s, %(country)s,
                    %(sales_zone)s, %(order_date)s, %(cust_email)s, %(city)s, %(ptyp_code)s, %(ptyp_desc)s
                )
                ON CONFLICT (order_id) DO NOTHING
                ''',
                row,
            )
        conn.commit()
    print("Carga terminada.")

if __name__ == "__main__":
    main()
