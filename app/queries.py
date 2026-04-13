from __future__ import annotations

from .settings import (
    IBMI_SOURCE_LIBRARY,
    IBMI_TARGET_LIBRARY,
    IBMI_SOURCE_TABLE,
    IBMI_TARGET_TABLE,
)

SOURCE_FULL = f"{IBMI_SOURCE_LIBRARY}.{IBMI_SOURCE_TABLE}"
TARGET_FULL = f"{IBMI_TARGET_LIBRARY}.{IBMI_TARGET_TABLE}"

# -----------------------------------------------------------------------------
# FECHA SEGURA
# -----------------------------------------------------------------------------
# ORDER_DATE en SALESTRANS fue definido como DATE, pero por los errores vistos
# conviene blindar las consultas que dependen de fechas.
# Si ORDER_DATE ya es DATE, DATE(ORDER_DATE) seguirá funcionando.
SAFE_DATE_EXPR = "DATE(order_date)"

# -----------------------------------------------------------------------------
# FILTROS BASE
# -----------------------------------------------------------------------------
BASE_FILTER = f"""
WHERE (? = 'All' OR sales_zone = ?)
  AND (? = 'All' OR ptyp_desc = ?)
  AND (? IS NULL OR YEAR({SAFE_DATE_EXPR}) = ?)
"""

# -----------------------------------------------------------------------------
# KPI PRINCIPALES
# -----------------------------------------------------------------------------
KPI_SQL = f"""
SELECT
    COALESCE(SUM(total_rev), 0) AS revenue,
    COALESCE(SUM(total_cost), 0) AS cost,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT cust_email) AS customers,
    DECIMAL(
        COALESCE(SUM(total_rev), 0) / NULLIF(COUNT(DISTINCT order_id), 0),
        15,
        2
    ) AS aov,
    DECIMAL(
        (COALESCE(SUM(total_rev), 0) - COALESCE(SUM(total_cost), 0))
        / NULLIF(COALESCE(SUM(total_rev), 0), 0),
        15,
        4
    ) AS gross_margin_pct
FROM {TARGET_FULL}
{BASE_FILTER}
"""

REPEAT_SQL = f"""
WITH customer_orders AS (
    SELECT
        cust_email,
        COUNT(DISTINCT order_id) AS orders
    FROM {TARGET_FULL}
    {BASE_FILTER}
    GROUP BY cust_email
)
SELECT
    COALESCE(SUM(CASE WHEN orders >= 2 THEN 1 ELSE 0 END), 0) AS repeat_customers,
    DECIMAL(
        COALESCE(SUM(CASE WHEN orders >= 2 THEN 1 ELSE 0 END), 0)
        / NULLIF(COUNT(*), 0),
        15,
        4
    ) AS repeat_rate
FROM customer_orders
"""

# -----------------------------------------------------------------------------
# SERIES TEMPORALES
# -----------------------------------------------------------------------------
MONTHLY_SQL = f"""
SELECT
    CHAR(YEAR({SAFE_DATE_EXPR})) || '-' ||
    SUBSTR(
        '00' || CHAR(MONTH({SAFE_DATE_EXPR})),
        LENGTH('00' || CHAR(MONTH({SAFE_DATE_EXPR}))) - 1,
        2
    ) AS month,
    COALESCE(SUM(total_rev), 0) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT cust_email) AS customers
FROM {TARGET_FULL}
{BASE_FILTER}
GROUP BY
    CHAR(YEAR({SAFE_DATE_EXPR})) || '-' ||
    SUBSTR(
        '00' || CHAR(MONTH({SAFE_DATE_EXPR})),
        LENGTH('00' || CHAR(MONTH({SAFE_DATE_EXPR}))) - 1,
        2
    )
ORDER BY
    CHAR(YEAR({SAFE_DATE_EXPR})) || '-' ||
    SUBSTR(
        '00' || CHAR(MONTH({SAFE_DATE_EXPR})),
        LENGTH('00' || CHAR(MONTH({SAFE_DATE_EXPR}))) - 1,
        2
    )
"""

# -----------------------------------------------------------------------------
# TOP PRODUCTOS
# -----------------------------------------------------------------------------
TOP_PRODUCTS_SQL = f"""
SELECT
    product,
    COALESCE(SUM(total_rev), 0) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    COALESCE(SUM(qty), 0) AS qty
FROM {TARGET_FULL}
{BASE_FILTER}
GROUP BY product
ORDER BY revenue DESC, product
FETCH FIRST ? ROWS ONLY
"""

# -----------------------------------------------------------------------------
# DISTRIBUCIONES
# -----------------------------------------------------------------------------
ZONE_SQL = f"""
SELECT
    sales_zone AS zone,
    COALESCE(SUM(total_rev), 0) AS revenue
FROM {TARGET_FULL}
GROUP BY sales_zone
ORDER BY revenue DESC, zone
"""

CATEGORY_SQL = f"""
SELECT
    ptyp_desc AS category,
    COALESCE(SUM(total_rev), 0) AS revenue
FROM {TARGET_FULL}
GROUP BY ptyp_desc
ORDER BY revenue DESC, category
"""

# -----------------------------------------------------------------------------
# COHORT RETENTION
# -----------------------------------------------------------------------------
# Se simplifica la lógica de cohortes:
# 1. Se calcula safe_date una vez
# 2. cohort_month = primer mes de compra del cliente
# 3. activity_month = mes de la compra actual
# 4. month_index = diferencia en meses
COHORT_FILTERED_SQL = f"""
WITH filtered_sales AS (
    SELECT
        cust_email,
        order_id,
        {SAFE_DATE_EXPR} AS safe_date
    FROM {TARGET_FULL}
    {BASE_FILTER}
),
first_purchase AS (
    SELECT
        cust_email,
        MIN(safe_date) AS first_date
    FROM filtered_sales
    GROUP BY cust_email
),
cohort_base AS (
    SELECT
        f.cust_email,
        f.order_id,
        fp.first_date - (DAY(fp.first_date) - 1) DAYS AS cohort_month_date,
        f.safe_date - (DAY(f.safe_date) - 1) DAYS AS activity_month_date
    FROM filtered_sales f
    JOIN first_purchase fp
      ON f.cust_email = fp.cust_email
),
cohort_activity AS (
    SELECT
        cohort_month_date,
        activity_month_date,
        (
            (YEAR(activity_month_date) - YEAR(cohort_month_date)) * 12
            + (MONTH(activity_month_date) - MONTH(cohort_month_date))
        ) AS month_index,
        COUNT(DISTINCT cust_email) AS active_customers
    FROM cohort_base
    GROUP BY
        cohort_month_date,
        activity_month_date
),
cohort_size AS (
    SELECT
        cohort_month_date,
        COUNT(DISTINCT cust_email) AS cohort_size
    FROM cohort_base
    WHERE activity_month_date = cohort_month_date
    GROUP BY cohort_month_date
)
SELECT
    CHAR(YEAR(a.cohort_month_date)) || '-' ||
    SUBSTR(
        '00' || CHAR(MONTH(a.cohort_month_date)),
        LENGTH('00' || CHAR(MONTH(a.cohort_month_date))) - 1,
        2
    ) AS cohort_month,
    a.month_index,
    a.active_customers,
    s.cohort_size,
    DECIMAL(
        DECIMAL(a.active_customers, 15, 4)
        / NULLIF(DECIMAL(s.cohort_size, 15, 4), 0),
        15,
        4
    ) AS retention_pct
FROM cohort_activity a
JOIN cohort_size s
  ON a.cohort_month_date = s.cohort_month_date
ORDER BY
    a.cohort_month_date,
    a.month_index
"""

# -----------------------------------------------------------------------------
# PRODUCT JOURNEY
# -----------------------------------------------------------------------------
JOURNEY_SQL = f"""
WITH ordered_sales AS (
    SELECT
        s1.cust_email,
        s1.order_id,
        s1.product AS source_product,
        (
            SELECT s2.product
            FROM {TARGET_FULL} s2
            WHERE s2.cust_email = s1.cust_email
              AND (
                    DATE(s2.order_date) > DATE(s1.order_date)
                 OR (
                        DATE(s2.order_date) = DATE(s1.order_date)
                    AND s2.order_id > s1.order_id
                 )
              )
            ORDER BY DATE(s2.order_date), s2.order_id
            FETCH FIRST 1 ROW ONLY
        ) AS target_product
    FROM {TARGET_FULL} s1
)
SELECT
    source_product,
    target_product,
    COUNT(*) AS transitions
FROM ordered_sales
WHERE target_product IS NOT NULL
GROUP BY source_product, target_product
ORDER BY transitions DESC, source_product, target_product
FETCH FIRST ? ROWS ONLY
"""

# -----------------------------------------------------------------------------
# OPCIONES DE FILTRO
# -----------------------------------------------------------------------------
FILTER_OPTIONS_SQL = f"""
SELECT DISTINCT
    sales_zone AS value
FROM {TARGET_FULL}
WHERE sales_zone IS NOT NULL
  AND sales_zone <> ''
ORDER BY value
"""

FILTER_CATEGORIES_SQL = f"""
SELECT DISTINCT
    ptyp_desc AS value
FROM {TARGET_FULL}
WHERE ptyp_desc IS NOT NULL
  AND ptyp_desc <> ''
ORDER BY value
"""

FILTER_YEARS_SQL = f"""
SELECT DISTINCT
    YEAR({SAFE_DATE_EXPR}) AS value
FROM {TARGET_FULL}
WHERE order_date IS NOT NULL
ORDER BY value DESC
"""

# -----------------------------------------------------------------------------
# CREACIÓN DE TABLA DESTINO
# -----------------------------------------------------------------------------
CREATE_TARGET_TABLE_SQL = f"""
CREATE TABLE {TARGET_FULL} (
    ORDER_ID VARCHAR(36) NOT NULL,
    FIRST_NAME VARCHAR(30),
    LAST_NAME VARCHAR(30),
    PROD_CODE VARCHAR(10),
    PRODUCT VARCHAR(30) NOT NULL,
    QTY DECIMAL(5,0) NOT NULL,
    COST_UNIT DECIMAL(11,2) NOT NULL,
    PRICE_UNIT DECIMAL(11,2) NOT NULL,
    TOTAL_COST DECIMAL(13,2) NOT NULL,
    TOTAL_REV DECIMAL(13,2) NOT NULL,
    COUNTRY VARCHAR(30),
    SALES_ZONE VARCHAR(10),
    ORDER_DATE DATE NOT NULL,
    CUST_EMAIL VARCHAR(100) NOT NULL,
    CITY VARCHAR(40),
    PTYP_CODE VARCHAR(5),
    PTYP_DESC VARCHAR(50),
    PRIMARY KEY (ORDER_ID)
)
"""

# -----------------------------------------------------------------------------
# CARGA DESDE ORIGEN
# -----------------------------------------------------------------------------
INSERT_FROM_SOURCE_SQL = f"""
INSERT INTO {TARGET_FULL} (
    ORDER_ID,
    FIRST_NAME,
    LAST_NAME,
    PROD_CODE,
    PRODUCT,
    QTY,
    COST_UNIT,
    PRICE_UNIT,
    TOTAL_COST,
    TOTAL_REV,
    COUNTRY,
    SALES_ZONE,
    ORDER_DATE,
    CUST_EMAIL,
    CITY,
    PTYP_CODE,
    PTYP_DESC
)
SELECT
    TRIM(ORDERID),
    TRIM(FIRSTNAME),
    TRIM(LASTNAME),
    TRIM(PRODCODE),
    TRIM(PRODUCT),
    QTY,
    COSTUNIT,
    PRICEUNIT,
    TOTALCOST,
    TOTALREV,
    TRIM(COUNTRY),
    TRIM(SALESZONE),
    ORDERDATE,
    LOWER(TRIM(CUSTEMAIL)),
    TRIM(CITY),
    TRIM(PTYPCODE),
    TRIM(PTYPDESC)
FROM {SOURCE_FULL}
"""
