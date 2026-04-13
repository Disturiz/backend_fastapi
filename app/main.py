from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .db import execute, fetch_all, fetch_one
from .queries import (
    CATEGORY_SQL,
    COHORT_FILTERED_SQL,
    CREATE_TARGET_TABLE_SQL,
    FILTER_CATEGORIES_SQL,
    FILTER_OPTIONS_SQL,
    FILTER_YEARS_SQL,
    INSERT_FROM_SOURCE_SQL,
    JOURNEY_SQL,
    KPI_SQL,
    MONTHLY_SQL,
    REPEAT_SQL,
    TOP_PRODUCTS_SQL,
    ZONE_SQL,
)
from .settings import (
    IBMI_SOURCE_LIBRARY,
    IBMI_TARGET_LIBRARY,
    IBMI_SOURCE_TABLE,
    IBMI_TARGET_TABLE,
)

app = FastAPI(title="Customer Analytics API sobre IBM i", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def metric_params(zone: str, category: str, year: Optional[int]) -> list[object]:
    return [zone, zone, category, category, year, year]


def to_float(value: object) -> float:
    return float(value or 0)


def to_int(value: object) -> int:
    return int(value or 0)


@app.get("/health")
def health():
    sample = fetch_one(
        "SELECT CURRENT DATE AS server_date, CURRENT TIME AS server_time FROM SYSIBM/SYSDUMMY1"
    )
    return {
        "status": "ok",
        "server_date": str(sample["server_date"]),
        "server_time": str(sample["server_time"]),
    }


@app.post("/api/admin/refresh-salestrans")
def refresh_salestrans():
    try:
        try:
            execute(CREATE_TARGET_TABLE_SQL)
            created = True
        except Exception as exc:  # table may already exist
            message = str(exc).upper()
            if (
                "ALREADY EXISTS" in message
                or "SQLSTATE=42710" in message
                or "CPF7302" in message
            ):
                created = False
            else:
                raise
        target_full = f"{IBMI_TARGET_LIBRARY}.{IBMI_TARGET_TABLE}"

        execute(f"DELETE FROM {target_full}")
        execute(INSERT_FROM_SOURCE_SQL)
        total = fetch_one(f"SELECT COUNT(*) AS total FROM {target_full}")

        return {
            "message": "Tabla SALESTRANS sincronizada desde VENTASPF",
            "library": IBMI_TARGET_LIBRARY,
            "source_table": IBMI_SOURCE_TABLE,
            "target_table": IBMI_TARGET_TABLE,
            "table_created": created,
            "rows_loaded": to_int(total["total"]),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/filter-options")
def filter_options():
    zones = [row["value"] for row in fetch_all(FILTER_OPTIONS_SQL)]
    categories = [row["value"] for row in fetch_all(FILTER_CATEGORIES_SQL)]
    years = [str(row["value"]) for row in fetch_all(FILTER_YEARS_SQL)]
    return {
        "zones": ["All", *zones],
        "categories": ["All", *categories],
        "years": ["All", *years],
    }


@app.get("/api/kpis")
def kpis(
    zone: str = Query("All"),
    category: str = Query("All"),
    year: Optional[int] = Query(None),
):
    params = metric_params(zone, category, year)
    main = fetch_one(KPI_SQL, params) or {}
    repeat = fetch_one(REPEAT_SQL, params) or {}
    return {
        "revenue": to_float(main.get("revenue")),
        "cost": to_float(main.get("cost")),
        "orders": to_int(main.get("orders")),
        "customers": to_int(main.get("customers")),
        "aov": to_float(main.get("aov")),
        "gross_margin_pct": to_float(main.get("gross_margin_pct")),
        "repeat_customers": to_int(repeat.get("repeat_customers")),
        "repeat_rate": to_float(repeat.get("repeat_rate")),
    }


@app.get("/api/monthly-sales")
def monthly_sales(
    zone: str = Query("All"),
    category: str = Query("All"),
    year: Optional[int] = Query(None),
):
    rows = fetch_all(MONTHLY_SQL, metric_params(zone, category, year))
    return [
        {
            "month": r["month"],
            "revenue": to_float(r["revenue"]),
            "orders": to_int(r["orders"]),
            "customers": to_int(r["customers"]),
        }
        for r in rows
    ]


@app.get("/api/top-products")
def top_products(
    zone: str = Query("All"),
    category: str = Query("All"),
    year: Optional[int] = Query(None),
    limit: int = Query(8, ge=1, le=20),
):
    rows = fetch_all(TOP_PRODUCTS_SQL, [*metric_params(zone, category, year), limit])
    return [
        {
            "product": r["product"],
            "revenue": to_float(r["revenue"]),
            "orders": to_int(r["orders"]),
            "qty": to_int(r["qty"]),
        }
        for r in rows
    ]


@app.get("/api/revenue-by-zone")
def revenue_by_zone():
    rows = fetch_all(ZONE_SQL)
    return [{"zone": r["zone"], "revenue": to_float(r["revenue"])} for r in rows]


@app.get("/api/revenue-by-category")
def revenue_by_category():
    rows = fetch_all(CATEGORY_SQL)
    return [
        {"category": r["category"], "revenue": to_float(r["revenue"])} for r in rows
    ]


@app.get("/api/cohorts")
def cohorts(
    zone: str = Query("All"),
    category: str = Query("All"),
    year: Optional[int] = Query(None),
):
    rows = fetch_all(COHORT_FILTERED_SQL, metric_params(zone, category, year))
    return [
        {
            "cohort_month": r["cohort_month"],
            "month_index": to_int(r["month_index"]),
            "active_customers": to_int(r["active_customers"]),
            "cohort_size": to_int(r["cohort_size"]),
            "retention_pct": to_float(r["retention_pct"]),
        }
        for r in rows
    ]


@app.get("/api/journey")
def journey(limit: int = Query(15, ge=1, le=50)):
    rows = fetch_all(JOURNEY_SQL, [limit])
    return [
        {
            "source_product": r["source_product"],
            "target_product": r["target_product"],
            "transitions": to_int(r["transitions"]),
        }
        for r in rows
    ]
