from app.db import execute, fetch_one
from app.queries import CREATE_TARGET_TABLE_SQL, INSERT_FROM_SOURCE_SQL
from app.settings import IBMI_LIBRARY, IBMI_TARGET_TABLE


def main() -> None:
    try:
        execute(CREATE_TARGET_TABLE_SQL)
        print("Tabla SALESTRANS creada.")
    except Exception as exc:
        if (
            "ALREADY EXISTS" in str(exc).upper()
            or "SQLSTATE=42710" in str(exc).upper()
            or "CPF7302" in str(exc).upper()
        ):
            print("La tabla SALESTRANS ya existía. Se reutilizará.")
        else:
            raise

    execute(f"DELETE FROM {IBMI_LIBRARY}/{IBMI_TARGET_TABLE}")
    execute(INSERT_FROM_SOURCE_SQL)
    total = fetch_one(
        f"SELECT COUNT(*) AS total FROM {IBMI_LIBRARY}/{IBMI_TARGET_TABLE}"
    )
    print(f"Carga finalizada. Registros en SALESTRANS: {int(total['total'])}")


if __name__ == "__main__":
    main()
