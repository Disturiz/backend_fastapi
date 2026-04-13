from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

import jaydebeapi
import jpype

from .settings import IBMI_JDBC_URL, IBMI_USER, IBMI_PASSWORD, JT400_JAR

JDBC_DRIVER = "com.ibm.as400.access.AS400JDBCDriver"
_JVM_LOCK = Lock()


def _ensure_credentials() -> None:
    if not IBMI_USER or not IBMI_PASSWORD:
        raise RuntimeError(
            "Faltan credenciales de IBM i. Configure IBMI_USER e IBMI_PASSWORD en el archivo .env"
        )

    if not IBMI_JDBC_URL:
        raise RuntimeError("Falta IBMI_JDBC_URL en el archivo .env")

    if not JT400_JAR:
        raise RuntimeError("Falta JT400_JAR en el archivo .env")


def _resolve_jar_path() -> str:
    jar_path = Path(JT400_JAR).expanduser()

    if not jar_path.is_absolute():
        jar_path = Path.cwd() / jar_path

    jar_path = jar_path.resolve()

    if not jar_path.exists():
        raise RuntimeError(f"No se encontró el driver JT400_JAR en la ruta: {jar_path}")

    return str(jar_path)


def _ensure_jvm_started(jar_path: str) -> None:
    if jpype.isJVMStarted():
        return

    with _JVM_LOCK:
        if jpype.isJVMStarted():
            return

        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            f"-Djava.class.path={jar_path}",
            convertStrings=True,
            ignoreUnrecognized=True,
        )


@contextmanager
def get_conn():
    _ensure_credentials()
    jar_path = _resolve_jar_path()
    _ensure_jvm_started(jar_path)

    conn = jaydebeapi.connect(
        JDBC_DRIVER,
        IBMI_JDBC_URL,
        [IBMI_USER, IBMI_PASSWORD],
    )

    try:
        yield conn
    finally:
        conn.close()


def _rows_to_dicts(cursor, rows: Iterable[tuple[Any, ...]]) -> list[dict[str, Any]]:
    columns = [col[0].lower() for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_one(sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            row = cur.fetchone()
            if row is None:
                return None
            return _rows_to_dicts(cur, [row])[0]
        finally:
            cur.close()


def fetch_all(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            rows = cur.fetchall()
            return _rows_to_dicts(cur, rows)
        finally:
            cur.close()


def execute(sql: str, params: list[Any] | None = None) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or [])
            conn.commit()
        finally:
            cur.close()
