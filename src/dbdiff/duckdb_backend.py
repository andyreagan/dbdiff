"""DuckDB backend for dbdiff.

Provides a cursor wrapper and helper functions that match the interface
used by the Vertica backend, allowing dbdiff to run entirely in-process
against DuckDB.
"""

import logging
from contextlib import contextmanager

import duckdb
import pandas as pd
from jinja2 import Environment, PackageLoader

JINJA_ENV = Environment(loader=PackageLoader("dbdiff", "templates"))
LOGGER = logging.getLogger(__name__)


class DuckDBCursorWrapper:
    """Wraps a duckdb.DuckDBPyConnection to provide a dict-cursor interface
    compatible with vertica_python's dict cursor."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn
        self._description = None

    def execute(self, sql: str) -> "DuckDBCursorWrapper":
        sql_stripped = sql.strip().rstrip(";").strip()
        # DuckDB auto-commits; skip explicit COMMIT statements
        if sql_stripped.upper() == "COMMIT":
            return self
        self._conn.execute(sql)
        self._description = self._conn.description
        return self

    def fetchall(self) -> list[dict]:
        if self._description is None:
            return []
        columns = [self._normalize_col_name(desc[0]) for desc in self._description]
        rows = self._conn.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _normalize_col_name(name: str) -> str:
        """Normalize DuckDB column names to match Vertica conventions.

        DuckDB uses 'count_star()' for COUNT(*), while Vertica uses 'COUNT'.
        """
        if name == "count_star()":
            return "COUNT"
        return name

    @property
    def description(self):
        return self._description


@contextmanager
def get_duckdb_cur(database: str = ":memory:"):
    """Context manager that yields a DuckDB cursor wrapper.

    Args:
        database: Path to DuckDB database file, or ":memory:" for in-memory.
    """
    conn = duckdb.connect(database)
    try:
        yield DuckDBCursorWrapper(conn)
    finally:
        conn.close()


def get_column_info(cur: DuckDBCursorWrapper, schema_name: str, table_name: str) -> pd.DataFrame:
    """Get column info from DuckDB's information_schema."""
    column_template = JINJA_ENV.get_template("table_columns.sql")
    cur.execute(
        column_template.render(schema_name=schema_name, table_name=table_name, dialect="duckdb")
    )
    df = pd.DataFrame(cur.fetchall())
    if df.shape[0] == 0:
        raise RuntimeError(f"{schema_name}.{table_name} has no columns.")
    return df


def get_column_info_lookup(cur: DuckDBCursorWrapper, schema_name: str, table_name: str) -> dict:
    x_table_info = get_column_info(cur, schema_name, table_name)
    LOGGER.info(
        "Column info DuckDB for "
        + schema_name
        + "."
        + table_name
        + ":\n"
        + x_table_info.head().to_string()
        + "\n..."
    )
    x_table_info["column_name"] = x_table_info["column_name"].str.lower()
    return {r.column_name: r.data_type for i, r in x_table_info.iterrows()}


def get_table_exists(cur: DuckDBCursorWrapper, schema_name: str, table_name: str) -> bool:
    table_exists_template = JINJA_ENV.get_template("table_exists.sql")
    cur.execute(
        table_exists_template.render(
            schema_name=schema_name, table_name=table_name, dialect="duckdb"
        )
    )
    result = cur.fetchall()
    return result[0]["COUNT"] == 1


def implicit_dtype_comparison(x_dtype: str, y_dtype: str) -> bool:
    """Can x_dtype be implicitly converted to y_dtype in DuckDB?

    DuckDB is generally more permissive with implicit casts than Vertica,
    but we use similar rules for consistency.
    """
    source = x_dtype.upper()
    target = y_dtype.upper()
    if "INT" in source:
        return (
            "BOOL" in target
            or "NUMERIC" in target
            or "FLOAT" in target
            or "INT" in target
            or "BIGINT" in target
        )
    elif "NUMERIC" in source or "DECIMAL" in source:
        return "FLOAT" in target or "NUMERIC" in target or "DECIMAL" in target
    elif "CHAR" in source or "VARCHAR" in source or "TEXT" in source:
        return "CHAR" in target or "VARCHAR" in target or "TEXT" in target or "FLOAT" in target
    elif "DATE" in source and "DATE" in target or "TIMESTAMP" in source and "TIMESTAMP" in target:
        return True
    else:
        return source == target
