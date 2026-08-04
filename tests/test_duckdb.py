"""Integration tests using DuckDB as the backend.

These tests mirror the Vertica integration tests but run entirely in-process
using DuckDB, requiring no external database server.

Run with: pytest tests/test_duckdb.py -m duckdb_integration
"""

import logging

import pandas as pd
import pytest

from dbdiff.duckdb_backend import (
    get_column_info,
    get_column_info_lookup,
    get_duckdb_cur,
    get_table_exists,
    implicit_dtype_comparison,
)
from dbdiff.main import (
    check_primary_key,
    create_joined_table,
    get_column_diffs_from_joined,
    get_diff_rows_from_joined,
    get_unmatched_rows,
    get_unmatched_rows_straight,
    select_distinct_rows,
    set_dialect,
)

pytestmark = pytest.mark.duckdb_integration

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

VALID_COL = {"comparable": True, "exclude": False}
INT_DTYPES = {d: "INTEGER" for d in ("x_dtype", "y_dtype")}
VARCHAR_DTYPES = {d: "VARCHAR" for d in ("x_dtype", "y_dtype")}
DATE_DTYPES = {d: "DATE" for d in ("x_dtype", "y_dtype")}
COMPARE_COLS = pd.DataFrame(
    {
        "data1": {**INT_DTYPES, **VALID_COL},
        "data2": {**INT_DTYPES, **VALID_COL},
        "data3": {**DATE_DTYPES, **VALID_COL},
        "data4": {**VARCHAR_DTYPES, **VALID_COL},
    }
).transpose()
JOIN_COLS = pd.DataFrame(
    {"join1": {**VARCHAR_DTYPES, **VALID_COL}, "join2": {**VARCHAR_DTYPES, **VALID_COL}}
).transpose()


@pytest.fixture(scope="session")
def cur():
    """Create a DuckDB in-memory cursor for the test session."""
    set_dialect("duckdb")
    with get_duckdb_cur() as c:
        yield c


def create_schema(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS dbdiff;")


def create_x_table(cur):
    cur.execute(
        "CREATE TABLE dbdiff.x_table ("
        " join1 VARCHAR, join2 VARCHAR, missingx INTEGER, missingx2 INTEGER,"
        " dtypemiss INTEGER, data1 INTEGER, data2 INTEGER, data3 DATE, data4 VARCHAR);"
    )
    cur.execute(
        "INSERT INTO dbdiff.x_table VALUES"
        " ('match1', 'matchdup21', 0, 0, 0, 0, 0, '2017-10-11', ''),"
        " ('match1', 'match22', 0, 0, 0, 0, 0, '2017-10-11', 'a'),"
        " ('match1', 'match23', 0, 0, 0, 1, 1, '2017-10-11', ''),"
        " ('match1', 'missx21', null, null, null, null, null, null, ''),"
        " ('match1', 'missx22', null, null, null, null, null, null, ''),"
        " ('missx11', null, null, null, null, null, null, null, ''),"
        " ('missx12', null, null, null, null, null, null, null, ''),"
        " (null, null, null, null, null, null, null, null, '');"
    )


def create_y_table(cur, case_off: bool = False):
    cur.execute(
        "CREATE TABLE dbdiff.y_table ("
        " join1 VARCHAR, join2 VARCHAR, missingy INTEGER,"
        " dtypemiss DATE, data1 INTEGER, data2 INTEGER, data3 DATE, data4 VARCHAR);"
    )
    data4_val = "A" if case_off else "a"
    cur.execute(
        "INSERT INTO dbdiff.y_table VALUES"
        " ('match1', 'matchdup21', 0, '2019-04-22', 0, 0, '2017-10-11', ''),"
        " ('match1', 'matchdup21', 0, '2019-04-22', 0, 0, '2017-10-11', ''),"
        f" ('match1', 'match22', 0, '2019-04-22', 0, 1, '2017-10-12', '{data4_val}'),"
        " ('match1', 'match23', 0, '2019-04-22', 0, 0, '2017-10-13', ''),"
        " ('match1', 'missy21', 0, '2019-04-22', 0, 0, null, ''),"
        " ('missy11', null, 0, '2019-04-22', 0, 0, null, '');"
    )


def drop_schema(cur):
    cur.execute("DROP SCHEMA IF EXISTS dbdiff CASCADE;")


def test_drop_data_start(cur):
    drop_schema(cur)


def test_create_data(cur):
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)


def test_get_column_info(cur):
    column_info = get_column_info(cur, "dbdiff", "x_table")
    assert isinstance(column_info, pd.DataFrame)
    assert "column_name" in column_info.columns
    assert "data_type" in column_info.columns


def test_get_column_info_lookup(cur):
    column_info_lookup = get_column_info_lookup(cur, "dbdiff", "x_table")
    assert "data1" in column_info_lookup
    assert len(column_info_lookup) == 9


def test_get_table_exists(cur):
    assert get_table_exists(cur, "dbdiff", "x_table")
    assert get_table_exists(cur, "dbdiff", "y_table")
    assert not get_table_exists(cur, "dbdiff", "z_table")


def test_implicit_dtype_comparison():
    assert implicit_dtype_comparison("INTEGER", "FLOAT")
    assert implicit_dtype_comparison("VARCHAR", "VARCHAR")
    assert not implicit_dtype_comparison("INTEGER", "DATE")


def test_check_primary_key(cur):
    assert check_primary_key(cur, "dbdiff", "x_table", ["join1", "join2"]) == 0
    assert check_primary_key(cur, "dbdiff", "x_table", ["join1"]) == 4


def test_select_distinct_rows(cur):
    x_table_rows = 8
    x_table_columns = 9
    new_table_schema, new_table_name = select_distinct_rows(cur, "dbdiff", "x_table", ["join1"])
    assert get_table_exists(cur, new_table_schema, "x_table_dedup")
    assert get_table_exists(cur, new_table_schema, "x_table_dup")
    cur.execute(f"SELECT * FROM {new_table_schema}.{new_table_name}")
    dedup = pd.DataFrame(cur.fetchall())
    assert dedup.shape[0] == 3
    assert dedup.shape[1] == x_table_columns
    cur.execute(f"SELECT * FROM {new_table_schema}.x_table_dup")
    dup = pd.DataFrame(cur.fetchall())
    assert dup.shape[0] == (x_table_rows - dedup.shape[0])
    assert dup.shape[1] == (x_table_columns + 1)


def test_create_joined_table(cur):
    create_joined_table(
        cur,
        x_schema="dbdiff",
        y_schema="dbdiff",
        x_table="x_table",
        y_table="y_table",
        join_cols=["join1", "join2"],
        compare_cols=pd.concat([COMPARE_COLS, JOIN_COLS]),
        joined_schema="dbdiff",
        joined_table="x_table_JOINED",
    )
    assert get_table_exists(cur, "dbdiff", "x_table_JOINED")
    cur.execute("SELECT * FROM dbdiff.x_table_JOINED")
    df = pd.DataFrame(cur.fetchall())
    assert df.shape[0] == 4
    assert df.shape[1] == ((COMPARE_COLS.shape[0] * 2) + JOIN_COLS.shape[0])


def test_get_unmatched_rows_straight(cur):
    join_cols = ["join1", "join2"]
    y_minus_x = [2, 1]
    x_minus_y = [3, 2]
    results = get_unmatched_rows_straight(
        cur, "dbdiff", "dbdiff", "x_table", "y_table", join_cols, 100
    )
    expected_results = {
        "x": {"count": sum(x_minus_y), "sample_shape": (sum(x_minus_y), len(join_cols))},
        "y": {"count": sum(y_minus_x), "sample_shape": (sum(y_minus_x), len(join_cols))},
    }
    assert results["x"]["count"] == expected_results["x"]["count"]
    assert results["x"]["sample"].shape[0] == expected_results["x"]["sample_shape"][0]
    assert results["x"]["sample"].shape[1] == expected_results["x"]["sample_shape"][1]
    assert results["y"]["count"] == expected_results["y"]["count"]
    assert results["y"]["sample"].shape[0] == expected_results["y"]["sample_shape"][0]
    assert results["y"]["sample"].shape[1] == expected_results["y"]["sample_shape"][1]


def test_get_unmatched_rows(cur):
    join_cols = ["join1", "join2"]
    y_minus_x = [2, 1]
    x_minus_y = [3, 2]
    results = get_unmatched_rows(cur, "dbdiff", "dbdiff", "x_table", "y_table", join_cols, 100)
    expected_results = {
        j: {
            side: {"count": d[i], "sample_shape": (d[i], i + 1)}
            for side, d in {"x": x_minus_y, "y": y_minus_x}.items()
        }
        for i, j in enumerate(join_cols)
    }
    for col, expected in expected_results.items():
        for side, expected_info in expected.items():
            assert results[col][side]["count"] == expected_info["count"]
            for i in (0, 1):
                assert results[col][side]["sample"].shape[i] == expected_info["sample_shape"][i]


def test_get_column_diffs_from_joined(cur):
    """Test getting column diffs directly from the joined table (no diff table)."""
    join_cols = ["join1", "join2"]

    all_col_info_df = pd.concat([COMPARE_COLS, JOIN_COLS])
    comparable_filter = (
        ~all_col_info_df.exclude
        & all_col_info_df.comparable
        & ~all_col_info_df.x_dtype.isnull()
        & ~all_col_info_df.y_dtype.isnull()
    )

    grouped_column_diffs = get_column_diffs_from_joined(
        cur=cur,
        output_schema="dbdiff",
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        join_cols=join_cols,
        max_rows_column=100,
        all_col_info_df=all_col_info_df,
        comparable_filter=comparable_filter,
        hierarchical=False,
    )
    # data1 has 1 diff, data2 has 2 diffs, data3 has 2 diffs
    assert len(grouped_column_diffs) > 0
    for col_name, info in grouped_column_diffs.items():
        assert "count" in info
        assert info["count"] > 0
        assert "df" in info
        assert "df_raw" in info


def test_get_diff_rows_from_joined(cur):
    """Test getting diff row summary from joined table."""
    join_cols = ["join1", "join2"]

    all_col_info_df = pd.concat([COMPARE_COLS, JOIN_COLS])
    comparable_filter = (
        ~all_col_info_df.exclude
        & all_col_info_df.comparable
        & ~all_col_info_df.x_dtype.isnull()
        & ~all_col_info_df.y_dtype.isnull()
    )

    grouped_column_diffs = get_column_diffs_from_joined(
        cur=cur,
        output_schema="dbdiff",
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        join_cols=join_cols,
        max_rows_column=100,
        all_col_info_df=all_col_info_df,
        comparable_filter=comparable_filter,
        hierarchical=False,
    )

    diff_summary = get_diff_rows_from_joined(
        cur=cur,
        grouped_column_diffs=grouped_column_diffs,
        output_schema="dbdiff",
        x_table="x_table",
        join_cols=join_cols,
        max_rows_all=100,
        skip_row_total=False,
    )
    assert "count" in diff_summary
    assert "total_count" in diff_summary
    assert diff_summary["total_count"] > 0


def test_main_end_to_end(cur):
    """Full end-to-end test using the main() function from cli.py."""
    from dbdiff.cli import main
    from dbdiff.report import html_report

    # Recreate tables to ensure clean state
    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    all_info = main(
        cur=cur,
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        output_schema="dbdiff",
        join_cols=["join1", "join2"],
        exclude_columns=set(),
        max_rows_all=10,
        max_rows_column=10,
        drop_output_tables=False,
        hierarchical_join=False,
        save_column_summary=False,
        save_column_summary_format="CSV",
        skip_row_total=False,
        use_diff_table=False,
        case_insensitive=False,
        dialect="duckdb",
    )

    assert "x_schema" in all_info
    assert "y_schema" in all_info
    assert "column_info" in all_info
    assert "diff_summary" in all_info
    # After dedup of y_table (which has duplicate matchdup21 rows),
    # only match22 and match23 remain as inner join matches
    assert all_info["total_row_count"] == 2

    # Generate HTML report
    report = html_report(**all_info)
    assert len(report) > 0
    assert "x_table" in report


def test_main_with_hierarchical_join(cur):
    """Test with hierarchical join enabled."""
    from dbdiff.cli import main

    # Recreate tables since previous test may have modified state
    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    all_info = main(
        cur=cur,
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        output_schema="dbdiff",
        join_cols=["join1", "join2"],
        exclude_columns=set(),
        max_rows_all=10,
        max_rows_column=10,
        drop_output_tables=False,
        hierarchical_join=True,
        save_column_summary=False,
        save_column_summary_format="CSV",
        skip_row_total=False,
        use_diff_table=False,
        case_insensitive=False,
        dialect="duckdb",
    )

    assert "hierarchical_join_info" in all_info
    assert len(all_info["hierarchical_join_info"]) > 0


def test_main_with_use_diff_table(cur):
    """Test with use_diff_table enabled."""
    from dbdiff.cli import main

    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    all_info = main(
        cur=cur,
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        output_schema="dbdiff",
        join_cols=["join1", "join2"],
        exclude_columns=set(),
        max_rows_all=10,
        max_rows_column=10,
        drop_output_tables=False,
        hierarchical_join=False,
        save_column_summary=False,
        save_column_summary_format="CSV",
        skip_row_total=False,
        use_diff_table=True,
        case_insensitive=False,
        dialect="duckdb",
    )

    assert "diff_summary" in all_info
    assert all_info["diff_summary"]["total_count"] > 0


def test_main_with_drop_output_tables(cur):
    """Test with drop_output_tables enabled."""
    from dbdiff.cli import main

    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    all_info = main(
        cur=cur,
        x_schema="dbdiff",
        x_table="x_table",
        y_schema="dbdiff",
        y_table="y_table",
        output_schema="dbdiff",
        join_cols=["join1", "join2"],
        exclude_columns=set(),
        max_rows_all=10,
        max_rows_column=10,
        drop_output_tables=True,
        hierarchical_join=False,
        save_column_summary=False,
        save_column_summary_format="CSV",
        skip_row_total=False,
        use_diff_table=False,
        case_insensitive=False,
        dialect="duckdb",
    )

    # After dedup of y_table (which has duplicate matchdup21 rows),
    # only match22 and match23 remain as inner join matches
    assert all_info["total_row_count"] == 2
    # Joined table should have been dropped
    assert not get_table_exists(cur, "dbdiff", "x_table_JOINED")


def test_drop_data_end(cur):
    drop_schema(cur)
