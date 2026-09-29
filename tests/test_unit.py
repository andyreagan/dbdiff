"""Unit tests that do not require a running Vertica instance.

These cover pure Python logic: dtype helpers, implicit type coercion,
report formatting, SQL template rendering, and CLI surface checks.
"""

import json

import pandas as pd
import pytest
from click.testing import CliRunner
from jinja2 import Environment, PackageLoader

from dbdiff import __version__
from dbdiff.main import is_date_like, is_numeric_like
from dbdiff.report import (
    excel_report,
    get_max_diferences,
    html_report,
    reformat_hierarchical_join_info,
    reformat_missing_join_info,
)
from dbdiff.vertica import implicit_dtype_comparison

JINJA_ENV = Environment(loader=PackageLoader("dbdiff", "templates"))


# ---------------------------------------------------------------------------
# is_numeric_like / is_date_like
# ---------------------------------------------------------------------------
class TestIsNumericLike:
    @pytest.mark.parametrize(
        "dtype",
        ["int", "INT", "integer", "INTEGER", "float", "FLOAT", "numeric", "NUMERIC(18,2)"],
    )
    def test_numeric_types(self, dtype):
        assert is_numeric_like(dtype)

    @pytest.mark.parametrize("dtype", ["varchar(10)", "date", "boolean", "timestamp"])
    def test_non_numeric_types(self, dtype):
        assert not is_numeric_like(dtype)


class TestIsDateLike:
    @pytest.mark.parametrize("dtype", ["date", "DATE", "datetime", "timestampdate"])
    def test_date_types(self, dtype):
        assert is_date_like(dtype)

    @pytest.mark.parametrize("dtype", ["int", "varchar(10)", "float", "boolean"])
    def test_non_date_types(self, dtype):
        assert not is_date_like(dtype)


# ---------------------------------------------------------------------------
# implicit_dtype_comparison
# ---------------------------------------------------------------------------
class TestImplicitDtypeComparison:
    """Tests for the Vertica implicit data-type coercion chart."""

    # INT → ...
    @pytest.mark.parametrize("target", ["bool", "numeric", "float", "int", "integer"])
    def test_int_converts(self, target):
        assert implicit_dtype_comparison("int", target)

    def test_int_does_not_convert_to_varchar(self):
        assert not implicit_dtype_comparison("int", "varchar")

    def test_int_does_not_convert_to_date(self):
        assert not implicit_dtype_comparison("int", "date")

    # NUMERIC → ...
    def test_numeric_converts_to_float(self):
        assert implicit_dtype_comparison("numeric(18,2)", "float")

    def test_numeric_converts_to_numeric(self):
        assert implicit_dtype_comparison("numeric(18,2)", "numeric(10,0)")

    def test_numeric_does_not_convert_to_varchar(self):
        assert not implicit_dtype_comparison("numeric(18,2)", "varchar")

    # CHAR / VARCHAR → ...
    def test_char_converts_to_char(self):
        assert implicit_dtype_comparison("varchar(10)", "varchar(20)")

    def test_char_converts_to_float(self):
        assert implicit_dtype_comparison("varchar(10)", "float")

    def test_char_does_not_convert_to_int(self):
        assert not implicit_dtype_comparison("varchar(10)", "int")

    # DATE → DATE
    def test_date_converts_to_date(self):
        assert implicit_dtype_comparison("date", "date")

    def test_date_does_not_convert_to_int(self):
        assert not implicit_dtype_comparison("date", "int")

    # Same type always matches
    @pytest.mark.parametrize("dtype", ["int", "float", "date", "varchar(10)"])
    def test_same_type(self, dtype):
        assert implicit_dtype_comparison(dtype, dtype)

    # Symmetry-ish: at least one direction should work for compatible types
    def test_int_numeric_bidirectional(self):
        assert implicit_dtype_comparison("int", "numeric") or implicit_dtype_comparison(
            "numeric", "int"
        )


# ---------------------------------------------------------------------------
# SQL template rendering (smoke tests — templates load and render)
# ---------------------------------------------------------------------------
class TestTemplateRendering:
    def test_table_rows(self):
        sql = JINJA_ENV.get_template("table_rows.sql").render(
            schema_name="myschema", table_name="mytable"
        )
        assert "myschema" in sql
        assert "mytable" in sql
        assert "COUNT" in sql.upper() or "count" in sql

    def test_table_rows_uniq(self):
        sql = JINJA_ENV.get_template("table_rows_uniq.sql").render(
            schema_name="s", table_name="t", join_cols="a, b"
        )
        assert "a, b" in sql

    def test_table_exists(self):
        sql = JINJA_ENV.get_template("table_exists.sql").render(schema_name="s", table_name="t")
        assert "s" in sql and "t" in sql

    def test_table_columns(self):
        sql = JINJA_ENV.get_template("table_columns.sql").render(schema_name="s", table_name="t")
        assert "s" in sql and "t" in sql

    def test_table_drop(self):
        sql = JINJA_ENV.get_template("table_drop.sql").render(schema_name="s", table_name="t")
        assert "DROP" in sql.upper()

    def test_all_keys_count(self):
        sql = JINJA_ENV.get_template("all_keys_count.sql").render(
            x_schema="sx",
            y_schema="sy",
            x_table="xt",
            y_table="yt",
            join_cols=["a", "b"],
            x=True,
            max_rows_column=10,
        )
        assert "sx" in sql

    def test_first_key_count(self):
        sql = JINJA_ENV.get_template("first_key_count.sql").render(
            x_schema="sx",
            y_schema="sy",
            x_table="xt",
            y_table="yt",
            join_col="a",
            x=True,
            max_rows_column=10,
        )
        assert "sx" in sql

    def test_create_temp_table(self):
        sql = JINJA_ENV.get_template("create_temp_table.sql").render(
            table_name="t", query="SELECT 1"
        )
        assert "SELECT 1" in sql

    def test_insert_diff(self):
        sql = JINJA_ENV.get_template("insert_diff.sql").render(
            joined_schema="js",
            joined_table="jt",
            diff_schema="ds",
            diff_table="dt",
            join_cols=["a", "b"],
            column="data1",
        )
        assert "data1" in sql


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------
class TestGetMaxDifferences:
    def test_empty(self):
        assert get_max_diferences({}) == 0

    def test_single(self):
        assert get_max_diferences({"col1": {"count": 5}}) == 5

    def test_first_key_wins(self):
        # dict is ordered, first key is the expected max
        from collections import OrderedDict

        d = OrderedDict([("col1", {"count": 10}), ("col2", {"count": 3})])
        assert get_max_diferences(d) == 10


class TestReformatMissingJoinInfo:
    def test_basic(self):
        d = {"x": {"count": 1}, "y": {"count": 2}}
        result = reformat_missing_join_info(d, "tbl_x", "tbl_y")
        assert "tbl_x" in result
        assert "tbl_y" in result
        assert result["tbl_x"]["count"] == 1
        assert result["tbl_y"]["count"] == 2


class TestReformatHierarchicalJoinInfo:
    def test_basic(self):
        d = {
            "col1": {"x": {"count": 1}, "y": {"count": 2}},
            "col2": {"x": {"count": 3}, "y": {"count": 4}},
        }
        result = reformat_hierarchical_join_info(d, "tbl_x", "tbl_y")
        assert "col1" in result
        assert "tbl_x" in result["col1"]
        assert result["col2"]["tbl_y"]["count"] == 4


# ---------------------------------------------------------------------------
# Report generation (smoke tests with minimal data)
# ---------------------------------------------------------------------------
def _make_report_kwargs():
    """Build a minimal set of kwargs for html_report / excel_report."""
    column_match_info = pd.DataFrame(
        {
            "data1": {
                "x_dtype": "int",
                "y_dtype": "int",
                "comparable": True,
                "exclude": False,
                "uncomparable": False,
            },
        }
    ).transpose()
    # Make sure the boolean columns are actually bool type
    column_match_info["comparable"] = column_match_info["comparable"].astype(bool)
    column_match_info["exclude"] = column_match_info["exclude"].astype(bool)
    column_match_info["uncomparable"] = column_match_info["uncomparable"].astype(bool)

    return {
        "x_schema": "s",
        "y_schema": "s",
        "x_table": "x_tbl",
        "y_table": "y_tbl",
        "join_cols": ["join1"],
        "diff_summary": {"count": 0, "total_count": 0, "sample": pd.DataFrame()},
        "total_row_count": 100,
        "column_info": {},
        "column_match_info": column_match_info,
        "missing_join_info": {
            "x": {"count": 0, "sample": pd.DataFrame()},
            "y": {"count": 0, "sample": pd.DataFrame()},
        },
        "hierarchical_join_info": {},
        "dedup_info": {"x_tbl": {"count": 0}, "y_tbl": {"count": 0}},
    }


class TestHtmlReport:
    def test_renders_without_error(self):
        report = html_report(**_make_report_kwargs())
        assert isinstance(report, str)
        assert "x_tbl" in report
        assert "y_tbl" in report

    def test_with_column_diffs(self):
        kwargs = _make_report_kwargs()
        kwargs["column_info"] = {
            "data1": {
                "count": 2,
                "df": pd.DataFrame({"x_data1": [1, 2], "y_data1": [3, 4], "COUNT": [1, 1]}),
                "df_raw": pd.DataFrame({"join1": ["a", "b"], "x_data1": [1, 2], "y_data1": [3, 4]}),
                "q": "SELECT ...",
                "q_raw": "SELECT ...",
            }
        }
        report = html_report(**kwargs)
        assert "data1" in report


class TestExcelReport:
    def test_returns_sheets(self):
        sheets = excel_report(**_make_report_kwargs())
        assert isinstance(sheets, list)
        assert len(sheets) >= 1
        # first sheet is Summary
        assert sheets[0][0] == "Summary"
        assert isinstance(sheets[0][1], pd.DataFrame)

    def test_with_missing_rows(self):
        kwargs = _make_report_kwargs()
        kwargs["missing_join_info"]["x"]["count"] = 2
        kwargs["missing_join_info"]["x"]["sample"] = pd.DataFrame({"join1": ["a", "b"]})
        sheets = excel_report(**kwargs)
        sheet_names = [s[0] for s in sheets]
        assert "Missing in x" in sheet_names


# ---------------------------------------------------------------------------
# CLI surface test (--help, --version)
# ---------------------------------------------------------------------------
class TestCLI:
    def test_help(self):
        from dbdiff.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "SCHEMA" in result.output
        assert "X_TABLE" in result.output

    def test_version(self):
        from dbdiff.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output


# ---------------------------------------------------------------------------
# CLI helper: get_summary_from_all_info
# ---------------------------------------------------------------------------
class TestGetSummaryFromAllInfo:
    def test_basic(self):
        from dbdiff.cli import get_summary_from_all_info

        all_info = {
            "x_schema": "s",
            "x_table": "x",
            "y_schema": "s",
            "y_table": "y",
            "join_cols": ["a"],
            "total_row_count": 10,
            "dedup_info": {},
            "column_info": {
                "col1": {
                    "count": 1,
                    "df": pd.DataFrame({"a": [1]}),
                }
            },
            "column_match_info": pd.DataFrame({"x_dtype": ["int"], "y_dtype": ["int"]}),
            "missing_join_info": {
                "x": {"count": 0, "sample": pd.DataFrame()},
                "y": {"count": 0, "sample": pd.DataFrame()},
            },
            "diff_summary": {"count": 0, "total_count": 0},
            "hierarchical_join_info": {},
        }
        summary = get_summary_from_all_info(all_info)
        # Should be JSON-serializable (DataFrames converted to dicts)
        json.dumps(summary, default=str)
        assert summary["x_schema"] == "s"
        assert isinstance(summary["column_info"]["col1"]["df"], list)


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
def test_version_exists():
    assert isinstance(__version__, str)
    assert len(__version__) > 0
