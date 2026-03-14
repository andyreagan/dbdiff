"""Test that all SQL templates render and parse correctly with sqlglot."""

import pandas as pd
import pytest
import sqlglot
from jinja2 import Environment, PackageLoader

# Use postgres dialect for parsing since Vertica is based on PostgreSQL
# and sqlglot doesn't have a specific Vertica dialect
PARSE_DIALECT = "postgres"

# Mapping from dbdiff dialect to sqlglot parse dialect
DIALECT_TO_SQLGLOT = {
    "vertica": "postgres",
    "postgres": "postgres",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
}


@pytest.fixture
def jinja_env():
    """Create a Jinja2 environment for rendering templates."""
    env = Environment(loader=PackageLoader("dbdiff", "templates"))
    env.globals["dialect"] = "vertica"
    return env


def make_jinja_env(dialect):
    """Create a Jinja2 environment for a specific dialect."""
    env = Environment(loader=PackageLoader("dbdiff", "templates"))
    env.globals["dialect"] = dialect
    return env


@pytest.fixture
def sample_compare_cols():
    """Sample dataframe for compare_cols with multiple columns."""
    return pd.DataFrame(
        {
            "x_dtype": ["INTEGER", "VARCHAR(100)", "FLOAT", "DATE"],
            "y_dtype": ["INTEGER", "VARCHAR(100)", "FLOAT", "DATE"],
        },
        index=["id", "name", "amount", "created_at"],
    )


@pytest.fixture
def sample_join_cols():
    """Sample join column list."""
    return ["id"]


class TestSQLTemplateRendering:
    """Test that SQL templates render and parse correctly."""

    def test_table_rows(self, jinja_env):
        """Test table_rows.sql template."""
        template = jinja_env.get_template("table_rows.sql")
        sql = template.render(schema_name="test_schema", table_name="test_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_table_rows_uniq(self, jinja_env):
        """Test table_rows_uniq.sql template."""
        template = jinja_env.get_template("table_rows_uniq.sql")
        sql = template.render(
            schema_name="test_schema", table_name="test_table", join_cols="id, name"
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_table_exists(self, jinja_env):
        """Test table_exists.sql template."""
        template = jinja_env.get_template("table_exists.sql")
        sql = template.render(schema_name="test_schema", table_name="test_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_table_columns(self, jinja_env):
        """Test table_columns.sql template."""
        template = jinja_env.get_template("table_columns.sql")
        sql = template.render(schema_name="test_schema", table_name="test_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_table_drop(self, jinja_env):
        """Test table_drop.sql template."""
        template = jinja_env.get_template("table_drop.sql")
        sql = template.render(schema_name="test_schema", table_name="test_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_temp_table(self, jinja_env):
        """Test create_temp_table.sql template."""
        template = jinja_env.get_template("create_temp_table.sql")
        sql = template.render(table_name="temp_table", query="SELECT * FROM test_schema.test_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_dedup(self, jinja_env):
        """Test create_dedup.sql template."""
        template = jinja_env.get_template("create_dedup.sql")
        sql = template.render(
            schema_name="test_schema",
            table_name="test_table",
            table_name_dedup="test_table_dedup",
            group_cols="id",
            join_cols="x.id <=> y.id",
            use_temp_table=False,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_dup(self, jinja_env):
        """Test create_dup.sql template."""
        template = jinja_env.get_template("create_dup.sql")
        sql = template.render(
            schema_name="test_schema",
            table_name="test_table",
            table_name_dup="test_table_dup",
            group_cols="id",
            join_cols="x.id <=> y.id",
            use_temp_table=False,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_joined_table(self, jinja_env, sample_compare_cols, sample_join_cols):
        """Test create_joined_table.sql template."""
        template = jinja_env.get_template("create_joined_table.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=sample_compare_cols,
            join_cols=sample_join_cols,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_joined_table_from_selectinto(
        self, jinja_env, sample_compare_cols, sample_join_cols
    ):
        """Test create_joined_table_from_selectinto.sql template."""
        template = jinja_env.get_template("create_joined_table_from_selectinto.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=sample_compare_cols,
            join_cols=sample_join_cols,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_insert_joined_table(self, jinja_env, sample_compare_cols, sample_join_cols):
        """Test insert_joined_table.sql template."""
        template = jinja_env.get_template("insert_joined_table.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=sample_compare_cols,
            join_cols=sample_join_cols,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_all_keys_count(self, jinja_env, sample_join_cols):
        """Test all_keys_count.sql template."""
        template = jinja_env.get_template("all_keys_count.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=sample_join_cols,
            x=True,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_all_keys_sample(self, jinja_env, sample_join_cols):
        """Test all_keys_sample.sql template."""
        template = jinja_env.get_template("all_keys_sample.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=sample_join_cols,
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_first_key_count(self, jinja_env):
        """Test first_key_count.sql template."""
        template = jinja_env.get_template("first_key_count.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_col="id",
            x=True,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_first_key_sample(self, jinja_env):
        """Test first_key_sample.sql template."""
        template = jinja_env.get_template("first_key_sample.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_col="id",
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_count(self, jinja_env):
        """Test sub_keys_count.sql template."""
        template = jinja_env.get_template("sub_keys_count.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_sample(self, jinja_env):
        """Test sub_keys_sample.sql template."""
        template = jinja_env.get_template("sub_keys_sample.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_grouped(self, jinja_env):
        """Test sub_keys_grouped.sql template."""
        template = jinja_env.get_template("sub_keys_grouped.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_insert_diff(self, jinja_env, sample_join_cols):
        """Test insert_diff.sql template."""
        template = jinja_env.get_template("insert_diff.sql")
        sql = template.render(
            diff_schema="output_schema",
            diff_table="diff_table",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=sample_join_cols,
            column="amount",
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_rows_sample(self, jinja_env, sample_join_cols):
        """Test diff_rows_sample.sql template."""
        template = jinja_env.get_template("diff_rows_sample.sql")
        sql = template.render(
            schema_name="output_schema",
            joined_table="joined_table",
            diff_table="diff_table",
            group_cols="id",
            join_cols="x.id <=> joined.id",
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column_summary(self, jinja_env):
        """Test diff_column_summary.sql template."""
        template = jinja_env.get_template("diff_column_summary.sql")
        sql = template.render(schema_name="output_schema", table_name="diff_table")
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column(self, jinja_env, sample_join_cols):
        """Test diff_column.sql template."""
        template = jinja_env.get_template("diff_column.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            diff_schema="output_schema",
            diff_table="diff_table",
            group_cols="id",
            join_cols="diff.id <=> joined.id",
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column_raw(self, jinja_env, sample_join_cols):
        """Test diff_column_raw.sql template."""
        template = jinja_env.get_template("diff_column_raw.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            diff_schema="output_schema",
            diff_table="diff_table",
            join_cols=sample_join_cols,
            join_cols_join="diff.id <=> joined.id",
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column_hier(self, jinja_env, sample_join_cols):
        """Test diff_column_hier.sql template."""
        template = jinja_env.get_template("diff_column_hier.sql")
        sql = template.render(
            column="amount",
            diff_schema="output_schema",
            diff_table="diff_table",
            join_cols="id",
            first_join_col="id",
            schema="x_schema",
            table="x_table",
            limit=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column_numeric_diffs_binned(self, jinja_env, sample_join_cols):
        """Test diff_column_numeric_diffs_binned.sql template."""
        template = jinja_env.get_template("diff_column_numeric_diffs_binned.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            diff_schema="output_schema",
            diff_table="diff_table",
            group_cols="id",
            join_cols="diff.id <=> joined.id",
            tiles=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_diff_column_numeric_diffs_sorted(self, jinja_env, sample_join_cols):
        """Test diff_column_numeric_diffs_sorted.sql template."""
        template = jinja_env.get_template("diff_column_numeric_diffs_sorted.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            diff_schema="output_schema",
            diff_table="diff_table",
            join_cols=sample_join_cols,
            join_cols_join="diff.id <=> joined.id",
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_count(self, jinja_env):
        """Test joined_count.sql template."""
        template = jinja_env.get_template("joined_count.sql")
        sql = template.render(
            column="amount", joined_schema="output_schema", joined_table="joined_table"
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column(self, jinja_env):
        """Test joined_column.sql template."""
        template = jinja_env.get_template("joined_column.sql")
        sql = template.render(
            column="amount", joined_schema="output_schema", joined_table="joined_table"
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_raw(self, jinja_env, sample_join_cols):
        """Test joined_column_raw.sql template."""
        template = jinja_env.get_template("joined_column_raw.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=sample_join_cols,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_hier(self, jinja_env, sample_join_cols):
        """Test joined_column_hier.sql template."""
        template = jinja_env.get_template("joined_column_hier.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=sample_join_cols,
            schema="x_schema",
            table="x_table",
            limit=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_numeric_diffs_binned(self, jinja_env):
        """Test joined_column_numeric_diffs_binned.sql template."""
        template = jinja_env.get_template("joined_column_numeric_diffs_binned.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            tiles=10,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_numeric_diffs_sorted(self, jinja_env, sample_join_cols):
        """Test joined_column_numeric_diffs_sorted.sql template."""
        template = jinja_env.get_template("joined_column_numeric_diffs_sorted.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=sample_join_cols,
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_rows_count(self, jinja_env):
        """Test joined_rows_count.sql template."""
        template = jinja_env.get_template("joined_rows_count.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            columns=["amount", "name"],
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_rows_sample(self, jinja_env):
        """Test joined_rows_sample.sql template."""
        template = jinja_env.get_template("joined_rows_sample.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            columns=["amount", "name"],
        )
        sqlglot.parse(sql, dialect=PARSE_DIALECT, error_level=sqlglot.ErrorLevel.RAISE)


@pytest.fixture(params=["postgres", "duckdb", "sqlite"])
def dialect_env(request):
    """Parametrized fixture providing (jinja_env, sqlglot_dialect) for each target dialect."""
    dialect = request.param
    env = make_jinja_env(dialect)
    return env, DIALECT_TO_SQLGLOT[dialect]


@pytest.fixture
def dialect_compare_cols():
    """Sample dataframe for compare_cols with multiple columns."""
    return pd.DataFrame(
        {
            "x_dtype": ["INTEGER", "VARCHAR(100)", "FLOAT", "DATE"],
            "y_dtype": ["INTEGER", "VARCHAR(100)", "FLOAT", "DATE"],
        },
        index=["id", "name", "amount", "created_at"],
    )


@pytest.fixture
def dialect_join_cols():
    """Sample join column list."""
    return ["id"]


class TestDialectTemplateRendering:
    """Test that dialect-specific SQL templates render and parse correctly for postgres, duckdb, and sqlite."""

    def test_create_joined_table(self, dialect_env, dialect_compare_cols, dialect_join_cols):
        """Test create_joined_table.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("create_joined_table.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=dialect_compare_cols,
            join_cols=dialect_join_cols,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_create_joined_table_from_selectinto(
        self, dialect_env, dialect_compare_cols, dialect_join_cols
    ):
        """Test create_joined_table_from_selectinto.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("create_joined_table_from_selectinto.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=dialect_compare_cols,
            join_cols=dialect_join_cols,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_insert_joined_table(self, dialect_env, dialect_compare_cols, dialect_join_cols):
        """Test insert_joined_table.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("insert_joined_table.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            joined_schema="output_schema",
            joined_table="joined_table",
            compare_cols=dialect_compare_cols,
            join_cols=dialect_join_cols,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_all_keys_count(self, dialect_env, dialect_join_cols):
        """Test all_keys_count.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("all_keys_count.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=dialect_join_cols,
            x=True,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_all_keys_sample(self, dialect_env, dialect_join_cols):
        """Test all_keys_sample.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("all_keys_sample.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=dialect_join_cols,
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_count(self, dialect_env):
        """Test sub_keys_count.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("sub_keys_count.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_sample(self, dialect_env):
        """Test sub_keys_sample.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("sub_keys_sample.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_sub_keys_grouped(self, dialect_env):
        """Test sub_keys_grouped.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("sub_keys_grouped.sql")
        sql = template.render(
            x_schema="x_schema",
            y_schema="y_schema",
            x_table="x_table",
            y_table="y_table",
            join_cols=["id", "name"],
            x=True,
            max_rows_column=10,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_count(self, dialect_env):
        """Test joined_count.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_count.sql")
        sql = template.render(
            column="amount", joined_schema="output_schema", joined_table="joined_table"
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column(self, dialect_env):
        """Test joined_column.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_column.sql")
        sql = template.render(
            column="amount", joined_schema="output_schema", joined_table="joined_table"
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_raw(self, dialect_env, dialect_join_cols):
        """Test joined_column_raw.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_column_raw.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=dialect_join_cols,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_column_hier(self, dialect_env, dialect_join_cols):
        """Test joined_column_hier.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_column_hier.sql")
        sql = template.render(
            column="amount",
            joined_schema="output_schema",
            joined_table="joined_table",
            join_cols=dialect_join_cols,
            schema="x_schema",
            table="x_table",
            limit=10,
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_rows_count(self, dialect_env):
        """Test joined_rows_count.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_rows_count.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            columns=["amount", "name"],
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)

    def test_joined_rows_sample(self, dialect_env):
        """Test joined_rows_sample.sql renders valid SQL for target dialect."""
        env, parse_dialect = dialect_env
        template = env.get_template("joined_rows_sample.sql")
        sql = template.render(
            joined_schema="output_schema",
            joined_table="joined_table",
            columns=["amount", "name"],
        )
        sqlglot.parse(sql, dialect=parse_dialect, error_level=sqlglot.ErrorLevel.RAISE)
