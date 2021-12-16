import json
import logging
import logging.config
from pathlib import Path
from typing import Any

import click
import pandas as pd
from jinja2 import Environment, PackageLoader
from vertica_python.vertica.cursor import Cursor

from dbdiff import __version__
from dbdiff.main import (check_primary_key, create_diff_table,
                         create_joined_table, get_all_col_info,
                         get_column_diffs, get_column_diffs_from_joined,
                         get_diff_columns, get_diff_rows,
                         get_diff_rows_from_joined, get_unmatched_rows,
                         get_unmatched_rows_straight, insert_diff_table,
                         select_distinct_rows)
from dbdiff.report import excel_report, html_report
from dbdiff.vertica import get_cur

JINJA_ENV = Environment(loader=PackageLoader('dbdiff', 'templates'))
DEFAULT_LOGGING_CONFIG = Path(__file__).with_name('logging.json')
LOGGER = logging.getLogger(__name__)


def initialize_logging(config: Path) -> None:
    """
    Initialize logging configuration from JSON config file.
    """
    with config.open() as fobj:
        dict_config = json.load(fobj)
    logging.config.dictConfig(dict_config)


def df_to_dict(x: Any) -> Any:
    if type(x) == pd.DataFrame:
        return x.to_dict('records')
    else:
        return x


def get_summary_from_all_info(d: dict) -> dict:
    return {
        'x_schema': d['x_schema'],
        'x_table': d['x_table'],
        'y_schema': d['y_schema'],
        'y_table': d['y_table'],
        'join_cols': d['join_cols'],
        'total_row_count': d['total_row_count'],
        'dedup_info': d['dedup_info'],
        'column_info': {col: {k: df_to_dict(v) for k, v in info.items()} for col, info in d['column_info'].items()},
        # this is a dataframe:
        'column_match_info': df_to_dict(d['column_match_info']),
        'missing_join_info': {side: {k: df_to_dict(v) for k, v in info.items()} for side, info in d['missing_join_info'].items()},
        # just the counts from the diff summary:
        'diff_summary': d['diff_summary'],
        'hierarchical_join_info': {col: {side: {k: df_to_dict(v) for k, v in info.items()} for side, info in col_info.items()} for col, col_info in d['hierarchical_join_info'].items()}
    }


@click.command()
@click.argument('schema')
@click.argument('x_table')
@click.argument('y_table')
@click.argument('join_cols')
@click.option('--y-schema', default=None, help='If the schema for the y_table is different, specify it.')
@click.option('--output-schema', default=None, help='If you want the schema for the output tables to be different, specify it.')
@click.option('--drop-output-tables', is_flag=True, help='Drop the joined and diff tables created and used here.')
@click.option('--x-table-query', is_flag=True, help="If X_TABLE is not a table in Vertica, but rather a query stored in a file, add this flag and the query will be read and instantiated into a temporary table. Ex: 'temp_xtable_name_to_use.sql'.")
@click.option('--y-table-query', is_flag=True, help='If Y_TABLE is not a table in Vertica, but rather a query stored in a file, add this flag and the query will be read and instantiated into a temporary table.')
@click.option('--exclude-columns', default="", help='Comma separated string of column names to exclude.')
@click.option('--hierarchical-join', is_flag=True, help='If multiple join keys, and join key #2 is a subset of join key #1. We expect matches for all of #1 from both tables even if we dont match on #1 and #2. This way, we can have more nuanced output by first breaking out missing on the first key.')
@click.option('--max-rows-all', default=10, help='Limit of full rows to pull that have differences.', show_default=True)
@click.option('--max-rows-column', default=10, help='Limit of grouped and raw column level differences to pull.', show_default=True)
@click.option('--output-format', type=click.Choice(['HTML', 'XLSX'], case_sensitive=False), default="HTML")
@click.option('--save-column-summary', is_flag=True, help='Save the column dtype and match summary.')
@click.option('--save-column-summary-format', type=click.Choice(['CSV', 'PICKLE'], case_sensitive=False), default="CSV")
@click.option('--skip-row-total', is_flag=True, help='Skip counting the total # of rows with differences, only use cell differences.')
@click.option('--use-diff-table', is_flag=True, help='Use a diff table in the middle.')
@click.option('--logging-config', type=Path, default=DEFAULT_LOGGING_CONFIG)
@click.option('--case-insensitive', is_flag=True, help='If using this flag, all case sensitivity is turned off.')
@click.option('--save-json-summary', is_flag=True, help='Save a .json file of the diff summary.')
@click.version_option(__version__)
def cli(schema: str, x_table: str, y_table: str,
        join_cols: str, y_schema: str, output_schema: str, drop_output_tables: bool,
        x_table_query: bool, y_table_query: bool, exclude_columns: str,
        hierarchical_join: bool, max_rows_all: int, max_rows_column: int,
        output_format: str, save_column_summary: bool,
        save_column_summary_format: str, skip_row_total: bool,
        use_diff_table: bool, logging_config: Path, case_insensitive: bool,
        save_json_summary: bool):
    """Compare two flat files X_TABLE and Y_TABLE, using Vertica as the join engine.
    Assume they are both in the same schema = SCHEMA.
    Join them on the columns in comma-separated string JOIN_COLS.
    Expects that the join columns have matching data type or will implicitly cast for comparison,
    and implicity cast into the type in [X_TABLE] for the JOINED table.
    Expects that all other columns with matchings names (those that will be compared)
    can be compared directly (it will cast all dtypes for comparison to the type in X_TABLE).

    Will drop [X_TABLE]_DIFF (if --use-diff-table) and will drop [X_TABLE]_JOINED if they exist."""
    # default y_schema to be the same as x
    if y_schema is None:
        y_schema = schema
    if output_schema is None:
        output_schema = schema
    join_cols_list = list(map(lambda x: x.lower(), join_cols.split(',')))
    exclude_columns_set = set(map(lambda x: x.lower(), exclude_columns.split(',')))
    initialize_logging(logging_config)

    with get_cur() as cur:
        if x_table_query:
            with open(x_table, 'r') as f:
                q = f.read()
            x_table = Path(x_table).stem
            LOGGER.info('Creating temp table from query for x.')
            q_create = JINJA_ENV.get_template('create_temp_table.sql').render(table_name=x_table, query=q)
            LOGGER.info(q_create)
            cur.execute(q_create)
            schema = 'v_temp_schema'
        if y_table_query:
            with open(y_table, 'r') as f:
                q = f.read()
            y_table = Path(y_table).stem
            LOGGER.info('Creating temp table from query for y.')
            q_create = JINJA_ENV.get_template('create_temp_table.sql').render(table_name=y_table, query=q)
            LOGGER.info(q_create)
            cur.execute(q_create)
            y_schema = 'v_temp_schema'

        all_info = main(
            cur=cur,
            x_schema=schema,
            x_table=x_table,
            y_schema=y_schema,
            y_table=y_table,
            output_schema=output_schema,
            join_cols=join_cols_list,
            exclude_columns=exclude_columns_set,
            max_rows_all=max_rows_all,
            max_rows_column=max_rows_column,
            drop_output_tables=drop_output_tables,
            hierarchical_join=hierarchical_join,
            save_column_summary=save_column_summary,
            save_column_summary_format=save_column_summary_format,
            skip_row_total=skip_row_total,
            use_diff_table=use_diff_table,
            case_insensitive=case_insensitive
        )

    if output_format == 'HTML':
        report = html_report(**all_info)
        with open(x_table + '_report.html', 'w') as f:
            f.write(report)
    elif output_format == 'XLSX':
        reports = excel_report(**all_info)
        writer = pd.ExcelWriter(x_table + '_report.xlsx', engine='xlsxwriter')
        for sheet_name, df in reports:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        writer.save()

    if save_json_summary:
        # get the parts of the info that aren't dataframes
        summary_info = get_summary_from_all_info(all_info)
        Path(f'{x_table}_diff_summary.json').write_text(
            json.dumps(summary_info, indent=4, default=str)
        )


def main(cur: Cursor,
         x_schema: str, x_table: str,
         y_schema: str, y_table: str,
         output_schema: str,
         join_cols: list,
         exclude_columns: set,
         max_rows_all: int,
         max_rows_column: int,
         drop_output_tables: bool,
         hierarchical_join: bool,
         save_column_summary: bool,
         save_column_summary_format: str,
         skip_row_total: bool,
         use_diff_table: bool,
         case_insensitive: bool):
    '''Main method to be called by CLI.
    A separate function from cli() so that it can be imported easily as well.'''

    if case_insensitive:
        LOGGER.info('Setting to case insensitive.')
        cur.execute("SET LOCALE TO 'en_US@colstrength=1';")
        # clear the results
        cur.fetchall()

    all_col_info_df = get_all_col_info(
        cur,
        x_schema,
        x_table,
        y_schema,
        y_table,
        exclude_columns,
        save_column_summary,
        save_column_summary_format
    )
    comparable_filter = (~all_col_info_df.exclude & all_col_info_df.comparable & ~all_col_info_df.x_dtype.isnull() & ~all_col_info_df.y_dtype.isnull())
    all_col_info_df['uncomparable'] = (~all_col_info_df.comparable) & (~all_col_info_df.x_dtype.isnull()) & (~all_col_info_df.y_dtype.isnull())
    # check that the join cols exist on both tables
    for col in join_cols:
        if all_col_info_df.loc[comparable_filter & (all_col_info_df.index == col), :].shape[0] == 0:
            raise RuntimeError('Column `{0}` not in comparable columns (missing from one, both, or bad dtype). Here is the info we do have about that col:\n'.format(col) + all_col_info_df.loc[col, :].to_string())

    LOGGER.info('Checking primary keys.')
    x = check_primary_key(cur=cur, schema=x_schema, table=x_table, join_cols=join_cols)
    y = check_primary_key(cur=cur, schema=y_schema, table=y_table, join_cols=join_cols)
    dedup_info = {x_table: {'count': x}, y_table: {'count': y}}

    # hard stop on primary key:
    # assert x == 0, '# non distinct rows in ' + x_table + ' is ' + str(x)
    # assert y == 0, '# non distinct rows in ' + y_table + ' is ' + str(y)

    if hierarchical_join:
        LOGGER.info('Getting rows that are missing on each join key.')
        hierarchical_join_info = get_unmatched_rows(
            cur=cur,
            x_schema=x_schema,
            y_schema=y_schema,
            x_table=x_table,
            y_table=y_table,
            join_cols=join_cols,
            max_rows_column=max_rows_column
        )
    else:
        hierarchical_join_info = {}

    # create sub-tables to allow a comparison:
    if x != 0:
        LOGGER.info('X table was not unique on join keys, creating _dedup and _dup versions.')
        schema, x_table = select_distinct_rows(
            cur,
            x_schema,
            x_table,
            join_cols,
            use_temp_tables=(drop_output_tables or x_schema == 'v_temp_schema')
        )
    if y != 0:
        LOGGER.info('Y table was not unique on join keys, creating _dedup and _dup versions.')
        y_schema, y_table = select_distinct_rows(
            cur,
            y_schema,
            y_table,
            join_cols,
            use_temp_tables=(drop_output_tables or y_schema == 'v_temp_schema')
        )

    LOGGER.info('Getting rows that did not match (not in joined table) after deduping.')
    missing_join_info = get_unmatched_rows_straight(
        cur=cur,
        x_schema=x_schema,
        y_schema=y_schema,
        x_table=x_table,
        y_table=y_table,
        join_cols=join_cols,
        max_rows_column=max_rows_column
    )

    # build the joined table
    LOGGER.info('Building joined table ' + (x_table + '_JOINED'))
    joined_row_count = create_joined_table(
        cur=cur,
        x_schema=x_schema,
        y_schema=y_schema,
        x_table=x_table,
        y_table=y_table,
        join_cols=join_cols,
        compare_cols=all_col_info_df.loc[comparable_filter, :],
        joined_schema=output_schema,
        joined_table=(x_table + '_JOINED')
    )

    if use_diff_table:
        # build the diff table
        LOGGER.info('Building diff table ' + (x_table + '_DIFF.'))
        create_diff_table(
            cur=cur,
            schema=output_schema,
            table=(x_table + '_DIFF'),
            join_cols=join_cols,
            all_col_info_df=all_col_info_df
        )
        for column in all_col_info_df.loc[comparable_filter & ~all_col_info_df.index.isin(join_cols), :].index.values:
            LOGGER.info('Inserting column ' + column + ' into diff table.')
            insert_diff_table(
                cur=cur,
                joined_schema=output_schema,
                joined_table=(x_table + '_JOINED'),
                diff_schema=output_schema,
                diff_table=(x_table + '_DIFF'),
                join_cols=join_cols,
                column=column
            )

        ############################################################################
        # Result 1: Get rows with at least N=1 difference (count, query, dataframe),
        ############################################################################
        diff_summary = get_diff_rows(cur, output_schema, x_table, join_cols, max_rows_all, skip_row_total)

        ############################################################################
        # Result 2: Get ordered list of columns by # of differences (query, dataframe).
        ############################################################################
        diff_columns = get_diff_columns(cur, output_schema, x_table)

        ############################################################################
        # Result 3: Get detailed column diffs.
        ############################################################################
        grouped_column_diffs = get_column_diffs(diff_columns, cur, output_schema, x_schema, x_table, y_schema, y_table, join_cols, max_rows_column, all_col_info_df, hierarchical_join)

    else:
        grouped_column_diffs = get_column_diffs_from_joined(
            cur=cur,
            output_schema=output_schema,
            x_schema=x_schema,
            x_table=x_table,
            y_schema=y_schema,
            y_table=y_table,
            join_cols=join_cols,
            max_rows_column=max_rows_column,
            all_col_info_df=all_col_info_df,
            comparable_filter=comparable_filter,
            hierarchical=hierarchical_join
        )
        diff_summary = get_diff_rows_from_joined(
            cur=cur,
            grouped_column_diffs=grouped_column_diffs,
            output_schema=output_schema,
            x_table=x_table,
            join_cols=join_cols,
            max_rows_all=max_rows_all,
            skip_row_total=skip_row_total
        )

    all_info = {
        'x_schema': x_schema,
        'y_schema': y_schema,
        'x_table': x_table,
        'y_table': y_table,
        'join_cols': join_cols,
        'total_row_count': joined_row_count,
        'column_info': grouped_column_diffs,
        'column_match_info': all_col_info_df,
        'missing_join_info': missing_join_info,
        'hierarchical_join_info': hierarchical_join_info,
        'dedup_info': dedup_info,
        'diff_summary': diff_summary,
    }

    if drop_output_tables:
        LOGGER.info("Dropping output tables. WARNING: queries in the report won't work!")
        cur.execute(JINJA_ENV.get_template('table_drop.sql').render(schema_name=output_schema, table_name=(x_table + '_JOINED')))
        if use_diff_table:
            cur.execute(JINJA_ENV.get_template('table_drop.sql').render(schema_name=output_schema, table_name=(x_table + '_DIFF')))

    return all_info
