import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
from jinja2 import Environment, PackageLoader
from vertica_python.vertica.cursor import Cursor

from dbdiff.vertica import get_column_info_lookup, implicit_dtype_comparison

JINJA_ENV = Environment(loader=PackageLoader('dbdiff', 'templates'))
LOGGER = logging.getLogger(__name__)


def is_numeric_like(dtype: str):
    dtype_l = dtype.lower()
    return any({'int' in dtype_l, 'float' in dtype_l, 'numeric' in dtype_l})


def is_date_like(dtype: str):
    dtype_l = dtype.lower()
    return ('date' in dtype_l)


def check_primary_key(cur: Cursor,
                      schema: str, table: str,
                      join_cols: list) -> int:
    '''Given a list of columns return the # of records for which they are NOT
    a primary key.'''

    cur.execute(JINJA_ENV.get_template('table_rows.sql').render(schema_name=schema, table_name=table))
    r = cur.fetchall()
    n_rows = r[0]['COUNT']
    cur.execute(JINJA_ENV.get_template('table_rows_uniq.sql').render(schema_name=schema, table_name=table, join_cols=', '.join(join_cols)))
    n_distinct_rows = cur.fetchall()[0]['COUNT']
    return n_rows - n_distinct_rows


def get_all_col_info(cur: Cursor, schema, x_table, y_schema, y_table, exclude_columns_set, save_column_summary, save_column_summary_format):
    LOGGER.info('Getting column info for both tables.')
    x_table_info_lookup = get_column_info_lookup(cur, schema, x_table)
    y_table_info_lookup = get_column_info_lookup(cur, y_schema, y_table)

    def comparable_(x, y) -> bool:
        # this doesn't capture the case where they both could be converted to float to be compared (two hop conversions):
        if (x is None) or (y is None):
            return False
        left_or_right = implicit_dtype_comparison(x, y) or implicit_dtype_comparison(y, x)
        return left_or_right

    all_keys = list(x_table_info_lookup.keys()) + list(y_table_info_lookup.keys())
    all_col_info = {col: {'x_dtype': x_table_info_lookup.get(col, None),
                          'y_dtype': y_table_info_lookup.get(col, None),
                          'comparable': comparable_(x_table_info_lookup.get(col, None), y_table_info_lookup.get(col, None)),
                          'exclude': (col in exclude_columns_set)
                          } for col in all_keys}
    LOGGER.debug(all_col_info)
    all_col_info_df = pd.DataFrame(all_col_info).transpose()

    if save_column_summary:
        if save_column_summary_format.lower() == 'csv':
            all_col_info_df.to_csv(Path(x_table + '_col_info.csv'))
        if save_column_summary_format.lower() == 'pickle':
            all_col_info_df.to_pickle(Path(x_table + '_col_info.pkl'))
    else:
        LOGGER.info("All column info:\n" + all_col_info_df.to_string())
    LOGGER.info("Missing columns in x:\n" + all_col_info_df.loc[all_col_info_df.x_dtype.isnull(), :].to_string())
    LOGGER.info("Missing columns in y:\n" + all_col_info_df.loc[all_col_info_df.y_dtype.isnull(), :].to_string())
    LOGGER.debug(all_col_info_df.comparable)
    LOGGER.debug(~all_col_info_df.comparable)
    LOGGER.info("These columns have incompatible dtypes, specifically neither of them can be implicitly converted to the other:\n" + all_col_info_df.loc[(~all_col_info_df.comparable).astype('bool'), :].to_string())

    return all_col_info_df


def select_distinct_rows(cur: Cursor,
                         schema: str, table: str,
                         join_cols: list,
                         use_temp_tables: bool = False) -> Tuple[str, str]:
    '''Select only the rows that are distinct on join_cols.
    *Instead of deleting the rows, we'll select those without duplicates into a
    new table, and return the name of that new table.
    Delete is inefficient, see: https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AnalyzingData/Optimizations/PerformanceConsiderationsForDELETEAndUPDATEQueries.htm
    And: https://www.vertica.com/blog/another-way-to-de-duplicate-table-rows-quick-tip/
    '''

    cur.execute(JINJA_ENV.get_template('table_drop.sql').render(schema_name=schema, table_name=(table + '_dedup')))
    q = JINJA_ENV.get_template('create_dedup.sql').render(
        schema_name=schema, table_name=table,
        table_name_dedup=(table + '_dedup'),
        group_cols=', '.join(join_cols),
        join_cols=' AND '.join(['x.{0} <=> y.{0}'.format(col) for col in join_cols]),
        use_temp_table=use_temp_tables
    )
    if use_temp_tables:
        q = JINJA_ENV.get_template('create_temp_table.sql').render(table_name=(table + '_dedup'), query=q)
    cur.execute(q)
    cur.execute('COMMIT;')
    cur.execute(JINJA_ENV.get_template('table_drop.sql').render(schema_name=schema, table_name=(table + '_dup')))
    q = JINJA_ENV.get_template('create_dup.sql').render(
        schema_name=schema, table_name=table,
        table_name_dup=(table + '_dup'),
        group_cols=', '.join(join_cols),
        join_cols=' AND '.join(['x.{0} <=> y.{0}'.format(col) for col in join_cols]),
        use_temp_table=use_temp_tables
    )
    if use_temp_tables:
        q = JINJA_ENV.get_template('create_temp_table.sql').render(table_name=(table + '_dup'), query=q)
    cur.execute(q)
    cur.execute('COMMIT;')

    return (schema, 'v_temp_schema')[use_temp_tables], '{table}_dedup'.format(table=table)


def create_joined_table(cur: Cursor, create_insert=False, **kwargs):
    """
    Joins two tables x and y.
    :param cur: vertica python Cursor
    :return: list - all queries run.
    """
    drop_q = JINJA_ENV.get_template('table_drop.sql').render(
        schema_name=kwargs['joined_schema'],
        table_name=kwargs['joined_table'])
    LOGGER.info(drop_q)
    cur.execute(drop_q)

    if create_insert:
        # these separately do CREATE TABLE and then
        # INSERT INTO
        create_q = JINJA_ENV.get_template('create_joined_table.sql').render(kwargs)
        LOGGER.info(create_q)
        cur.execute(create_q)
        insert_q = JINJA_ENV.get_template('insert_joined_table.sql').render(kwargs)
        LOGGER.info(insert_q)
        cur.execute(insert_q)
    else:
        # this does a SELECT INTO
        join_q = JINJA_ENV.get_template('create_joined_table_from_selectinto.sql').render(kwargs)
        LOGGER.info(join_q)
        cur.execute(join_q)

    LOGGER.info('COMMIT;')
    cur.execute('COMMIT;')

    table_rows_q = JINJA_ENV.get_template('table_rows.sql').render(
        schema_name=kwargs['joined_schema'],
        table_name=kwargs['joined_table'])
    LOGGER.info(table_rows_q)
    cur.execute(table_rows_q)
    r = cur.fetchall()
    joined_row_count = r[0]['COUNT']
    return joined_row_count


def get_unmatched_rows_straight(cur: Cursor,
                                x_schema: str, y_schema: str,
                                x_table: str, y_table: str,
                                join_cols: list,
                                max_rows_column: int) -> Dict[str, Dict[str, Any]]:
    all_keys_count = JINJA_ENV.get_template('all_keys_count.sql')
    all_keys_sample = JINJA_ENV.get_template('all_keys_sample.sql')

    results = {'left': {'count': 0, 'query': 'select ...', 'sample': pd.DataFrame()},
               'right': {'count': 0, 'query': 'select ...', 'sample': pd.DataFrame()}}

    for side in {'left', 'right'}:
        q = all_keys_count.render({'x_schema': x_schema, 'y_schema': y_schema,
                                   'x_table': x_table, 'y_table': y_table,
                                   'join_cols': join_cols, 'left': (side == 'left')})
        cur.execute(q)
        r = cur.fetchall()
        results[side]['count'] = r[0]['COUNT']
        results[side]['query'] = all_keys_sample.render({'x_schema': x_schema, 'y_schema': y_schema,
                                                         'x_table': x_table, 'y_table': y_table,
                                                         'join_cols': join_cols, 'max_rows_column': max_rows_column,
                                                         'left': (side == 'left')})
        cur.execute(results[side]['query'])
        results[side]['sample'] = pd.DataFrame(cur.fetchall())

    return results


def get_unmatched_rows(cur: Cursor,
                       x_schema: str, y_schema: str,
                       x_table: str, y_table: str,
                       join_cols: list,
                       max_rows_column: int) -> Dict[Any, Dict[str, Dict[str, Any]]]:
    '''
    Pull out rows that are unmatched between the two tables on the join columns.
    If looking at this hierarchically, we consider the join by
    key a, then key a+b (where a matched), then key a+b+c (where a+b matched), etc
    to see at what level we're missing things.
    '''
    results = {col: {'left': {'count': 0, 'query': 'select ...', 'sample': pd.DataFrame()},
                     'right': {'count': 0, 'query': 'select ...', 'sample': pd.DataFrame()}} for col in join_cols}

    first_key_count = JINJA_ENV.get_template('first_key_count.sql')
    first_key_t = JINJA_ENV.get_template('first_key_sample.sql')

    LOGGER.info('Getting rows that did not match on only the first join column: ' + join_cols[0] + '.')
    for side in {'left', 'right'}:
        q = first_key_count.render({'x_schema': x_schema, 'y_schema': y_schema,
                                    'x_table': x_table, 'y_table': y_table,
                                    'col': join_cols[0], 'left': (side == 'left')})
        cur.execute(q)
        r = cur.fetchall()
        results[join_cols[0]][side]['count'] = r[0]['COUNT']
        results[join_cols[0]][side]['query'] = first_key_t.render({'x_schema': x_schema, 'y_schema': y_schema,
                                                                   'x_table': x_table, 'y_table': y_table,
                                                                   'col': join_cols[0], 'max_rows': max_rows_column,
                                                                   'left': (side == 'left')})
        cur.execute(results[join_cols[0]][side]['query'])
        results[join_cols[0]][side]['sample'] = pd.DataFrame(cur.fetchall())

    sub_keys_count = JINJA_ENV.get_template('sub_keys_count.sql')
    sub_keys_t = JINJA_ENV.get_template('sub_keys_sample.sql')
    sub_keys_g = JINJA_ENV.get_template('sub_keys_grouped.sql')

    for i in range(1, len(join_cols)):
        LOGGER.info('Getting rows that did not match on the ' + str(i + 1) + '-nd/rd/th join column: ' + join_cols[i] + '.')
        LOGGER.info('This is equivalent to joining the tables on unique rows of ' + ','.join(join_cols[:(i + 1)]) + ' where all but the last already exist.')

        for side in {'left', 'right'}:
            q = sub_keys_count.render({'x_schema': x_schema, 'y_schema': y_schema,
                                       'x_table': x_table, 'y_table': y_table,
                                       'columns': join_cols[:(i + 1)], 'left': (side == 'left')})
            cur.execute(q)
            r = cur.fetchall()
            results[join_cols[i]][side]['count'] = r[0]['COUNT']
            results[join_cols[i]][side]['query'] = sub_keys_t.render({'x_schema': x_schema, 'y_schema': y_schema,
                                                                      'x_table': x_table, 'y_table': y_table,
                                                                      'columns': join_cols[:(i + 1)], 'max_rows': max_rows_column,
                                                                      'left': (side == 'left')})
            cur.execute(results[join_cols[i]][side]['query'])
            results[join_cols[i]][side]['sample'] = pd.DataFrame(cur.fetchall())
            results[join_cols[i]][side]['query_grouped'] = sub_keys_g.render({'x_schema': x_schema, 'y_schema': y_schema,
                                                                              'x_table': x_table, 'y_table': y_table,
                                                                              'columns': join_cols[:(i + 1)], 'max_rows': max_rows_column,
                                                                              'left': (side == 'left')})
            cur.execute(results[join_cols[i]][side]['query_grouped'])
            results[join_cols[i]][side]['sample_grouped'] = pd.DataFrame(cur.fetchall())

    return results


def create_diff_table(cur: Cursor,
                      schema: str, table: str,
                      join_cols: list, all_col_info_df: pd.DataFrame) -> str:
    drop_q = JINJA_ENV.get_template('table_drop.sql').render(schema_name=schema, table_name=table)
    # so simple that putting into a template would make this harder to follow...
    q = 'CREATE TABLE {schema}.{table} ( {columns}, column_name VARCHAR(255) );'.format(
        schema=schema,
        table=table,
        columns=', '.join(all_col_info_df.loc[all_col_info_df.index.isin(join_cols)].apply(lambda x: ' '.join([x.name, x.x_dtype]), axis=1).values)
    )
    cur.execute(drop_q)
    cur.execute(q)
    return q


def insert_diff_table(cur: Cursor, **kwargs) -> None:
    cur.execute(JINJA_ENV.get_template('insert_diff.sql').render(kwargs))
    cur.execute('COMMIT;')


def get_diff_rows(cur: Cursor,
                  output_schema: str,
                  x_table: str,
                  join_cols: list,
                  max_rows_all: int,
                  skip_row_total: bool = False) -> dict:
    LOGGER.debug("Getting diff rows")
    # first get the count
    q = JINJA_ENV.get_template('table_rows.sql').render(
        schema_name=output_schema,
        table_name=(x_table + '_DIFF'))
    LOGGER.info(q)
    cur.execute(q)
    diff_total_count = cur.fetchall()[0]['COUNT']
    if skip_row_total:
        LOGGER.debug("Skipping sample of rows with differences, query to get that sample, and the total # of rows with > 0 differences. Returning only 'total_count', the sum of cell-by-cell differences.")
        return {'total_count': diff_total_count}

    q = JINJA_ENV.get_template('table_rows_uniq.sql').render(schema_name=output_schema, table_name=(x_table + '_DIFF'), join_cols=', '.join(join_cols))
    LOGGER.info(q)
    cur.execute(q)
    diff_row_count = cur.fetchall()[0]['COUNT']

    # we'll pull all columns from the joined table
    q = JINJA_ENV.get_template('diff_rows_sample.sql').render(
        schema_name=output_schema,
        joined_table=(x_table + '_JOINED'),
        diff_table=(x_table + '_DIFF'),
        group_cols=', '.join(join_cols),
        join_cols=' AND '.join(['x.{0} <=> joined.{0}'.format(col) for col in join_cols])
    )
    LOGGER.info(q)
    cur.execute(q + ' LIMIT ' + str(max_rows_all))
    diff_rows = pd.DataFrame(cur.fetchall())

    return {'query': q, 'sample': diff_rows,
            'count': diff_row_count, 'total_count': diff_total_count}


def get_diff_rows_from_joined(cur: Cursor,
                              grouped_column_diffs: dict,
                              output_schema: str,
                              x_table: str,
                              join_cols: list,
                              max_rows_all: int,
                              skip_row_total: bool = False) -> dict:
    '''Get diff rows from joined table.

    Non self-explanatory argument specifics:

    - grouped_column_diffs:
    - max_rows_all: number of rows to get for the sample (only relevant if skip_row_total=F)
    - skip_row_total: skip sample of rows with differences, query to get that sample, and the total # of rows with > 0 differences. Return only 'total_count', the sum of cell-by-cell differences.

    Returned data specifics:

    - dict with 4 keys:
        - total_count: total number of cell-by-cell differences between the two tables.
        - query: query to get a sample of rows with >0 differences.
        - sample: dataframe of those sample rows
        - count: count of rows with >0 differences.
    '''
    LOGGER.debug("Getting diff rows: get_diff_rows_from_joined()")

    diff_total_count = sum([info['count'] for info in grouped_column_diffs.values()])
    if skip_row_total or (len(grouped_column_diffs) == 0):
        LOGGER.debug("Skipping sample of rows with differences, query to get that sample, and the total # of rows with > 0 differences. Returning only 'total_count', the sum of cell-by-cell differences.")
        return {'total_count': diff_total_count}

    LOGGER.info(grouped_column_diffs)
    q = JINJA_ENV.get_template('joined_rows_count.sql').render(
        joined_schema=output_schema,
        joined_table=(x_table + '_JOINED'),
        columns=grouped_column_diffs.keys()
    )
    LOGGER.info(q)
    cur.execute(q)
    diff_row_count = cur.fetchall()[0]['COUNT']

    # we'll pull all columns from the joined table
    q = JINJA_ENV.get_template('joined_rows_sample.sql').render(
        joined_schema=output_schema,
        joined_table=(x_table + '_JOINED'),
        columns=grouped_column_diffs.keys()
    )
    LOGGER.info(q)
    cur.execute(q + ' LIMIT ' + str(max_rows_all))
    diff_rows = pd.DataFrame(cur.fetchall())

    return {'query': q, 'sample': diff_rows,
            'count': diff_row_count, 'total_count': diff_total_count}


def get_diff_columns(cur: Cursor, output_schema: str, x_table: str) -> pd.DataFrame:
    LOGGER.debug("Getting diff columns")
    # The # of columns has a hard limit (~1600 in Vertica?) so don't worry about
    # pulling the count first or limiting the results
    q = JINJA_ENV.get_template('diff_column_summary.sql').render(
        schema_name=output_schema,
        table_name=(x_table + '_DIFF'))
    cur.execute(q)
    return pd.DataFrame(cur.fetchall())


def get_column_diffs(diff_columns: pd.DataFrame, cur: Cursor,
                     output_schema: str,
                     x_schema: str, x_table: str,
                     y_schema: str, y_table: str,
                     join_cols: list,
                     max_rows_column: int,
                     all_col_info_df: pd.DataFrame,
                     hierarchical: bool = False) -> dict:
    LOGGER.debug("Getting column diffs")
    # get total count, list of most common differing pairs for each column
    # list of (count, query, df)
    grouped_column_diffs = {row.column_name: {'count': row['COUNT']} for i, row in diff_columns.iterrows()}

    for column_name, info in grouped_column_diffs.items():
        LOGGER.info('Getting detailed diff for column: ' + str(column_name) + ' with ' + str(info['count']) + ' differences.')
        q = JINJA_ENV.get_template('diff_column.sql').render(
            column=column_name,
            joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
            diff_schema=output_schema, diff_table=(x_table + '_DIFF'),
            group_cols=', '.join(join_cols),
            join_cols=' AND '.join(['diff.{0} <=> joined.{0}'.format(col) for col in join_cols]),
        )
        info['q'] = q
        q_raw = JINJA_ENV.get_template('diff_column_raw.sql').render(
            column=column_name,
            joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
            diff_schema=output_schema, diff_table=(x_table + '_DIFF'),
            join_cols=join_cols,
            join_cols_join=' AND '.join(['diff.{0} <=> joined.{0}'.format(col) for col in join_cols]),
        )
        info['q_raw'] = q_raw
        cur.execute(q + ' LIMIT ' + str(max_rows_column))
        info['df'] = pd.DataFrame(cur.fetchall())
        cur.execute(q_raw + ' LIMIT ' + str(max_rows_column))
        info['df_raw'] = pd.DataFrame(cur.fetchall())
        if hierarchical:
            for schema, table, side in ((x_schema, x_table, 'x'), (y_schema, y_table, 'y')):
                for limit in (None, max_rows_column):
                    q_h = JINJA_ENV.get_template('diff_column_hier.sql').render(
                        column=column_name,
                        diff_schema=output_schema,
                        diff_table=(x_table + '_DIFF'),
                        join_cols=', '.join(join_cols),
                        first_join_col=join_cols[0],
                        schema=schema,
                        table=table,
                        limit=limit
                    )
                    if limit is None:
                        info['q_h_' + side] = q_h
                    else:
                        cur.execute(q_h)
                        info['df_h_' + side] = pd.DataFrame(cur.fetchall())
        row = all_col_info_df.loc[column_name, :]
        is_numeric = (is_numeric_like(row.x_dtype) and is_numeric_like(row.y_dtype))
        is_date = (is_date_like(row.x_dtype) and is_date_like(row.y_dtype))
        if is_numeric or is_date:
            info['q_n'] = JINJA_ENV.get_template('diff_column_numeric_diffs_binned.sql').render(
                column=column_name,
                joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                diff_schema=output_schema, diff_table=(x_table + '_DIFF'),
                group_cols=', '.join(join_cols),
                join_cols=' AND '.join(['diff.{0} <=> joined.{0}'.format(col) for col in join_cols]),
                tiles=min({max({1, info['count']}), 10}))
            cur.execute(info['q_n'])
            info['df_n'] = pd.DataFrame(cur.fetchall())
            info['q_n_sample'] = JINJA_ENV.get_template('diff_column_numeric_diffs_sorted.sql').render(
                column=column_name,
                joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                diff_schema=output_schema, diff_table=(x_table + '_DIFF'),
                join_cols=join_cols,
                join_cols_join=' AND '.join(['diff.{0} <=> joined.{0}'.format(col) for col in join_cols]),
            )
            cur.execute(info['q_n_sample'] + ' LIMIT ' + str(max_rows_column))
            info['df_n_sample'] = pd.DataFrame(cur.fetchall())
    return grouped_column_diffs


def get_column_diffs_from_joined(cur: Cursor,
                                 output_schema: str,
                                 x_schema: str, x_table: str,
                                 y_schema: str, y_table: str,
                                 join_cols: list,
                                 max_rows_column: int,
                                 all_col_info_df: pd.DataFrame,
                                 comparable_filter,
                                 hierarchical: bool = False) -> dict:
    '''Get column-by-column diffs directly from the joined table.

    Non self-explanatory argument specifics:

    - max_rows_column: number of rows to pull for sample differing cells on each column.
    - all_col_info_df: dataframe with the following columns:
        - index: column names.
        - x_dtype.
        - y_dtype.
    - comparable_filter: an 0/1 index on all_col_info_df to filter on columns to compare.
        - in cli.py, this filter/index is set using datatype matching and the user-supplied list of columns to exclude.
    - hierarchical: if true, additional outputs are included for each columns that are samples with the join keys.

    Returned data specifics:
    - dict grouped_column_diffs:
        - each `key` is a string column name for columns that matches on name between x and y tables (and are comparable on dtype, not excluded by user-supplied list).
        - each `value` is a dictionary with the following keys:
            - {'count': diff_count, 'df': df, 'df_raw': df_raw, 'q': q, 'q_raw': q_raw}.
            - if `hierarchical` is true: `{q,d}_h_{x,y}` (q for query, d for dataframe sample) from x and y tables, respectively.
            - if numeric of date: `{q,df}_n{,_sample}` (q for query, df for dataframe), the `_sample` is the biggest diffs, while the former are the binned differences.
    '''
    column_list_to_compare = all_col_info_df.loc[comparable_filter & ~all_col_info_df.index.isin(join_cols), :].index.values
    LOGGER.info("Getting column diffs for columns:")
    LOGGER.info(",".join(column_list_to_compare))
    grouped_column_diffs = {}

    for column in column_list_to_compare:
        LOGGER.info("=" * 80)
        LOGGER.info(column)
        joined_count_q = JINJA_ENV.get_template('joined_count.sql').render(
            column=column,
            joined_schema=output_schema,
            joined_table=(x_table + '_JOINED')
        )
        LOGGER.info(joined_count_q)
        cur.execute(joined_count_q)
        diff_count = cur.fetchall()[0]['COUNT']
        if diff_count > 0:
            LOGGER.info('Getting detailed diff for column: ' + str(column) + ' with ' + str(diff_count) + ' differences.')
            q = JINJA_ENV.get_template('joined_column.sql').render(
                column=column,
                joined_schema=output_schema, joined_table=(x_table + '_JOINED')
            )
            q_raw = JINJA_ENV.get_template('joined_column_raw.sql').render(
                column=column,
                joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                join_cols=join_cols
            )
            LOGGER.info(q)
            cur.execute(q + ' LIMIT ' + str(max_rows_column))
            df = pd.DataFrame(cur.fetchall())
            LOGGER.info(q_raw)
            cur.execute(q_raw + ' LIMIT ' + str(max_rows_column))
            df_raw = pd.DataFrame(cur.fetchall())
            grouped_column_diffs[column] = {'count': diff_count, 'df': df, 'df_raw': df_raw, 'q': q, 'q_raw': q_raw}
            LOGGER.info(grouped_column_diffs[column])

            if hierarchical:
                for schema, table, side in ((x_schema, x_table, 'x'), (y_schema, y_table, 'y')):
                    for limit in (None, max_rows_column):
                        q_h = JINJA_ENV.get_template('joined_column_hier.sql').render(
                            column=column,
                            joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                            join_cols=join_cols,
                            schema=schema,
                            table=table,
                            limit=limit
                        )
                        if limit is None:
                            grouped_column_diffs[column]['q_h_' + side] = q_h
                        else:
                            cur.execute(q_h)
                            grouped_column_diffs[column]['df_h_' + side] = pd.DataFrame(cur.fetchall())
            row = all_col_info_df.loc[column, :]
            is_numeric = (is_numeric_like(row.x_dtype) and is_numeric_like(row.y_dtype))
            is_date = (is_date_like(row.x_dtype) and is_date_like(row.y_dtype))
            if is_numeric or is_date:
                grouped_column_diffs[column]['q_n'] = JINJA_ENV.get_template('joined_column_numeric_diffs_binned.sql').render(
                    column=column,
                    joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                    tiles=min({max({1, grouped_column_diffs[column]['count']}), 10}))
                cur.execute(grouped_column_diffs[column]['q_n'])
                grouped_column_diffs[column]['df_n'] = pd.DataFrame(cur.fetchall())
                grouped_column_diffs[column]['q_n_sample'] = JINJA_ENV.get_template('joined_column_numeric_diffs_sorted.sql').render(
                    column=column,
                    joined_schema=output_schema, joined_table=(x_table + '_JOINED'),
                    join_cols=join_cols
                )
                cur.execute(grouped_column_diffs[column]['q_n_sample'] + ' LIMIT ' + str(max_rows_column))
                grouped_column_diffs[column]['df_n_sample'] = pd.DataFrame(cur.fetchall())
        else:
            LOGGER.info('NOT getting detailed diff for column: ' + str(column) + ' with ' + str(diff_count) + ' differences.')
    LOGGER.info(len(grouped_column_diffs))
    grouped_column_diffs_sorted = {x: grouped_column_diffs[x] for x in sorted(grouped_column_diffs.keys(), key=lambda x: grouped_column_diffs[x]['count'], reverse=True)}
    LOGGER.info(len(grouped_column_diffs_sorted))
    return grouped_column_diffs_sorted
