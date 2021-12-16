import logging
import os
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from dbdiff.main import check_primary_key
from dbdiff.main import create_diff_table
from dbdiff.main import create_joined_table
from dbdiff.main import get_column_diffs
from dbdiff.main import get_diff_columns
from dbdiff.main import get_diff_rows
from dbdiff.main import get_unmatched_rows
from dbdiff.main import get_unmatched_rows_straight
from dbdiff.main import insert_diff_table
from dbdiff.main import select_distinct_rows
from dbdiff.cli import cli
from dbdiff.vertica import get_column_info
from dbdiff.vertica import get_column_info_lookup
from dbdiff.vertica import get_cur
from dbdiff.vertica import get_table_exists

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
VALID_COL = {'comparable': True, 'exclude': False}
INT_DTYPES = {d: 'int' for d in {'x_dtype', 'y_dtype'}}
VARCHAR_DTYPES = {d: 'varchar(10)' for d in {'x_dtype', 'y_dtype'}}
DATE_DTYPES = {d: 'date' for d in {'x_dtype', 'y_dtype'}}
COMPARE_COLS = pd.DataFrame({'data1': {**INT_DTYPES, **VALID_COL},
                             'data2': {**INT_DTYPES, **VALID_COL},
                             'data3': {**DATE_DTYPES, **VALID_COL},
                             'data4': {**VARCHAR_DTYPES, **VALID_COL},}).transpose()
JOIN_COLS = pd.DataFrame({'join1': {**VARCHAR_DTYPES, **VALID_COL},
                          'join2': {**VARCHAR_DTYPES, **VALID_COL}}).transpose()


@pytest.fixture(scope='session')
def cur():
    # vsql -d docker -u dbadmin
    # export VERTICA_HOST="localhost"
    # export VERTICA_PORT="5433"
    # export VERTICA_DATABASE="docker"
    # export VERTICA_USERNAME="dbadmin"
    # export VERTICA_PASSWORD=""
    os.environ['VERTICA_HOST'] = 'localhost'
    os.environ['VERTICA_PORT'] = '5433'
    os.environ['VERTICA_DATABASE'] = 'docker'
    os.environ['VERTICA_USERNAME'] = 'dbadmin'
    os.environ['VERTICA_PASSWORD'] = ''
    with get_cur() as c:
        yield c


def create_schema(cur):
    cur.execute('CREATE SCHEMA dbdiff;')


def create_x_table(cur):
    cur.execute('CREATE TABLE dbdiff.x_table ( join1 varchar(10), join2 varchar(10), missingx int, missingx2 int, dtypemiss int, data1 int, data2 int, data3 date, data4 varchar(10));')
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'match1', 'matchdup21', 0, 0, 0, 0, 0, '2017-10-11', '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'match1', 'match22', 0, 0, 0, 0, 0, '2017-10-11', 'a');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'match1', 'match23', 0, 0, 0, 1, 1, '2017-10-11', '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'match1', 'missx21', null, null, null, null, null, null, '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'match1', 'missx22', null, null, null, null, null, null, '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'missx11', null, null, null, null, null, null, null, '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select 'missx12', null, null, null, null, null, null, null, '');")
    cur.execute("INSERT INTO dbdiff.x_table ( join1, join2, missingx, missingx2, dtypemiss, data1, data2, data3, data4) (select null, null, null, null, null, null, null, null, '');")
    cur.execute('COMMIT;')


def create_y_table(cur, case_off: bool = False):
    cur.execute('CREATE TABLE dbdiff.y_table ( join1 varchar(10), join2 varchar(10), missingy int, dtypemiss date, data1 int, data2 int, data3 date, data4 varchar(10));')
    cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'matchdup21', 0, '2019-04-22', 0, 0, '2017-10-11', '');")
    cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'matchdup21', 0, '2019-04-22', 0, 0, '2017-10-11', '');")
    if case_off:
        # here, we'll uppercase the 'A' so that these don't match
        cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'match22', 0, '2019-04-22', 0, 1, '2017-10-12', 'A');")
    else:
        cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'match22', 0, '2019-04-22', 0, 1, '2017-10-12', 'a');")
    cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'match23', 0, '2019-04-22', 0, 0, '2017-10-13', '');")
    cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'match1', 'missy21', 0, '2019-04-22', 0, 0, null, '');")
    cur.execute("INSERT INTO dbdiff.y_table ( join1, join2, missingy, dtypemiss, data1, data2, data3, data4) (select 'missy11', null, 0, '2019-04-22', 0, 0, null, '');")
    cur.execute('COMMIT;')


def drop_schema(cur):
    cur.execute('DROP SCHEMA IF EXISTS dbdiff CASCADE;')


def test_drop_data_start(cur):
    drop_schema(cur)


def test_create_data(cur):
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)


def test_get_column_info(cur):
    column_info = get_column_info(cur, 'dbdiff', 'x_table')
    assert type(column_info) == pd.DataFrame
    assert 'column_name' in column_info.columns
    assert 'data_type' in column_info.columns


def test_get_column_info_lookup(cur):
    column_info_lookup = get_column_info_lookup(cur, 'dbdiff', 'x_table')
    assert 'data1' in column_info_lookup
    assert column_info_lookup['join1'].lower() == 'varchar(10)'
    assert column_info_lookup['data1'] == 'int'
    assert column_info_lookup['data2'] == 'int'
    assert column_info_lookup['data3'] == 'date'
    assert len(column_info_lookup) == 9


def test_get_table_exists(cur):
    assert get_table_exists(cur, 'dbdiff', 'x_table')
    assert get_table_exists(cur, 'dbdiff', 'y_table')
    assert not get_table_exists(cur, 'dbdiff', 'z_table')


def test_check_primary_key(cur):
    assert (
        check_primary_key(
            cur,
            'dbdiff',
            'x_table',
            ['join1', 'join2']
        ) == 0
    )
    assert check_primary_key(cur, 'dbdiff', 'x_table', ['join1']) == 4


def test_select_distinct_rows(cur):
    x_table_rows = 8
    x_table_columns = 9
    for use_temp_tables in {True, False}:
        new_table_schema, new_table_name = select_distinct_rows(cur, 'dbdiff', 'x_table', ['join1'])
        # check that new table gets create with N rows
        assert get_table_exists(cur, new_table_schema, 'x_table_dup')
        assert get_table_exists(cur, new_table_schema, 'x_table_dedup')
        cur.execute('select * from {schema}.{table}'.format(
            schema=new_table_schema,
            table=new_table_name)
        )
        dedup = pd.DataFrame(cur.fetchall())
        assert dedup.shape[0] == 3
        assert dedup.shape[1] == x_table_columns
        cur.execute('select * from {schema}.{table}'.format(
            schema=new_table_schema,
            table='x_table_dup')
        )
        dup = pd.DataFrame(cur.fetchall())
        assert dup.shape[0] == (x_table_rows - dedup.shape[0])
        assert dup.shape[1] == (x_table_columns + 1)


def test_create_joined_table(cur):
    create_joined_table(
        cur,
        x_schema='dbdiff',
        y_schema='dbdiff',
        x_table='x_table',
        y_table='y_table',
        join_cols=['join1', 'join2'],
        compare_cols=pd.concat([COMPARE_COLS, JOIN_COLS]),
        joined_schema='dbdiff',
        joined_table='x_table_JOINED'
    )
    # check that it is created and has the right number of rows, columns...
    assert get_table_exists(cur, 'dbdiff', 'x_table_JOINED')
    cur.execute(
        'select * from {schema}.{table}'.format(
            schema='dbdiff',
            table='x_table_JOINED'
        )
    )
    df = pd.DataFrame(cur.fetchall())
    assert df.shape[0] == 4
    # double the comparing columns (x_* and y_*), and pk/join columns:
    assert df.shape[1] == ((COMPARE_COLS.shape[0] * 2) + JOIN_COLS.shape[0])


def test_get_unmatched_rows_straight(cur):
    join_cols = ['join1', 'join2']
    # these are a bit flipped:
    # the missing_x_join are counts for rows in y that are missing in x
    # and the missing_y_join are counts for rows *in x* that are *not in y*
    # the name coming from the *not in y* part of it.
    y_minus_x = [2, 1]  # formerly, "missing_x_join", now using set notation
    x_minus_y = [3, 2]  # etc
    results = get_unmatched_rows_straight(
        cur,
        'dbdiff',
        'dbdiff',
        'x_table',
        'y_table',
        join_cols,
        100
    )
    expected_results = {
        'x': {
            'count': sum(x_minus_y),
            'sample_shape': (sum(x_minus_y), len(join_cols))
        },
        'y': {
            'count': sum(y_minus_x),
            'sample_shape': (sum(y_minus_x), len(join_cols))
        }
    }
    print(results)
    print(expected_results)
    assert results['x']['count'] == expected_results['x']['count']
    assert results['x']['sample'].shape[0] == expected_results['x']['sample_shape'][0]
    assert results['x']['sample'].shape[1] == expected_results['x']['sample_shape'][1]
    assert results['y']['count'] == expected_results['y']['count']
    assert results['y']['sample'].shape[0] == expected_results['y']['sample_shape'][0]
    assert results['y']['sample'].shape[1] == expected_results['y']['sample_shape'][1]


def test_get_unmatched_rows(cur):
    join_cols = ['join1', 'join2']
    # these are, again, a litte wierd. see note in test_get_unmatched_rows_straight()
    y_minus_x = [2, 1]
    x_minus_y = [3, 2]
    results = get_unmatched_rows(
        cur,
        'dbdiff',
        'dbdiff',
        'x_table',
        'y_table',
        join_cols,
        100
    )
    expected_results = {
        j: {
            side: {
                'count': d[i],
                'sample_shape': (d[i], i + 1)
            } for side, d in {
                'x': x_minus_y,
                'y': y_minus_x
            }.items()
        } for i, j in enumerate(join_cols)
    }
    for col, expected in expected_results.items():
        logging.info(col)
        for side, expected_info in expected.items():
            logging.info(side)
            logging.info(results[col][side]['count'])
            logging.info(results[col][side]['sample'])
            assert 'sample' in results[col][side]
            assert 'query' in results[col][side]
            assert results[col][side]['count'] == expected_info['count']
            for i in {0, 1}:
                assert results[col][side]['sample'].shape[i] == expected_info['sample_shape'][i]
            if col == 'join2':
                assert 'sample_grouped' in results[col][side]
                assert 'query_grouped' in results[col][side]


def test_create_diff_table(cur):
    create_diff_table(cur, 'dbdiff', 'x_table_DIFF', ['join1', 'join2'],
                      pd.concat([COMPARE_COLS, JOIN_COLS]))
    assert get_table_exists(cur, 'dbdiff', 'x_table_DIFF')


def test_insert_diff_table(cur):
    cur.execute('select * from dbdiff.x_table_JOINED')
    logging.info(cur.fetchall())
    cur.execute('select * from dbdiff.x_table_DIFF')
    logging.info(cur.fetchall())
    insert_diff_table(
        cur,
        joined_schema='dbdiff',
        joined_table='x_table_JOINED',
        diff_schema='dbdiff',
        diff_table='x_table_DIFF',
        join_cols=['join1', 'join2'],
        column='data1'
    )
    cur.execute('select * from {schema}.{table}'.format(schema='dbdiff', table='x_table_DIFF'))
    df = pd.DataFrame(cur.fetchall())
    assert df.shape[0] == 1
    assert df.shape[1] == 3
    insert_diff_table(
        cur,
        joined_schema='dbdiff',
        joined_table='x_table_JOINED',
        diff_schema='dbdiff',
        diff_table='x_table_DIFF',
        join_cols=['join1', 'join2'],
        column='data2'
    )
    cur.execute('select * from {schema}.{table}'.format(schema='dbdiff', table='x_table_DIFF'))
    df = pd.DataFrame(cur.fetchall())
    assert df.shape[0] == 3
    assert df.shape[1] == 3


# def test_implicit_dytpe_comparison():
#     implicit_dytpe_comparison(x_dtype, y_dtype)


def test_get_diff_rows(cur):
    diff_summary = get_diff_rows(
        cur,
        'dbdiff',
        'x_table',
        ['join1', 'join2'],
        100
    )
    assert diff_summary['count'] == 2
    assert diff_summary['total_count'] == 3
    assert diff_summary['sample'].shape[0] == 2
    # assert diff_summary['sample'].shape[1] == 10


def test_get_diff_columns(cur):
    df = get_diff_columns(cur, 'dbdiff', 'x_table')
    assert df.shape[0] == 2
    assert df.shape[1] == 2


def test_get_column_diffs(cur):
    join_cols = ['join1', 'join2']
    diff_columns = get_diff_columns(cur, 'dbdiff', 'x_table')

    grouped_column_diffs = get_column_diffs(
        diff_columns, cur,
        'dbdiff',
        'dbdiff',
        'x_table',
        'dbdiff',
        'y_table',
        ['join1', 'join2'],
        100,
        COMPARE_COLS,
        True
    )
    logging.info(grouped_column_diffs)

    data1_misses = 1
    data2_misses = 2

    expected = {'data1': {'count': data1_misses, 'df_shape': (data1_misses, 3),
                          'df_raw_shape': (data1_misses, 2 + len(join_cols)),
                          'df_h_x_shape': (5, 1 + len(join_cols)),
                          'df_h_y_shape': (5, 1 + len(join_cols))},
                'data2': {'count': data2_misses, 'df_shape': (data2_misses, 3),
                          'df_raw_shape': (data2_misses, 2 + len(join_cols)),
                          'df_h_x_shape': (5, 1 + len(join_cols)),
                          'df_h_y_shape': (5, 1 + len(join_cols))}}

    for column_name in expected.keys():
        grouped_column_diffs[column_name]
        logging.info(grouped_column_diffs[column_name])
        assert expected[column_name]['count'] == grouped_column_diffs[column_name]['count']
        for q_name in {'q', 'q_raw', 'q_h_x', 'q_h_y'}:
            assert q_name in grouped_column_diffs[column_name]
        for i in {0, 1}:
            assert expected[column_name]['df_shape'][i] == grouped_column_diffs[column_name]['df'].shape[i]
            assert expected[column_name]['df_raw_shape'][i] == grouped_column_diffs[column_name]['df_raw'].shape[i]
            assert expected[column_name]['df_h_x_shape'][i] == grouped_column_diffs[column_name]['df_h_x'].shape[i]
            assert expected[column_name]['df_h_y_shape'][i] == grouped_column_diffs[column_name]['df_h_y'].shape[i]


def test_drop_data_end(cur):
    drop_schema(cur)


def test_main(cur):
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    base_options = ['dbdiff', 'x_table', 'y_table', 'join1,join2']
    runner = CliRunner()

    def runner_wrapper(runner, base_options, addl_options):
        result = runner.invoke(
            cli,
            base_options + addl_options,
            catch_exceptions=False
        )
        # if result.exit_code != 0:
        #     print(result.output)
        #     logging.info(str(result.exception) + str(result.exc_info))
        assert result.exit_code == 0

    runner_wrapper(runner, base_options, [])
    # save the report:
    # Path('x_table_report.html').rename('base_report.html')
    # clear the output
    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur)

    runner_wrapper(runner, base_options, ['--drop-output-tables'])
    runner_wrapper(runner, base_options, ['--drop-output-tables', '--output-format=XLSX'])
    runner_wrapper(runner, base_options, ['--hierarchical-join'])
    runner_wrapper(runner, base_options, ['--drop-output-tables'])
    runner_wrapper(runner, base_options, ['--drop-output-tables', '--output-format=XLSX'])
    runner_wrapper(runner, base_options, ['--use-diff-table'])
    runner_wrapper(runner, base_options, ['--hierarchical-join', '--use-diff-table'])

    Path('x_table_report.html').unlink()
    Path('x_table_report.xlsx').unlink()

    Path('x_table_temp.sql').write_text('select * from dbdiff.x_table')
    x_table_temp_options = ['dbdiff', 'x_table_temp.sql', 'y_table', 'join1,join2', '--x-table-query']
    runner_wrapper(runner, x_table_temp_options, ['--drop-output-tables'])
    runner_wrapper(runner, x_table_temp_options, ['--drop-output-tables', '--output-format=XLSX'])
    runner_wrapper(runner, x_table_temp_options, ['--hierarchical-join'])

    Path('y_table_temp.sql').write_text('select * from dbdiff.y_table')
    both_table_temp_options = ['dbdiff', 'x_table_temp.sql', 'y_table_temp.sql', 'join1,join2', '--x-table-query', '--y-table-query']
    runner_wrapper(runner, both_table_temp_options, ['--drop-output-tables'])
    runner_wrapper(runner, both_table_temp_options, ['--drop-output-tables', '--output-format=XLSX'])
    runner_wrapper(runner, both_table_temp_options, ['--hierarchical-join'])
    runner_wrapper(runner, both_table_temp_options, ['--save-column-summary'])
    runner_wrapper(runner, both_table_temp_options, ['--save-column-summary', '--save-column-summary-format=pickle'])


    Path('x_table_temp.sql').unlink()
    Path('y_table_temp.sql').unlink()
    Path('x_table_temp_report.html').unlink()
    Path('x_table_temp_report.xlsx').unlink()
    Path('x_table_temp_col_info.csv').unlink()
    Path('x_table_temp_col_info.pkl').unlink()

    # clear the output
    drop_schema(cur)
    create_schema(cur)
    create_x_table(cur)
    create_y_table(cur, case_off=True)

    runner_wrapper(runner, base_options, [])
    # Path('x_table_report.html').rename('case_on.html')
    runner_wrapper(runner, base_options, ['--case-insensitive'])
    # Path('x_table_report.html').rename('case_off.html')
    runner_wrapper(runner, base_options, ['--save-json-summary'])
    runner_wrapper(runner, base_options, ['--hierarchical-join', '--save-json-summary'])

    Path('x_table_report.html').unlink()
    Path('x_table_diff_summary.json').unlink()
