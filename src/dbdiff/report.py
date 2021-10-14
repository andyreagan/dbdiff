import pandas as pd
from jinja2 import Environment, PackageLoader

MAX_EXCEL_SHEET_NAME_LEN = 31
JINJA_ENV = Environment(loader=PackageLoader('dbdiff', 'templates'))


def html_report(x_schema: str, y_schema: str, x_table: str, y_table: str, join_cols: list,
                diff_summary: dict, total_row_count: int,
                column_info: dict,
                column_match_info: pd.DataFrame,
                missing_join_info: dict, hierarchical_join_info: dict,
                dedup_info: dict) -> str:

    def comma(value, format='{0:,d}'):
        return format.format(value)

    def code(value, codeclass='plaintext'):
        return '<code class="{0}">{1}</code>'.format(codeclass, value)

    def dfhtml(df, classes=["table", "table-bordered", "table-striped", "table-hover", "table-sm"]):
        return df.to_html(index=False, classes=classes)

    JINJA_ENV.filters['comma'] = comma
    JINJA_ENV.filters['code'] = code
    JINJA_ENV.filters['dfhtml'] = dfhtml

    t = JINJA_ENV.get_template('report.html')
    if len(list(column_info.values())) > 0:
        max_differences = list(column_info.values())[0]['count']
    else:
        max_differences = 0
    missing_join_info = {x_table: missing_join_info['right'], y_table: missing_join_info['left']}
    for col in hierarchical_join_info:
        hierarchical_join_info[col] = {x_table: hierarchical_join_info[col]['right'], y_table: hierarchical_join_info[col]['left']}
    column_match_info['uncomparable'] = (~column_match_info.comparable) & (~column_match_info.x_dtype.isnull()) & (~column_match_info.y_dtype.isnull())
    print(column_match_info['uncomparable'])
    return t.render({'x_schema': x_schema, 'y_schema': y_schema,
                     'x_table': x_table, 'y_table': y_table,
                     'join_cols': join_cols,
                     'diff_summary': diff_summary,
                     'total_row_count': total_row_count,
                     'column_info': column_info,
                     'max_differences': max_differences,
                     'missing_join_info': missing_join_info,
                     'hierarchical_join_info': hierarchical_join_info,
                     'dedup_info': dedup_info,
                     # can't do these filters in Jinja
                     # could write a filter function that takes a list of
                     # positive and a list of negative filter columns
                     # but this is good enough for the one case:
                     'compared_column_count': ((~column_match_info.exclude) & column_match_info.comparable & (~column_match_info.x_dtype.isnull()) & (~column_match_info.y_dtype.isnull()) & (~column_match_info.index.isin(join_cols))).sum(),
                     'column_match_info': column_match_info})


def excel_report(x_schema: str, y_schema: str,
                 x_table: str, y_table: str,
                 join_cols: list,
                 diff_summary: dict,
                 total_row_count: int,
                 column_info: dict,
                 column_match_info: pd.DataFrame,
                 missing_join_info: dict, hierarchical_join_info: dict) -> list:
    '''
    Return a list with [(sheet_name: str, df: pd.DataFrame) ... ]
    '''
    all_sheets = []
    summary_sheet_data = [{'Summary': 'Diff report between tables {x_table} (herein, "x") and {y_table} (herein, "y").'.format(
        x_table=x_table,
        y_table=y_table
    )}]
    summary_sheet_data.append({'Summary': '----'})
    summary_sheet_data.append({'Summary': 'There are {x_missing_count} rows in {y_table} that are not in {x_table}.'.format(
        x_table=x_table,
        y_table=y_table,
        x_missing_count=missing_join_info['left']['count']
    )})
    summary_sheet_data.append({'Summary': 'There are {y_missing_count} rows in {x_table} that are not in {y_table}.'.format(
        x_table=x_table,
        y_table=y_table,
        y_missing_count=missing_join_info['right']['count']
    )})
    summary_sheet_data.append({'Summary': "There are {diff_row_count} rows matched between tables that don't line up exactly.".format(diff_row_count=diff_summary['count'])})
    summary_sheet_data.append({'Summary': 'There are {column_info} columns that have differences.'.format(column_info=len(column_info))})
    if len(list(column_info.values())) > 0:
        max_differences = list(column_info.values())[0]['count']
    else:
        max_differences = 0
    summary_sheet_data.append({'Summary': 'The maximum number of differences on any individual column is {max_differences}.'.format(max_differences=max_differences)})
    all_sheets.append(('Summary', pd.DataFrame(summary_sheet_data)))

    if missing_join_info['left']['count'] > 0:
        # all_sheets.append(('Missing rows in {x_table}'.format(x_table=x_table), x_missing_ids))
        all_sheets.append(('Missing in x', missing_join_info['left']['sample']))
    if missing_join_info['right']['count'] > 0:
        # all_sheets.append(('Missing rows in {y_table}'.format(y_table=y_table), y_missing_ids))
        all_sheets.append(('Missing in y', missing_join_info['right']['sample']))
    if diff_summary['count'] > 0:
        all_sheets.append(('Mismatched rows', diff_summary['sample']))
    for column, info in column_info.items():
        all_sheets.append((column[:MAX_EXCEL_SHEET_NAME_LEN], info['df_raw']))
    return all_sheets
