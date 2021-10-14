import logging
import os
import ssl
from contextlib import contextmanager

import pandas as pd
import requests
import vertica_python
from dotenv import find_dotenv, load_dotenv
from jinja2 import Environment, PackageLoader
from vertica_python.vertica.cursor import Cursor

JINJA_ENV = Environment(loader=PackageLoader('dbdiff', 'templates'))
LOGGER = logging.getLogger(__name__)


def get_column_info(cur: Cursor, schema_name: str,
                    table_name: str) -> pd.DataFrame:
    column_template = JINJA_ENV.get_template('table_columns.sql')
    cur.execute(column_template.render(schema_name=schema_name,
                                       table_name=table_name))
    # go ahead and convert (before checking size), overhead is low.
    df = pd.DataFrame(cur.fetchall())
    if df.shape[0] == 0:
        raise RuntimeError('{schema}.{table} has no columns.'.format(schema=schema_name, table=table_name))
    return df


def get_column_info_lookup(cur: Cursor, schema_name: str,
                           table_name: str) -> dict:
    x_table_info = get_column_info(cur, schema_name, table_name)
    LOGGER.info("Column info Vertica for " + schema_name + "." + table_name + ":\n" + x_table_info.head().to_string() + "\n...")
    x_table_info['column_name'] = x_table_info['column_name'].str.lower()
    return {r.column_name: r.data_type for i, r in x_table_info.iterrows()}


def get_table_exists(cur: Cursor, schema_name: str,
                     table_name: str) -> bool:
    table_exists_template = JINJA_ENV.get_template('table_exists.sql')
    cur.execute(table_exists_template.render(schema_name=schema_name,
                                             table_name=table_name))
    return (cur.fetchall()[0]['COUNT'] == 1)


def implicit_dtype_comparison(x_dtype: str, y_dtype: str) -> bool:
    '''Can x_dtype be implicitly converted to y_dtype?
    https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/DataTypeCoercionChart.htm
    '''
    source = x_dtype.upper()
    target = y_dtype.upper()
    if 'INT' in source:  # {'INT', 'INTEGER'} since numeric can look like NUMERIC(18,2), etc
        return ('BOOL' in target or
                'NUMERIC' in target or
                'FLOAT' in target or
                'INT' in target)
    elif 'NUMERIC' in source:
        return ('FLOAT' in target or
                'NUMERIC' in target)
    # don't worry about LONG VARCHAR not converting to VARCHAR right now
    elif 'CHAR' in source:
        return ('CHAR' in target or
                'FLOAT' in target)
    elif 'DATE' in source and 'DATE' in target:
        return True
    else:
        return source == target


@contextmanager
def get_cur() -> Cursor:
    '''Build a connection.

    For connection options,
    this function will look for a file, recursively up from this directory,
    named .config.sh.
    If such a file exists, the keys VERTICA_*
    will be used to make the connection.
    Else, it will use the available environment variables.

    For SSL, if the environment variable CERT_LINK is set,
    this function will get the file at that uri and use it.
    If no CERT_LINK is set and VERTICA_SSL is set one of:
    ['1', 'true', 'yes', 'please']
    then this function will use the system's default context
    (openssl's context (set this with openssl's env variable config)).
    If neither CERT_LINK nor VERTICA_SSL are set,
    this will not use SSL.
    '''
    load_dotenv(find_dotenv('.config.sh'))

    conninfo = dict(host=os.environ.get('VERTICA_HOST', '').strip(),
                    port=int(os.environ.get('VERTICA_PORT', '').strip()),
                    database=os.environ.get('VERTICA_DATABASE', '').strip(),
                    user=os.environ.get('VERTICA_USERNAME', '').strip(),
                    password=os.environ.get('VERTICA_PASSWORD', '').strip(),
                    connection_timeout=int(os.environ.get(
                        'VERTICA_CONNECTION_TIMEOUT', '36000').strip()),
                    # read_timeout=int(os.environ.get(
                    #     'VERTICA_READ_TIMEOUT', '36000').strip()),
                    # this is in the docs, but not used!
                    unicode_error=os.environ.get(
                        'VERTICA_UNICODE_ERROR', 'strict').strip())
    # let this one use the vertica-python default:
    # connection_load_balance=True
    LOGGER.debug(conninfo)

    if os.environ.get('CERT_LINK') is not None:
        LOGGER.debug('getting cert from uri')
        resp = requests.get(str(os.environ.get('CERT_LINK')))
        resp.raise_for_status()
        ssl_data = resp.content

        context = ssl.create_default_context(cadata=ssl_data.decode('ascii', 'ignore'))
        context.check_hostname = False
        conninfo['ssl'] = context
    else:
        use_ssl_env = os.environ.get('VERTICA_SSL')
        if (use_ssl_env is not None) and ((use_ssl_env == '1') or (use_ssl_env.lower() in {'true', 'yes', 'please'})):
            context = ssl.create_default_context()
            context.check_hostname = False
            conninfo['ssl'] = context

    with vertica_python.connect(**conninfo) as conn:
        with conn.cursor('dict') as cur:
            try:
                yield cur
            finally:
                conn.close()
