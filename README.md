Overview
========

[![Unit Tests](https://github.com/andyreagan/dbdiff/actions/workflows/unit-test.yml/badge.svg)](https://github.com/andyreagan/dbdiff/actions/workflows/unit-test.yml)
[![PyPI version](https://badge.fury.io/py/dbdiff.svg)](https://badge.fury.io/py/dbdiff)

Compare two database tables that are expected to be exactly the same.

Supported Backends
==================

`dbdiff` supports multiple database backends:

- **PostgreSQL** - Use `--dialect postgresql` (or `-d postgresql`)
- **DuckDB** - Use `--dialect duckdb` (or `-d duckdb`)
- **SQLite** - Use `--dialect sqlite` (or `-d sqlite`)
- **Vertica** - Use `--dialect vertica` (or `-d vertica`, the default)

Each dialect supports the same core diffing functionality, with minor variations in SQL template rendering to accommodate backend-specific syntax.

Design
======

There are a handful of design decisions to be made here.
The goal is to find and show cell-by-cell differences from two tabular data sources.

First,
We will use SQL templates rendered in Python code and use a SQL database backend to build the combined table and to pull differences off of.
Some alternatives here could be using Pandas as the tabular backend,
and all of the code in Python using the Pandas API.
Another,
that would be better for bigger data sets than Pandas,
would be to use PySpark DataFrames and either the SQL or Python API to analyze them.
A project that use Pandas or PySpark is [datacompy](https://github.com/capitalone/datacompy).

With a SQL database as the backend
(and we assume data is already in the database),
the inputs to the Python process:
two sets of data as (schema, table) and a list of join columns.
Python will look at the two tables,
find all columns that match on name
(no premature optimization to allow users to specify a mapping - that will have to a view or copied table passed to this process),
make sure that the join columns you passed are in both tables and they form a primary key on both tables.
It will then generate two persisted tables in the database,

- the "joined" table,
- and the "diff" table.

These are created as "regular" tables,
though they could for this purpose be local temp tables
(and maybe that's a good thing to do,
but leaving them in the database for now unless you specify `--drop-output-tables`).
Since this is the default behavior,
we create them first as tables
(not as temp tables then copy into table if persistence specified).
Then from this diff table,
the Python renders SQL templates which pull out three sets of
results:

1. The list of rows with any differences.
2. An ordered list of columns with differences (ordered by the number of differences).
3. The grouped `x_col`, `y_col` pairs where the values don’t align, ordered by the count of such pairs.

The output are those two tables,
along with an HTML or Excel report of the differences.

See "usage" for more options that have been baked in,
like the ability to skip specified columns.

Installation
============

Simply

    pip install dbdiff


Usage
=====

Once you've installed, configure the environment variables for your chosen database backend.

Database Connection Configuration
----------------------------------

### Vertica (default)

Set these environment variables for Vertica connections:

- `VERTICA_HOST`: example 'localhost'.
- `VERTICA_PORT`: example '5433'.
- `VERTICA_DATABASE`: example 'docker'.
- `VERTICA_USERNAME`: example 'dbadmin'.
- `VERTICA_PASSWORD`: example ''.
- (optional) `VERTICA_CONNECTION_TIMEOUT`: default is '36000'.
- (optional) `VERTICA_READ_TIMEOUT`: default is '36000'.
- (optional) `VERTICA_UNICODE_ERROR`: default is 'strict'.
- (optional) `CERT_LINK`: the full http address of a cert file to be used for SSL connection to Vertica. Will be pulled from the web and used to make the SSL connection if the variable is set.
- (optional) `VERTICA_SSL`: if `CERT_LINK` is _not_ set, and this matches (case-insensitive) `'1'`, `'true'`, `'yes'`, `'please'`, use the system SSL configuration to make an SSL connection to Vertica.

### PostgreSQL

Set these environment variables for PostgreSQL connections:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `POSTGRES_USERNAME`
- `POSTGRES_PASSWORD`

### DuckDB

DuckDB uses a local database file. You can specify the database path via the `--duckdb-path` option (defaults to `dbdiff.duckdb`).

### SQLite

SQLite uses a local database file. You can specify the database path via the `--sqlite-path` option (defaults to `dbdiff.sqlite`).

You can also define environment variables in a `.config.sh` file.
Next, pass the args needed by:

    dbdiff --help

Beyond the following notes provided by `--help`,
the individual functions in `{cli,vertica,report}.py` are each documented.

```
Usage: dbdiff [OPTIONS] SCHEMA X_TABLE Y_TABLE JOIN_COLS

  Compare two flat files X_TABLE and Y_TABLE. Assume they are both in the
  same schema = SCHEMA. Join them on the columns in comma-separated string
  JOIN_COLS. Expects that the join columns have matching data type or will
  implicitly cast for comparison, and implicity cast into the type in
  [X_TABLE] for the DIFF table. Expects that all other columns with
  matchings names (those that will be compared) can be compared directly (it
  will cast all dtypes for comparison to the type in X_TABLE).

  Will drop [X_TABLE]_DIFF and [X_TABLE]_JOINED if they exist.

Options:
  -d, --dialect [vertica|postgresql|duckdb|sqlite]
                                  Database dialect to use  [default: vertica]
  --y-schema TEXT                 If the schema for the y_table is different,
                                  specify it.
  --output-schema TEXT            If you want the schema for the output tables
                                  to be different, specify it.
  --drop-output-tables            Drop the joined and diff tables created and
                                  used here.
  --x-table-query                 If X_TABLE is not a table in the database,
                                  but rather a query stored in a file, add
                                  this flag and the query will be read and
                                  instantiated into a temporary table. Ex:
                                  'temp_xtable_name_to_use.sql'.
  --y-table-query                 If Y_TABLE is not a table in the database,
                                  but rather a query stored in a file, add
                                  this flag and the query will be read and
                                  instantiated into a temporary table.
  --exclude-columns TEXT          Comma separated string of column names to
                                  exclude.
  --hierarchical-join             If multiple join keys, and join key #2 is a
                                  subset of join key #1. We expect matches for
                                  all of #1 from both tables even if we dont
                                  match on #1 and #2. This way, we can have
                                  more nuanced output by first breaking out
                                  missing on the first key.
  --max-rows-all INTEGER          Limit of full rows to pull that have
                                  differences.  [default: 10]
  --max-rows-column INTEGER       Limit of grouped and raw column level
                                  differences to pull.  [default: 10]
  --output-format [HTML|XLSX]
  --duckdb-path TEXT              Path to DuckDB database file  [default:
                                  dbdiff.duckdb]
  --sqlite-path TEXT              Path to SQLite database file  [default:
                                  dbdiff.sqlite]
  --help                          Show this message and exit.

```

Development
===========

Unit tests (no Vertica required) cover dtype helpers, implicit type coercion,
SQL template rendering, report generation, and CLI surface checks:

    pytest tests/test_unit.py

Integration tests
-----------------

The integration test suite in `tests/test_dbdiff.py` requires a running Vertica instance.
These tests are marked with `@pytest.mark.integration` and are **skipped by default**.

To run them, start a Vertica instance and then:

    pytest -m integration

> **Note:** As of March 2026, Vertica / OpenText no longer provides a public
> Community Edition Docker image (`vertica/vertica-ce`). The image was removed
> from Docker Hub, so CI integration tests have been disabled. If Vertica
> begins publishing a public container image again in the future, integration
> tests should be re-enabled in CI.
>
> **Maintenance item:** Periodically check whether a public Vertica CE container
> becomes available (e.g. on Docker Hub under `vertica/` or `opentext/`).
