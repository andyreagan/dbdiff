Overview
========

Compare two tables on Vertica,
that are expected to be exactly the same.

Design
======

There are a handful of design decisions to be made here.
The goal is to find and show cell-by-cell differences from two tabular data sources.

First,
We will use SQL templates rendered in Python code and use Vertica as a backend to build the combined table and to pull differences off of.
Some alternatives here could be using Pandas as the tabular backend,
and all of the code in Python using the Pandas API.
Another,
that would be better for bigger data sets than Pandas,
would be to use PySpark DataFrames and either the SQL or Python API to analyze them.
A project that use Pandas or PySpark is [datacompy](https://github.com/capitalone/datacompy).

With Vertica as the backend
(and we assume data is already in Vertica),
the inputs to the Python process:
two sets of data as (schema, table) and a list of join columns.
Python will look at the two tables,
find all columns that match on name
(no premature optimization to allow users to specify a mapping - that will have to a view or copied table passed to this process),
make sure that the join columns you passed are in both tables and they form a primary key on both tables.
It will then generate two persisted tables in Vertica,

- the “joined” table,
- and the “diff” table.

These are created as “regular” tables,
though they could for this purpose be local temp tables
(and maybe that’s a good thing to do,
but leaving them in Vertica for now unless you specify `--drop-output-tables`).
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

Once you've installed, should be as simple as setting the following environment variables:

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

You can also define any of these in a `.config.sh` file.
Next, pass the args needed by:

    dbdiff --help

Beyond the following notes provided by `--help`,
the individual functions in `{cli,vertica,report}.py` are each documented.

```
Usage: dbdiff [OPTIONS] SCHEMA X_TABLE Y_TABLE JOIN_COLS

  Compare two flat files X_TABLE and Y_TABLE, using Vertica as the join
  engine. Assume they are both in the same schema = SCHEMA. Join them on the
  columns in comma-separated string JOIN_COLS. Expects that the join columns
  have matching data type or will implicitly cast for comparison, and
  implicity cast into the type in [X_TABLE] for the DIFF table. Expects that
  all other columns with matchings names (those that will be compared) can
  be compared directly (it will cast all dtypes for comparison to the type
  in X_TABLE).

  Will drop [X_TABLE]_DIFF and [X_TABLE]_JOINED if they exist.

Options:
  --y-schema TEXT              If the schema for the y_table is different,
                               specify it.
  --output-schema TEXT         If you want the schema for the output tables to
                               be different, specify it.
  --drop-output-tables         Drop the joined and diff tables created and
                               used here.
  --x-table-query              If X_TABLE is not a table in Vertica, but
                               rather a query stored in a file, add this flag
                               and the query will be read and instantiated
                               into a temporary table. Ex:
                               'temp_xtable_name_to_use.sql'.
  --y-table-query              If Y_TABLE is not a table in Vertica, but
                               rather a query stored in a file, add this flag
                               and the query will be read and instantiated
                               into a temporary table.
  --exclude-columns TEXT       Comma separated string of column names to
                               exclude.
  --hierarchical-join          If multiple join keys, and join key #2 is a
                               subset of join key #1. We expect matches
                               for all of #1 from both tables even if we dont
                               match on #1 and #2. This way, we can have more
                               nuanced output by first breaking out missing on
                               the first key.
  --max-rows-all INTEGER       Limit of full rows to pull that have
                               differences.  [default: 10]
  --max-rows-column INTEGER    Limit of grouped and raw column level
                               differences to pull.  [default: 10]
  --output-format [HTML|XLSX]
  --help                       Show this message and exit.

```

Development
===========

The tests rely on a running instance of Vertica.
Locally, in a separate terminal window, you can start one of these like:

    docker run -p 5433:5433 jbfavre/vertica:9.2.0-7_centos-7

To run the all tests run:

    tox
