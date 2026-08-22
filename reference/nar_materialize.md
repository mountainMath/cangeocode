# Write a lazy query into a permanent table, creating it or adding to it

The append branch renders the same lazy pipeline to SQL and inserts it
in one pass, rather than staging it to a second temporary table first.
\`INSERT ... BY NAME\` matches columns by name, so a column-order
difference between the pipeline and the existing table is a no-op
instead of a silent transposition.

## Usage

``` r
nar_materialize(con, query, name, append)
```

## Arguments

- con:

  A writable DuckDB connection

- query:

  A lazy \`dbplyr\` table

- name:

  Target table name

- append:

  Whether the table already exists and should be added to

## Value

The connection, invisibly
