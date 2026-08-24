# Record what was imported, in the same metadata table NAR uses

Keyed with an \`rqa\_\` prefix rather than in a table of its own, so
\[nar_metadata()\] reports the whole state of the database in one read
and a database with no RQA simply has no such keys.

## Usage

``` r
rqa_write_metadata(con, csv)
```

## Arguments

- con:

  A writable DuckDB connection

- csv:

  Path the register was read from

## Value

The connection, invisibly
