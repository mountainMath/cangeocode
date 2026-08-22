# Update the recorded coverage of an existing database

Written last on an append path, after the data and the derived tables
are in place, so an interrupted append under-reports what it holds
rather than over-reporting it. The cost of the former is one redundant
download; the cost of the latter is a province that silently answers
nothing.

## Usage

``` r
nar_set_coverage(con, provinces)
```

## Arguments

- con:

  A writable DuckDB connection

- provinces:

  \`"ALL"\` or a character vector of abbreviations

## Value

The connection, invisibly
