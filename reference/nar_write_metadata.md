# Record NAR database metadata

Record NAR database metadata

## Usage

``` r
nar_write_metadata(con, version, provinces = nar_all_provinces())
```

## Arguments

- con:

  A writable DuckDB connection

- version:

  Normalized NAR version string

- provinces:

  Coverage marker: \`"ALL"\`, or the two-letter abbreviations the import
  actually loaded

## Value

The connection, invisibly
