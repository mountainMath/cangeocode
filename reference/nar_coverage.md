# Provinces a NAR database holds

Reads the \`provinces\` metadata key. Databases built before schema
version 6 have no such key and were necessarily national, so the
fallback is the whole country – the same pattern \[nar_crs()\] uses for
databases predating the \`crs\` key.

## Usage

``` r
nar_coverage(con)
```

## Arguments

- con:

  A DuckDB connection

## Value

\`"ALL"\`, or a character vector of two-letter abbreviations
