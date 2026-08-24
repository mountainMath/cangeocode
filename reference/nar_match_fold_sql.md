# \[nar_match_fold()\] as a SQL expression over one column

The same transform DuckDB-side, so the gazetteer's own spelling is
folded the way the probe was. It has to stay in step with
\[nar_match_fold()\] character for character, which \`test-normalize.R\`
asserts over a fixture of the shapes it exists for.

## Usage

``` r
nar_match_fold_sql(col)
```

## Arguments

- col:

  A SQL column reference

## Value

A SQL expression string
