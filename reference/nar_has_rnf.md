# Is the road network file available on this connection?

Both tables, not either. \[rnf_import()\] writes \`RnfSegments\` first
and \`RnfStreets\` second, so a run that died partway through leaves a
database that reads as having no RNF rather than as having half of it.

## Usage

``` r
nar_has_rnf(con)
```

## Arguments

- con:

  A DuckDB connection

## Value

\`TRUE\` when the RNF tables are present
