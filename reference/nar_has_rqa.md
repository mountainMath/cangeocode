# Is RQA available on this connection?

Both tables, not either: \[rqa_import()\] writes \`RqaAddresses\` first
and \`RqaStreets\` second, so a run that died partway through leaves a
database that reads as having no RQA rather than as having half of it.

## Usage

``` r
nar_has_rqa(con)
```

## Arguments

- con:

  A DuckDB connection

## Value

\`TRUE\` when the RQA tables are present
