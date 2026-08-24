# Build the RqaAddresses and RqaStreets tables

One pass over the CSV. \`all_varchar\` is not optional: the register
writes empty strings rather than nulls and \`numero_municipal\` infers
as a number in some partitions and a string in others, so letting DuckDB
guess produces a schema that depends on which rows it sampled.

\`IN_NAR\` is a left semi-join against this database's own
\`Addresses\`, keyed on forward sortation area, civic number and the
folded street name – the same fold the gazetteer matches on, so
\`ST-\`/\`Sainte\` and hyphen-versus-space are already handled. Both NAR
name families are unioned into the key, because neither is complete on
its own. It is deliberately an \*equality\* test and not a containment
one, so it over-reports the gap by roughly 14 NAR sometimes carries the
same address under a longer name that contains RQA's. Containment has no
equijoin key and would turn a scan into a product.

## Usage

``` r
rqa_build_tables(con, csv)
```

## Arguments

- con:

  A writable DuckDB connection with the spatial macros registered

- csv:

  Path to \`RQA.csv\`

## Value

The connection, invisibly
