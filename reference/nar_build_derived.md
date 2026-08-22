# Build the gazetteer tables normalize_address() resolves against

These are aggregates over the whole \`Addresses\` table, so on an append
they are rebuilt rather than added to – a street that gained addresses
needs its counts and civic-number range recomputed, not extended. They
cost a few grouped scans, which is small beside the import that preceded
them.

## Usage

``` r
nar_build_derived(con)
```

## Arguments

- con:

  A writable DuckDB connection

## Value

The connection, invisibly
