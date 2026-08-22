# Write a probe subset to a temporary table and run one tier's query

The two NAR tiers differ only in their SQL, so the temp-table round trip
is shared. The table is dropped on exit rather than left for the
connection to clean up, because a caller-supplied connection outlives
the call and geocoding in a loop would otherwise accumulate them.

## Usage

``` r
nar_geocode_run_tier(probe, todo, con, sql_fn, bounds)
```

## Arguments

- probe:

  The full probe table

- todo:

  Row indices still needing a position

- con:

  A NAR connection

- sql_fn:

  A function of \`(table_name, bounds)\` returning SQL

- bounds:

  A spatial restriction, or \`""\`

## Value

The query result, possibly zero rows
