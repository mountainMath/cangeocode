# Write a probe subset to a temporary table and run one tier's query

The two NAR tiers differ only in their SQL, so the temp-table round trip
is shared. The table is dropped on exit rather than left for the
connection to clean up, because a caller-supplied connection outlives
the call and geocoding in a loop would otherwise accumulate them.

The empty probe is written and queried like any other rather than
short-circuited, so a caller always gets a result with the query's own
columns and types. Skipping it would return a shapeless
\`data.frame()\`, and every caller would need its own idea of what the
columns should have been.

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
