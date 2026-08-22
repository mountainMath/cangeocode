# The exact-match geocoding query

Kept as its own function, like \[nar_gazetteer_sql()\], so the SQL can
be read and tested without a database.

A building point always outranks a blockface one for the same address,
and \`ADDR_GUID\` breaks any remaining tie so the answer is stable
across runs rather than depending on scan order. The second aggregation
exists only to measure ambiguity: it rejoins the chosen point to every
candidate that satisfied the query and reports how many distinct points
there were and how far the furthest of them sits from the one returned.

## Usage

``` r
nar_geocode_exact_sql(probe, bounds = "")
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- bounds:

  A spatial restriction from \[nar_geocode_bounds()\], or \`""\`

## Value

A single SQL string
