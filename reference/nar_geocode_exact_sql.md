# The exact-match geocoding query

Kept as its own function, like \[nar_gazetteer_sql()\], so the SQL can
be read and tested without a database. The candidate set it collapses is
the same one \[nar_geocode_matches_sql()\] enumerates.

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
