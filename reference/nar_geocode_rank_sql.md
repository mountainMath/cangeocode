# The rank that decides which candidate a tier answers with

One window expression, defined once, because both readings of a
candidate set are supposed to agree: \[nar_geocode_best_sql()\] keeps
the row it puts first and \[nar_geocode_ranked_sql()\] returns them all
in that same order, so \`match_rank == 1\` in \[geocode_matches()\] is
by construction the record \[geocode()\] answered with. Written twice
they would drift, and the drift would be invisible – an enumeration that
quietly disagreed with the answer it exists to explain.

## Usage

``` r
nar_geocode_rank_sql(rank)
```

## Arguments

- rank:

  The tier's \`ORDER BY\` expression

## Value

A SQL fragment
