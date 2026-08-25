# The query behind \[geocode_matches()\]

Every NAR record that satisfied the query, ranked. The candidate set is
built by the same \[nar_geocode_candidates()\] and
\[nar_geocode_civic_key()\] the exact tier uses, so the two cannot
disagree about what matched – only about how much of it to report.

## Usage

``` r
nar_geocode_matches_sql(probe, bounds = "")
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- bounds:

  A spatial restriction from \[nar_geocode_bounds()\], or \`""\`

## Value

A single SQL string
