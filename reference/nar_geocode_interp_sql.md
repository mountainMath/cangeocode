# The interpolation query

Finds the nearest known civic number below and above the one asked for,
\*\*on the same side of the street\*\*, and places the address on the
straight line between them in proportion to where its number falls in
the numbering.

Three things in here are load-bearing:

\* \`(a.CIVIC_NO is what makes this accurate: 4.2 m median error against
35.2 m when both sides are pooled. \* Candidates are restricted to
\`geom_source = 'building'\`. Interpolating between two blockface
centroids would compound a 176 m error at each end. \* The final \`WHERE
lo_n IS NOT NULL AND hi_n IS NOT NULL\` is the refusal to extrapolate.
Both flanks are required; a number past the end of the run returns
nothing at all.

Duplicate civic numbers are averaged first. NAR carries one row per
address rather than per civic number, so a building with units
contributes many rows at one point, and \`arg_max\` over the raw rows
would pick an arbitrary one.

## Usage

``` r
nar_geocode_interp_sql(probe, bounds = "")
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- bounds:

  A spatial restriction from \[nar_geocode_bounds()\], or \`""\`

## Value

A single SQL string
