# The Quebec geocoder tier

The tier behind \`geocode(method = c(..., "qc"))\`. Only the rows its
predecessors left unplaced \*\*and\*\* that are in Quebec are sent – the
service covers no other province.

Unlike the other two online tiers this one costs a request per
\*thousand\* rows rather than per row, so naming it is cheap even on a
large unplaced tail.

## Usage

``` r
nar_geocode_tier_qc(res, out, todo, con, bounds = NULL, ...)
```

## Arguments

- res:

  The parsed components, after any authoritative override

- out:

  The result so far, as \[nar_geocode_match()\] builds it

- todo:

  Row indices still needing a position

- con:

  A NAR connection, for the storage CRS

- bounds:

  An \`sfc\` in the storage CRS, or \`NULL\`

- ...:

  Passed to \[qc_geocode()\]

## Value

\`out\`, with the rows the service placed filled in
