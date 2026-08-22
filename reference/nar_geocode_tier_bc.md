# The BC Address Geocoder tier

The tier behind \`geocode(method = c(..., "bc"))\`. Only the rows its
predecessors left unplaced \*\*and\*\* that are in British Columbia are
sent – the service covers no other province, and asked about an Ontario
address it answers with whatever BC place shares the name.

## Usage

``` r
nar_geocode_tier_bc(res, out, todo, con, bounds = NULL, ...)
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

  Passed to \[bc_geocode()\]

## Value

\`out\`, with the rows the service placed filled in
