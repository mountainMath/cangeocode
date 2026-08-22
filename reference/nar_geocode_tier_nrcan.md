# The NRCan geolocator tier

The tier behind \`geocode(method = c(..., "nrcan"))\`. Unlike the BC
tier there is no province restriction to apply – the service is
national. It is offered only the rows its predecessors left unplaced,
and it must sit last: its median answer is an order of magnitude coarser
than a NAR building point and its tail is far longer.

## Usage

``` r
nar_geocode_tier_nrcan(res, out, todo, con, bounds = NULL, ...)
```

## Arguments

- res:

  The parsed components, after any authoritative override

- out:

  The result so far, as \[nar_geocode_match()\] builds it

- todo:

  Row indices still needing a position

- con:

  A NAR connection, for the storage CRS and the gazetteer

- bounds:

  An \`sfc\` in the storage CRS, or \`NULL\`

- ...:

  Passed to \[nrcan_geocode()\]

## Value

\`out\`, with the rows the service placed filled in
