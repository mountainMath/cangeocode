# Resolve parsed components by running the requested tiers in priority order

Each tier is offered only the rows its predecessors left unplaced.
Running them in sequence rather than running them all and picking the
best answer is worth the extra temp table: a NAR tier is a full scan of
the 17.4M-row \`Addresses\` table, and the all-exact case is the common
one, so the later tiers usually see almost nothing.

That scan is also why neither NAR query goes through \`Streets\` or
wants an index. Measured on the 2026-06 release, the folded street-key
join costs 0.05s for a 5-row probe and \*\*0.08s for a 200-row probe\*\*
– the scan is the whole cost and every probe row shares it, exactly as
with the radius query. Batch your addresses into one call rather than
looping.

## Usage

``` r
nar_geocode_match(
  res,
  con,
  method = c("nar", "nar_interpolate"),
  bounds = "",
  bounds_geom = NULL,
  auth_mun = FALSE,
  ...
)
```

## Arguments

- res:

  Parsed components, as \[normalize_address()\] returns

- con:

  A NAR connection

- method:

  Tiers to try, in priority order

- bounds:

  A spatial restriction from \[nar_geocode_bounds_sql()\], or \`""\`

- bounds_geom:

  The same restriction as an \`sfc\`, for the tiers that run outside the
  database

- auth_mun:

  Whether \`MUN_NAME\` is the caller's authoritative value

- ...:

  Passed to the \`bc\` tier

## Value

A data frame with one row per row of \`res\`, carrying \`ADDR_GUID\`,
\`match_method\`, \`uncertainty_m\`, \`n_matches\`, \`x\` and \`y\`
