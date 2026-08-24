# Coerce the many ways a caller can hand over points

\[qc_reverse_geocode()\] takes lon/lat vectors, a matrix, a data frame
or an \`sf\` object, and the rest of it should only ever see an \`sfc\`.

## Usage

``` r
nar_qc_points(x, y = NULL, crs = 4326)
```

## Arguments

- x:

  What the caller passed

- y:

  Latitudes, when \`x\` is numeric

- crs:

  CRS of \`x\` and \`y\` when they are numeric

## Value

An \`sfc\` of POINTs
