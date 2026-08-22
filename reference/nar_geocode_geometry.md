# Attach coordinates to a geocoding result

Storage-CRS coordinates in, either \`lon\`/\`lat\` columns or an \`sf\`
object out. The reprojection is done in \`sf\` rather than in DuckDB
because these are freshly computed coordinates rather than a stored
geometry column, and because it leaves the axis-order handling to
\`sf\`, which always means lon/lat – the \`always_xy\` trap that
\[collect_nar()\] has to work around does not arise. The storage CRS is
still read from the database rather than assumed.

## Usage

``` r
nar_geocode_geometry(out, x, y, con, crs = 4326, geometry = FALSE)
```

## Arguments

- out:

  The result data frame

- x, y:

  Coordinates in the storage CRS, \`NA\` where nothing matched

- con:

  A NAR connection

- crs:

  Target CRS

- geometry:

  Whether to return an \`sf\` object

## Value

\`out\` with coordinates attached
