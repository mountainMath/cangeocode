# Turn lon/lat columns into whatever the caller asked for

Shared by \[qc_geocode()\] and \[qc_reverse_geocode()\] so the two
cannot drift. The service is asked for EPSG:4326 and \`sf\` means
lon/lat by that name, so there is no axis-order question to get wrong
before the transform.

## Usage

``` r
nar_qc_finish(out, crs = 4326, geometry = FALSE)
```

## Arguments

- out:

  A data frame carrying \`lon\` and \`lat\`

- crs:

  CRS to return

- geometry:

  Whether to return an \`sf\` object

## Value

\`out\`, reprojected, with an \`sf\` geometry column or \`lon\`/\`lat\`
