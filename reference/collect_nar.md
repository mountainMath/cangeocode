# Collect a NAR table as an sf object

Collects a lazy NAR table into an `sf` object. Geometry is transferred
as WKB rather than WKT, and the CRS is read from the database rather
than assumed, so the result is correct for any NAR version.

## Usage

``` r
collect_nar(tbl, crs = NULL)
```

## Arguments

- tbl:

  nar table to collect

- crs:

  Optional CRS to return the geometry in. Defaults to the CRS the
  geometry is stored in (EPSG:3347). Pass e.g. `"EPSG:4326"` for
  longitude/latitude; coordinates are always returned in lon/lat order.

## Value

An sf object. The internal \`x\`/\`y\` storage-coordinate columns are
dropped: they duplicate the geometry and would not survive a
reprojection.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- nar_connection()
nar_sf <- con |>
  dplyr::tbl("Addresses") |>
  head(20) |>
  collect_nar()
} # }
```
