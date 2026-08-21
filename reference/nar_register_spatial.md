# Register the NAR spatial macros on a connection

Creates the \`nar\_\*\` TEMP macros that every spatial operation in the
package goes through. They are temporary, so they are recreated on each
connection and work against read-only databases and databases built by
earlier versions of the package.

Every transform between the storage CRS and lon/lat passes
\`always_xy\`. EPSG:4269, like most authority-defined geographic CRSs,
declares its axes in latitude/longitude order, while this package – and
sf – always speak longitude/latitude. Without the flag DuckDB reads a
longitude of -123 as a latitude and quietly returns \`POINT (inf inf)\`
rather than an error.

The macros are:

- \`nar_point(lon, lat)\`:

  a longitude/latitude pair as a point in the storage CRS

- \`nar_xy(x, y)\`:

  a coordinate pair that is already in the storage CRS

- \`nar_lon(geom)\`, \`nar_lat(geom)\`:

  the inverse: stored geometry back to longitude/latitude

- \`nar_geom(geom)\`:

  tags stored geometry with its CRS, enabling DuckDB's CRS-mismatch
  check

- \`nar_store(geom)\`:

  the inverse, dropping the tag so the column stays RTREE-indexable

- \`nar_wkb(geom)\`:

  WKB for transfer to sf, mapping NULL geometry to an empty point

## Usage

``` r
nar_register_spatial(con, crs = nar_crs(con))
```

## Arguments

- con:

  A DuckDB connection

- crs:

  CRS of the stored geometry, read from the database by default

## Value

The connection, invisibly
