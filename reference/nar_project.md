# Resolve a coordinate input to the NAR storage CRS

The single place where user-supplied coordinates are parsed, so that an
`sf` object and a bare lon/lat pair are treated identically and each is
reprojected exactly once. Reprojecting in sf rather than in DuckDB keeps
the transform under the caller's PROJ configuration, which is what
decides whether a WGS84-to-NAD83 datum shift is applied.

## Usage

``` r
nar_project(x, crs = 4326, storage_crs = nar_storage_crs())
```

## Arguments

- x:

  An `sf`/`sfc` POINT object, or a length-2 numeric longitude/latitude
  vector

- crs:

  CRS of \`x\` when \`x\` is a bare numeric vector, or when an `sf`
  object carries no CRS. Defaults to EPSG:4326.

- storage_crs:

  CRS to return coordinates in

## Value

A length-2 numeric vector of coordinates in \`storage_crs\`
