# Resolve \`within\` to a geometry in the storage CRS

Split out from the SQL so the same restriction can be enforced twice:
pushed into the NAR query as a predicate, and applied in R to points
that came from somewhere else – the BC fallback, which is a separate
service and cannot be given this package's SQL.

## Usage

``` r
nar_geocode_bounds_geom(within, crs, con)
```

## Arguments

- within:

  An \`sf\`/\`sfc\`/\`sfg\`, an \`st_bbox\`, or a length-4 numeric

- crs:

  CRS to interpret a bare numeric or an untagged geometry in

- con:

  A NAR connection, for the storage CRS

## Value

An \`sfc\` in the storage CRS, or \`NULL\`
