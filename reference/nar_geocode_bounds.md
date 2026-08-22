# Turn a spatial restriction into a SQL fragment

Two clauses, and both are wanted. The bounding box is compared against
the \`x\`/\`y\` columns, which is the cheap half: those are plain
\`DOUBLE\` columns with per-row-group zonemaps, so DuckDB skips whole
row groups whose range cannot satisfy it instead of reading them.
\`ST_Within\` then makes the restriction exact for a genuine polygon.
For a rectangle the second clause is nearly redundant, but not quite – a
rectangle in the caller\\s CRS is not a rectangle in the storage CRS –
and it keeps one code path rather than two.

The outline is densified before reprojection. Transforming only the
corners of a longitude/latitude rectangle into a projected CRS and
taking the box of the result clips the bulge along each edge, which
would silently drop addresses inside the region the caller asked for.

## Usage

``` r
nar_geocode_bounds(within, crs, con)
```

## Arguments

- within:

  An \`sf\`/\`sfc\` object, an \`st_bbox\`, a length-4 numeric \`c(xmin,
  ymin, xmax, ymax)\`, or \`NULL\`

- crs:

  CRS to interpret \`within\` in when it carries none

- con:

  A NAR connection, for the storage CRS

## Value

A SQL fragment to append to the join condition, or \`""\`
