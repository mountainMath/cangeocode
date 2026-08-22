# The SQL predicate for a resolved \`within\` geometry

The bounding box goes first so the zonemap prefilter on \`x\`/\`y\` can
skip row groups before the polygon test is evaluated – the same
mechanism \[nar_within_radius()\] relies on.

## Usage

``` r
nar_geocode_bounds_sql(g)
```

## Arguments

- g:

  An \`sfc\` in the storage CRS, or \`NULL\`

## Value

A SQL fragment, or \`""\`
