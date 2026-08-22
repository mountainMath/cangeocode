# Recover the point geometry from a geocode() result

\[geocode()\] returns either an \`sf\` object or plain \`lon\`/\`lat\`
columns, and \[bc_validate()\] has to work with whichever it is handed.

## Usage

``` r
nar_geocode_points(g)
```

## Arguments

- g:

  A result from \[geocode()\]

## Value

An \`sfc\` of POINTs in EPSG:4326, empty where nothing was placed
