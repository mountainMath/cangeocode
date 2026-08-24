# Find or fetch the RNF shapefile

Find or fetch the RNF shapefile

## Usage

``` r
rnf_resolve_shp(shp = NULL, release = "latest")
```

## Arguments

- shp:

  An explicit path, or \`NULL\`

- release:

  Two-digit release year, or \`"latest"\`

## Value

A list of \`shp\` (path), \`dir\` (extraction directory), \`release\`
and \`temporary\` (whether this function created the directory and may
delete it)
