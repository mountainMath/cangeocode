# CRS of the geometry in a NAR database

Reads the CRS recorded at import time, falling back to the package
default for databases built before metadata was recorded.

## Usage

``` r
nar_crs(con)
```

## Arguments

- con:

  A DuckDB connection

## Value

CRS identifier string
