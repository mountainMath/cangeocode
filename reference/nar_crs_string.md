# Render a CRS as a string DuckDB's spatial extension accepts

DuckDB wants an authority string such as \`"EPSG:4326"\`; the bare
number \`4326\` that \`sf\` and the rest of this package take happily is
a binder error there. Everything user-supplied is funnelled through here
so a numeric CRS works the same way in \[collect_nar()\] as it does in
\[reverse_geocode()\].

## Usage

``` r
nar_crs_string(crs)
```

## Arguments

- crs:

  An EPSG code, an authority string, or an \`sf\` crs object

## Value

A length-1 character CRS identifier
