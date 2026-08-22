# Provinces a NAR database holds, as a user-facing value

The public read of what \[nar_connection()\] actually downloaded. A
national database reports every province rather than the internal
\`"ALL"\` marker, so the return value can be compared against a
\`PROV_ABVN\` column without special-casing.

## Usage

``` r
nar_provinces(con)
```

## Arguments

- con:

  A NAR connection, as returned by \[nar_connection()\]

## Value

A character vector of two-letter province abbreviations

## Examples

``` r
if (FALSE) { # \dontrun{
con <- nar_connection(provinces = "BC")
nar_provinces(con)
#> [1] "BC"
} # }
```
