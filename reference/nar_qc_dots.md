# Pick the \`...\` arguments the Quebec tier understands

\`geocode(...)\` serves online tiers whose arguments do not overlap, and
an argument meant for one of them must not error the others. Same job as
\[nar_nrcan_dots()\] and the same reason.

## Usage

``` r
nar_qc_dots(dots, supplied = c("x", "prov", "geometry", "crs", "con"))
```

## Arguments

- dots:

  The \`...\` from \[nar_geocode_match()\]

- supplied:

  Argument names the tier fills in itself

## Value

The subset of \`dots\` \[qc_geocode()\] declares
