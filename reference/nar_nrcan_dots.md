# Pick the \`...\` arguments the geolocator tier understands

\`geocode(...)\` has to serve two online tiers whose arguments do not
overlap. The BC tier keeps receiving all of \`...\`, because
\[bc_geocode()\] forwards what it does not recognize to its own service
as a query parameter and a filter would break that. \[nrcan_geocode()\]
has no such passthrough – the geolocator takes one query parameter – so
its formals are a closed set and unknown names can be dropped rather
than raising an error about an argument meant for the other tier.

Derived from the formals rather than listed, so an argument added to
\[nrcan_geocode()\] does not have to be remembered here.

## Usage

``` r
nar_nrcan_dots(dots)
```

## Arguments

- dots:

  \`list(...)\` as \[nar_geocode_match()\] captured it

## Value

The subset of \`dots\` to forward
