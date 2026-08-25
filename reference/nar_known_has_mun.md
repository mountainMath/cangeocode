# Which rows had their municipality asserted rather than resolved

A caller who names the municipality has settled the question the swap
penalty exists to arbitrate, so those rows report \`mun_evidence\`
\`"kept"\` and no remap – there was nothing for the gazetteer to
substitute.

## Usage

``` r
nar_known_has_mun(k, n)
```

## Arguments

- k:

  The recycled \`known\` frame, or \`NULL\`

- n:

  The number of rows

## Value

A logical vector
