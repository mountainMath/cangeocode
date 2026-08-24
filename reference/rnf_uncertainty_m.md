# The positional error an RNF interpolation carries

\`max(95, 0.35 \* len_m)\`, which is a 90th-percentile error like every
other \`uncertainty_m\` in this package, and like them it is measured
rather than assumed.

It is two-part because the error is. A short segment is dominated by the
setback and the side offset, which do not shrink with the block, so a
flat floor is the right shape there; a long one is dominated by how far
along the block the range put the house, which scales. Measured over the
addresses the tier actually recovered, it covers 91.7 segments longer
than 600 m, where a flat 110 m covers 90.4 flat model looks equivalent
in the aggregate and fails exactly where the answer is least certain.

## Usage

``` r
rnf_uncertainty_m(len_m)
```

## Arguments

- len_m:

  Segment length in metres

## Value

A numeric vector, metres
