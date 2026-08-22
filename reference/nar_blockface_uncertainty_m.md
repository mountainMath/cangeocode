# The positional error a blockface point carries

NAR's blockface representative point is the centroid of one side of a
street between two intersections, so an address that has no building
point is placed at a segment centroid shared with every other address on
that segment. Measured over 1.85M addresses that carry \*both\* points,
the building-to-blockface separation has a median of 50 m and a 90th
percentile of \*\*176 m\*\*, which is the constant used here.

It is a lower bound for the addresses that actually need it. Only the
1.14M blockface-only addresses ever fall back to this point, and they
skew rural, where blockfaces are longer than the national mix this was
measured over.

## Usage

``` r
nar_blockface_uncertainty_m()
```

## Value

A single number, metres
