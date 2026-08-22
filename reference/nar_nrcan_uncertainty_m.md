# What a surviving NRCan answer is worth

Unlike the BC figures, this one is \*\*measured\*\*, on the same
90th-percentile basis as the rest of the package: the distance between a
surviving geolocator answer and NAR's own building point, over the
\`REPEATABLE (42)\` sample \`data-raw/probe_geolocator.R\` draws. Two
runs give p90 = 115 m over 204 survivors and 152 m over 88, and the
constant is the \*\*more conservative of the two\*\* rather than the
better-sampled one.

\*\*Do not read it as comparable to \`nar_blockface\`'s 176 m even
though it is smaller.\*\* The two distributions have very different
shapes: a blockface error is bounded by the length of a blockface, while
this one is a percentile on a long tail – p95 212 m, p99 648 m, worst
survivor 2.7 km. Half the survivors land inside 33 m and a few land in
the wrong part of town, so filtering on \`uncertainty_m\` alone treats
the two tiers as interchangeable when they are not. See
\`inst/notes/geocoding-status.md\`.

## Usage

``` r
nar_nrcan_uncertainty_m()
```

## Value

A single number, metres
