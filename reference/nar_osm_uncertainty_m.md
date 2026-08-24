# What a surviving OSM answer is worth

\*\*Not measured, and therefore not asserted.\*\* Every other
uncertainty constant in this package is a 90th percentile against a
stated reference – \`nar_nrcan_uncertainty_m()\` is 150 m over the
sample \`data-raw/probe_geolocator.R\` draws – and there is no
equivalent run for this source yet. \`data-raw/probe_osm.R\` is the
harness that would produce one; until it has been run over a national
sample, a surviving answer reports \`NA\` rather than a number invented
to look like the others.

This is also why \[osm_geocode()\] is not wired into \[geocode()\] as a
tier. \`uncertainty_m\` is a column callers filter on, and a tier
contributing \`NA\` to it would quietly make that filter mean something
different depending on which tier answered.

What can be said without measuring: a surviving answer is a
\`place_rank\` 30 object carrying its own house number, which is an
address someone entered or imported rather than a position interpolated
along a range. That is a different \*\*kind\*\* of answer from the
geolocator's, not necessarily a better one – OSM's Canadian address
coverage is uneven and concentrated in municipalities whose open data
was imported.

## Usage

``` r
nar_osm_uncertainty_m()
```

## Value

A single number, metres, or \`NA\`
