# What each BC match precision is worth

The BC Address Geocoder \*\*always answers\*\*. Feed it \`"1234
Nonexistentzzz Rd, Victoria, BC"\` and it returns the centre of Victoria
with a score of 48 rather than nothing at all, so a response is not a
match and \`matchPrecision\` is the field that decides what was actually
resolved. This table maps its vocabulary onto the same \`match_method\`
and \`uncertainty_m\` contract \[geocode()\] uses.

\*\*The metres here are the service's own precision semantics translated
into order-of-magnitude figures, not a measurement.\*\* Unlike every
other number in this package they were not derived from data: BC
publishes \`locationPositionalAccuracy\` as the categorical
\`high\`/\`medium\`/\`low\`/\`coarse\` and no distance at all. They are
deliberately pessimistic, and calibrating them against NAR building
points over a BC sample is the obvious next step – see
\`inst/notes/geocoding-status.md\`. Treat them as a ranking that is safe
to filter on, not as an error bar comparable to the NAR tiers'.

## Usage

``` r
nar_bc_precision(precision)
```

## Arguments

- precision:

  The \`matchPrecision\` value from a feature

## Value

A one-row data frame of \`match_method\` and \`uncertainty_m\`
