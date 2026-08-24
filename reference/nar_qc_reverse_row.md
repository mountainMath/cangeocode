# Read one reverse-geocode response into a result row

Split out from the request so the response shape is testable against a
saved fixture with no network, the way \[nar_qc_locations()\] is for the
forward direction.

This endpoint answers with a bare \`address\` object rather than a
\`locations\` array, and it reports \*\*no distance\*\*, which is why
\`qc_dist_m\` is left empty here for \[qc_reverse_geocode()\] to
measure. Asked about a point with nothing within \`distance\`, the
service returns an object carrying an \`error\` and no \`address\`,
which is a refusal and not a failure.

## Usage

``` r
nar_qc_reverse_row(d)
```

## Arguments

- d:

  The parsed response, or \`NULL\` if the body would not parse

## Value

A one-row data frame, all-\`NA\` when there was no answer
