# Sort parsed addresses into buckets by the shape they parsed as

Assigns each row exactly one pattern, testing in the order
\[nar_address_patterns()\] lists. The order is what makes the buckets
useful: the regional and structural quirks are checked before the
ordinary forms, so \`grid\` and \`french_street\` describe the addresses
that are actually unusual rather than being swamped by the
\`civic_street\` majority they overlap with. A row therefore reports the
most specific thing true of it, and only that.

## Usage

``` r
nar_address_pattern(res, traits, marks)
```

## Arguments

- res:

  A tibble from \[nar_parse_rules()\]

- traits:

  A character vector of comma-joined parse traits, one per row

- marks:

  A character vector from \[nar_delivery_marks()\]

## Value

A factor with the levels \[nar_address_patterns()\] gives
