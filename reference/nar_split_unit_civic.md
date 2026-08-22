# Split a hyphenated unit-civic token

\`302-1055\` is a unit and a civic number, the near-universal Canadian
convention. The trailing half must be all digits and the leading half a
short alphanumeric label; a longer leading half is a hyphenated street
name, not a unit.

## Usage

``` r
nar_split_unit_civic(tok)
```

## Arguments

- tok:

  A single token

## Value

A list with \`unit\` and \`civic\`, the latter \`NA\` if the token did
not split
