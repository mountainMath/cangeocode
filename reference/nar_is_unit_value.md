# Does this token look like a unit number rather than a word?

The test that keeps the ambiguous designators in
\`nar_lex_unit_ambiguous\` – which is \`STE\`, Suite and equally Sainte
– from taking an ordinary word as their value. A unit number carries a
digit (\`600\`, \`4B\`, \`5TH\`) or is a lone letter (\`A\`). It is
applied only to those designators: \`APT BSMT\` and \`APT TRLR\` are
real units whose value is a word, and requiring a number everywhere
collapses them into the street name.

## Usage

``` r
nar_is_unit_value(x)
```

## Arguments

- x:

  A character vector of tokens, unfolded

## Value

A logical vector
