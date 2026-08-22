# Score how completely the rules parsed each address

A blunt completeness score, not a probability: the share of the
components that a joinable address needs which actually came out
populated. Layer 2 replaces it with a match score where it can.

## Usage

``` r
nar_rules_confidence(res)
```

## Arguments

- res:

  A tibble of parsed components

## Value

A numeric vector in \`\[0, 1\]\`
