# Canonicalize a supplied province

\`"British Columbia"\`, \`"B.C."\` and \`"bc"\` are the same constraint,
and NAR stores only the last of them. Anything the province lexicon does
not recognize is passed through folded rather than refused – it will
simply match nothing, which is the honest outcome for a province code
that is not one.

## Usage

``` r
nar_known_prov(v)
```

## Arguments

- v:

  A character vector

## Value

A character vector of two-letter codes
