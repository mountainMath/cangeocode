# Fold a municipality name to the lexicon's match key

The lexicon is keyed the way \[nar_norm_text()\] leaves input – periods
stripped, commas spaced out – because NAR keeps the periods that input
does not: \`ST. JOHN'S\` (54,129 addresses), \`SAULT STE. MARIE\`
(36,711) and \`ST. ALBERT\` (29,097) can otherwise never meet a key
built from tokens.

## Usage

``` r
nar_mun_key(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector of match keys
