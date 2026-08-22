# Fold text to an accent-insensitive, case-insensitive match key

NAR stores accented street types verbatim, so folding to ASCII is only
ever applied to the \*key\* a lookup matches on, never to a value that
ends up in the output. The value handed back is the lexicon's
\`canonical\` column, accents intact.

## Usage

``` r
nar_fold(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector, uppercased and transliterated to ASCII
