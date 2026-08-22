# Normalize an address string ahead of tokenizing

Uppercases, drops the punctuation that only ever decorates abbreviations
(\`St.\`, \`boul.\`, \`B.C.\`), folds the several dash and fraction
characters real address data arrives with onto one form, and pads commas
so they survive as their own tokens. Accents are \*kept\* – NAR stores
them, and the normalizer's output should match – so folding to ASCII
happens only in \[nar_fold()\], at match time, and never on the way out.

## Usage

``` r
nar_norm_text(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector
