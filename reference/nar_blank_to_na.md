# Empty strings back to NA

NAR stores an absent street type or direction as \`”\` rather than NULL,
but the normalizer's contract is \`NA\` for absent.

## Usage

``` r
nar_blank_to_na(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector
