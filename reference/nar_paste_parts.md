# Join the address parts that are present with single spaces

The vectorised equivalent of \`paste(na.omit(c(...)), collapse = " ")\`
applied per row, returning \`""\` when every part is missing. Interior
spacing inside a part is preserved, so a street name is never reflowed.

## Usage

``` r
nar_paste_parts(...)
```

## Arguments

- ...:

  Equal-length vectors of address components

## Value

A character vector with no missing values
