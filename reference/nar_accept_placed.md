# Which rows of a geocoding result carry a position

The package's own test for placed, applied to whichever representation
the result is in: \`lon\`/\`lat\` columns, or an \`sf\` geometry whose
unplaced rows are empty points.

## Usage

``` r
nar_accept_placed(x)
```

## Arguments

- x:

  A data frame from \[geocode()\]

## Value

A logical vector over the rows of \`x\`
