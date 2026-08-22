# Recycle an authoritative constraint to one value per input

Length 1 or length \`n\` and nothing in between: a partial vector would
recycle silently and constrain the wrong rows.

## Usage

``` r
nar_recycle(v, n, what)
```

## Arguments

- v:

  The supplied value

- n:

  Number of inputs

- what:

  Argument name, for the error message

## Value

A character vector of length \`n\`
