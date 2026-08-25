# Put a supplied component into the shape the parser produces

Through \[nar_norm_text()\], the same normalization the address string
gets, so an asserted \`"Howie Centre"\` and a parsed \`HOWIE CENTRE\`
are one value. Without it the override would be authoritative and
unmatchable at the same time.

## Usage

``` r
nar_known_text(v)
```

## Arguments

- v:

  A character vector

## Value

A character vector
