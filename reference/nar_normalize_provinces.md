# Resolve however a caller named a province to the canonical abbreviation

Accepts the two-letter abbreviation, the two-digit SGC code (as a string
or a number), or the full name, in any case, and returns the
abbreviation. \`"ALL"\`, \`"all"\`, \`"national"\` and \`"canada"\` all
resolve to the whole-country marker.

Unrecognized input is an error listing what is available rather than a
silent drop: a typo'd province would otherwise produce a database that
is quietly missing the data the caller asked for, and the mistake would
only surface later as unmatched addresses.

## Usage

``` r
nar_normalize_provinces(x)
```

## Arguments

- x:

  Character or numeric vector of province identifiers, or \`NULL\`

## Value

A character vector of two-letter abbreviations, or the single value
\`"ALL"\`; \`NULL\` passes through unchanged
