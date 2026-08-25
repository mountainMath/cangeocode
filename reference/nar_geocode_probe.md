# The probe table a geocoding query joins against

Drops the rows nothing could be done with – no street name, or no civic
number to place along it – and blanks the \`NA\`s, because the SQL
treats an absent component as "do not constrain on this" and \`NULL\`
would instead make every comparison against it unknown.

## Usage

``` r
nar_geocode_probe(res)
```

## Arguments

- res:

  Parsed components, as \[normalize_address()\] returns

## Value

A data frame with a \`row_id\` back-reference into \`res\`
