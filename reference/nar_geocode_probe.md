# The probe table a geocoding query joins against

Drops the rows nothing could be done with – no street name, or no civic
number to place along it – and blanks the \`NA\`s, because the SQL
treats an absent component as "do not constrain on this" and \`NULL\`
would instead make every comparison against it unknown.

## Usage

``` r
nar_geocode_probe(res, auth_mun = FALSE)
```

## Arguments

- res:

  Parsed components, as \[normalize_address()\] returns

- auth_mun:

  Whether \`MUN_NAME\` is the caller's authoritative value, which sends
  it down the \`MunAlias\` route instead of the direct one

## Value

A data frame with a \`row_id\` back-reference into \`res\`
