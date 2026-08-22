# How many NAR addresses sit in a municipality of this name

The province is a preference rather than a filter. NAR's own
\`MunAlias\` province can disagree with the one the string named – a
mailing city near a boundary, a name shared across two provinces – and
refusing the place on that basis would throw away the evidence over a
detail the arbitration does not turn on.

## Usage

``` r
nar_municipality_n(key, prov = NA_character_)
```

## Arguments

- key:

  A folded municipality key, as \[nar_mun_key()\] builds it

- prov:

  A two-letter province code, or \`NA\`

## Value

The address count, or \`NA\` when no municipality of that name exists
