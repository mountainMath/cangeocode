# Does this database carry the street gazetteer?

\`Streets\` and \`PostalMun\` arrived in schema version 4, and
\`MunAlias\` in version 5. Databases built before it are still perfectly
usable – they just skip this layer – so the check is a capability probe
rather than an error.

## Usage

``` r
nar_has_streets(con)
```

## Arguments

- con:

  A NAR connection

## Value

\`TRUE\` when both gazetteer tables are present
