# Layout version of the NAR database

Bumped when the import produces a materially different database. Version
2 added the \`x\`/\`y\` coordinate columns that make radius queries
fast, and corrected the datum used to read NAR's lon/lat columns.
Version 3 fell back to the blockface centroid for addresses with no
building point and added \`geom_source\` to tell the two apart. Version
4 added the \`Streets\` gazetteer that \[normalize_address()\] resolves
against, and version 5 the \`MunAlias\` and \`PostalMun\` tables that
let a municipality be reached by any of its names, or by postal code
alone. Databases built by earlier versions still work, with the features
that need the newer tables degrading rather than failing; see
\[nar_within_radius()\] and \[nar_has_streets()\].

## Usage

``` r
nar_schema_version()
```

## Value

Integer schema version
