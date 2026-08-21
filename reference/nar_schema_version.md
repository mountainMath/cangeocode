# Layout version of the NAR database

Bumped when the import produces a materially different database. Version
2 added the \`x\`/\`y\` coordinate columns that make radius queries
fast, and corrected the datum used to read NAR's lon/lat columns.
Version 3 fell back to the blockface centroid for addresses with no
building point and added \`geom_source\` to tell the two apart.
Databases built by earlier versions still work; see
\[nar_within_radius()\].

## Usage

``` r
nar_schema_version()
```

## Value

Integer schema version
