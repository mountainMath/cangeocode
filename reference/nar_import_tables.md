# Build or extend the Addresses and Locations tables

The geometry decisions live here and are made once, so the append path
cannot drift from the create path – an appended province whose
\`x\`/\`y\` disagreed with its \`geom\` would silently break the
bounding-box prefilter for those rows alone, which is close to
undebuggable.

The building point (BG) is the primary geometry; where it is absent the
blockface centroid (BF) stands in – a much coarser point shared by every
address on one side of a street – and \`geom_source\` records which was
used. \`x\`/\`y\` mirror whichever point \`geom\` ended up holding
rather than \`BG\` alone: DuckDB maintains min/max zonemaps for plain
numeric columns, and the bounding-box prefilter in
\[nar_within_radius()\] uses them to skip most row groups, so they have
to agree with the geometry they are filtering.

## Usage

``` r
nar_import_tables(con, address_arrow, location_arrow, append)
```

## Arguments

- con:

  A writable DuckDB connection

- address_arrow:

  Arrow dataset over the Address CSVs

- location_arrow:

  Arrow dataset over the Location CSVs

- append:

  Whether to insert into existing tables

## Value

The connection, invisibly
