# Pick one candidate per row and measure the set it came from

The shape both record-resolving tiers share – NAR's and Quebec's – with
only the candidate set, the rank and the column names differing. They
were written out twice before, which meant the ambiguity measurements
were maintained twice.

The second aggregation exists only to measure that ambiguity: it rejoins
the chosen point to every candidate that satisfied the query and reports
how many distinct points there were, how many distinct records, and how
far the furthest of the points sits from the one returned. Points and
records are counted separately because they routinely differ: every unit
of a multi-unit building is its own address at the building's one
coordinate, so \`n_records\` can be 19 where \`n_points\` is 1 – see
\[geocode()\].

## Usage

``` r
nar_geocode_best_sql(cand, rank, cols, id, postal)
```

## Arguments

- cand:

  SQL producing the candidate set, with a \`row_id\` and \`x\`/\`y\`

- rank:

  The tier's \`ORDER BY\` expression

- cols:

  The select list of chosen-row columns, aliased \`b\`

- id:

  The candidate table's record identifier, counted for \`n_records\`

- postal:

  The candidate table's postal-code column

## Value

A single SQL string
