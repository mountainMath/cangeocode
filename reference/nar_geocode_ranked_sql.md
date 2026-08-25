# Return every candidate instead of one, in the order the tier ranks them

The other reading of the same candidate set \[nar_geocode_best_sql()\]
collapses. No aggregation and no \`QUALIFY\`: the rank becomes a column
rather than a filter, so row 1 of each \`row_id\` is the row the
collapsing query would have kept.

## Usage

``` r
nar_geocode_ranked_sql(cand, rank)
```

## Arguments

- cand:

  SQL producing the candidate set, with a \`row_id\`

- rank:

  The tier's \`ORDER BY\` expression

## Value

A single SQL string
