# Choose between candidate parses on the evidence rules alone can muster

Without a database the only evidence available is whether the
municipality each reading proposes is a place that exists, which is
exactly the question the readings disagree about. \`TH25 VANCOUVER\` is
not a municipality and \`VANCOUVER\` is; \`100 MILE HOUSE\` is one and
\`MILE HOUSE\` is not, which is why the same rule cannot be written as a
heuristic about token shapes.

The order is: a municipality that exists beats one that does not, then
the completeness score, then candidate order – so the baseline reading
is displaced only by a candidate that is strictly better evidenced,
never by a tie. When a connection is supplied
\[nar_resolve_gazetteer()\] arbitrates again over the same candidates,
with better evidence.

## Usage

``` r
nar_arbitrate_rules(cand)
```

## Arguments

- cand:

  A candidate tibble carrying \`.row\`, \`.cand\`, \`MUN_NAME\`,
  \`PROV_ABVN\` and \`confidence\`

## Value

An integer vector of the winning row of \`cand\` per \`.row\`
