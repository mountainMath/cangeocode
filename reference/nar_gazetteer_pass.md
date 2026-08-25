# Score one gazetteer against every candidate reading, and adopt the winner

The machinery both passes share: build the probe, score it
database-side, take one winner per input row, and write the canonical
values back. Only the eligible rows, the query and the \`parse_source\`
label differ, which is why they are arguments rather than two copies of
this.

## Usage

``` r
nar_gazetteer_pass(
  res,
  cand,
  con,
  eligible,
  sql_fn,
  source,
  threshold = 0.85,
  name_threshold = 0.9,
  prepare = NULL,
  keep_refused = FALSE,
  mun_swap_penalty = 1,
  known = NULL
)
```

## Arguments

- res:

  The rows being resolved, carrying \`.row\`

- cand:

  Every candidate reading, carrying \`.row\`, \`.cand\` and \`.probe\`

- con:

  An open NAR connection

- eligible:

  Logical over \`cand\`: which readings this pass may probe

- sql_fn:

  A function of \`(probe_table, name_threshold)\` returning SQL

- source:

  The \`parse_source\` value a match from this pass earns

- threshold:

  Minimum combined score for a match to be accepted

- name_threshold:

  Minimum name similarity, passed to \`sql_fn\`

- prepare:

  Optional function of \`con\`, run once there is work to do

- keep_refused:

  Whether to also adopt the best sub-threshold match, flagged in
  \`refused_for\`

- mun_swap_penalty:

  The multiplier this pass\\s query applied, used only to tell a refusal
  the penalty caused from a refusal it did not

- known:

  The recycled \`known\` frame, or \`NULL\`; see \[nar_known()\]

## Value

\`res\`, with matched rows replaced by their canonical values
