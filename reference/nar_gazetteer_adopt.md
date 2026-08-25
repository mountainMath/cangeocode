# Write a gazetteer winner back over the parse it corrects

Write a gazetteer winner back over the parse it corrects

## Usage

``` r
nar_gazetteer_adopt(res, cand, best, source, refused_for = NA_character_)
```

## Arguments

- res:

  The rows being resolved, carrying \`.row\`

- cand:

  Every candidate reading, carrying \`.probe\`

- best:

  One winning row per input, from \[nar_gazetteer_winner()\]

- source:

  The \`parse_source\` value a match from this pass earns

- refused_for:

  \`NA\`, or the gate these rows failed

## Value

\`res\`, with the matched rows replaced by their canonical values
