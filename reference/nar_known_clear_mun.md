# Drop a parsed mailing city that contradicts an asserted jurisdiction

\`CSD_NAME\` and \`MUN_NAME\` both constrain, which is what lets a
caller narrow to one community inside an amalgamated city. That is only
the right reading when the caller supplied both. A caller who asserted
\`CSD_NAME = "Vancouver"\` over a string that says \`Toronto\` has
\*contradicted\* the parse, and leaving the parsed mailing city in place
would let it veto the assertion – the search would run in the
intersection of two jurisdictions that do not overlap and return
nothing, which is the confident wrong answer \`known\` exists to
prevent. The mailing city is cleared instead, and the gazetteer or the
tier fills it back in from whatever it actually matched.

## Usage

``` r
nar_known_clear_mun(res, k, rows = seq_len(nrow(res)))
```

## Arguments

- res:

  A parse, one row per address

- k:

  The recycled \`known\` frame, or \`NULL\`

- rows:

  Which row of \`k\` each row of \`res\` belongs to

## Value

\`res\`, with the contradicted mailing city removed
