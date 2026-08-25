# Overwrite parsed components with the ones the caller asserted

Applied twice in \[normalize_address()\]: before the gazetteer, so it
restricts on what was asserted rather than on what the string happened
to yield, and after it, so a substitution the gazetteer would otherwise
make cannot overwrite the caller.

## Usage

``` r
nar_known_apply(res, k, rows = seq_len(nrow(res)))
```

## Arguments

- res:

  A parse, one row per address

- k:

  The recycled \`known\` frame, or \`NULL\`

- rows:

  Which row of \`k\` each row of \`res\` belongs to

## Value

\`res\`, with the asserted components written in
