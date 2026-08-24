# A surface-to-canonical lookup, with the French reading resolved in R

The lexicons are language-conditioned and the preference logic lives in
\[nar_lex_lookup()\], so it is resolved here – once, over a few hundred
surfaces – rather than reimplemented in SQL where it would be free to
drift. Quebec takes the French reading: \`AVENUE\` is \`AV\`, not
\`AVE\`.

## Usage

``` r
rqa_lex_map(lex)
```

## Arguments

- lex:

  A lexicon data frame

## Value

A two-column data frame of \`surface_fold\` and \`canonical\`
