# Look a surface form up in a lexicon, preferring the province's language

Resolution is language-conditioned: \`AVENUE\` canonicalizes to \`AVE\`
in Ontario but \`AV\` in Quebec, and \`WEST\` to \`W\` against \`O\`. A
row tagged \`"both"\` matches either language. When the preferred
language has no entry the lookup falls back to any language, so a French
street type in an English province (\`RUE\` in Ottawa) still resolves
rather than being dropped.

## Usage

``` r
nar_lex_lookup(surface, lex, lang = "en")
```

## Arguments

- surface:

  A character vector of already-folded surface forms

- lex:

  A lexicon data frame with \`surface_fold\`, \`canonical\` and \`lang\`

- lang:

  A character vector of \`"en"\` / \`"fr"\`, recycled against
  \`surface\`

## Value

A character vector of canonical tokens, \`NA\` where nothing matched
