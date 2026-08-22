# Locate the street type among the remaining tokens

French types lead the name (\`RUE NOTRE-DAME\`, \`CH DU LAC\`) while
English types trail it (\`QUEEN ST\`), so the two languages are scanned
from opposite ends. A type is never taken from the only token left – a
street genuinely named \`PARK\` or \`GREEN\` has to keep its name.

Where several positions are structurally valid, as happens in a
comma-less string whose municipality contains a street-type word, the
tie breaks on how often each type occurs in NAR.

## Usage

``` r
nar_take_type(toks, lang = "en")
```

## Arguments

- toks:

  A character vector of tokens

- lang:

  \`"en"\` or \`"fr"\`

## Value

A list with \`type\`, the preceding \`rest\`, and any tokens \`after\`
it
