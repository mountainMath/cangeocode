# Lift comma-delimited unit segments out of a split address

A segment that is nothing but a unit – \`", 320,"\`, \`", \# 500,"\`,
\`", Suite 600,"\`, \`", 5th Floor,"\` – is a form real filings use
constantly, and one the segment split otherwise mangles: it is neither
the street nor the municipality, so it gets absorbed into whichever
neighbour it is handed to. \`"9320 Boulevard Saint-Laurent, 320,
Montreal"\` read its street name as \`SAINT-LAURENT 320\` before this
existed.

The last remaining segment is the municipality, so a unit segment has to
come out before that choice is made rather than after it.

## Usage

``` r
nar_take_unit_segments(segs, lang = "en")
```

## Arguments

- segs:

  A list of token vectors from \[nar_split_commas()\]

- lang:

  \`"en"\` or \`"fr"\`

## Value

A list of \`unit\` (or \`NA\`) and the remaining \`segs\`
