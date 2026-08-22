# Take a numbered rural road off the front of a street

The prairie grid and its cousins name a road with a phrase and a number
and no street type at all: NAR files \`Range Road 272\` as
\`OFFICIAL_STREET_NAME\` with \`OFFICIAL_STREET_TYPE\` empty, and the
same holds for Alberta township roads, New Brunswick routes, Ontario
concessions and county roads, and Manitoba's \`Mun\` roads. Left to the
ordinary path these parse as name \`RANGE\` type \`RD\` plus a stray
\`272\`, which joins to nothing.

Two collisions make this narrower than it looks. \`Highway 7\` is
\*not\* one of these – NAR stores it as name \`7\` type \`HWY\`, 115,175
rows – so highways are deliberately absent from the crosswalk. And
\`Route\` splits by province: New Brunswick writes typeless \`Route
105\` (50,942 rows) while Quebec files \`Route 132\` as name \`132\`
type \`ROUTE\` (56,673 rows). Entries carrying a \`prov\` therefore fire
only in that province, and never when the province is unknown, which
leaves the commoner reading in place.

## Usage

``` r
nar_take_numbered_road(toks, prov = NA_character_)
```

## Arguments

- toks:

  A character vector of tokens, civic number already removed

- prov:

  A two-letter province code, or \`NA\`

## Value

A list with \`name\` (\`NA\` if no match) and the \`after\` tokens the
phrase did not consume
