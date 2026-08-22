# Parse one normalized address string into its components

Walks the tokens left to right: unit, then civic number and suffix, then
direction, street type and name. The order matters – the unit has to
come off before the civic number, or \`302-1055\` reads as a civic
number of 302 and the real one is lost.

## Usage

``` r
nar_parse_one(s, lang = "en", prov = NA_character_, mun_fixed = NA_character_)
```

## Arguments

- s:

  A single normalized string, province and postal code already removed

- lang:

  \`"en"\` or \`"fr"\`, deciding the canonical forms

- prov:

  A two-letter province code, or \`NA\`. Only the numbered-road step
  consults it, and only for the entries that are province-specific.

- mun_fixed:

  A municipality already taken off the string by
  \[nar_mun_anchor_variants()\], or \`NA\` to locate one here. When it
  is supplied every remaining token is the street: the comma split no
  longer nominates a municipality, and whatever trails the street type
  is dropped rather than becoming one. That is what makes a trailing
  comma inconsequential – \`"6093 Iona Dr TH25"\` and \`"6093 Iona Dr
  TH25 ,"\` are the same token stream once the comma is gone.

## Value

A one-row data frame of components
