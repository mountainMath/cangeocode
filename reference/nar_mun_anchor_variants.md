# Readings that take the municipality off the end before parsing the street

The trailing token run is tested against the municipality inventory,
longest first, and every run that names a real place becomes a candidate
whose remainder is parsed with the municipality already decided. Both
lengths can be real – \`NORTH BAY\` and \`BAY\` are each municipalities
– so both are offered rather than the longer one being assumed.

This is what makes a trailing comma inconsequential. \`"... TH25,
Vancouver"\` parses today only because the comma bounds the
municipality, and \`"... TH25 Vancouver"\` fails for want of it;
anchoring reaches the same remainder from both, and the comma stops
carrying the parse.

Two guards keep this from inventing splits. A run is only considered if
it lies inside the last comma segment – a municipality never spans a
comma the writer put in – and a candidate is dropped unless a street
name survives in the remainder, which is what stops \`"123 Kingston"\`
from resolving to the city of Kingston with no street at all.

## Usage

``` r
nar_mun_anchor_variants(s, lang = "en", prov = NA_character_)
```

## Arguments

- s:

  A single normalized string, province and postal code already removed

- lang:

  \`"en"\` or \`"fr"\`, deciding the canonical forms

- prov:

  A two-letter province code, or \`NA\`. Only the numbered-road step
  consults it, and only for the entries that are province-specific.

## Value

A list of one-row data frames, possibly empty
