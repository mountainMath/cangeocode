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

Four guards keep this from inventing splits, and the last two are what
let the gate open as wide as it now does.

\* A run is only considered if it lies inside the last comma segment –
see \[nar_mun_anchor_runs()\]. \* A candidate is dropped unless a street
name survives in the remainder, which is what stops \`"123 Kingston"\`
from resolving to the city of Kingston with no street at all. \* \*\*A
residue that is not a street name counts as no street name.\*\* Every
place name that also does duty as a street name fails here rather than
at the gate: \`135 de Nantes\` anchors Nantes and leaves \`DE\`, \`22
avenue de la Durantaye\` leaves \`DE LA\`, \`80 rue Albanel\` leaves
\`RUE\`. Particules are not a name, and neither is a street type
standing alone. \* \*\*A run that is a street type has to be one the
street can spare.\*\* \`TRAIL\` is a municipality in Ontario and a
street type everywhere, and \`82 Fesroches Trail\` is the second; \`4830
scott ave terrace\` is the first, and the only thing that separates them
is that the street in it still names a type of its own once \`TERRACE\`
is taken away. Same for \`maple ridge\`, \`bowen island\`, \`brentwood
bay\` and \`qualicum beach\`, all of which end in a NAR street type.

## Usage

``` r
nar_mun_anchor_variants(toks, lang = "en", prov = NA_character_)
```

## Arguments

- toks:

  A token vector, as \[nar_tokens()\] produces

- lang:

  \`"en"\` or \`"fr"\`, deciding the canonical forms

- prov:

  A two-letter province code, or \`NA\`. Only the numbered-road step
  consults it, and only for the entries that are province-specific.

## Value

A list of one-row data frames, possibly empty
