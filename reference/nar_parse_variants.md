# Every reading of one address string worth arbitrating between

Candidate 1 is the ordinary left-to-right parse, unchanged. The rest
come from two places. \[nar_mun_anchor_variants()\] finds the
municipality first and hands the parser the remainder, and it fires only
when \[nar_baseline_is_defective()\] says the baseline is visibly
broken. \[nar_dir_lead_variant()\] puts a stripped leading compass word
back into the street name, and it fires whenever there was one – the
whole point of that defect is that the baseline looks perfectly
well-formed.

Identical readings are collapsed, which is the common case: a
comma-delimited string already puts the municipality in a segment of its
own, so anchoring rediscovers the same split the comma made and there is
nothing to arbitrate.

## Usage

``` r
nar_parse_variants(s, lang = "en", prov = NA_character_)
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

A data frame of one row per candidate, in priority order, carrying the
columns \[nar_parse_one()\] returns plus \`strategy\` and \`.cand\`
