# The gazetteer scoring query

Kept as its own function so the scoring can be read and tested without a
database. Two branches, selected by whether the row has a locality to
restrict candidates to:

\* \*\*fuzzy\*\*, when a municipality was named or a postal code
supplies one – name similarity (weight 0.72) plus agreement on street
type (0.10), direction (0.06) and the civic number falling inside the
street's range (0.12). The last three only ever add: a string that
omitted them should not be penalised for it, but one that supplied them
and agrees should outrank a competing street that does not. \*
\*\*exact\*\*, when it has neither – an indexed equality on either name
family, scored the same way but discounted for the absent locality, and
answering only with what every candidate of that name agrees on.

## Usage

``` r
nar_gazetteer_sql(probe, name_threshold = 0.9)
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- name_threshold:

  Minimum name similarity for the fuzzy branch

## Value

A single SQL string
