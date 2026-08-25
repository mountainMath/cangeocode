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
nar_gazetteer_sql(probe, name_threshold = 0.9, mun_swap_penalty = 0.88)
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- name_threshold:

  Minimum name similarity for the fuzzy branch

- mun_swap_penalty:

  Score multiplier for a candidate in a municipality other than the one
  the string named, where the two are not attested aliases. \`1\`
  disables it.

## Value

A single SQL string

## The penalty for changing the municipality

The fuzzy branch widens the candidate set from the municipality that was
written to every municipality sharing its census subdivision, which is
how a misspelt street in a small community is found at all. But the
widening is coarse – \`MILFORD, NS\` reaches 166 communities over 127 km
through Halifax Regional Municipality alone – and inside that set a
street in the \*wrong\* community can outscore the right one on a single
edit, because agreement on the street type (0.10) buys more than one
Damerau-Levenshtein step costs (0.072 at the gate).

So a candidate whose mailing municipality is not the one the string
named has its whole score multiplied by \`mun_swap_penalty\` – unless
the two names are \*attested aliases\*, meaning they share a full postal
code somewhere in NAR (see \[nar_mun_copostal()\]). The penalty is
multiplicative rather than a subtraction so that it scales with how good
the match otherwise was, and it reorders as well as refuses: an
unattested swap now loses to a lower-scoring candidate in the
municipality that was actually written.

Two cases are exempt and both matter. A string that named no
municipality at all has nothing to have swapped – its locality came from
the postal code. And a name NAR files no mail under (\`p.mun_testable\`
false) cannot be checked for attestation either way, so penalising it
would fine the string for being unusual rather than for being wrong.

\*\*Why 0.88.\*\* With the 0.85 acceptance threshold it means an
unattested swap has to score 0.966 before the penalty, which is a street
name that is exact or one keystroke from it \*and\* agreement on
everything else the string supplied: the rule is that two uncertainties
at once is one too many. Swept against Nova Scotia's PVSC points, over
32,887 exact unambiguous building matches carrying 98 errors past 5 km:

\| penalty \| matches lost \| of those, past 5 km \| 5 km errors removed
\| \| — \| — \| — \| — \| \| 0.92 \| 107 \| 15.9 \| 0.90 \| 151 \| 12.6
\| \*\*0.88\*\* \| \*\*503\*\* \| \*\*9.2 \| 0.86 \| 566 \| 8.3 \| 0.85
\| 1126 \| 5.1

0.88 is the knee and the exchange rate is what picks it, not the error
count: 0.90 to 0.88 buys 27 gross errors for 352 matches, 0.88 to 0.86
buys one more for 63. Below 0.85 the penalty stops discriminating at all
– every unattested swap is refused whatever else it got right, and 85
costs were within 100 m of an independent reading. The base rate makes
the column in the middle the one to read: 0.3 cut that is 9.2

One province, and the knee is what is provincial about it – the
mechanism a fuzzy name compounding an unattested swap is not. \`1\`
restores the behaviour before this existed and \`0.85\` refuses every
unattested swap outright.
