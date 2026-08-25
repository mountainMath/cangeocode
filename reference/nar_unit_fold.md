# Put a parsed unit into the vocabulary the address files use

NAR stores a unit as Canada Post spells one – \`BSMT\`, \`UPPR\`,
\`LWR\` – and a person types \`Basement\`, \`Sous-sol\`, \`Upper\`.
Those are the bare labels \[normalize_address()\] already goes out of
its way to recognize, so failing to match them afterwards would be
recognizing a word in order to throw it away.

\*\*The translation runs one way only, and that is the point.\*\* It is
applied to the \*input\* and never to the stored column, because the
stored column does not need it: of NAR's 5.96M units, \`BASEMENT\`
appears zero times, \`UPPER\` once and \`GROUND\` once, against 137,413
\`BSMT\` and 22,757 \`UPPR\`. So this is a translation into NAR's
vocabulary, not a fold both sides share – which means it carries none of
the keep-the-two-halves-identical hazard that \[nar_match_fold()\] does.

Zero padding is \*\*not\*\* normalized, having been measured and
declined: 11,966 of 5.96M units carry an interior leading zero,
essentially all of them \`PH01\`-style penthouse labels, and a rule that
turned \`PH01\` into \`PH1\` would be reaching for 0.2 whose label is
meaningfully padded.

## Usage

``` r
nar_unit_fold(x)
```

## Arguments

- x:

  A parsed \`APT_NO_LABEL\`, or \`""\` for none

## Value

The unit as NAR would spell it
