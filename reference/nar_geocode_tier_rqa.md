# The RQA geocoding tier

Looks the civic number up in Quebec's own register. Quebec only, and
gated on the parsed province exactly as the online \[qc_geocode()\] tier
is: \`RqaAddresses\` holds nothing outside Quebec, so an address that
never named a province would otherwise be placed there on the strength
of a street name that exists in every province.

\`match_method\` carries RQA's own positional class rather than one
label, because the register states how each point was placed and the
classes are not interchangeable – only a quarter of them are building
placements. \`uncertainty_m\` is \`0\` for \`rqa_building\` and \`NA\`
otherwise: nothing in this package has measured what \`Geocodee\` or
\`Incertaine\` are worth on the ground, and a number invented here would
be indistinguishable from a measured one. Where several RQA points
satisfy the query the spread between them is reported, which is a
genuine lower bound whatever the class.

## Usage

``` r
nar_geocode_tier_rqa(res, out, probe, todo, con, bounds = "")
```

## Arguments

- res:

  Parsed components, for the province gate

- out:

  The result so far

- probe:

  The probe table

- todo:

  Row indices still needing a position

- con:

  A NAR connection

- bounds:

  A spatial restriction, or \`""\`

## Value

\`out\`, with this tier's answers filled in
