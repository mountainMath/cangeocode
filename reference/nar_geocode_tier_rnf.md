# The road-network interpolation tier

Places a civic number on the street segment whose address range contains
it, at the position the range implies, offset to the side of the
centreline the range belongs to.

\*\*It refuses when more than one segment matches.\*\* That is not
caution about an unmeasured risk, it is the measurement: on the rows
this tier recovered, the unambiguous ones sit a median 34 m from the
answer \`geocode()\` gives when it can answer at all, while the
ambiguous ones have a 90th percentile of 1,678 m and one in eight of
them lands more than a kilometre away. The whole gross-error tail is
ambiguity – the same street name reached in two municipalities the input
did not separate, or two segments of one street whose imputed ranges
overlap – and it costs 9 rows in 96 to remove it. The refusal is
reported rather than hidden: \`match_method\` reads \`rnf_ambiguous\`
and \`n_matches\` says how many segments were in contention.

The municipality is resolved through NAR's \`MunAlias\` and \*also\*
compared directly against RNF's own census subdivision name. Both are
needed: a mailing city is what a caller writes and only \`MunAlias\`
knows it is a CSD, but 8.3 altogether, and for those \`MunAlias\` has
nothing to say – they are exactly the streets this tier exists to reach.

## Usage

``` r
nar_geocode_tier_rnf(out, probe, todo, con, bounds = NULL)
```

## Arguments

- out:

  The result so far

- probe:

  The probe table

- todo:

  Row indices still needing a position

- con:

  A NAR connection

- bounds:

  The \`within\` restriction as an \`sfc\` in the storage CRS, or
  \`NULL\`. Unlike the other database tiers this one cannot take
  \[nar_geocode_bounds_sql()\]'s output, which constrains the
  \`x\`/\`y\` columns \`Addresses\` has and \`RnfSegments\` does not.

## Value

\`out\`, with this tier's answers filled in
