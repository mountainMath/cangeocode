# The exact NAR tier

Looks the civic number up directly. Answers \`nar_building\` or
\`nar_blockface\` depending on which point NAR carries, or
\`nar_no_geometry\` when it carries the record but no coordinates.

## Usage

``` r
nar_geocode_tier_nar(out, probe, todo, con, bounds = "")
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

  A spatial restriction, or \`""\`

## Value

\`out\`, with this tier's answers filled in
