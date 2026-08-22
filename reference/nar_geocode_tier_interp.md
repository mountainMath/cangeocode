# The NAR interpolation tier

Places a civic number NAR does not carry between the nearest known
civics of the same parity on either side of it.

## Usage

``` r
nar_geocode_tier_interp(out, probe, todo, con, bounds = "")
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
