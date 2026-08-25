# Everything a geocoding call has to settle before it can query

Shared by \[geocode()\] and \[geocode_matches()\], which ask the same
question of the same database and differ only in whether they report the
record chosen or all of them. Resolving the connection, checking that
the tiers named have something to run against, parsing, applying the
authoritative overrides and building the spatial restriction are the
same work in both, and drifting apart on any of them – most of all on
the overrides – would make the enumeration describe a different search
than the answer it is meant to explain.

The tier availability checks run \*\*before any parsing\*\* rather than
when a tier is first reached: whether a tier runs at all depends on what
its predecessors left unplaced, so a missing import would otherwise
surface on one batch and stay silent on the next.

## Usage

``` r
nar_geocode_setup(x, prov, mun, within, method, crs, version, con)
```

## Arguments

- x:

  Address strings, or a parsed data frame

- prov, mun, within:

  Constraints, as in \[geocode()\]

- method:

  The tiers that will be run, already validated

- crs:

  The CRS \`within\` is expressed in

- version, con:

  Which database to use

## Value

A list of \`con\`, the parsed \`res\`, and \`bounds\` as an \`sfc\` or
\`NULL\`
