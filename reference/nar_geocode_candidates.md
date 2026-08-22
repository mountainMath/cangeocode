# Every NAR address on the street a probe row names

One branch per NAR street-name family, unioned, rather than one join
with \`OFFICIAL = x OR MAIL = x\`. \*\*This is a 99x difference and it
is not a micro-optimization.\*\* An \`OR\` across two columns has no
equijoin key, so DuckDB falls back to a nested loop over the whole
17.4M-row table: the interpolation tier, which has no civic-number
equality to rescue it, took \*\*15.87s\*\* written that way and
\*\*0.16s\*\* as a union, for byte-identical results. The exact tier hid
the problem, because \`CIVIC_NO = p.civic\` gave the planner a hash key
of its own.

\`UNION\` and not \`UNION ALL\`: the two families agree for most
addresses, and the select list carries \`ADDR_GUID\`, so the set union
drops exactly the rows both branches matched and nothing else.

## Usage

``` r
nar_geocode_candidates(probe, select, extra = "", bounds = "")
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- select:

  The select list, with the probe aliased \`p\` and \`Addresses\` \`a\`

- extra:

  Tier-specific predicates, appended to the join condition

- bounds:

  A spatial restriction from \[nar_geocode_bounds()\], or \`""\`

## Value

A SQL fragment producing the candidate set
