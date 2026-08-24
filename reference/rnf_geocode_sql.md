# The road-network interpolation query

Finds every segment of the named street, in the named municipality,
whose address range on one side or the other contains the civic number,
and places the address along the one that matched.

Four things in here are load-bearing, and all four are measured in
\`inst/notes/road-network-file.md\`:

\* The join is on \`MATCH_FOLD\`, not on the plain name fold, for the
same reason \[rqa_geocode_sql()\] joins that way: the rows that reach
this tier are the ones \[normalize_address()\]'s gazetteer could not
resolve against NAR, so they still carry the caller's own spelling and
\`ST-\`/\`Sainte\` and hyphen-versus-space have to fold together for
them to join at all. \* The \*\*side is chosen by parity\*\* and then
everything follows from it – the range that positions the address, and
the direction the 13 m offset goes. RNF's left and right are relative to
the direction the segment was digitized, and that convention is real
rather than nominal: the civic number's parity agrees with the range on
the side it geometrically sits on 94.2 \*between\* the two sides; it
does not veto one. An even number inside an odd range on the only side
that has one is still placed there, because a parity mismatch is not
evidence that the range is wrong – the segment may be a single
generalized centreline where the ground has two carriageways, or the
civic number itself may be misfiled – and refusing would drop a real
address to avoid an error the width of a street. \* The \*\*5 placed on
the intersection node itself. With the setback and the offset the median
error is 24.3 m, against 32.1 m for the setback alone, 34.5 m placed
plainly on the line, and 49.3 m for the segment midpoint. \* The offset
direction comes from the \*\*local\*\* direction of travel, taken over a
4 endpoints. A curved block would put the house on the wrong side of a
chord drawn end to end. DuckDB's spatial extension has no
\`ST_OffsetCurve\`, so this is the 2-D cross product by hand; positive
is left of travel.

There is no extrapolation. A civic number outside every range on the
street is not placed at the nearest end of the nearest segment, it is
not placed at all – the same refusal \[nar_geocode_interp_sql()\] makes,
and for the same reason.

## Usage

``` r
rnf_geocode_sql(probe, bounds = "")
```

## Arguments

- probe:

  Name of the temp table holding the parsed components

- bounds:

  The \`within\` restriction as WKT in the storage CRS, or \`""\`

## Value

A single SQL string
