# Check NAR geocoding results against the BC Address Geocoder

Re-geocodes each address with the BC service and reports how far its
answer sits from the one already obtained, in metres. It is the way to
answer a question \[geocode()\] cannot answer about itself – whether a
match is right at all – and where the two differ, \*\*BC's answer is the
more reliable\*\*: it is a parcel-level provincial authority, while NAR
is a national compilation of what provinces and municipalities supplied.

## Usage

``` r
bc_validate(g, x = g$input, ...)
```

## Arguments

- g:

  A result from \[geocode()\], with \`lon\`/\`lat\` columns or \`sf\`
  geometry.

- x:

  The address strings to send. Defaults to \`g\$input\`.

- ...:

  Passed to \[bc_geocode()\], including \`min_score\` and \`api_key\`.

## Value

\`g\` with \`bc_match_method\`, \`bc_score\`, \`bc_precision\` and
\`bc_dist_m\` appended. \`bc_dist_m\` is \`NA\` where either side has no
point.

## What a disagreement does and does not prove

The two sources are \*\*not independent\*\* – BC's geocoder and NAR's BC
records plausibly share upstream data – so \`bc_dist_m\` is not a
benchmark of NAR's accuracy. Small distances can be two views of one
underlying record agreeing with itself, and the distribution is a lower
bound on how far apart genuinely independent sources would sit. Use it
to find suspect rows, which it does well, rather than to estimate the
error \`uncertainty_m\` excludes.

Rows outside British Columbia are skipped rather than sent, since the
service does not cover them and would answer about a BC place of the
same name.

## Examples

``` r
if (FALSE) { # \dontrun{
g <- geocode(c("525 Superior St, Victoria, BC", "800 Robson St, Vancouver, BC"))
bc_validate(g)
} # }
```
