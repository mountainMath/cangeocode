# Check NAR geocoding results against the BC Address Geocoder

Re-geocodes each address with the BC service and reports how far its
answer sits from the one already obtained, in metres. This is the only
independent positional source currently wired up, and it is the way to
answer questions \[geocode()\] cannot answer about itself – whether a
match is right at all, and how much error NAR's own points carry, which
\`uncertainty_m\` explicitly excludes.

Rows outside British Columbia are skipped rather than sent, since the
service does not cover them and would answer about a BC place of the
same name.

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

## Examples

``` r
if (FALSE) { # \dontrun{
g <- geocode(c("525 Superior St, Victoria, BC", "800 Robson St, Vancouver, BC"))
bc_validate(g)
} # }
```
