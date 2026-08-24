# Check NAR geocoding results against the Quebec government geocoder

The Quebec counterpart to \[bc_validate()\]: re-geocodes each address
with the provincial service and reports how far its answer sits from the
one already obtained, in metres. It also returns the service's canonical
form of the address, which for Quebec is often the more useful half – an
accent, a particle or a cardinal point that the parser read differently
shows up in \`qc_address\` even where \`qc_dist_m\` is small.

## Usage

``` r
qc_validate(g, x = g$input, ...)
```

## Arguments

- g:

  A result from \[geocode()\], with \`lon\`/\`lat\` columns or \`sf\`
  geometry.

- x:

  The address strings to send. Defaults to \`g\$input\`.

- ...:

  Passed to \[qc_geocode()\], including \`min_score\` and
  \`batch_size\`.

## Value

\`g\` with \`qc_match_method\`, \`qc_locator\`, \`qc_score\`,
\`qc_address\`, \`qc_postal\` and \`qc_dist_m\` appended. \`qc_dist_m\`
is \`NA\` where either side has no point.

## What a disagreement does and does not prove

the service's locators are named \`RQA_Adresse\` and \`RQA_Rue\`, so it
is built on the \*\*Répertoire québécois des adresses\*\* – the same
MRNF product that also reaches NAR. The two sources are therefore
\*\*not independent\*\*, in the same way and for the same reason
\[bc_validate()\]'s are not, and \`qc_dist_m\` is a lower bound on how
far apart genuinely independent sources would sit rather than a
benchmark of NAR. Use it to find suspect rows.

Rows outside Quebec are skipped rather than sent, since the service does
not cover them.

## Examples

``` r
if (FALSE) { # \dontrun{
g <- geocode(c("1 Rue Notre-Dame Ouest, Montreal, QC",
               "1000 rue de la Gauchetiere Ouest, Montreal, QC"))
qc_validate(g)
} # }
```
