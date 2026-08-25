# Sort Canadian address strings into structural buckets

Reports the shape each address parsed as, without returning the parse
itself. The point is triage: run it over a column of addresses and the
buckets separate the ordinary cases from the ones that need attention –
\`po_box\` and \`rural_route\` will never match NAR whatever you do to
them, \`unparsed\` and \`street_only\` are the rows to look at by hand,
and the regional forms tell you which conventions your data actually
contains.

This is the same value \[normalize_address()\] returns in its
\`pattern\` column; use that instead when you want the components too.

## Usage

``` r
address_pattern(x, prov = NULL, ...)
```

## Arguments

- x:

  A character vector of address strings

- prov:

  Optional two-letter province code (recycled against \`x\`) to use when
  the string does not name one. Canonicalization is
  language-conditioned, so this materially changes the result:
  \`"avenue"\` normalizes to \`AVE\` in Ontario and \`AV\` in Quebec.

- ...:

  Passed to the gazetteer layer when \`con\` is supplied, and ignored
  otherwise – \`threshold\`, \`name_threshold\`, \`mun_swap_penalty\`
  and \`keep_refused\`; see \[nar_resolve_gazetteer()\]. \`keep_refused
  = TRUE\` adds a \`refused_for\` column and resolves the rows the
  threshold would have left unresolved, flagged with the gate they
  failed.

## Value

A factor, one element per element of \`x\`, with these levels in this
order – each address gets the first one that applies:

- \`po_box\`:

  A post office box, \`case postale\`, or general delivery.

- \`rural_route\`:

  An \`RR\`/\`SS\` rural route, with or without site and compartment.
  Like \`po_box\`, a delivery instruction rather than a place, and
  absent from NAR entirely.

- \`intersection\`:

  Two streets joined by \`&\` rather than a civic number.

- \`numbered_road\`:

  A typeless numbered rural road: the prairie \`Range Road 272\` and
  \`Township Road 514\`, New Brunswick's \`Route 105\`, Ontario
  concessions and county roads.

- \`grid\`:

  A numbered street with a quadrant, the Calgary and Edmonton convention
  – \`96A Street NW\`.

- \`numeric_street\`:

  A numbered street with no quadrant: \`25th Ave\`, \`Line 5\`.

- \`french_street\`:

  The French order, with the type leading the name: \`rue de Vernon\`,
  \`boulevard du President-Kennedy\`.

- \`unit_civic\`:

  An ordinary street address carrying a unit.

- \`civic_street\`:

  An ordinary street address.

- \`street_only\`:

  A street with no civic number.

- \`postal_only\`:

  Nothing but a postal code.

- \`unparsed\`:

  Empty, or nothing the rules recognised.

## See also

\[normalize_address()\], which produces this alongside the parsed
components.

## Examples

``` r
address_pattern(c("53222 Range Road 272, Spruce Grove, AB",
                  "9819 96A Street NW, Edmonton, AB",
                  "845, rue de Vernon, Gatineau, QC",
                  "PO Box 40, Iqaluit, NU"))
#> [1] numbered_road grid          french_street po_box       
#> 12 Levels: po_box rural_route intersection numbered_road ... unparsed
```
