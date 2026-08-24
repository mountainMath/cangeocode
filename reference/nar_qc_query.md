# Render parsed components the way the Quebec locator expects them

\[nar_address_string()\] renders the NAR canonical form – street type
\*after\* the name, direction abbreviated – and this service will not
read it. It is an Esri locator built on the Répertoire québécois des
adresses, whose reference strings are French-canonical: \`Rue Notre-Dame
Ouest\`, type first and direction spelled out. Sent the NAR form the
locator does not degrade gracefully; it stops matching.

Measured by \`data-raw/probe_qc.R\` (\`PROBE_PART=render\`) on 400 NAR
Quebec addresses with building points, half of them carrying a
direction, by share resolved to a civic point:

\| rendering \| civic \| street only \| unmatched \| \| — \| — \| — \| —
\| \| \`NOTRE-DAME RUE O\` – \[nar_address_string()\] \| 31.5 \| NAR
order, direction spelled out \| 58.0 \| FR order, direction spelled out
\| 58.8 \| NAR order, type and direction spelled out \| 95.0 \| \*\*FR
order, type and direction\*\* – this function \| \*\*95.5

So \*\*the abbreviations are what break it, and the word order barely
matters\*\*: spelling out the direction is worth 26 points, spelling out
the street type another 37, and the order under one. The order is used
anyway because it costs nothing and it is the form the service answers
in, which keeps the floor comparing like with like.

The failure is also silent in the worst way. \`1 RUE NOTRE-DAME O,
MONTREAL\` returns a \*street centroid\* scoring 92.4 where the correct
civic point scores 82.5, so the abbreviated form does not merely lose
the address, it replaces it with a confident wrong answer several
hundred metres away.

Both expansions are French: the direction table below, and the street
type from \`nar_lex_types\` by taking the longest French surface for
each canonical (\`BOUL\` to \`BOULEVARD\`, \`CH\` to \`CHEMIN\`). A
canonical with no French surface is sent unchanged.

## Usage

``` r
nar_qc_query(res)
```

## Arguments

- res:

  Parsed components, as \[normalize_address()\] returns

## Value

A character vector of query strings, one per row
