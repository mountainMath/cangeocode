# Resolve either input form to a frame of address components

\[address_key()\] and \[format_address()\] both take a
\[normalize_address()\] result \*or\* the strings it would be given, so
that a caller who only wants the output string never has to know the
column names. A data frame has already been parsed, which is why
\`prov\` and \`con\` are refused for one – silently ignoring them would
drop a constraint the caller asked for.

## Usage

``` r
nar_as_components(x, known = NULL, con = NULL)
```

## Arguments

- x:

  A data frame of components or a character vector of addresses

- known:

  Components the caller already has, as a named list of vectors each
  length 1 or \`length(x)\` – \`list(PROV_ABVN = "NS", MUN_NAME = "Howie
  Centre")\`. Authoritative: each one overrides what the string parsed
  to, lands on the returned row, and restricts the gazetteer.
  \`MUN_NAME\` is the mailing city and \`CSD_NAME\` the administrative
  one, and they are different searches; see \[nar_known()\] for the full
  key list and for why the two are separate. \`PROV_ABVN\` additionally
  reaches the parser, where it materially changes the result:
  canonicalization is language-conditioned, so \`"avenue"\` normalizes
  to \`AVE\` in Ontario and \`AV\` in Quebec.

- con:

  An open NAR connection. Supplying one enables gazetteer resolution;
  without it parsing is lexicon-only. The caller keeps ownership – a
  connection passed here is left open, matching \[reverse_geocode()\].

## Value

A data frame carrying the component columns
