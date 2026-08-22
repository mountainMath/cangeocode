# Resolve either input form to a frame of address components

\[address_key()\] and \[format_address()\] both take a
\[normalize_address()\] result \*or\* the strings it would be given, so
that a caller who only wants the output string never has to know the
column names. A data frame has already been parsed, which is why
\`prov\` and \`con\` are refused for one – silently ignoring them would
drop a constraint the caller asked for.

## Usage

``` r
nar_as_components(x, prov = NULL, con = NULL)
```

## Arguments

- x:

  A data frame of components or a character vector of addresses

- prov:

  Optional two-letter province code (recycled against \`x\`) to use when
  the string does not name one. Canonicalization is
  language-conditioned, so this materially changes the result:
  \`"avenue"\` normalizes to \`AVE\` in Ontario and \`AV\` in Quebec.

- con:

  An open NAR connection. Supplying one enables gazetteer resolution;
  without it parsing is lexicon-only. The caller keeps ownership – a
  connection passed here is left open, matching \[reverse_geocode()\].

## Value

A data frame carrying the component columns
