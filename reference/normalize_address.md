# Normalize Canadian address strings into NAR components

Parses free-text Canadian addresses into the structured components NAR
is keyed on, which is what a forward geocode has to join against.
Parsing is deterministic: a tokenizer plus the closed street-type,
direction and province vocabularies that NAR itself uses. Supplying
\`con\` additionally resolves the result against the NAR street
gazetteer, which corrects misspellings and fills in components the
string left ambiguous.

## Usage

``` r
normalize_address(x, prov = NULL, con = NULL, ...)
```

## Arguments

- x:

  A character vector of address strings

- prov:

  Optional two-letter province code (recycled against \`x\`) to use when
  the string does not name one. Canonicalization is
  language-conditioned, so this materially changes the result:
  \`"avenue"\` normalizes to \`AVE\` in Ontario and \`AV\` in Quebec.

- con:

  An open NAR connection. Supplying one enables gazetteer resolution;
  without it parsing is lexicon-only. The caller keeps ownership – a
  connection passed here is left open, matching \[reverse_geocode()\].

- ...:

  Additional arguments (currently unused)

## Value

A tibble with one row per element of \`x\`, carrying the NAR-shaped
columns \`APT_NO_LABEL\`, \`CIVIC_NO\`, \`CIVIC_NO_SUFFIX\`,
\`STREET_NAME\`, \`STREET_TYPE\`, \`STREET_DIR\`, \`MUN_NAME\`,
\`PROV_ABVN\` and \`POSTAL_CODE\`, alongside the original \`input\`, the
structural \`pattern\` it parsed as (see \[address_pattern()\] for the
buckets), a \`confidence\` in \`\[0, 1\]\` and a \`parse_source\` of
\`"rules"\` or \`"gazetteer"\`.

## Comma-less input

Addresses that separate their parts with commas parse most reliably,
because the commas bound the street from the municipality. A comma-less
string such as \`"100 queen st w toronto"\` has to guess where the
street ends, and the guess is only as good as the street-type vocabulary
– a municipality whose name contains a street-type word (Port Hope,
Grand Falls) can be mis-split. Passing \`con\` resolves these against
the gazetteer and is strongly recommended for messy input.

## Examples

``` r
normalize_address("302-1055 W Georgia St, Vancouver, BC V6E 3P3")
#> # A tibble: 1 × 13
#>   input APT_NO_LABEL CIVIC_NO CIVIC_NO_SUFFIX STREET_NAME STREET_TYPE STREET_DIR
#>   <chr> <chr>           <dbl> <chr>           <chr>       <chr>       <chr>     
#> 1 302-… 302              1055 NA              GEORGIA     ST          W         
#> # ℹ 6 more variables: MUN_NAME <chr>, PROV_ABVN <chr>, POSTAL_CODE <chr>,
#> #   pattern <fct>, confidence <dbl>, parse_source <chr>
normalize_address("1234A-990 boul. du President-Kennedy Ouest, Montreal, QC")
#> # A tibble: 1 × 13
#>   input APT_NO_LABEL CIVIC_NO CIVIC_NO_SUFFIX STREET_NAME STREET_TYPE STREET_DIR
#>   <chr> <chr>           <dbl> <chr>           <chr>       <chr>       <chr>     
#> 1 1234… 1234A             990 NA              DU PRESIDE… BOUL        O         
#> # ℹ 6 more variables: MUN_NAME <chr>, PROV_ABVN <chr>, POSTAL_CODE <chr>,
#> #   pattern <fct>, confidence <dbl>, parse_source <chr>

if (FALSE) { # \dontrun{
con <- nar_connection()
normalize_address("100 queen st w toronto on", con = con)
DBI::dbDisconnect(con)
} # }
```
