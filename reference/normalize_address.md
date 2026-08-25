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

  Passed to the gazetteer layer when \`con\` is supplied, and ignored
  otherwise – \`threshold\`, \`name_threshold\` and
  \`mun_swap_penalty\`; see \[nar_resolve_gazetteer()\].

## Value

A tibble with one row per element of \`x\`, carrying the NAR-shaped
columns \`APT_NO_LABEL\`, \`CIVIC_NO\`, \`CIVIC_NO_SUFFIX\`,
\`STREET_NAME\`, \`STREET_TYPE\`, \`STREET_DIR\`, \`MUN_NAME\`,
\`PROV_ABVN\` and \`POSTAL_CODE\`, alongside the original \`input\`, the
structural \`pattern\` it parsed as (see \[address_pattern()\] for the
buckets), a \`confidence\` in \`\[0, 1\]\`, a \`mun_remapped\` flag with
its \`mun_evidence\` companion, and a \`parse_source\` naming which
layer settled the row: \`"rules"\` for the lexicon-only parse,
\`"gazetteer"\` for a match against NAR's streets, and \`"rqa"\` for one
against Quebec's own register – available only once \[rqa_import()\] has
been run, and meaning the street was canonicalized against a register
NAR does not carry it in, so a join against \`Addresses\` will still not
find it.

\`mun_remapped\` and \`mun_evidence\` are the pair to read before
trusting \`MUN_NAME\`. See the section below.

## When the municipality is not the one you wrote

The gazetteer restricts its candidate streets through \`MunAlias\`,
which keys on the \*\*census subdivision\*\* rather than on the
community. So writing \`MILFORD, NS\` admits every street in all three
CSDs that name resolves to – Halifax Regional Municipality among them,
which is 166 communities and 225,837 addresses spanning 127 km.
Whichever street wins is then reported with \*its\* own
\`MAIL_MUN_NAME\`, which need not be the one that was written.

That substitution is usually the feature working: it is how a rural
community reaches the mailing municipality NAR files it under, and
\`Toronto\` resolving to \`SCARBOROUGH\` is the value a join against NAR
actually needs. But the same step is what puts an address in the wrong
community when the name it wrote could not be matched exactly, and
neither \`confidence\` nor – downstream – \[geocode()\]'s \`n_matches\`
can see that it happened.

\`mun_remapped\` reports it: \`TRUE\` when the municipality being handed
back is not the one the string named, \`FALSE\` when it is, and \`NA\`
when the row carries no municipality at all. It is \`TRUE\` for a
municipality the string never named as well, since that is also a place
chosen by the gazetteer rather than asserted by the input.

\`mun_evidence\` says \*why\* the substitution was allowed to stand,
which is the part that decides how much to worry. Three of its six
values are attestations, and all three are read out of NAR rather than
out of a curated alias list:

- \`kept\`:

  nothing was substituted – NAR files the address under the name that
  was written.

- \`copostal\`:

  the two names appear on the same \*full\* six-character postal code
  somewhere in NAR, so they are two labels for one delivery geography.
  \`HOWIE CENTER\` and \`SYDNEY\` share three.

- \`csd\`:

  the name written is the census subdivision the street sits in. This is
  what carries amalgamations and legacy names – \`Toronto\` for a street
  NAR still mails to \`NORTH YORK\` – which no postal code will ever
  attest, because the merger did not merge the delivery names.

- \`unattested\`:

  checked against both, and corroborated by neither. This is the class
  the swap penalty fines; see \[nar_resolve_gazetteer()\].

- \`untestable\`:

  NAR files no postal-coded mail under the name that was written, so
  there was nothing to test the substitution against. An absence of
  evidence about an unknown name is not evidence of a bad swap, and this
  class is exempt from the penalty for that reason.

- \`inferred\`:

  the string named no municipality and one was determined for it.

\`NA\` in both columns means the row carries no municipality at all.
\[geocode()\] carries the pair into \`uncertainty_m\`, and prices only
the three unattested ones – measured against an independent reading of
the same houses, an attested substitution lands no further out than a
municipality the input got right.

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
#> # A tibble: 1 × 15
#>   input APT_NO_LABEL CIVIC_NO CIVIC_NO_SUFFIX STREET_NAME STREET_TYPE STREET_DIR
#>   <chr> <chr>           <dbl> <chr>           <chr>       <chr>       <chr>     
#> 1 302-… 302              1055 NA              GEORGIA     ST          W         
#> # ℹ 8 more variables: MUN_NAME <chr>, PROV_ABVN <chr>, POSTAL_CODE <chr>,
#> #   pattern <fct>, confidence <dbl>, parse_source <chr>, mun_remapped <lgl>,
#> #   mun_evidence <chr>
normalize_address("1234A-990 boul. du President-Kennedy Ouest, Montreal, QC")
#> # A tibble: 1 × 15
#>   input APT_NO_LABEL CIVIC_NO CIVIC_NO_SUFFIX STREET_NAME STREET_TYPE STREET_DIR
#>   <chr> <chr>           <dbl> <chr>           <chr>       <chr>       <chr>     
#> 1 1234… 1234A             990 NA              DU PRESIDE… BOUL        O         
#> # ℹ 8 more variables: MUN_NAME <chr>, PROV_ABVN <chr>, POSTAL_CODE <chr>,
#> #   pattern <fct>, confidence <dbl>, parse_source <chr>, mun_remapped <lgl>,
#> #   mun_evidence <chr>

if (FALSE) { # \dontrun{
con <- nar_connection()
normalize_address("100 queen st w toronto on", con = con)
DBI::dbDisconnect(con)
} # }
```
