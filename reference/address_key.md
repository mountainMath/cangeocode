# Build a match key from parsed address components

Collapses a parsed address into a single string that two spellings of
the same address share, which is what joining or deduplicating two
address lists needs. Components are folded to an accent- and
case-insensitive form and punctuation is dropped, so \`St. John's\` and
\`SAINT JOHNS\` key alike, and the fields are laid out broad to narrow –
province, municipality, street, civic number – so sorting the keys
clusters a street together.

Normalizing is what does the real work here; the key only makes the
result joinable. Pass \`con\` (or normalize with it first) whenever the
input is messy: the gazetteer is what turns a misspelled street into
NAR's own spelling, and two lists cannot key alike on a name only one of
them got right.

## Usage

``` r
address_key(x, unit = FALSE, sep = "|", prov = NULL, con = NULL)
```

## Arguments

- x:

  Either a data frame of parsed components, as returned by
  \[normalize_address()\], or a character vector of address strings to
  normalize first.

- unit:

  Include the unit number in the key? The default \`FALSE\` keys a
  \*building\*, so every suite in a tower collapses to one key. Set it
  to \`TRUE\` to key a tenant instead – at the cost that the unit is the
  least reliably parsed component, so rows that agree on everything else
  will split whenever one of them wrote its suite somewhere the parser
  did not find it.

- sep:

  The separator between fields. It only has to be a character the
  components cannot contain; the default is fine unless a downstream
  tool treats \`\|\` specially.

- prov, con:

  Passed to \[normalize_address()\], and only allowed when \`x\` is a
  character vector – a data frame has already been parsed.

## Value

A character vector, one element per row of \`x\`, and \`NA\` for any row
with no street name. Those are rows nothing could be keyed from, and
they are \`NA\` rather than an empty key so they cannot all collapse
onto each other. Note that \`dplyr\`'s joins match \`NA\` to \`NA\` by
default, so filter them out or pass \`na_matches = "never"\`.

## See also

\[format_address()\] for the human-readable form,
\[normalize_address()\] for the components themselves.

## Examples

``` r
# Five spellings of one building, one key.
address_key(c("1055 W Georgia St, Vancouver, BC",
              "1055 West Georgia Street, Vancouver, British Columbia",
              "Suite 1500 - 1055 W Georgia St, Vancouver BC",
              "#1500-1055 west georgia st., vancouver, b.c.",
              "1055 WEST GEORGIA ST, VANCOUVER, BC V6E 3P3"))
#> [1] "BC|VANCOUVER|GEORGIA|ST|W|1055|" "BC|VANCOUVER|GEORGIA|ST|W|1055|"
#> [3] "BC|VANCOUVER|GEORGIA|ST|W|1055|" "BC|VANCOUVER|GEORGIA|ST|W|1055|"
#> [5] "BC|VANCOUVER|GEORGIA|ST|W|1055|"

# Keying the tenant instead of the building keeps the suites apart.
address_key(c("Suite 1500 - 1055 W Georgia St, Vancouver BC",
              "Suite 800 - 1055 W Georgia St, Vancouver BC"), unit = TRUE)
#> [1] "BC|VANCOUVER|GEORGIA|ST|W|1055||1500"
#> [2] "BC|VANCOUVER|GEORGIA|ST|W|1055||800" 
```
