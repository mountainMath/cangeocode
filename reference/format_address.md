# Render parsed address components back into one line

The readable counterpart to \[address_key()\]: the canonical components
written out the way an address is written, with the unit hyphenated onto
the civic number and the postal code spaced. Use it to show what a parse
actually resolved to, or to write a cleaned address column back out.

The street type is placed by language, not by province: French types
lead the name (\`123 Rue Notre-Dame E\`) and English types follow it
(\`123 Main St W\`), so a \`Rue\` in Ottawa still reads correctly.

Component \*case\* is left exactly as parsed, which for a
gazetteer-resolved row means NAR's own convention: street names in mixed
case (\`Burrard\`, \`McTavish\`), types and directions in capitals
(\`ST\`, \`NW\`), municipalities in capitals. That mix is deliberate
rather than an oversight – re-casing a name would fight capitals NAR is
careful about, and NAR is the authority the rest of this package defers
to. A rules-only row has nothing to defer to and comes back upper case
throughout.

## Usage

``` r
format_address(x, known = NULL, con = NULL)
```

## Arguments

- x:

  Either a data frame of parsed components, as returned by
  \[normalize_address()\], or a character vector of address strings to
  normalize first.

- known, con:

  Passed to \[normalize_address()\], and only allowed when \`x\` is a
  character vector – a data frame has already been parsed.

## Value

A character vector, one element per row of \`x\`, and \`NA\` for a row
with no components at all.

## See also

\[address_key()\] for the joinable form.

## Examples

``` r
format_address(c("302-1055 w georgia st, vancouver bc v6e3p3",
                 "12 1/2 rue notre-dame e, montreal, quebec",
                 "100 queen street west, toronto, ontario"))
#> [1] "302-1055 GEORGIA ST W, VANCOUVER, BC V6E 3P3"
#> [2] "12 1/2 RUE NOTRE-DAME E, MONTREAL, QC"       
#> [3] "100 QUEEN ST W, TORONTO, ON"                 
```
