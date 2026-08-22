# Rebuild an address string from parsed components

The BC service takes a string, so the components have to be re-rendered
to reach it. Rebuilding rather than forwarding the original input is
what carries the authoritative \`prov\`/\`mun\` constraints through:
those overwrite the parsed columns, and a caller who asserted a
municipality would otherwise watch it be ignored the moment a row fell
through to the fallback.

## Usage

``` r
nar_bc_address_string(res)
```

## Arguments

- res:

  A \[normalize_address()\] result

## Value

A character vector of address strings
