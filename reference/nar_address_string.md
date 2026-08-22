# Rebuild an address string from parsed components

Both online services take a string, so the components have to be
re-rendered to reach them. Rebuilding rather than forwarding the
original input is what carries the authoritative \`prov\`/\`mun\`
constraints through: those overwrite the parsed columns, and a caller
who asserted a municipality would otherwise watch it be ignored the
moment a row fell through to the fallback.

## Usage

``` r
nar_address_string(res, suffix = TRUE)
```

## Arguments

- res:

  A \[normalize_address()\] result

- suffix:

  Whether to keep the civic-number suffix. Default \`TRUE\`, which is
  what the address actually is. \[nrcan_geocode()\] passes \`FALSE\`
  because NRCan's geolocator looks for the house number as a run of one
  to five digits bounded by word boundaries, and there is no word
  boundary inside \`990A\`, so it sees no house number at all and falls
  back to a street centroid that the tier then rejects. Dropping the
  suffix hides nothing from the floor, which compares \`CIVIC_NO\`
  alone.

## Value

A character vector of address strings
