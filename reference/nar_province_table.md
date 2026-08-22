# Province and territory crosswalk

The three ways a province is named in this package, in one place.
\`code\` is the Standard Geographical Classification two-digit
identifier, which is what NAR's \`PROV_CODE\` column holds \*\*and what
the member files inside the StatCan bulk zip are named by\*\* –
\`Address_59.csv\` is British Columbia. \`abvn\` is the two-letter
abbreviation NAR carries in \`MAIL_PROV_ABVN\`, which is also what
\[normalize_address()\] and \[geocode()\] speak. \`name\` is for
messages and prompts.

The pairing was verified against the 2026-06 release rather than
assumed: every \`PROV_CODE\` in \`Addresses\` maps to exactly one
\`MAIL_PROV_ABVN\`.

## Usage

``` r
nar_province_table()
```

## Value

A data frame with \`code\`, \`abvn\` and \`name\`
