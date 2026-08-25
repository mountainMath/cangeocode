# The columns \[geocode_matches()\] reports for each NAR record

Chosen to answer the question the function exists for – why are these
separate records, and does the difference matter. \`APT_NO_LABEL\`,
\`MAIL_POSTAL_CODE\`, \`MAIL_MUN_NAME\` and \`BU_USE\` are what actually
distinguish the units of one building; \`LOC_GUID\` is what shows they
\*are\* one building; both street-name families are carried because
either may be the one that matched.

## Usage

``` r
nar_geocode_match_cols()
```

## Value

A character vector of \`Addresses\` column names
