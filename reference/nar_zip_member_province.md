# Which zip members belong to which provinces

NAR names its member files by SGC province code, optionally split into
parts: \`Addresses/Address_35_part_3.csv\` is Ontario, and
\`Addresses/Address_11.csv\` is Prince Edward Island in one piece.
Anything that is not a per-province CSV – the user guides, the readme,
the directory entries – gets \`NA\` and is carried along regardless,
since it is negligible in size and the guides are worth having.

## Usage

``` r
nar_zip_member_province(names)
```

## Arguments

- names:

  Member names from \[nar_zip_directory()\]

## Value

A character vector of province abbreviations, \`NA\` where the member is
not province-specific
