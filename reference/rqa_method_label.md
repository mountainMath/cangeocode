# RQA's positional-quality classes as \`match_method\` labels

The register's own vocabulary, transliterated. An unrecognized class
becomes \`rqa_other\` rather than being dropped, so a future RQA release
that adds one still places its addresses.

## Usage

``` r
rqa_method_label(quality)
```

## Arguments

- quality:

  RQA's \`qualite_positionnement_geometrique\`

## Value

A character vector of \`match_method\` values
