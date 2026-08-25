# Render a NAR address row as a string

Assembles the \`address\` column \[reverse_geocode()\] returns from the
columns of an \`Addresses\` row. Built column-wise rather than with
\`rowwise()\`: the query itself stays flat as the radius grows, but
row-by-row formatting did not – 27k matches spent ~2.4s here against
~0.06s in the database.

The mail family is preferred and the official one stands in for it, as a
unit rather than field by field: \`MAIL_STREET_NAME\` is empty for
957,307 of NAR 2026-06's 17.4M addresses, and on every one of those rows
\`MAIL_STREET_TYPE\` is empty too, so a per-field fallback would put an
official name next to a mail type it was never spelled against. 957,213
of them carry an official name; the remaining 94 have no street at all.
\`MAIL_MUN_NAME\` is empty for 39,691, where \`CSD_ENG_NAME\` stands in
– the same surface \`MunAlias\` already treats as a name for the
municipality, and one derived from the coordinate, which is what a
reverse geocode was asked about. Nothing stands in for the 57,154 rows
with no postal code.

Every part that is missing is dropped rather than rendered, which is the
other half of the fix: the components arrive as \`NA\` and \`paste0()\`
spells an \`NA\` "NA".

## Usage

``` r
nar_row_address(row)
```

## Arguments

- row:

  A data frame of \`Addresses\` columns with empty strings already
  turned into \`NA\`

## Value

A character vector, one element per row, with no missing values
