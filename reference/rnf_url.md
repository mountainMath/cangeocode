# The Road Network File download

One URL, in one place. \`release\` is the two-digit release year StatCan
names the file by; \`a\` is the shapefile.

\*\*Only the shapefile is published for every release.\*\* The download
form offers four formats – \`a\` shapefile, \`g\` GML, \`f\` file
geodatabase, \`p\` GeoPackage – but the archive is not uniform: 20, 22,
23, 24 and 25 all serve \`a\`, while only 25 serves \`p\`. An importer
that reached for the GeoPackage would work this year and break on last
year's release. The shapefile is also the format that reads cleanly: the
GeoPackage carries 13 CircularStrings that DuckDB's spatial extension
refuses outright, and the shapefile spells the same 2,251,726 features
as plain LINESTRINGs.

## Usage

``` r
rnf_url(release)
```

## Arguments

- release:

  Two-digit release year, e.g. \`"25"\`

## Value

A URL string
