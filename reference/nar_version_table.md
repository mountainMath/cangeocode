# Extract the version table from the StatCan publication page

Split out from \[available_nar_versions()\] so the parsing can be
exercised without a network round trip. A layout change on the StatCan
side is the most likely way version discovery breaks, so it fails loudly
rather than returning an empty table.

## Usage

``` r
nar_version_table(page, overview_url)
```

## Arguments

- page:

  Parsed HTML, from \`xml2::read_html()\`

- overview_url:

  URL the page came from, used to resolve relative links

## Value

A tibble of \`version\`, \`url\`, \`Date\` and \`path\`, newest first
