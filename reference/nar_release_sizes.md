# Download size per province for a NAR release

Reads the release's zip index – a few kilobytes over range requests, no
data transfer – and totals the compressed size of each province's
members. Used to put real numbers in front of the interactive prompt
rather than estimates.

## Usage

``` r
nar_release_sizes(url)
```

## Arguments

- url:

  URL of the StatCan release zip

## Value

A data frame of \`abvn\`, \`name\` and \`mb\`, plus an \`ALL\` row,
ordered as \[nar_province_table()\] is
