# Decide what still has to be downloaded

Compares what the cached database already holds against what was asked
for, and returns the provinces to fetch plus whether they are added to
the existing database or replace it.

The rules are the ones a user would state: a national database satisfies
everything, so asking for a province it already contains downloads
nothing; asking for provinces a partial database lacks adds only the
missing ones; and asking for the whole country when only a province is
cached rebuilds, since the national release is being downloaded in full
regardless.

\`refresh\` re-downloads whatever the database currently covers, so
refreshing a British Columbia database does not silently turn it into a
national one.

## Usage

``` r
nar_import_plan(nar_path, provinces, refresh)
```

## Arguments

- nar_path:

  Path to the \`\<version\>.duckdb\` file

- provinces:

  Canonical abbreviations, \`"ALL"\`, or \`NULL\` for unspecified

- refresh:

  Whether the caller asked to rebuild

## Value

A list of \`fetch\` (provinces to download, \`NULL\` when the caller
must still be asked, empty when nothing is needed) and \`append\`
