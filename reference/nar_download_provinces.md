# Download only the members of a NAR release a set of provinces needs

Reads the release's zip index over range requests, selects the members
for the requested provinces, and writes them to a local archive. The
bytes actually transferred are reported, because the whole point of this
path is that they are a fraction of the release.

## Usage

``` r
nar_download_provinces(url, provinces, dest)
```

## Arguments

- url:

  URL of the StatCan release zip

- provinces:

  Canonical province abbreviations

- dest:

  Path to write the reduced archive to

## Value

\`dest\`, invisibly
