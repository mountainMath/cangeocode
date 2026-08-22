# Get NAR data

This function downloads the NAR data if necessary and returns a
connection to the NAR database.

The StatCan release is one ~1.7 GB zip whose member files are split by
province, and the server honours HTTP range requests, so \`provinces\`
can restrict the download to the provinces that are actually wanted –
192 MB for British Columbia, 10 MB for Prince Edward Island. The
addresses are the same NAR rows either way, so a partial database
geocodes its own provinces exactly as well as a national one does; it
simply holds nothing outside them, which \[geocode()\] reports as
\`not_covered\` rather than as a failure to match.

Coverage is recorded in the database and checked before anything is
downloaded. A national database already satisfies every request, and
asking for a province an existing partial database lacks adds just that
province rather than rebuilding.

## Usage

``` r
nar_connection(version = "latest", provinces = NULL, refresh = FALSE)
```

## Arguments

- version:

  Version of the NAR database to connect to. Default is "latest".

- provinces:

  Provinces to make available, as two-letter abbreviations, SGC codes or
  full names – or \`"all"\` for the whole country. \`NULL\`, the
  default, keeps whatever a cached database already holds; when there is
  nothing cached it prompts in an interactive session and downloads the
  whole country otherwise.

- refresh:

  Logical indicating whether to refresh the local cache of the NAR
  database.

## Value

A connection to the NAR database containing Addresses and Locations
tables

## Examples

``` r
if (FALSE) { # \dontrun{
con <- nar_connection()

# British Columbia only: 192 MB rather than 1.7 GB.
bc <- nar_connection(provinces = "BC")
nar_provinces(bc)

# Add a province to an existing partial database.
bc_ab <- nar_connection(provinces = c("BC", "AB"))
} # }
```
