# Get NAR data

This function downloads the NAR data if necessary and returns a
connection the NAR database

## Usage

``` r
nar_connection(version = "latest", refresh = FALSE)
```

## Arguments

- version:

  Version of the NAR database to connect to. Default is "latest".

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
} # }
```
