# Scrape availabe NAR versions from the StatCan website

Scrape availabe NAR versions from the StatCan website

## Usage

``` r
available_nar_versions(refresh = FALSE)
```

## Arguments

- refresh:

  Logical indicating whether to refresh the cached version list

## Value

A tibble with available NAR versions and their URLs

## Examples

``` r
if (FALSE) { # \dontrun{
versions <- available_nar_versions()
} # }
```
