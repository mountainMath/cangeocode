
# cangeocode

<!-- badges: start -->
[![R-CMD-check](https://github.com/mountainMath/cangeocode/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/mountainMath/cangeocode/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`cangeocode` geocodes and reverse geocodes Canadian addresses. It is built on
Statistics Canada's [National Address Repository](https://www.statcan.gc.ca/en/lode/databases/nar)
(NAR), a national list of civic addresses with coordinates, which the package
imports into a local [DuckDB](https://duckdb.org) database.

Everything runs on your own machine: no API keys, no rate limits, and no
address ever leaves it.

## Installation

``` r
remotes::install_github("mountainMath/cangeocode")
```

## Setup

Before the first call, tell the package where to keep its database by setting
`NAR_CACHE_PATH`. There is no default, and every entry point stops with an
error if it is unset. Add it to your `~/.Renviron`:

```
NAR_CACHE_PATH=~/data/nar
```

The first call then downloads the NAR release from StatCan and imports it.
**Expect this to take a while and to use real disk** — a few hundred megabytes
over the wire (the package raises R's timeout to 20 minutes, because the
StatCan connection can be slow) and roughly **5 GB on disk** for the 2026-06
release. Every call after that opens the existing database instantly.

## Documentation

Full reference and articles: <https://mountainmath.github.io/cangeocode/>

- [Getting started](https://mountainmath.github.io/cangeocode/articles/cangeocode.html)
  (`vignette("cangeocode")`) — setup, reverse geocoding, output types, precision
- [Querying the NAR database directly](https://mountainmath.github.io/cangeocode/articles/querying-nar.html)
  (`vignette("querying-nar")`) — using the database with dplyr and `sf`

## Example

Find the addresses closest to a coordinate:

``` r
library(cangeocode)

reverse_geocode(c(-123.2, 49.25), match_radius = 100)
#> # A tibble: 16 × 32
#>    address                                        dist geom_source ...
#>  1 4176 W KING EDWARD AVE, VANCOUVER V6S1N3       23.5 building
#>  2 4172 W KING EDWARD AVE, VANCOUVER V6S1N3       27.4 building
#>  3 4182 W KING EDWARD AVE, VANCOUVER V6S1N3       32.3 building
#>  ...
```

Coordinates are longitude/latitude (EPSG:4326) by default; `sf` points and
other CRSs work too. `match_radius` and `dist` are in **metres**. `output`
picks between all matches, the components of the closest one, or a single
formatted address string, and `geometry = TRUE` returns an `sf` object.

Reverse geocoding many points is much faster if you open the connection once
and hand it over, rather than letting each call open its own:

``` r
con <- nar_connection()
addresses <- lapply(points, \(p) reverse_geocode(p, output = "address", con = con))
DBI::dbDisconnect(con)
```

That same connection is a plain DBI connection, so the full address table is
open to dplyr — filtered, joined, and aggregated inside DuckDB:

``` r
library(dplyr)

con <- nar_connection()

tbl(con, "Addresses") |>
  filter(MAIL_MUN_NAME == "VANCOUVER", MAIL_STREET_NAME == "KINGSWAY") |>
  select(ADDR_GUID, CIVIC_NO, MAIL_POSTAL_CODE, geom) |>
  collect_nar()          # returns an sf object
```

## Match precision

Most NAR addresses carry a point for the building itself. About 7% do not, and
for those the package falls back to the **blockface** point — the centre of one
side of a street between two intersections — which is shared by every address
on that stretch and is a good deal coarser. Results say which was used in the
`geom_source` column, so filter on it when a `dist` needs to mean what it
appears to mean. A further ~65,000 addresses have no coordinates at all.

## Data source

NAR is published by Statistics Canada under the
[Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence).
This package is not affiliated with or endorsed by Statistics Canada.
