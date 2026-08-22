
# cangeocode

<!-- badges: start -->
[![R-CMD-check](https://github.com/mountainMath/cangeocode/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/mountainMath/cangeocode/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`cangeocode` geocodes and reverse geocodes Canadian addresses. It is built on
Statistics Canada's [National Address Repository](https://www.statcan.gc.ca/en/lode/databases/nar)
(NAR), a national list of civic addresses with coordinates, which the package
imports into a local [DuckDB](https://duckdb.org) database.

Everything runs on your own machine: no API keys, no rate limits, and no
address ever leaves it. The one exception is opt-in — `bc_geocode()` and
`geocode(fallback = "bc")` call the Province of British Columbia's public
geocoder, and nothing contacts it unless you ask for it by name.

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

Two longer notes ship with the package and say what does *not* work yet, with
the measurements behind each claim:
`system.file("notes", "geocoding-status.md", package = "cangeocode")` and
`system.file("notes", "address-normalization-status.md", package = "cangeocode")`.

## Geocoding

Turn address strings into coordinates:

``` r
library(cangeocode)

geocode(c("1055 W Georgia St, Vancouver BC",
          "100 Queen St W, Toronto, ON",
          "1225 rue Notre-Dame Ouest, Montreal, QC",
          "9999 Jasper Ave, Edmonton, AB"))
#>                                     input     match_method uncertainty_m
#> 1         1055 W Georgia St, Vancouver BC     nar_building           0.0
#> 2             100 Queen St W, Toronto, ON     nar_building           0.0
#> 3 1225 rue Notre-Dame Ouest, Montreal, QC     nar_building           0.0
#> 4           9999 Jasper Ave, Edmonton, AB nar_interpolated          21.3
#>          lon      lat
#> 1 -123.12141 49.28529
#> 2  -79.38250 43.65150
#> 3  -73.56459 45.49367
#> 4 -113.49028 53.54101
```

One row per input, in input order, carrying the parsed address components
alongside the result. `geometry = TRUE` returns an `sf` object instead of
`lon`/`lat`.

**Every result says how it was found and what that cost.** `match_method` is
`nar_building` when the civic number is in NAR with its own point,
`nar_blockface` when only a street-segment centroid is available,
`nar_interpolated` when the number was placed between the flanking civics, and
`none` when nothing resolved. `uncertainty_m` is the 90th-percentile error the
package adds relative to NAR's own point — `0` for a building match, 176 m for
a blockface one, half the flanking span for an interpolated one. It says
nothing about NAR's own error, which is a separate quantity — the notes linked
above measure that separately, for one province.

Interpolation is deliberately conservative: same side of the street only, and
it **refuses to extrapolate** past the last known civic rather than guessing.
Turn it off with `interpolate = FALSE` to keep only addresses NAR actually
carries.

Batch rather than loop — the street-name join costs about the same for 5
addresses as for 200, and every row in a call shares it.

### Constraining the search

`prov`, `mun` and `within` are **authoritative**: they override whatever the
address string said, and the override lands on the returned row too.

``` r
# A bounding box, an sf polygon, or an st_bbox all work.
geocode(addresses, prov = "BC", within = sf::st_bbox(vancouver))
```

`mun` resolves through NAR's municipality aliases, so `mun = "TORONTO"` still
finds the addresses NAR files under `SCARBOROUGH`.

## British Columbia: an independent geocoder

`bc_geocode()` binds the Province of BC's
[Address Geocoder](https://geocoder.api.gov.bc.ca/). It covers BC only, needs
no API key, and is useful in two ways.

As a **fallback**, for the BC addresses NAR cannot place:

``` r
geocode(addresses, fallback = "bc")
```

On a sample of 600 BC addresses the NAR pathway placed 524 and gave up on 76;
the fallback resolved 75 of those, 31 of them at address level.

As **validation**, since it is an independent source of position:

``` r
g <- geocode(c("525 Superior St, Victoria, BC", "800 Robson St, Vancouver, BC"))
bc_validate(g)
#>                           input     match_method bc_match_method bc_dist_m
#> 1 525 Superior St, Victoria, BC     nar_building        bc_civic       6.9
#> 2  800 Robson St, Vancouver, BC nar_interpolated        bc_civic     104.1
```

**A response from this service is not a match.** It always answers — feed it
`"1234 Nonexistentzzz Rd, Victoria, BC"` and it returns the centre of Victoria
with a score of 48 rather than an error. So `match_method` is derived from the
service's own precision vocabulary, and `min_score` (default 60) rejects what
it scored badly. Rejected rows keep their `bc_score` and `bc_faults`, so you
can see what was thrown away and why.

This is the one path on which addresses leave your machine. Requests are
throttled to be a good citizen of a free public service; register for an API
key and pass it as `api_key` for anything large.

## Reverse geocoding

Find the addresses closest to a coordinate:

``` r
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

## Querying the database directly

Every entry point takes a `con` argument. Reusing one connection across many
calls is much faster than letting each call open its own:

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

## Where NAR's points come from

This applies to both directions of geocoding. Most NAR addresses carry a point
for the building itself. About 7% do not, and
for those the package falls back to the **blockface** point — the centre of one
side of a street between two intersections — which is shared by every address
on that stretch and is a good deal coarser. Results say which was used in the
`geom_source` column — and forward geocoding in `match_method` — so filter on
it when a distance needs to mean what it appears to mean. A further ~65,000
addresses have no coordinates at all, which `geocode()` reports as
`nar_no_geometry` rather than as a failure to find the address.

Note that even a building point is a *representative* point: the StatCan user
guide allows it to be the road access point or the driveway, and publishes no
accuracy figure. That is why `uncertainty_m = 0` means "this package added
nothing", not "this point is exact".

## Data source

NAR is published by Statistics Canada under the
[Statistics Canada Open Licence](https://www.statcan.gc.ca/en/reference/licence).
The BC Address Geocoder is a service of the Province of British Columbia and
carries its own terms, which its responses link to. This package is not
affiliated with or endorsed by either.
