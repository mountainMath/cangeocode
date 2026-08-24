# Reverse geocode Quebec coordinates with the Quebec government geocoder

The nearest Quebec address to each point, from the same service
\[qc_geocode()\] uses.

## Usage

``` r
qc_reverse_geocode(x, y = NULL, crs = 4326, distance = 100, rate = 5)
```

## Arguments

- x:

  Longitudes, or an \`sf\`/\`sfc\` object of points, or a two-column
  matrix or data frame of longitude and latitude.

- y:

  Latitudes, when \`x\` is a numeric vector.

- crs:

  CRS of \`x\` and \`y\` when they are numeric, and the CRS the result
  is returned in. Default EPSG:4326.

- distance:

  Search radius in metres. Default 100. The service returns nothing
  beyond it rather than reaching across a municipality.

- rate:

  Requests per second. Default 5.

## Value

A data frame with one row per point: \`qc_address\`, \`qc_postal\`,
\`qc_city\`, \`qc_dist_m\`, \`lon\` and \`lat\` of the address that was
found, all \`NA\` where nothing was within \`distance\`.

## Why this exists when reverse_geocode() already does

it is the only \*\*online\*\* reverse geocoder bound in this package –
neither NRCan's geolocator nor the BC Address Geocoder offers one, and
the Government of Canada's Nominatim instance is not a tier for licence
reasons. It is therefore a second, independent-of-your-import answer for
Quebec, useful for checking \[reverse_geocode()\] rather than for
replacing it: \[reverse_geocode()\] is local, national, batched against
the whole database and returns \`output = "multiple"\` neighbours, none
of which this does.

One HTTP request per point – the service's batch endpoint is
forward-only.

## See also

\[reverse_geocode()\], which is NAR-backed, local and national.

## Examples

``` r
if (FALSE) { # \dontrun{
qc_reverse_geocode(-73.5672, 45.5017)
} # }
```
