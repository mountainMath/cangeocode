# Geocode Canadian addresses with OpenStreetMap data

A binding to the \*\*Nominatim instance NRCan hosts at
\`maps.canada.ca\`\*\*, which searches OpenStreetMap data. It is
national, needs no API key and no local database, and it is the only
source in this package that is genuinely independent of Statistics
Canada.

## Usage

``` r
osm_geocode(
  x,
  known = NULL,
  rate = 1,
  retries = 3,
  limit = 10,
  structured = TRUE,
  geometry = FALSE,
  crs = 4326,
  con = NULL
)
```

## Arguments

- x:

  A character vector of address strings, or a data frame of parsed
  components as \[normalize_address()\] returns. Components are needed
  either way, since the floors compare the answer against them; passing
  a parsed frame just avoids parsing twice.

- known:

  Components the caller already has, passed to \[normalize_address()\]
  when \`x\` is a character vector. See \[nar_known()\].

- rate:

  Requests per second, and also the largest burst allowed before
  throttling starts. Default 1; see the courtesy section above before
  raising it.

- retries:

  How many times to send an address before giving up on it. Default 3,
  and \`1\` disables retrying. Precautionary rather than measured here –
  see \[nar_osm_transient()\]. Rows that exhaust their retries report
  \`request failed\` in \`osm_reject\` rather than \`no answer\`.

- limit:

  How many results to ask for per address, default 10. The floors read
  all of them, and a single address is often several OSM objects.

- structured:

  Whether to send \`street\`/\`city\`/\`state\` as separate parameters
  rather than one query string. Default \`TRUE\`; see
  \[nar_osm_query()\].

- geometry:

  Whether to return an \`sf\` object. Default \`FALSE\`.

- crs:

  CRS for the returned coordinates, default EPSG:4326.

- con:

  An open NAR connection, optional. It is used only to give the parse of
  the \*caller's\* input a gazetteer; the service itself needs nothing
  local, and the answer is parsed without one.

## Value

A data frame with one row per input: \`input\`, \`match_method\`
(\`"osm"\` or \`"none"\`), \`uncertainty_m\`, \`n_matches\`,
\`osm_rank\`, \`osm_category\`, \`osm_title\`, \`osm_licence\`,
\`osm_reject\`, and either \`lon\`/\`lat\` or an \`sf\` geometry column.

## Read this before using it

\*\*Results are OpenStreetMap data under the ODbL\*\*, which is a
materially different obligation from the Open Government Licence
covering NAR, the BC Address Geocoder and NRCan's geolocator. The ODbL
requires attribution and carries share-alike terms that attach to
\*derived databases\*, so coordinates from here mixed into a table that
is then published are not in the same licensing position as the rest of
this package's output. Every row carries the service's own
\`osm_licence\` string so the obligation travels with the data.

This is why the function is \*\*exported but not wired into
\[geocode()\]\*\*: nothing fires it unless it is called by name, and no
default tier chain can mix ODbL coordinates into a result without the
caller having decided to.

## What a response means

Unlike NRCan's geolocator, \*\*this service will say it has nothing.\*\*
Asked for an address that does not exist it returns an empty array, and
asked for a civic number it does not hold on a street it does hold, it
returns the street at \`place_rank\` 26 and no house number. Neither is
a confident wrong answer.

The floors are applied anyway, and they are the same two the geolocator
tier applies: a result must be house-level, and its components must
agree with the ones that were sent. \`osm_reject\` says which floor a
row failed and keeps the \`display_name\` it failed on, so the rejection
can be inspected.

\*\*Coverage is the open question, not accuracy.\*\* OSM's Canadian
address coverage is uneven – excellent in municipalities whose open
address data was imported, sparse elsewhere – and it has not been
measured here. Neither has the positional error, which is why
\`uncertainty_m\` comes back \`NA\`; see \[nar_osm_uncertainty_m()\] and
\`data-raw/probe_osm.R\`.

## Reverse geocoding

The instance does offer \`/reverse\`, unlike either other online source
here, and it is not bound. \[reverse_geocode()\] is NAR-backed, local,
and does not carry the ODbL question.

## Network use and courtesy

One HTTP request per address; there is no batch endpoint. \*\*The
default \`rate\` is 1 request per second\*\*, which is Nominatim's own
convention rather than a measured limit – nothing published says what
this instance will tolerate, and it exists to serve GeoView rather than
this package. Raising it is the caller's decision to make and
\`geo@nrcan-rncan.gc.ca\` is the address to ask at. \`httr2\` is
required and lives in \`Suggests\`, so the package never contacts the
network unless this function is called.

## See also

\[nrcan_geocode()\] and \[bc_geocode()\], the two services \[geocode()\]
will run as tiers.

## Examples

``` r
if (FALSE) { # \dontrun{
osm_geocode("990 Bute St, Vancouver, BC")

# The address the geolocator answers with a Rue Notre-Dame Ouest 500 km away.
osm_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")$osm_title

# A refusal rather than a substitution: the street, at rank 26.
osm_geocode("28 Silver St, Corner Brook, NL")[, c("osm_title", "osm_reject")]
} # }
```
