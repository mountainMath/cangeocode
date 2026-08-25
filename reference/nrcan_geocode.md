# Geocode Canadian addresses with NRCan's geolocator

A binding to Natural Resources Canada's
\[geolocator\](https://geolocator.api.geo.ca/geolocation/en/locate?q=Ottawa),
the service behind \`geo.ca\`. It is \*\*national and needs no API key
and no local database\*\*, which makes it the one pathway in this
package that works before anything has been downloaded – and the reason
it exists here, since on accuracy it is far behind the NAR tiers.

## Usage

``` r
nrcan_geocode(
  x,
  known = NULL,
  rate = 5,
  retries = 3,
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
  throttling starts. Default 5.

- retries:

  How many times to send an address before giving up on it. Default 3,
  and \`1\` disables retrying. The service loses roughly one request in
  twelve to a transient HTTP 500 that a single re-send almost always
  fixes – see \[nar_nrcan_transient()\] – so this is worth about 8
  points of coverage, not a rounding error. Rows that exhaust their
  retries report \`request failed\` in \`nrcan_reject\` rather than \`no
  answer\`.

- geometry:

  Whether to return an \`sf\` object. Default \`FALSE\`.

- crs:

  CRS for the returned coordinates, default EPSG:4326.

- con:

  An open NAR connection, optional. It is used only to give the parse a
  gazetteer; the service itself needs nothing local.

## Value

A data frame with one row per input: \`input\`, \`match_method\`
(\`"nrcan"\` or \`"none"\`), \`uncertainty_m\`, \`n_matches\`,
\`nrcan_kind\`, \`nrcan_qualifier\`, \`nrcan_title\`, \`nrcan_reject\`,
and either \`lon\`/\`lat\` or an \`sf\` geometry column.

## What a response means

\*\*The service always answers, and it has no score.\*\* Asked for an
address it does not have, it returns a street of a similar name
somewhere else in Canada, or a populated place, with no field
distinguishing that from a hit. So the filtering is done here, and it is
strict: a result must be an interpolated position on a \`Street\`, and
the address in its \`title\` must re-parse to the address that was sent.
\`nrcan_reject\` says which floor a row failed, and the title it failed
on is kept so the rejection can be inspected.

The floors are applied to \*\*every\*\* result the service returned, not
just the one it ranked first, because the ranking and the floor answer
different questions – see \[nar_nrcan_floors()\]. The service returns up
to 25 results in one response, so this costs no extra request.
\`n_matches\` counts how many of them passed; the best-ranked one is the
row that is returned.

Measured against NAR's own building points over a 423-address national
sample: \*\*48 of 33 m and a 90th percentile of 115 m. The other 52
service so much as answers this refuses – 27 as a street centroid or a
populated place, and a further 15 answer about a different address.
\`geocode()\` places 84.9 input exactly, at 0 m. \*\*This is a fallback,
not a substitute\*\*; see \`inst/notes/geocoding-status.md\`. That
sample predates the scan of the whole result list, which is worth about
a further point of recall.

Addresses with a civic-number \*\*suffix\*\* are a special case worth
knowing about: the service cannot see a house number in \`990A\` at all,
so the suffix is dropped from the query. Measured over 20 suffixed NAR
points, that moves them from 0 placed to 16.

## Reverse geocoding

The service does not offer it. There is no coordinate endpoint –
\`locate\` answers \`Missing query parameter 'q'\` to a \`lat\`/\`lon\`
query, and the retired \`geogratis\` host redirects here. Use
\[reverse_geocode()\], which is NAR-backed and local.

## Network use and courtesy

One HTTP request per address; there is no batch endpoint. Requests are
throttled to \`rate\` per second, and the ones the service drops are
re-sent up to \`retries\` times with httr2's exponential backoff, so a
run makes slightly more requests than it has addresses. \`httr2\` is
required and lives in \`Suggests\`, so the package never contacts the
network unless this function is called.

Results are subject to NRCan's terms and the Open Government Licence –
Canada.

## See also

\[geocode()\], which can run this as its last tier; \[bc_geocode()\] for
the BC-only service.

## Examples

``` r
if (FALSE) { # \dontrun{
nrcan_geocode("100 Water Street, Charlottetown, PE")

# What was rejected, and why: the street was found, the civic number was not.
nrcan_geocode("1155 Robson Street, Vancouver, BC")[, c("nrcan_title",
                                                       "nrcan_reject")]

# The service ranks a Rue Notre-Dame Ouest in Lorrainville first, 500 km
# away; the one in Montreal is seventh in the same response, and the floor
# is what tells them apart.
nrcan_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")$nrcan_title
} # }
```
