# Geocode British Columbia addresses with the BC Address Geocoder

A binding to the Province of British Columbia's public \[Address
Geocoder\](https://geocoder.api.gov.bc.ca/). It covers BC only, and
complements the NAR pathway in two ways: as a fallback for BC addresses
\[geocode()\] cannot place, and as a second positional source to check
NAR against – see \[bc_validate()\].

## Usage

``` r
bc_geocode(
  x,
  min_score = 60,
  api_key = NULL,
  rate = 5,
  geometry = FALSE,
  crs = 4326,
  ...
)
```

## Arguments

- x:

  A character vector of address strings.

- min_score:

  Minimum score, 0–100, for a result to count as a match. Default 60.
  Anything below is reported as \`none\`, with the score and faults
  still filled in so you can see what was rejected.

- api_key:

  Optional API key, sent as the \`apikey\` header.

- rate:

  Requests per second, and also the largest burst allowed before
  throttling starts. Default 5.

- geometry:

  Whether to return an \`sf\` object. Default \`FALSE\`.

- crs:

  CRS for the returned coordinates, default EPSG:4326.

- ...:

  Additional query parameters passed to the service, for example
  \`locationDescriptor = "accessPoint"\` or \`interpolation =
  "linear"\`.

## Value

A data frame with one row per input: \`input\`, \`match_method\`,
\`uncertainty_m\`, \`bc_score\`, \`bc_precision\`, \`bc_descriptor\`,
\`bc_accuracy\`, \`bc_address\`, \`bc_faults\`, and either
\`lon\`/\`lat\` or an \`sf\` geometry column.

## What a response means

\*\*The service always answers.\*\* Given \`"1234 Nonexistentzzz Rd,
Victoria, BC"\` it returns the centre of Victoria with a score of 48,
not an error, so the presence of a result says nothing. \`match_method\`
is derived from the response's \`matchPrecision\` and is the field to
read: \`bc_site\` and \`bc_civic\` are addresses, \`bc_block\` is
interpolated along a block, and \`bc_street\` and \`bc_locality\` are
answers about a place rather than an address. \`min_score\` additionally
rejects matches the service itself scored poorly, and \`bc_faults\` says
why it did.

\`uncertainty_m\` is on the same scale as \[geocode()\]'s but is \*\*not
measured\*\* – see \[nar_bc_precision()\] for exactly what it is and is
not.

## Network use and courtesy

One HTTP request per address; there is no public batch endpoint.
Requests are throttled to \`rate\` per second, and the default of 5 is
deliberately conservative – this is a free public service and a large
job should register for an API key and pass it as \`api_key\`. \`httr2\`
is required and lives in \`Suggests\`, so the package never contacts the
network unless this function is called.

Results are subject to the Province of British Columbia's terms; the
response carries its own copyright notice and licence links.

## Which point on the property

BC can return several different points for one address –
\`locationDescriptor\` selects among the parcel centroid, the rooftop,
the front door, the \`accessPoint\` on the curb at the driveway, and the
\`routingPoint\` on the road centreline in front of it. The default is
the service's own \`any\`, which returns whatever "main location" it
holds and only falls back to an access point when it holds none.

\*\*Asking does not mean getting.\*\* Over 400 NAR BC building points
(\`data-raw/probe_bc.R\`), \`frontDoorPoint\`, \`rooftopPoint\` and
\`parcelPoint\` returned a point identical to \`any\` on \*\*100 does
not search for the requested kind, it returns the main location it has,
which was a parcel point for 359 of the 400 and a rooftop for 19. Only
\`accessPoint\` and \`routingPoint\` are genuinely different points, and
\`bc_descriptor\` reports which one actually came back.

\*\*The default is also the closest to NAR\*\*, which is not what the
NAR User Guide's "may be the road access point or the driveway" would
suggest. Distance to NAR's own building point, same 378 addresses:

\| requested \| p50 \| p90 \| within 10 m \| \| — \| —: \| —: \| —: \|
\| \`any\` / \`parcelPoint\` / \`frontDoorPoint\` / \`rooftopPoint\` \|
\*\*20.2 m\*\* \| 99.1 m \| \*\*38 \| \`accessPoint\` \| 28.9 m \| 98.7
m \| 11 \| \`routingPoint\` \| 31.6 m \| 99.7 m \| 6

So there is nothing to be gained by changing the default, and
\`accessPoint\` is the one to ask for when the \*road-side\* position is
what is wanted – routing, service delivery – rather than the building.

## Examples

``` r
if (FALSE) { # \dontrun{
bc_geocode("525 Superior St, Victoria, BC")

# What the service could not make sense of, and what it fell back to.
bc_geocode("525 Superor Steet, Victoia, BC")$bc_faults
} # }
```
