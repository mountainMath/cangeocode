# The Canada-hosted Nominatim endpoint

Kept in one place so a test can point it somewhere else.

\*\*This is not \`nominatim.openstreetmap.org\`\*\*, and the difference
is the reason this binding exists at all. The public OSM instance caps
use at one request per second and its usage policy forbids bulk
geocoding of an address list outright, which is exactly what this
package does. \`maps.canada.ca\` runs its own Nominatim, keyless, and
NRCan's own geolocator aggregator queries it under the \`nominatim\`
service key
(\`backend/geolocator-bucket-content/services/nominatim-schema.json\` in
\`Canadian-Geospatial-Platform/geoview-api-geolocator\`).

That removes the policy objection and replaces it with an unanswered
one: the instance exists to serve GeoView, nothing published says what
bulk use is acceptable, and the aggregator's own timeout against it is 3
seconds, which does not suggest a generous provision.
\`geo@nrcan-rncan.gc.ca\` is the contact its README names. Hence the
deliberately slow default \`rate\`.

## Usage

``` r
nar_osm_url()
```

## Value

A single URL
