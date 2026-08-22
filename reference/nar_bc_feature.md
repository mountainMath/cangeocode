# Turn one BC Address Geocoder feature into a result row

Split out from the request so the response shape can be tested against a
saved fixture with no network. Takes the parsed JSON of a whole response
and reads its first feature, since the package only ever asks for one.

## Usage

``` r
nar_bc_feature(resp, min_score = 60)
```

## Arguments

- resp:

  The parsed response, as \[jsonlite::fromJSON()\] with \`simplifyVector
  = FALSE\` returns it

- min_score:

  Scores below this are reported as \`none\`

## Value

A one-row data frame
