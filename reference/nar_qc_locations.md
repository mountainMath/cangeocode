# Read one batch response into result rows

Split out from the request so the response shape can be tested against a
saved fixture with no network.

Two things about this response are easy to get wrong and are handled
here. \*\*The service answers out of order\*\* – send three addresses
and the locations come back 3, 1, 2 – so rows are placed by their
\`ResultID\` rather than by position, and an id the service dropped
stays an unmatched row. And \*\*the coordinates are read from
\`location\`, never from the \`Latitude\` and \`Longitude\`
attributes\*\*: those are rendered in the service's French locale with a
comma for the decimal mark (\`"45,5061613986714"\`), which
\`as.numeric()\` turns into \`NA\` on a good day and into a different
number on a bad one, and they are empty for a street-level match whose
\`location\` is populated.

## Usage

``` r
nar_qc_locations(resp, n)
```

## Arguments

- resp:

  The parsed response, as \[jsonlite::fromJSON()\] with \`simplifyVector
  = FALSE\` returns it

- n:

  How many addresses were sent, so dropped ids still get a row

## Value

A data frame of \`n\` rows in the order the addresses were sent
