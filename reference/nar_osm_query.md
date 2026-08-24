# Build one Nominatim query from parsed components

Two shapes, chosen by \`structured\`, both built on
\[nar_osm_street()\].

\*\*Structured\*\* sends \`street\`, \`city\` and \`state\` as separate
parameters, which is the shape this binding exists for: the parse never
has to be flattened into one string and recovered on the other side.
\`street\` still carries the civic number and the street together,
because that is the parameter Nominatim defines.

\*\*Free text\*\* sends the same three joined by commas. It is kept
because structured search in Nominatim requires every element supplied
to match, which can reject an address that free text would find under a
municipality the caller spelled differently – and because a knob the
probe harness can flip is how that gets measured instead of guessed. The
two agreed on every probe address tried so far, which is not a sample.

## Usage

``` r
nar_osm_query(res, structured = TRUE)
```

## Arguments

- res:

  Parsed components, one row per address

- structured:

  Whether to send separate parameters

## Value

A list of named lists, one per row, ready for \`req_url_query()\`
