# Which of Nominatim's address fields is the municipality

OSM has no single municipality field. What comes back depends on how the
place is tagged: Vancouver arrives as \`city\`, Corner Brook as
\`town\`, and smaller places as \`village\` or \`municipality\`. The
first of those that is present is taken.

\`suburb\`, \`neighbourhood\`, \`quarter\` and \`city_district\` are
deliberately \*\*not\*\* in the list even though they are often the only
other locality field present. They sit below the municipality, not
beside it – \`West End\` for a Vancouver address, \`Vieux-Montreal\` for
a Montreal one – and treating one as the municipality would fail the
agreement floor against the municipality that was actually asked for.

## Usage

``` r
nar_osm_mun(addr)
```

## Arguments

- addr:

  One result's \`address\` object, as a named list

## Value

A single string, or \`NA\`
