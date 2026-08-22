# Separate "not in the gazetteer" from "not in this database"

A partial NAR import holds only the provinces it downloaded, so an
address in a province it does not hold cannot match however good the
parse is. Reporting that as \`none\` would say the address is wrong; it
says instead that this database was never asked to know.

Only rows whose province is both \*\*parsed\*\* and demonstrably outside
the coverage are marked. An unparsed province stays \`none\`, because
nothing has been established about it, and a national database marks
nothing at all.

## Usage

``` r
nar_geocode_mark_uncovered(out, res, con)
```

## Arguments

- out:

  The result so far

- res:

  Parsed components

- con:

  A NAR connection

## Value

\`out\`, with \`match_method\` set to \`"not_covered"\` where it applies
