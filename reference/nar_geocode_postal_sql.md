# The postal code of the record that was matched

An aggregate over the \*candidate\* set rather than a column read off
the row that was returned, and that is the whole point of it. NAR
carries one row per address, so a civic number with units contributes
many rows to \`cand\`; the tier picks one of them for its coordinates,
and picking one of them for a postal code as well would be a coin flip
wherever the units of a building do not share one. They usually do –
98.6 carry a single postal code – but the 1.4 since a building large
enough to split across postal codes is large.

So the value is reported only when every candidate agrees, and is
\`NULL\` otherwise. The empty-string fold makes a missing postal code
participate in that agreement rather than being skipped by
\`count(DISTINCT)\`: a set that is half \`NULL\` reports nothing, not
the half that had a value.

It is an aggregate over whatever \`cand\` holds by the time it runs, so
\[nar_geocode_unit_filter()\] having narrowed the set to one unit is
what turns a declined postal code into a reported one – 55 of 5,000
corpus filings.

## Usage

``` r
nar_geocode_postal_sql(col)
```

## Arguments

- col:

  The postal-code column, qualified with the candidate alias

## Value

A SQL fragment, aliased \`match_postal_code\`
