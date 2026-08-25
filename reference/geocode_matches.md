# Every NAR record behind a geocoding answer

\[geocode()\] returns one row per address and reports how many NAR
records stood behind it in \`n_records\`. This returns those records –
one row each, in the order the tier ranks them, so \`match_rank == 1\`
is by construction the record \`geocode()\` answered with.

It exists because \`n_records\` is routinely greater than 1 and that is
usually \*\*not\*\* an error. NAR files every unit of a multi-unit
building as its own address at the building's single coordinate, and 47
addresses it places share a coordinate with at least one other, so the
collapse is the normal case rather than the exceptional one. What varies
is whether the collapsed records differ in a way that matters to you,
and the only way to find out is to look at them.

A unit in the input narrows this set exactly as it narrows
\[geocode()\]'s answer, because it is the same candidate set:
\`geocode_matches("49321 Range Road 72, Unit 9")\` returns that one
record, and a unit NAR does not carry there returns all nineteen.

## Usage

``` r
geocode_matches(
  x,
  known = NULL,
  within = NULL,
  geometry = FALSE,
  crs = 4326,
  version = "latest",
  con = NULL
)
```

## Arguments

- x:

  A character vector of address strings, or a data frame of already
  parsed components as returned by \[normalize_address()\]. Passing the
  data frame lets you parse once and geocode repeatedly, or edit a parse
  before resolving it.

- known:

  Components the caller already has, as a named list of vectors each
  length 1 or \`length(x)\`. \*\*Authoritative\*\*: each overrides
  whatever the address string said, lands on the returned row, and
  constrains the search. \`PROV_ABVN\` also reaches
  \[normalize_address()\], where knowing the province disambiguates the
  parse.

  The two municipality keys are two different searches. \`MUN_NAME\` is
  the \*\*mailing city\*\*, compared straight at NAR's
  \`MAIL_MUN_NAME\`. \`CSD_NAME\` is the \*\*census subdivision\*\*,
  resolved through NAR's alias set – so \`CSD_NAME = "Toronto"\` reaches
  the addresses NAR files under \`SCARBOROUGH\` and \`MUN_NAME =
  "Toronto"\` does not. Supply both to narrow to one community inside an
  amalgamated city. See \[nar_known()\] for the full key list.

  \`CSD_NAME\` also comes back as an output column, reporting the census
  subdivision the match turned out to be in – which is a weaker claim
  than the constraint, since the search was not restricted to it. A
  parse handed back to \`geocode()\` therefore answers exactly as the
  string did; only a \`CSD_NAME\` you assert here, or one on a frame you
  built yourself, restricts anything. \[nar_known_csd()\] has the
  address that proves the difference.

- within:

  A spatial restriction: an \`sf\`/\`sfc\` object, an \`st_bbox\`, or a
  length-4 numeric \`c(xmin, ymin, xmax, ymax)\`, interpreted in \`crs\`
  unless it carries its own. \*\*Authoritative\*\*, and applied to every
  tier.

- geometry:

  Whether to return an \`sf\` object with POINT geometry. Unmatched rows
  get an empty point. Default \`FALSE\`, which returns \`lon\` and
  \`lat\` columns instead.

- crs:

  CRS for the returned coordinates, default EPSG:4326.

- version:

  NAR version to query, passed to \[nar_connection()\]. Ignored when
  \`con\` is supplied.

- con:

  An open NAR connection to reuse. The caller keeps ownership: a
  connection passed in here is left open, while one opened internally is
  closed again before returning.

## Value

A data frame with one row per matched NAR record: \`input_id\` (the
index of the address in \`x\`), \`input\`, \`match_rank\`, the record
columns listed by \`nar_geocode_match_cols()\`, and either
\`lon\`/\`lat\` or an \`sf\` geometry column. Zero rows if nothing
matched anything.

## What it does not do

This is the \*\*exact NAR tier only\*\* – the same candidate set
\[geocode()\]'s \`"nar"\` tier collapses, built by the same code. There
is deliberately no \`method\` argument, because no other tier has a
candidate set to enumerate: interpolation stands between two civic
numbers and resolves to no record at all, \`"rnf"\` interpolates along a
street segment, and the online services return an answer rather than a
set. An address only those tiers can place therefore has no matches
here, which is the correct answer and not a gap. Quebec's \`"rqa"\` tier
does resolve to records, but they are RQA rows with RQA columns and
would not stack with NAR's.

Past \`match_rank == 1\` the order carries no meaning. It is the tier's
tie-break – building points before blockface before none, then
\`ADDR_GUID\` – which exists to make the \*first\* row reproducible, not
to rank the rest. Sort on whatever you are actually asking about.

An address that matched nothing contributes \*\*no rows\*\*, so the
result is not aligned with the input the way \[geocode()\]'s is;
\`input_id\` indexes back into it. Use \[geocode()\] when you need one
row per address.

## See also

\[geocode()\], which collapses this to one row per address.

## Examples

``` r
if (FALSE) { # \dontrun{
# One point, nineteen addresses: the units of one property, and the four
# postal codes between them are why geocode() reports no match_postal_code.
geocode_matches("49321 Range Road 72")

# The usual workflow -- resolve first, then look only where it collapsed.
g <- geocode(addresses)
geocode_matches(addresses[g$n_records > 1])
} # }
```
