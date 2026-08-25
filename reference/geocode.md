# Geocode Canadian addresses to coordinates

Parses each address with \[normalize_address()\] and resolves the
result, returning one row per input in input order. \`method\` names the
tiers to try and the order to try them in; the column \`match_method\`
records which one answered:

\* \*\*\`nar_building\`\*\* – the civic number is in NAR and carries its
own building representative point. This is the exact match. \*
\*\*\`nar_blockface\`\*\* – the civic number is in NAR but has only a
blockface point, the centroid of one side of a street between two
intersections. \* \*\*\`nar_no_geometry\`\*\* – the civic number is in
NAR and \`ADDR_GUID\` names the record, but NAR holds no coordinates for
it and none could be interpolated. 65k addresses are in this state. It
is a different answer from \`none\`, which means the address was not
found at all. \* \*\*\`nar_interpolated\`\*\* – the civic number is
\*not\* in NAR, so the position is interpolated between the nearest
known civic numbers of the same parity on either side of it. See the
section below. \* \*\*\`rqa_building\`\*\*, \*\*\`rqa_geocoded\`\*\*,
\*\*\`rqa_uncertain\`\*\*, \*\*\`rqa_lot\`\*\*, \*\*\`rqa_other\`\*\* –
answered by the \`rqa\` tier, which carries Quebec's own
positional-quality class rather than one label. Only \`rqa_building\` is
a building placement, and it is the only one that reports an
\`uncertainty_m\`. See \[rqa_import()\]. \* \*\*\`rnf_interpolated\`\*\*
– answered by the \`rnf\` tier: the civic number fell inside the address
range Statistics Canada's road network file gives for one side of one
street segment, and the address is placed along that segment. See
\[rnf_import()\]. \* \*\*\`rnf_ambiguous\`\*\* – the \`rnf\` tier found
the civic number in the ranges of \*\*several\*\* segments and refused
to choose between them. No coordinates, and \`n_matches\` says how many
were in contention. It is reported rather than silently dropped because
the whole of that tier's gross-error tail is these rows; see the tier
description below. \* \*\*\`bc_site\`\*\*, \*\*\`bc_civic\`\*\*,
\*\*\`bc_block\`\*\*, \*\*\`bc_street\`\*\*, \*\*\`bc_locality\`\*\* –
answered by the \`bc\` tier. See \[bc_geocode()\]. \* \*\*\`nrcan\`\*\*
– answered by the \`nrcan\` tier. One value rather than several, because
only one class of geolocator answer survives its floors at all. See
\[nrcan_geocode()\]. \* \*\*\`qc_address\`\*\* – answered by the \`qc\`
tier. Also one value: the service's other locator resolves a street
rather than an address, and the tier does not place a row from it. See
\[qc_geocode()\]. \* \*\*\`not_covered\`\*\* – the address parsed to a
province this database does not hold, so no tier could have matched it.
Only a partial import (see the \`provinces\` argument of
\[nar_connection()\]) ever produces this, and it is deliberately
distinct from \`none\`: the address may be perfectly good. \*
\*\*\`none\`\*\* – nothing resolved.

## Usage

``` r
geocode(
  x,
  prov = NULL,
  mun = NULL,
  within = NULL,
  method = c("nar", "nar_interpolate"),
  geometry = FALSE,
  crs = 4326,
  version = "latest",
  con = NULL,
  ...
)
```

## Arguments

- x:

  A character vector of address strings, or a data frame of already
  parsed components as returned by \[normalize_address()\]. Passing the
  data frame lets you parse once and geocode repeatedly, or edit a parse
  before resolving it.

- prov:

  Province code(s) to constrain the search to, length 1 or
  \`length(x)\`. \*\*Authoritative\*\*: it overrides whatever the
  address string said, and is also passed to \[normalize_address()\],
  where knowing the province additionally disambiguates the parse.

- mun:

  Municipality name(s) to constrain the search to, length 1 or
  \`length(x)\`. \*\*Authoritative\*\*, overriding the string. Resolved
  through NAR's alias set rather than matched against the mailing city,
  so \`"Toronto"\` reaches the addresses NAR files under
  \`SCARBOROUGH\`, and a name that denotes several jurisdictions means
  all of them. Combine with \`prov\` when a name is used in more than
  one province.

- within:

  A spatial restriction: an \`sf\`/\`sfc\` object, an \`st_bbox\`, or a
  length-4 numeric \`c(xmin, ymin, xmax, ymax)\`, interpreted in \`crs\`
  unless it carries its own. \*\*Authoritative\*\*, and applied to every
  tier.

- method:

  Tiers to try, in priority order: any of \`"nar"\`, \`"rqa"\`,
  \`"nar_interpolate"\`, \`"rnf"\`, \`"bc"\`, \`"nrcan"\` and \`"qc"\`.
  Default \`c("nar", "nar_interpolate")\`, which is the offline pair.
  See the section below.

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

- ...:

  Passed to the online tiers named in \`method\`, and to the parse.
  Gazetteer arguments – \[nar_resolve_gazetteer()\]'s,
  \`mun_swap_penalty\` among them – are forwarded to
  \[normalize_address()\] when \`x\` is a character vector, and ignored
  when it is a data frame someone else has already parsed.
  \`keep_refused = TRUE\` is the one worth knowing about: it places the
  matches the gazetteer's threshold would have turned away and flags
  them in \`refused_for\`, which turns an invisible false negative into
  an answer \[geocode_accept()\] can drop again. \`rate\` is understood
  by all of them; \`api_key\` is \[bc_geocode()\]'s, as is anything else
  it does not recognize, which it forwards to its own service as a query
  parameter. \[nrcan_geocode()\] and \[qc_geocode()\] are each given
  only the arguments they declare, so a BC-only argument passed
  alongside \`"nrcan"\` reaches the BC tier alone rather than erroring.
  Note that \`min_score\` is understood by \[bc_geocode()\] and
  \[qc_geocode()\] both, and means different things to them – see
  \[qc_geocode()\] on why its score is not a ranking. Unused when
  \`method\` names no online tier.

## Value

A data frame with one row per input, carrying every column
\[normalize_address()\] returns – \`mun_remapped\` and \`mun_evidence\`
among them – plus \`ADDR_GUID\`, \`match_method\`, \`uncertainty_m\`,
\`n_matches\`, \`n_records\`, \`match_postal_code\`, and either
\`lon\`/\`lat\` or an \`sf\` geometry column. \`POSTAL_CODE\` is the
\*parsed input\* – what the address string itself said, or \`NA\` when
it said nothing – while \`match_postal_code\` is what the matched record
carries; see the section below.

## Choosing the tiers

\`method\` is a vector of tier names in priority order. Each tier is
offered only the rows its predecessors left without a position, so the
order is what decides which answer wins:

\* \*\*\`"nar"\`\*\* – look the civic number up in NAR directly. Answers
\`nar_building\`, \`nar_blockface\` or \`nar_no_geometry\`. \*
\*\*\`"rqa"\`\*\* – look the civic number up in the \*\*Repertoire
quebecois des adresses\*\*, Quebec's own register, which has to be
imported once with \[rqa_import()\] and lives beside \`Addresses\`
rather than in it. Quebec only, offline, and it holds roughly 308,000
civic addresses NAR does not. It belongs \*\*after \`"nar"\` and before
\`"nar_interpolate"\`\*\*: a register point beats an interpolated one,
and NAR's own building point beats both. Placed there it is worth about
a third of Quebec's unplaceable tail outright and replaces an
interpolated guess – median 23 m from RQA's own coordinate, with 7 \*
\*\*\`"nar_interpolate"\`\*\* – place a civic number NAR does not carry
between its known neighbours. Answers \`nar_interpolated\`. \*
\*\*\`"rnf"\`\*\* – interpolate along Statistics Canada's \*\*Road
Network File\*\*, which has to be imported once with \[rnf_import()\]
and lives beside \`Addresses\` rather than in it. Offline, national, and
it answers for streets NAR does not carry at all – which is what
separates it from \`"nar_interpolate"\`, whose flanking civics have to
come from NAR itself. It belongs \*\*after \`"nar_interpolate"\`\*\*:
where both can answer, NAR's own neighbours are about six times more
accurate. On a 5,000-address sample of business filings it placed a
quarter of what the offline pair left unplaced, the largest recovery any
tier here has offered, and \*\*it refuses whenever more than one segment
matches\*\*, which is where its accuracy comes from rather than a
nicety. \* \*\*\`"bc"\`\*\* – ask the Province of BC's \[Address
Geocoder\]\[bc_geocode()\]. British Columbia only, and \*\*this makes
one network request per unplaced BC row\*\*; nothing contacts it unless
the tier is named. The constraints are honoured: what is sent is rebuilt
from the components after any \`prov\`/\`mun\` override, and a point
outside \`within\` is discarded rather than returned. \*
\*\*\`"qc"\`\*\* – ask the Quebec government's
\[geocoder\]\[qc_geocode()\]. Answers \`qc_address\`. Quebec only, and
the one online tier that does \*\*not\*\* cost a request per row: it
batches 1000 addresses per request, so naming it is cheap even on a
large unplaced tail. It is also the only one that refuses – it returns
no point rather than a locality centroid when it cannot match – so its
answers need less rejecting than the others'. \* \*\*\`"nrcan"\`\*\* –
ask NRCan's national \[geolocator\]\[nrcan_geocode()\]. Answers
\`nrcan\`. One network request per unplaced row, and it covers the whole
country, so unlike \`"bc"\` and \`"qc"\` there is no province that
excludes a row from being sent. \*\*It belongs last.\*\* Its surviving
answers are roughly interpolation-grade at the median with a much longer
tail, and it has no score of its own – everything that separates a hit
from a confident wrong answer is done by re-parsing the returned title,
which is strict but not free of false positives.

The default \`c("nar", "nar_interpolate")\` is offline and prefers a
real NAR record over an interpolated one. It does \*\*not\*\* include
\`"rqa"\`, which would otherwise appear and disappear depending on
whether \[rqa_import()\] had been run; in Quebec, \`c("nar", "rqa",
"nar_interpolate")\` is the recommended offline set once it has.
\`method = "nar"\` keeps only the addresses NAR actually carries.
\`c("nar", "nar_interpolate", "bc")\` adds the BC service as a last
resort, and \`c("bc", "nar")\` prefers it over NAR wherever it answers.
\`"qc"\` is the same shape as \`"bc"\` for the other province that
publishes its own geocoder. \`"nrcan"\` is the national counterpart to
both and is the only tier that answers with no local database at all,
which is the case it exists for; it should be named after every other
tier, never before one.

A row NAR holds without coordinates (\`nar_no_geometry\`) is passed on
to the next tier: knowing the address exists is worth reporting, but it
is not worth withholding a position a later tier can supply, and the
\`ADDR_GUID\` found survives whichever tier ends up placing the row.
Note that the reverse costs something – a tier that never runs for a row
reports nothing about it, so putting \`"nar"\` last means interpolated
rows carry no \`ADDR_GUID\`.

## Interpolation

Only civics of the \*\*same parity\*\* are used, because odd and even
numbers sit on opposite sides of the street and pooling them is markedly
worse: measured by leave-one-out over all 10.6M distinct NAR civic
points, same-side interpolation has a median error of 4.2 m against 35.2
m for both sides pooled, and beats simply taking the nearest known civic
(16.9 m).

\*\*Extrapolation is refused.\*\* A civic number past the last known one
on its side has no second point to interpolate against, and guessing
from the run's spacing is close to worthless – median error 15.1 m but a
90th percentile of 237 m, barely better than the nearest neighbour it
would displace. Those rows fall through to the next tier rather than
carrying a number that looks like the others. 7.3

## Constraining the search

\`prov\`, \`mun\` and \`within\` are assertions about where the address
is, not hints. Each overrides whatever the string itself claimed – a row
geocoded with \`prov = "BC"\` comes back with \`PROV_ABVN\` reading
\`BC\` no matter what was written – and they compose, so \`prov\` plus
\`mun\` is the province-and-postal-city case and either can be combined
with a polygon.

They earn their keep twice over. They resolve the ambiguity that
\`n_matches\` otherwise only reports, since a bare \`100 Main St\` means
something definite once the municipality is fixed. And \`within\` is
close to free: the bounding box is compared against the stored
\`x\`/\`y\` columns, which DuckDB prunes with per-row-group zonemaps
rather than scanning – the same mechanism that makes
\[reverse_geocode()\] fast.

## Uncertainty

\`uncertainty_m\` estimates the \*\*90th-percentile positional error
this package's method introduces, relative to NAR's own building
point.\*\* It is 0 for \`nar_building\`, 176 for \`nar_blockface\`, and
half the distance between the two flanking civics for
\`nar_interpolated\`.

For \`rnf_interpolated\` it is \`max(95, 0.35 \* len_m)\` in the
segment's length, which is two-part because the error is: a short block
is dominated by the setback and the side offset, which do not shrink
with it, and a long one by how far along the block the range put the
house, which does scale.

That last figure is measured, and it holds across scales: the ratio of
error to flanking span has a 90th percentile of 0.50 in every span
bucket from under 50 m to over 2 km (0.496–0.522). So a 40 m gap between
neighbours gives 20 m and a 3 km gap gives 1.5 km, and filtering on
\`uncertainty_m\` is the way to drop the interpolations that are too
coarse to use.

\*\*NAR's own error is not included and is not estimated.\*\* The User
Guide warns that a building point "may not correspond exactly to the
physical center of the building structure itself" – it can be the road
access point or the driveway – and that offset is neither published nor
consistent, so \`uncertainty_m = 0\` means "this package added nothing",
not "this point is exact".

\`n_matches\` counts the distinct NAR points that satisfied the query.
Anything above 1 means the address was ambiguous – most often a street
name the input did not pin to a municipality – and \`uncertainty_m\` is
then widened to the distance from the point returned to the furthest
rejected candidate.

\*\*\`n_matches == 1\` is not the safety guarantee it looks like\*\*,
and this is where the remaining widening comes from. One candidate means
one was found, not that the right one was among those searched – and
when the gazetteer substituted the municipality, the uniqueness was
manufactured by the same step that chose the place, because the street
was searched for only in the municipality the gazetteer had already
decided on. In Nova Scotia, measured against PVSC's independent points,
one exact unambiguous match in 180 was more than a kilometre wrong and
85

Two things answer that, and neither of them is \`n_matches\`. The
gazetteer fines a municipality swap nothing attests – the attestations
being co-postal partners read out of NAR itself and the census
subdivision the street already sits in – which more than halves the
errors past 5 km in that same 40,000-row Nova Scotia sample, 98 to 42,
and takes the kilometre rate from one exact unambiguous match in 192 to
one in 286, at a cost of 373 exact matches; see \[nar_gazetteer_sql()\].
And what survives is \*reported\*: \`uncertainty_m\` is floored per
\[nar_remap_uncertainty_m()\] according to \`mun_evidence\`, which
records \*how\* the substitution was attested, so an unattested remap no
longer claims the 0 m an exact civic match would otherwise imply – while
a remap a postal code or a census subdivision vouches for is left alone,
because measured against PVSC it lands no further out than a
municipality the input got right. Both flags are
\[normalize_address()\]'s and are returned alongside the answer; read
\`mun_remapped\` directly when what you need is \*whether\* the place
was chosen for you rather than how far the error might be, because the
risk it carries lives in a tail no distance at the 90th percentile
describes.

## How many matched

\`n_matches\` and \`n_records\` count two different things and the gap
between them is the point of having both.

\`n_matches\` counts distinct \*\*points\*\*. It is the ambiguity
measure: it is what widens \`uncertainty_m\`, and it is what tells you
the answer may be in the wrong place. \`n_records\` counts distinct
\*\*NAR addresses\*\*, which is usually the larger number, and it tells
you the answer may be in the right place but stand for more than one
thing.

They come apart because NAR files every unit of a multi-unit building as
its own address, all at the building's one coordinate. \`49321 Range
Road 72\` in Brazeau County, Alberta returns \`n_matches = 1\` and
\`n_records = 19\`: there is exactly one place to put it and nineteen
addresses there, units 1 through 29, and the input named none of them.
This is not a corner case – \*\*47 the addresses NAR places share their
coordinate with at least one other address.\*\*

Naming the unit is what closes the gap. Where the input carries an
\`APT_NO_LABEL\` and NAR holds that unit at that civic number, the
candidates are narrowed to it: \`49321 Range Road 72, Unit 9\` is one
record rather than nineteen. The narrowing \*\*narrows or it does
nothing\*\* – a unit NAR has no row for is dropped rather than enforced,
and the address is placed as though it had been written without one.
That fallback is not defensive tidiness. Over 5,000 Corporations Canada
filings, 27.6 tier can match, 72.5 to exactly one record\*\*, while the
remaining 27.5 carry there – enforcing it would take those 327 addresses
from placed to unplaced. Over the whole corpus the narrowing cuts
118,937 matched records to 25,955, and the inputs reporting more than
one record from 1,422 to 578.

A record count above 1 is therefore not a warning by itself. It is a
warning when the collapsed records disagree about something you care
about, and the one such disagreement reported today is the postal code:
\`match_postal_code\` goes \`NA\` rather than pick one. The Brazeau
County address is \`NA\` for that reason – its nineteen units carry four
postal codes between them, and naming one of the nineteen fills it in.

\`n_records\` is 0 wherever no record was matched: every interpolated
row that did not first hit the \`nar\` or \`rqa\` tier, and every online
tier.

## Two postal codes

the result carries two postal-code columns and they answer different
questions. \`POSTAL_CODE\` comes from \[normalize_address()\] and is
\*\*what the input string said\*\* – \`NA\` when it said nothing, which
is the usual case for an address typed without one.
\`match_postal_code\` is \*\*what the matched record carries\*\*, and it
is filled in from the source rather than from the input.

Only the tiers that match a record can fill it: the \`nar\` tier
(\`nar_building\`, \`nar_blockface\` and \`nar_no_geometry\` alike – an
address NAR holds without coordinates still has a postal code) and the
\`rqa\` tier. It then \*\*survives whichever tier ends up placing the
row\*\*, exactly as \`ADDR_GUID\` does, so a \`nar_interpolated\` row
carries a postal code when the exact tier found the record first and NAR
simply had no coordinates for it. A row interpolated without such a hit,
an \`rnf_interpolated\` row and every online answer leave it \`NA\`:
none of them resolve to a record with a postal code of its own, an
interpolated point sits between two addresses that may not share one,
and guessing which flank to copy would produce a value indistinguishable
from a looked-up one.

It is also \`NA\` when the candidates disagree. NAR holds one row per
address, so a civic number with units contributes many rows, and 1.4 –
4.2 more than one postal code. Where the input names no unit – the usual
case – nothing in the query says which of those rows was meant, and
reporting one of them would be a coin flip. \`100 Queen St W, Toronto\`
is one: NAR carries it as \`M5H2N1\` and \`M5H2N2\` both. A postal code
in the \*input\* does not break the tie either, since it is what the
address claims rather than something the query established. Naming the
unit does break it, because it narrows the candidates rather than
choosing among them: 55 of the 5,000 corpus filings gain a
\`match_postal_code\` for that reason alone.

## See also

\[geocode_accept()\], for applying your own bar to the result without
re-running the query.

## Examples

``` r
if (FALSE) { # \dontrun{
geocode("1055 W Georgia St, Vancouver BC")

# Only addresses NAR actually carries -- nothing interpolated.
geocode(addresses, method = "nar")

# Add the BC service as a last resort. Makes network requests.
geocode(addresses, method = c("nar", "nar_interpolate", "bc"))

# Quebec's own register, offline, after one rqa_import().
geocode(addresses, method = c("nar", "rqa", "nar_interpolate"))

# The road network file reaches streets NAR does not carry, after one
# rnf_import().
geocode(addresses, method = c("nar", "nar_interpolate", "rnf"))

# NRCan's geolocator is national, so it can back up the whole country.
geocode(addresses, method = c("nar", "nar_interpolate", "nrcan"))

# Parse once, resolve many times, and keep only the precise matches.
parsed <- normalize_address(addresses)
g <- geocode(parsed, geometry = TRUE)
g[g$uncertainty_m <= 25, ]
} # }
```
