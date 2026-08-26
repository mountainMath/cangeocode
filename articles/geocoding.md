# Geocoding addresses

Forward geocoding — an address string in, a coordinate out — is
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md).
It parses each string with
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md),
resolves the result against NAR, and hands back one row per input in
input order, saying for each one **how** it was found and **what that
cost**.

This vignette covers the whole path: the basic call, reading the columns
that qualify every result, choosing which methods to try, constraining
the search, and checking an answer against a second source. The parsing
step has a vignette of its own —
[`vignette("address-normalization")`](https://mountainmath.github.io/cangeocode/articles/address-normalization.md)
— since normalizing addresses is useful whether or not a coordinate
comes out at the end.

``` r

library(cangeocode)
library(dplyr)
```

No database connection is opened here, and none of the
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
calls below take one.
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
opens its own when `con` is not supplied — and, since opening a 5 GB
database costs about half a second, keeps it open for the next call
rather than closing it. So a loop over ten thousand addresses pays for
the connection once, and nothing has to be threaded through to make that
happen.

[`open_nar()`](https://mountainmath.github.io/cangeocode/reference/open_nar.md)
opens that connection up front and
[`close_nar()`](https://mountainmath.github.io/cangeocode/reference/close_nar.md)
ends it. Neither is required; they are there to name a release other
than the latest without repeating it at every call site, and to control
when the database is held — an import needs the write lock, so
[`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md),
[`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md)
and `nar_connection(refresh = TRUE)` close the connection themselves and
say so.

Once a connection is open, `version = "latest"` means *the release that
is open*, not whatever StatCan has published since. Name a version to
move to a different one.

Passing `con` explicitly still does something the implicit connection
cannot: it says unambiguously which database answered. And for
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
it is not about cost at all — there `con` is the switch that turns NAR’s
street gazetteer on, so it changes the answer rather than the speed.
That is why the one example below that opens a connection is a parsing
example.

## The basic call

The eight addresses below are deliberately awkward: a unit, a lowercased
province spelled out, a French street, a prairie range road, a New
Brunswick route, a typo, and a post office box.

``` r

addresses <- c(
  "1055 W Georgia St, Vancouver BC",
  "302-1055 west georgia street, vancouver, british columbia",
  "100 Queen St W, Toronto, ON M5H 2N2",
  "845, rue de Vernon, Gatineau, QC",
  "34221 Range Road 272, Red Deer County, AB",
  "5491 Route 11, Brantville, NB",
  "29 HPCKING AVE, SAULT STE. MARIE, ON",
  "PO Box 40, Iqaluit, NU"
)

g <- geocode(addresses)

g |>
  mutate(input = substr(input, 1, 38),
         uncertainty_m = round(uncertainty_m, 1)) |>
  select(input, match_method, uncertainty_m, lon, lat)
#>                                    input     match_method uncertainty_m        lon      lat
#> 1        1055 W Georgia St, Vancouver BC     nar_building           0.0 -123.12141 49.28529
#> 2 302-1055 west georgia street, vancouve     nar_building           0.0 -123.12141 49.28529
#> 3    100 Queen St W, Toronto, ON M5H 2N2     nar_building           0.0  -79.38250 43.65150
#> 4       845, rue de Vernon, Gatineau, QC     nar_building           0.0  -75.81013 45.45202
#> 5 34221 Range Road 272, Red Deer County,     nar_building           0.0 -113.73504 51.91683
#> 6          5491 Route 11, Brantville, NB nar_interpolated          83.9  -64.94581 47.40144
#> 7   29 HPCKING AVE, SAULT STE. MARIE, ON     nar_building           0.0  -84.36311 46.53467
#> 8                 PO Box 40, Iqaluit, NU             none            NA         NA       NA
```

Seven of the eight resolved, and the one that did not is the one that
never could: NAR is a list of civic addresses and holds no post office
boxes at all.

Several of these took work that is worth noticing. `HPCKING` is not a
street — the gazetteer matched it against the streets NAR actually has
in Sault Ste. Marie and returned `Hocking`. `west georgia street` and
`W Georgia St` both landed on the same canonical street.
`Range Road 272` survived intact instead of being read as a street
called `Range` of type `RD`. And the Gatineau address kept its type in
front of the name, where French puts it.

Batch rather than loop. The street-name join is a scan whose cost every
row in a call shares — about 0.05 s for a 5-row probe and 0.08 s for a
200-row one — so one call with a thousand addresses is worlds away from
a thousand calls.

## Every result says how it was found

`match_method` is the column to read before anything else.

| value | meaning |
|----|----|
| `nar_building` | the civic number is in NAR with its own building point |
| `nar_blockface` | in NAR, but only a blockface centroid is available |
| `nar_interpolated` | not in NAR; placed between the flanking civic numbers |
| `nar_no_geometry` | in NAR — `ADDR_GUID` names the record — but unplaceable |
| `not_covered` | in a province this database does not hold |
| `none` | not found |

`not_covered` only ever appears against a database imported for selected
provinces (`nar_connection(provinces = )`). It is kept apart from `none`
because it says nothing about the address: no tier could have matched
it, so the parse was never tested.

`uncertainty_m` prices each of those: it is the **90th-percentile error
this package adds relative to NAR’s own building point**. Zero for an
exact match, 176 m for a blockface one (the measured p90
building-to-blockface separation), and half the flanking span for an
interpolated one.

That last figure is not a guess either. The ratio of interpolation error
to flanking span is scale-invariant — its 90th percentile is 0.50 in
every span bucket from under 50 m to over 2 km — so half the span is the
p90 error whatever the scale. Which means `uncertainty_m` is a real
filter:

``` r

g |>
  filter(match_method != "none", uncertainty_m <= 50) |>
  nrow()
#> [1] 6
```

**What it does not include is NAR’s own error.** The StatCan user guide
allows a building representative point to be the road access point or
the driveway, and publishes no accuracy figure, so `uncertainty_m = 0`
means “this package added nothing”, not “this point is exact”.

The third qualifier is `n_matches`, the number of distinct points that
satisfied the query. Anything above 1 means the address was ambiguous —
most often a street name that was never pinned to a municipality — and
`uncertainty_m` widens to the distance out to the furthest rejected
candidate.

``` r

geocode(c("100 Main St", "100 Main St, Moncton, NB")) |>
  select(input, match_method, n_matches, uncertainty_m)
#>                      input match_method n_matches uncertainty_m
#> 1              100 Main St nar_building       139       4043776
#> 2 100 Main St, Moncton, NB nar_building         1             0
```

The first row is an exact `nar_building` match and still worthless:
there are 139 of them and the uncertainty is four thousand kilometres,
which is the width of the country. `match_method` describes the
*quality* of the match, not whether it was the one you meant — read
`n_matches` alongside it, or fix the question with the constraints
below.

### The place you were given may not be the place you named

`n_matches == 1` is the qualifier it is easiest to over-read. It means
one candidate was found, not that the right one was among the candidates
searched — and those two come apart in a specific way. When the string
names a community the address register does not file mail under, the
parser resolves it through the census subdivision it belongs to, which
widens the search from that community to every community in it.
`MILFORD, NS` becomes all 166 of Halifax Regional Municipality’s, spread
over 127 km. Usually that is the feature working. Occasionally the only
street of that name in the whole regional municipality is 60 km from the
one you meant, and it comes back as a unique exact match.

So
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
reports whether the municipality it hands back is the one you wrote, and
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
passes the flag through:

``` r

geocode(c("1741 Brunswick St, Halifax, NS",
          "25 River Rd, Moser River, NS",
          "36 Lakeview Dr, Howie Centre, NS")) |>
  select(input, MUN_NAME, mun_remapped, mun_evidence, n_matches, uncertainty_m)
#>                              input         MUN_NAME mun_remapped mun_evidence n_matches
#> 1   1741 Brunswick St, Halifax, NS          HALIFAX        FALSE         kept         1
#> 2     25 River Rd, Moser River, NS          HALIFAX         TRUE     copostal         1
#> 3 36 Lakeview Dr, Howie Centre, NS CONQUERALL MILLS         TRUE   untestable         1
#>   uncertainty_m
#> 1             0
#> 2             0
#> 3           118
```

All three are exact building matches with one candidate each. The second
is 118 km from Moser River: there is no `River Rd` filed under that
community, and the one in Halifax — the same regional municipality —
answered for it. The third is 388 km from Howie Centre, on the other
side of the province.

Beside the flag, `mun_evidence` records *how* the substitution was
justified, because not all of them are equally suspect:

| `mun_evidence` | what it means |
|----|----|
| `kept` | not a substitution — the register files the address under the name you wrote |
| `copostal` | the two names appear on the same full postal code in the register, so they are two labels for one delivery geography |
| `csd` | the name you wrote is the census subdivision the street sits in — an amalgamation or a legacy name, `Toronto` for a street still mailed to `North York` |
| `untestable` | the name you wrote takes no postal-coded mail at all, so there was nothing to check it against |
| `unattested` | checked, and nothing corroborated it |
| `inferred` | you named no municipality; the register determined one |

The first three are the attested ones, and `uncertainty_m` leaves them
alone. That is a measurement rather than a courtesy: against an
independent reading of the same houses, an attested remap lands at a
90th percentile of 52 m, *below* the 57 m of addresses whose
municipality was never touched. The other three are floored at 118 m,
their own pooled 90th percentile — which is why the third row above does
not claim the 0 m an exact civic match would otherwise imply.

The scoring uses the same evidence: a substitution nothing corroborates
is fined, so it has to be an exact or one-keystroke street name agreeing
on everything else the string gave before it can win. That more than
halves the errors past 5 km.

None of it is a guarantee, and `uncertainty_m` is the weakest part. The
second row is *attested* and still 118 km wrong: the risk a remap
carries lives in a tail no 90th percentile describes — the unattested
classes run 1.6–1.8% past 5 km, the untouched ones 0.05% — so if a
kilometre-scale error is unacceptable, filter on `mun_remapped` itself
rather than on the metres.

### One place, several addresses

`n_matches` counts *points*, and beside it `n_records` counts the *NAR
addresses* that matched. They are usually different numbers, and the gap
is not noise:

``` r

geocode(c("4025 W 38th Ave, Vancouver BC",
          "6093 Iona Dr, Vancouver BC",
          "49321 Range Road 72")) |>
  select(input, match_method, n_matches, n_records, match_postal_code)
#>                           input match_method n_matches n_records match_postal_code
#> 1 4025 W 38th Ave, Vancouver BC nar_building         1         1            V6N2Y8
#> 2    6093 Iona Dr, Vancouver BC nar_building         1        33            V6T0B2
#> 3           49321 Range Road 72 nar_building         1        19              <NA>
```

All three are unambiguous as *places* — one point each. Only the first
is unambiguous as an *address*. NAR files every unit of a multi-unit
building as its own address at the building’s single coordinate, so the
second matched 33 records and the third 19. This is not exotic: **47% of
the addresses NAR places share their coordinate with at least one other
address.**

So a high `n_records` is not by itself a problem. Geocoding a building
with 33 units to that building’s point is the correct answer to the
question that was asked; unless the input says which unit it means,
there is nothing to choose between them, and choosing would return the
same point anyway. `n_records` matters when the collapsed records
*disagree* about something you were relying on — and the disagreement
reported today is the postal code. The second address keeps its, because
all 33 units share it. The third loses its, because those 19 records
carry four postal codes between them.

The two columns fail differently, and that is why both are there.
`n_matches` above 1 says the point may be in the wrong place.
`n_records` above 1 says the point is in the right place but stands for
more than one thing.

### Looking at the records

[`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
returns them, one row each:

``` r

geocode_matches("49321 Range Road 72") |>
  select(match_rank, APT_NO_LABEL, MAIL_MUN_NAME, MAIL_POSTAL_CODE, lon, lat) |>
  head(6)
#>   match_rank APT_NO_LABEL  MAIL_MUN_NAME MAIL_POSTAL_CODE      lon      lat
#> 1          1            9 BRAZEAU COUNTY           T7A2A2 -114.922 53.24723
#> 2          2            7 BRAZEAU COUNTY           T7A2A2 -114.922 53.24723
#> 3          3           25 DRAYTON VALLEY           T7A1R9 -114.922 53.24723
#> 4          4            2 BRAZEAU COUNTY           T7A2A2 -114.922 53.24723
#> 5          5            1 BRAZEAU COUNTY           T7A2A2 -114.922 53.24723
#> 6          6           10 BRAZEAU COUNTY           T7A1R8 -114.922 53.24723
```

Nineteen units of one property, on one coordinate, split across two
mailing municipalities and four postal codes. The rows are ranked the
way the tier ranks them, so `match_rank == 1` is the record
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
answered with — the two queries collapse the same candidate set with the
same ordering expression, so that is a guarantee rather than a
coincidence. Past the first row the order carries no meaning: it is the
`ADDR_GUID` tie-break, which exists to make the first row reproducible.

The usual way to use it is not to call it on everything. Resolve first,
then open up only what collapsed:

``` r

addr <- c("49321 Range Road 72", "4025 W 38th Ave, Vancouver BC")
collapsed <- geocode(addr)$n_records > 1
geocode_matches(addr[collapsed])$APT_NO_LABEL
#>  [1] "9"  "7"  "25" "2"  "1"  "10" "15" "5"  "6"  "17" "3"  "23" "4"  "29" "13" "8"  "27" "21" "19"
```

It reads the exact NAR tier and nothing else, and takes no `method`
argument, because no other tier has a set to enumerate. Interpolation
stands *between* two civic numbers and resolves to no record; the road
network file interpolates along a segment; the online services return an
answer rather than a candidate set. So an address that only those tiers
could place has no matches here:

``` r

nrow(geocode_matches("9999 Jasper Ave, Edmonton, AB"))
#> [1] 0
```

That is the right answer and not a gap — nothing was collapsed, because
nothing was resolved to a record in the first place. It does mean the
result is not aligned with the input the way
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)’s
is; `input_id` indexes back into it.

### Naming the unit

The collapse closes when the input says which unit it means:

``` r

geocode(c("49321 Range Road 72",
          "49321 Range Road 72, Unit 9",
          "49321 Range Road 72, Unit 999")) |>
  select(input, APT_NO_LABEL, match_method, n_records, match_postal_code)
#>                           input APT_NO_LABEL match_method n_records match_postal_code
#> 1           49321 Range Road 72         <NA> nar_building        19              <NA>
#> 2   49321 Range Road 72, Unit 9            9 nar_building         1            T7A2A2
#> 3 49321 Range Road 72, Unit 999          999 nar_building        19              <NA>
```

The parsed unit is matched against NAR’s own, so the second row resolves
to a single record — and gains the postal code the nineteen-record set
had to decline. The third names a unit that property does not have, and
is placed anyway, exactly as though it had been written without one.

That fallback is what makes the narrowing safe to apply by default, and
it is not a formality. Across the 5,000 Corporations Canada filings this
package measures itself on, 1,189 addresses supply a unit *and* match
NAR records — and **27.5% of those units are not in NAR at the civic
number they were written against.** Enforcing the unit would take 327
addresses from placed to unplaced, a far worse trade than a wide
`n_records`. Where the unit is there, the narrowing is total: every one
of the other 862 collapses to exactly one record. Over the whole draw,
118,937 matched records become 25,955, and 55 addresses gain a
`match_postal_code`.

[`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
narrows identically, because it is the same candidate set:

``` r

geocode_matches("49321 Range Road 72, Unit 9") |>
  select(match_rank, APT_NO_LABEL, MAIL_MUN_NAME, MAIL_POSTAL_CODE)
#>   match_rank APT_NO_LABEL  MAIL_MUN_NAME MAIL_POSTAL_CODE
#> 1          1            9 BRAZEAU COUNTY           T7A2A2
```

Unit labels that are words are translated into the vocabulary the
address files use, so `Basement` and `Sous-sol` both find NAR’s `BSMT`.
Zero padding is not touched — `PH01` and `PH1` stay different labels —
because 0.20% of NAR’s units are padded, and a rule that unpadded them
would need an opinion about every label that is padded on purpose.

## Choosing the methods

`method` names the tiers to try and the order to try them in. Each tier
is offered only the rows the ones before it left without a position, so
the order *is* the priority.

``` r

addr <- "9999 Jasper Ave, Edmonton, AB"

geocode(addr, method = "nar")$match_method
#> [1] "none"
geocode(addr, method = c("nar", "nar_interpolate"))$match_method
#> [1] "nar_interpolated"
```

`"nar"` alone keeps only the addresses NAR actually carries, which is
the right choice when a false position is worse than no position. The
default pair adds interpolation, and on a 5,000-address sample of real
Corporations Canada registered offices that lifts coverage from 87.9% to
92.4%.

Interpolation is deliberately conservative. It uses the **same side of
the street only** — pooling both sides costs a median 35.2 m against 4.2
m — and it **refuses to extrapolate** past the last known civic number
on a side rather than continuing the run’s spacing, which scores a
respectable 15.1 m median but a 237 m 90th percentile. Those rows come
back `none`.

There are further tiers below: `"rqa"`, offline and Quebec-only;
`"rnf"`, offline and national, which interpolates along the road network
instead of along NAR’s own addresses; and `"bc"`, `"nrcan"` and `"qc"`,
which call online services. `"rqa"` and `"rnf"` both need an import of
their own first, and
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
says so up front if you name one that is not there.

## Constraining the search

`known` and `within` are assertions about where the address is, not
hints. `known` takes whatever components you already have, named as the
output names them, and every one of them overrides what the string said,
constrains the search, and lands on the returned row.

``` r

geocode("100 Queen St W, Vancouver, BC",
        known = list(PROV_ABVN = "ON", CSD_NAME = "Toronto")) |>
  select(input, PROV_ABVN, MUN_NAME, CSD_NAME, match_method, lon, lat)
#>                           input PROV_ABVN MUN_NAME CSD_NAME match_method      lon     lat
#> 1 100 Queen St W, Vancouver, BC        ON  TORONTO  TORONTO nar_building -79.3825 43.6515
```

The string names Vancouver; `known` says Toronto, Ontario; the search
runs in Toronto and the row reports Toronto. A result that reported a
municipality different from the one it was constrained to would
misdescribe what was actually searched.

### Two kinds of city

`MUN_NAME` and `CSD_NAME` are different questions, and this is where the
difference bites. `MUN_NAME` is the **mailing city** — the name on the
envelope, compared straight at NAR’s own. `CSD_NAME` is the **census
subdivision**, the administrative unit, resolved through NAR’s alias
set. That matters more than it sounds: NAR files a great many Toronto
addresses under `SCARBOROUGH`, `ETOBICOKE` and `NORTH YORK`, so
`CSD_NAME = "Toronto"` reaches them and `MUN_NAME = "Toronto"` does not.

The two do not nest — one mailing city can span several jurisdictions
and one jurisdiction carries many mailing cities — so asking for the
wrong one is not a near miss. Supply both and both constrain, which is
how you narrow to one community inside an amalgamated city. A parsed
mailing city that contradicts a `CSD_NAME` you asserted is dropped
rather than left to veto it.

`CSD_NAME` is also an output column, and there it means something
weaker: the census subdivision the record that matched happens to be in,
not a jurisdiction the search was confined to. The two are deliberately
not the same claim, so a parse handed back to
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
answers exactly as the string did.

The same list is how you hand over an address that never was one string.
An assessment roll with the civic number, the street and the community
in columns of their own can be passed as `known` directly, and only the
parts you do not have are read off the text.

`within` takes an `sf` polygon, an `st_bbox`, or a bare
`c(xmin, ymin, xmax, ymax)`, and is close to free — the bounding box is
compared against stored coordinate columns that DuckDB prunes with
per-row-group zonemaps rather than scanning.

``` r

downtown <- c(-123.13, 49.28, -123.11, 49.29)

geocode(c("1055 W Georgia St, Vancouver, BC",
          "4001 W King Edward Ave, Vancouver, BC"),
        within = downtown) |>
  select(input, match_method)
#>                                   input match_method
#> 1      1055 W Georgia St, Vancouver, BC nar_building
#> 2 4001 W King Edward Ave, Vancouver, BC         none
```

Both addresses exist; only one is downtown.

## Working with the parse

[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
returns every column
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
produced alongside the result, so the parse is always there to inspect
when a row surprises you.

``` r

g |>
  select(CIVIC_NO, STREET_NAME, STREET_TYPE, STREET_DIR, MUN_NAME, PROV_ABVN,
         pattern, parse_source)
#>   CIVIC_NO    STREET_NAME STREET_TYPE STREET_DIR         MUN_NAME PROV_ABVN       pattern
#> 1     1055        Georgia          ST          W        VANCOUVER        BC  civic_street
#> 2     1055        Georgia          ST          W        VANCOUVER        BC    unit_civic
#> 3      100          Queen          ST          W          TORONTO        ON  civic_street
#> 4      845      de Vernon         RUE       <NA>         GATINEAU        QC french_street
#> 5    34221 Range Road 272        <NA>       <NA>  RED DEER COUNTY        AB numbered_road
#> 6     5491       Route 11        <NA>       <NA>       BRANTVILLE        NB numbered_road
#> 7       29        Hocking         AVE       <NA> SAULT STE. MARIE        ON  civic_street
#> 8       NA      PO BOX 40        <NA>       <NA>          IQALUIT        NU        po_box
#>   parse_source
#> 1    gazetteer
#> 2    gazetteer
#> 3    gazetteer
#> 4    gazetteer
#> 5    gazetteer
#> 6    gazetteer
#> 7    gazetteer
#> 8        rules
```

`parse_source` says whether the row cleared NAR’s street gazetteer or
fell back to the rules alone, `confidence` carries the gazetteer’s score
where it applies, and `pattern` is the structural shape the string
parsed as. A `none` result is very often a parse worth reading rather
than a missing address — note row 8 above, where `po_box` says the input
was never going to resolve, because NAR holds no post office boxes.

### Two postal codes

The result carries two postal-code columns, and they answer different
questions. `POSTAL_CODE` comes from the parse — it is what the input
string said, and is empty for the addresses that were typed without one.
`match_postal_code` is what the *matched record* carries, filled in from
the database rather than from the input:

``` r

g |>
  mutate(input = substr(input, 1, 38)) |>
  select(input, match_method, POSTAL_CODE, match_postal_code)
#>                                    input     match_method POSTAL_CODE match_postal_code
#> 1        1055 W Georgia St, Vancouver BC     nar_building        <NA>            V6E0B6
#> 2 302-1055 west georgia street, vancouve     nar_building        <NA>            V6E0B6
#> 3    100 Queen St W, Toronto, ON M5H 2N2     nar_building      M5H2N2              <NA>
#> 4       845, rue de Vernon, Gatineau, QC     nar_building        <NA>            J9J3K4
#> 5 34221 Range Road 272, Red Deer County,     nar_building        <NA>            T4G0M4
#> 6          5491 Route 11, Brantville, NB nar_interpolated        <NA>            E9H2A8
#> 7   29 HPCKING AVE, SAULT STE. MARIE, ON     nar_building        <NA>            P6C2B8
#> 8                 PO Box 40, Iqaluit, NU             none        <NA>              <NA>
```

Only the tiers that resolve to a record can fill the second column: the
`nar` tier and the `rqa` tier. It then survives whichever tier ends up
placing the row, exactly as `ADDR_GUID` does — which is why row 6 above
is `nar_interpolated` and still carries a postal code. NAR held that
address, with no coordinates for it; the exact tier read the record and
interpolation supplied the position. A row interpolated with no such
hit, and every answer from an online service, reports nothing rather
than copying a neighbour’s: an interpolated point sits *between* two
addresses that need not share a postal code.

The other empty one is more interesting. Row 3 matched exactly, and NAR
carries `100 Queen St W, Toronto` twice — once as `M5H2N1` and once as
`M5H2N2`. NAR holds one row per address, so a civic number with units
contributes many rows, and about one civic number in seventy spans more
than one postal code — 4.2% of addresses, since the buildings this
happens to are large ones.
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
does not match on unit, so nothing in the query says which of those rows
was meant, and the column stays empty rather than reporting a coin flip.
The postal code in the input string does not break the tie either: that
is what the address claims, not something the lookup established.

You can also parse once and geocode repeatedly, which is useful when you
want to correct a parse before resolving it, or to try the same
addresses under different constraints without paying for the parse each
time.

``` r

con <- nar_connection()

norm <- normalize_address("29 HPCKING AVE, SAULT STE. MARIE, ON", con = con)
norm$STREET_NAME
#> [1] "Hocking"

geocode(norm)$match_method
#> [1] "nar_building"

DBI::dbDisconnect(con)
```

Normalization is a task in its own right, and
[`vignette("address-normalization")`](https://mountainmath.github.io/cangeocode/articles/address-normalization.md)
covers it properly: what the gazetteer fixes, why canonical forms depend
on the province, using
[`address_pattern()`](https://mountainmath.github.io/cangeocode/reference/address_pattern.md)
to separate addresses that are wrong from addresses that were never
going to resolve, and matching two lists of addresses to each other
without geocoding anything.

## Deciding which errors you would rather make

Every qualifier above tells you something about one answer. Turning them
into a policy — *these* answers I will use, *those* I will not — is a
separate decision, and not one the geocoder can make for you. Which
error you would rather make depends on what the points are for:
aggregating to a dissemination area and dispatching a vehicle to an
address want opposite mistakes.

The asymmetry is worth naming, because it drives everything else here. A
**false negative is visible**: the row comes back with `NA` coordinates,
and you can count it. A **false positive is invisible by construction**
— it looks exactly like a good answer, and it is 118 km away. So the
defaults stay conservative where an answer is decided and generous where
it is reported.

[`geocode_accept()`](https://mountainmath.github.io/cangeocode/reference/geocode_accept.md)
is where you draw your own line. It re-runs nothing; it reads the
columns the result already carries, withdraws the coordinates from the
rows that fail your bar, and records which test each one failed:

``` r

hard <- c(addresses,
          "551 Victoria Dr, Birch Grove, NS",
          "36 Lakeview Dr, Howie Centre, NS",
          "100 Main St")

g <- geocode(hard)

strict <- geocode_accept(g, attested_only = TRUE, unambiguous = TRUE,
                         max_uncertainty = 100)

table(strict$rejected_for, useNA = "no")
#> 
#>      ambiguous unattested_mun 
#>              2              1
```

Each test is off by default: `method`, `refused`, `attested_only`,
`unambiguous`, `postal_code`, `max_uncertainty` and `min_confidence`. A
row is charged to the first one it fails, in that order.

A rejected row keeps everything except its position. `ADDR_GUID`,
`match_method`, `uncertainty_m` and the parse all stay, so you can see
what was turned away and argue with it; only the coordinates go:

``` r

strict |>
  filter(!is.na(rejected_for)) |>
  select(input, match_method, uncertainty_m, n_matches, mun_evidence,
         rejected_for, lon)
#>                              input     match_method uncertainty_m n_matches mun_evidence
#> 1    5491 Route 11, Brantville, NB nar_interpolated       83.9406         2         kept
#> 2 36 Lakeview Dr, Howie Centre, NS     nar_building      118.0000         1   untestable
#> 3                      100 Main St     nar_building  4043775.8020       139         <NA>
#>     rejected_for lon
#> 1      ambiguous  NA
#> 2 unattested_mun  NA
#> 3      ambiguous  NA
```

Rows that were never placed are not rejections — `rejected_for` stays
`NA` there, so `table(rejected_for)` counts what your bar cost and
nothing else.

Because nothing is re-queried, moving the line is cheap, which matters:
nobody picks the right bar on the first try, and resolving forty
thousand addresses takes minutes.

``` r

sapply(c(25, 100, 250, Inf), function(u)
  mean(!is.na(geocode_accept(g, max_uncertainty = u)$lon)))
#> [1] 0.5454545 0.6363636 0.7272727 0.8181818
```

### Seeing the answers that were refused

The other direction is a match the gazetteer rejected. When it scores a
match below its threshold, the row comes back unresolved — and from the
outside that is indistinguishable from the street not existing. You get
no rejected answer, no score, and no evidence class. That is a false
negative with nothing to read.

``` r

refusable <- c("551 Victoria Dr, Birch Grove, NS",
               "16 Kelry Dr, East Dover, NS",
               "1741 Brunswick St, Halifax, NS")

geocode(refusable) |>
  select(input, STREET_NAME, MUN_NAME, match_method)
#>                              input STREET_NAME    MUN_NAME match_method
#> 1 551 Victoria Dr, Birch Grove, NS    VICTORIA BIRCH GROVE         none
#> 2      16 Kelry Dr, East Dover, NS       KELRY  EAST DOVER         none
#> 3   1741 Brunswick St, Halifax, NS   Brunswick     HALIFAX nar_building
```

`keep_refused = TRUE` reports them instead:

``` r

geocode(refusable, keep_refused = TRUE) |>
  select(input, STREET_NAME, MUN_NAME, confidence, mun_evidence, refused_for,
         match_method, uncertainty_m)
#>                              input STREET_NAME MUN_NAME confidence mun_evidence refused_for
#> 1 551 Victoria Dr, Birch Grove, NS    Victoria   SYDNEY      0.792   unattested    mun_swap
#> 2      16 Kelry Dr, East Dover, NS       Kelly  HALIFAX      0.833     copostal       score
#> 3   1741 Brunswick St, Halifax, NS   Brunswick  HALIFAX      1.000         kept        <NA>
#>       match_method uncertainty_m
#> 1 nar_interpolated      341.0394
#> 2     nar_building        0.0000
#> 3     nar_building        0.0000
```

Both were real answers all along. `refused_for` names the gate each one
failed:

- `"mun_swap"` — the score cleared the threshold *before* the
  municipality-swap penalty and not after. The street matched and the
  municipality did not, which is exactly the case where you may know
  something the register does not: Birch Grove and Sydney are both in
  Cape Breton Regional Municipality, and there is no `Victoria Dr` filed
  under Birch Grove.
- `"score"` — everything else. `Kelry` is one keystroke from the
  `Kelly Dr` the register carries, and the near-miss on the name is what
  put the combined score at 0.833 rather than over 0.85.

Note what `uncertainty_m` does *not* do here: the second row is an exact
building match and reports 0 m, because that column describes the error
the **method** introduces and knows nothing about whether the street was
the right street. The refusal is the warning; the metres are not.

So the natural pairing is one pass with them and one without —
`geocode_accept(refused = FALSE)` takes them back out again, and the
difference is what the threshold is buying you:

``` r

r <- geocode(hard, keep_refused = TRUE)

c(refused_kept    = mean(!is.na(r$lon)),
  refused_dropped = mean(!is.na(geocode_accept(r, refused = FALSE)$lon)))
#>    refused_kept refused_dropped 
#>       0.9090909       0.8181818
```

One limit: only matches that cleared the *name* similarity gate can be
reported this way. That gate is applied inside the database query, so a
street name too far from every candidate never comes back at all — which
is deliberate, since it is the gate that stops a matching street type
from carrying an unrelated street over the line.

### How to actually use this

The pattern that works is **geocode once with everything on, then split
three ways**. Resolve with `keep_refused = TRUE` — it costs nothing
extra and can only add rows — and let
[`geocode_accept()`](https://mountainmath.github.io/cangeocode/reference/geocode_accept.md)
do the deciding afterwards, as many times as it takes:

``` r

r <- geocode(hard, keep_refused = TRUE)

a <- geocode_accept(r, refused = FALSE, attested_only = TRUE,
                    unambiguous = TRUE, max_uncertainty = 100)

a |>
  mutate(outcome = case_when(!is.na(lon)          ~ "use",
                             !is.na(rejected_for) ~ "review",
                             TRUE                 ~ "not placed")) |>
  count(outcome, rejected_for)
#>      outcome   rejected_for n
#> 1 not placed           <NA> 1
#> 2     review      ambiguous 2
#> 3     review        refused 1
#> 4     review unattested_mun 1
#> 5        use           <NA> 6
```

Three outcomes, not two, and the middle one is the reason to bother:

- **use** — cleared your bar. Coordinates present.
- **review** — an answer exists and you turned it down. `rejected_for`
  says why, and every column the decision rested on is still there. This
  is the pile worth hand-checking, and it is usually small.
- **not placed** — nothing was ever found. No amount of loosening the
  bar helps; these need a better address string, a different tier, or
  nothing.

Filtering with `filter(!is.na(lon))` collapses the last two together and
throws away the distinction, which is the one piece of information you
cannot recover afterwards.

### Choosing a bar

There is no universally right setting, but the task usually decides it:

| what you are doing | a reasonable starting bar |
|----|----|
| aggregating to a census geography — DA, CSD, health region | `attested_only = TRUE`, and nothing else. At that scale 100 m of positional error is invisible and a 118 km municipality swap is not. |
| mapping points, neighbourhood scale | `attested_only = TRUE, unambiguous = TRUE, max_uncertainty = 100` |
| anything address-exact — dispatch, service eligibility, joining to a parcel | add `method = "nar_building"`. Nothing interpolated, nothing online, nothing that resolved to a street rather than a building. |
| maximum recall, hand-checked after | `keep_refused = TRUE` and no bar at all. Sort by `confidence` and work down; `refused_for == "mun_swap"` is where local knowledge pays. |
| matching two address lists to each other | don’t geocode at all — [`vignette("address-normalization")`](https://mountainmath.github.io/cangeocode/articles/address-normalization.md) does this without a coordinate. |

Three things to keep in mind while tuning:

- **`attested_only` is the one to reach for first.** It is the cheapest
  test by a wide margin: in a 40,000-address Nova Scotia sample the
  unverified evidence classes are 2.3% of the rows and roughly a third
  of everything landing more than five kilometres out. The other tests
  trade far more recall per error removed.
- **A row is charged only to the *first* test it fails.** So the counts
  in `table(rejected_for)` are not what each test costs on its own — run
  them one at a time if you need to attribute the loss.
- **Don’t use `min_confidence` as a stand-in for positional accuracy.**
  It scores how well the string matched a street name, not where the
  point landed. The Kelry row above scores 0.833 and sits on an exact
  building point; the Howie Centre row scores 0.900 and is 388 km out.

Finally, report what the bar cost.
`mean(is.na(a$lon)) - mean(is.na(r$lon))` is the share of addresses you
gave up, and it belongs next to whatever the coordinates were used for —
a study that geocodes 94% of its subjects and one that geocodes 71% of
them are not describing the same population.

## Geometry

`geometry = TRUE` returns an `sf` object instead of `lon`/`lat` columns,
with an empty point for the rows that did not resolve. `crs` picks the
CRS — EPSG:4326 by default, or `NULL` to keep NAR’s own projected
storage CRS, in which case distances come out in metres.

``` r

pts <- geocode(addresses[1:4], geometry = TRUE)

sf::st_geometry(pts)
#> Geometry set for 4 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: -123.1214 ymin: 43.6515 xmax: -75.81013 ymax: 49.28529
#> Geodetic CRS:  WGS 84
#> POINT (-123.1214 49.28529)
#> POINT (-123.1214 49.28529)
#> POINT (-79.3825 43.6515)
#> POINT (-75.81013 45.45202)
```

## Checking an answer

[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
runs the other direction, which makes it a cheap sanity check on a
coordinate you just derived.

``` r

geocode("100 Queen St W, Toronto, ON", geometry = TRUE) |>
  reverse_geocode(match_radius = 50) |>
  select(address, dist) |>
  head(3)
#> # A tibble: 3 × 2
#>   address                            dist
#>   <chr>                             <dbl>
#> 1 100 W QUEEN ST, TORONTO M5H2N1      0  
#> 2 10-100 W QUEEN ST, TORONTO M5H2N2   0  
#> 3 700-65 W QUEEN ST, TORONTO M5H2M5  32.1
```

For British Columbia there is a stronger check available: the Province
of BC publishes its own [Address
Geocoder](https://geocoder.api.gov.bc.ca/), a parcel-level provincial
authority.
[`bc_validate()`](https://mountainmath.github.io/cangeocode/reference/bc_validate.md)
geocodes a result again through that service and reports the distance
between the two answers. Where they disagree, BC’s answer is generally
the more reliable of the two — but the two sources are not independent
of each other, so a small distance is weaker evidence than it looks, and
the distances are a way to find suspect rows rather than a measurement
of how accurate NAR is.

**This is the one path in the package that sends an address off your
machine**, and nothing reaches it unless you call one of these
functions.

``` r

bc <- geocode(c("525 Superior St, Victoria, BC",
                "800 Robson St, Vancouver, BC",
                "3800 Finnerty Rd, Victoria, BC"))

bc_validate(bc) |>
  mutate(bc_dist_m = round(bc_dist_m, 1)) |>
  select(input, match_method, bc_match_method, bc_score, bc_dist_m)
#>                            input     match_method bc_match_method bc_score bc_dist_m
#> 1  525 Superior St, Victoria, BC     nar_building        bc_civic      100       6.9
#> 2   800 Robson St, Vancouver, BC nar_interpolated        bc_civic      100     104.1
#> 3 3800 Finnerty Rd, Victoria, BC     nar_building        bc_civic       96     492.8
```

Read the distances as **disagreement, not error**. The two sources
define their points differently — NAR’s may be the driveway, BC’s is a
parcel point — so a gap contains both sources’ error plus that
definitional difference. Over 250 BC addresses the median disagreement
on the `nar_building` tier is about 20 m, which is the number to keep in
mind when reading a `uncertainty_m` of 0. The third row above is the
University of Victoria, where “the address” is a kilometre-wide campus
and the two services have picked different sensible points on it — a
large disagreement that is nobody’s mistake.

The same service can also be used as a *tier*, for the BC addresses NAR
cannot place at all:

``` r

hard <- c("2912 West Broadway, Vancouver, BC",
          "1 Nesters Rd, Whistler, BC",
          "7165 Nakiska Dr, Vernon, BC")

geocode(hard)$match_method
#> [1] "none" "none" "none"

geocode(hard, method = c("nar", "nar_interpolate", "bc")) |>
  select(input, match_method, uncertainty_m)
#>                               input match_method uncertainty_m
#> 1 2912 West Broadway, Vancouver, BC     bc_civic            20
#> 2        1 Nesters Rd, Whistler, BC    bc_street           500
#> 3       7165 Nakiska Dr, Vernon, BC     bc_civic            20
```

On a sample of 600 BC addresses that NAR’s own pathway placed 524 of,
the tier resolved 75 of the remaining 76 — 31 of them at address level,
the rest to a block or a street.

A response from that service is not by itself a match, because **it
always answers**. Ask it for an address that does not exist and it
returns the centre of the nearest town, not an error:

``` r

bc_geocode("1234 Nonexistentzzz Rd, Victoria, BC") |>
  select(match_method, bc_score, bc_precision, bc_address)
#>   match_method bc_score bc_precision   bc_address
#> 1         none       48     LOCALITY Victoria, BC
```

Two independent floors apply: the service’s own precision vocabulary has
to name a usable match, and `bc_score` has to clear `min_score`, which
defaults to 60. Here it is the score that rejects it — a `LOCALITY`
answer is a legitimate result when the service is confident, it is just
not an address. Either way a rejected row keeps its score and its
`bc_faults`, so what was thrown away stays readable rather than
vanishing. See
[`?bc_geocode`](https://mountainmath.github.io/cangeocode/reference/bc_geocode.md).

## Quebec’s own register

Quebec publishes its address register, the *Répertoire québécois des
adresses*, in full — and it carries about 750,000 certified addresses
more than NAR’s Quebec extract does.
[`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md)
loads it into the same database, in tables of its own:

``` r

rqa_import()   # a ~780 MB download, once
```

Once imported, `"rqa"` is available as a tier. It is offline, it only
ever looks at rows that parsed to Quebec, and it belongs **before**
interpolation:

``` r

geocode(addresses, method = c("nar", "rqa", "nar_interpolate"))
```

On 4,000 Corporations Canada filings with a Quebec address, that takes
coverage from 88.5% to 90.1% — but the number to look at is a different
one. The share placed on a *register* point rather than an interpolated
one goes from 82.7% to **89.1%**. Of the 258 filings the tier places,
only 62 were unplaced before; the other 196 were being interpolated
between two neighbours and now carry the register’s own coordinate, a
median of 26 m away. It costs nothing measurable, because the tier only
ever sees the rows NAR left behind.

It is not in the default `method`, because the tables only exist if you
ran the import — and
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
will tell you so, up front, rather than when the tier is first reached.

`match_method` reports the register’s own positional class, not one flat
label:

``` r

geocode(c("3190 Boulevard Laurier Est, Saint-Hyacinthe QC",
          "1255 Rue Peel, Montreal QC"),
        method = c("nar", "rqa", "nar_interpolate")) |>
  select(match_method, uncertainty_m, lon, lat)
#>   match_method uncertainty_m       lon      lat
#> 1 rqa_geocoded            NA -72.91517 45.63606
#> 2 nar_building             0 -73.57239 45.49992
```

`rqa_building` means a building point, and gets `uncertainty_m = 0` for
the same reason `nar_building` does. `rqa_geocoded`, `rqa_uncertain` and
`rqa_lot` get `NA` — the register places 30% of these rows by a method
it labels *incertaine* itself, and nothing here has measured what that
is worth on the ground. The class is reported so you can filter on it.

One condition: the register is **CC-BY 4.0**, where NAR is an open
government licence. Anything published from these points has to carry
the attribution, which
[`rqa_attribution()`](https://mountainmath.github.io/cangeocode/reference/rqa_attribution.md)
returns.

## The road network file

The single largest thing left unplaced is an address whose street is not
in NAR at all — a subdivision built since the last release, a road NAR
never carried. Statistics Canada’s **Road Network File** has every
street in the country, with an address range on each side of each
segment, and
[`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md)
loads it into the same database in tables of its own:

``` r

rnf_import()   # a ~340 MB download, once
```

Once imported, `"rnf"` is available as a tier. It is offline and
national, and it belongs **after** interpolation — a point placed
between two known civic numbers beats one placed along a range:

``` r

geocode(addresses, method = c("nar", "nar_interpolate", "rnf"))
```

On the 5,000 filings above that takes coverage from 92.4% to **94.3%**:
93 of the 379 the offline pair gives up on, a quarter of the residual
and the largest recovery any tier here offers.

``` r

new_streets <- c("1435 Celebration Dr, Pickering, ON L1W 0C4",
                 "192037A TWP RD 665, Athabasca County, AB T0A 0M0",
                 "1545 Maley Dr, Sudbury, ON P3A 4R7")

geocode(new_streets, method = c("nar", "nar_interpolate"))$match_method
#> [1] "none" "none" "none"

geocode(new_streets, method = c("nar", "nar_interpolate", "rnf")) |>
  select(match_method, n_matches, uncertainty_m, lon, lat)
#>       match_method n_matches uncertainty_m        lon      lat
#> 1 rnf_interpolated         1       95.0000  -79.07905 43.83198
#> 2 rnf_interpolated         1      413.7086 -112.78776 54.74719
#> 3    rnf_ambiguous         3            NA         NA       NA
```

The third row is the tier **refusing**. When more than one segment of
that name in that municipality has a range containing the number, there
is no way to tell which one was meant, so `match_method` is
`rnf_ambiguous`, `n_matches` says how many were in contention, and no
coordinate comes back. That is where this file’s gross errors live —
ambiguous rows run a 90th percentile of 1.7 km against 108 m for the
rest — and the refusal costs 7 rows in 5,000.

It is a coarse tier and says so. `uncertainty_m` is
`max(95, 0.35 × len_m)` and is never 0: the position is a fraction along
a centreline, offset 13 m to the correct side, not a building. Against
200,000 NAR building points it lands a median 24.3 m away — about six
times worse than `nar_interpolate`, which is why it sits below it — with
a 90th percentile of 93.3 m. Filter on `uncertainty_m` rather than
treating every row alike; the second row above is a township road long
enough that the tier admits to 414 m.

One caution the file itself cannot give you: its ranges carry **no
provenance flag**, so an observed range and one the file imputed are the
same bytes. Everything quoted here comes from measuring the file against
NAR, which is also why the tier is worth less than the overlap suggests
— checked against the filer’s own postal code, rows the tier recovers
sit a median 149 m from their urban FSA centroid against 60 m for rows
NAR also placed. The addresses NAR cannot place are genuinely harder.

## The national geolocator

`"nrcan"` is the other online tier, backed by NRCan’s
[geolocator](https://geolocator.api.geo.ca/). Unlike `"bc"` it covers
the whole country, and it needs no local database at all — which makes
it the only tier that can answer before a NAR release has been imported,
and the one that covers provinces a single-province import does not
hold.

``` r

geocode(addresses, method = c("nar", "nar_interpolate", "nrcan"))
```

It has the same trap as the BC service, in a sharper form: **it always
answers, and it gives you no score to disbelieve.** Asked for
`1 Rue Notre-Dame Ouest, Montreal, QC` its top-ranked answer is a real,
precisely interpolated position on a real Rue Notre-Dame Ouest — in
Lorrainville, 500 km away — with nothing in the response marking it as a
substitution.

So the tier re-parses the address the service hands back and requires it
to agree, component by component, with the one that was sent: same
street name, same civic number, and no contradiction on type, direction,
municipality or province. It applies that test to **every** result in
the response rather than only the top-ranked one, which costs nothing —
the service returns up to 25 in a single reply — and matters more often
than you would guess. The Montréal address above is in that same reply,
ranked seventh, and it is the one the tier returns. Where two survive,
`n_matches` says so.

Roughly half of what is asked is still rejected, and a rejected row
keeps a `nrcan_reject` column naming the component that disagreed, so
what was thrown away stays readable. One value in that column is not
about the address at all: the service drops roughly one request in
twelve with an HTTP 500, so
[`nrcan_geocode()`](https://mountainmath.github.io/cangeocode/reference/nrcan_geocode.md)
re-sends them (`retries`, default 3) and reports the ones that never
came back as `request failed` rather than as an address it had no answer
for. Of the answers that survive, the median sits 33 m from NAR’s own
building point and the 90th percentile at 115 m, which is what
`uncertainty_m = 150` reports.

Two caveats worth having before you reach for it. As a fallback for
NAR’s own gaps it is worth much less than the BC tier — it places about
8% of what NAR leaves unplaced, because the addresses NAR lacks are
largely the ones no national compilation has. And it does not reverse
geocode;
[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
is NAR-backed and runs locally. See
[`?nrcan_geocode`](https://mountainmath.github.io/cangeocode/reference/nrcan_geocode.md).

## OpenStreetMap, and why it is not a tier

There is a third online service bound in this package, and it is
deliberately not something `method` can name.
[`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
queries the [Nominatim](https://maps.canada.ca/nominatim/search)
instance Natural Resources Canada hosts — not the volunteer-funded
`nominatim.openstreetmap.org`, whose usage policy forbids bulk
geocoding.

``` r

osm_geocode("990 Bute St, Vancouver, BC")
```

The reason it is a separate call rather than a tier is the licence, not
the accuracy. OpenStreetMap data is ODbL: attribution plus share-alike,
with obligations that attach to a derived database. Every other source
here is under an attribution-only open licence, and a default tier would
fold a handful of ODbL rows into your result table and quietly change
what you may do with the whole of it. So the choice is yours to make,
and every row carries the service’s own `osm_licence` string.

It behaves unlike the other two in one welcome respect: **it refuses.**
Given an address it does not have, it returns nothing rather than a
plausible substitution, and where it knows the street but not the civic
number it says so rather than offering a point. What is not yet known is
its coverage — OpenStreetMap’s Canadian addresses are concentrated in
cities with municipal open-data imports, and no national comparison has
been run, which is why an `osm` row reports `uncertainty_m` as `NA`
rather than a number someone made up. See
[`?osm_geocode`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md).

## Where this falls short

Measured on 5,000 Corporations Canada registered offices,
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
places 92.4% with the default methods, and 94.3% with `"rnf"` appended.
The ceiling for a NAR-only pathway was put at around 93% and is
essentially met, so what headroom is left is in the tiers that reach
outside NAR rather than in NAR itself.

The residual decomposes roughly as: 3.7% whose street is not in NAR
anywhere in the province, 3.8% where the street exists but the civic
number could not be reached even by interpolation, 1.4% that never
parsed, and a remainder where the street exists under a municipality
that did not match. Read the ranking rather than the shares — the parser
and the road network file both eat into it.

The notes that ship with the package carry the measurements behind every
figure quoted here, along with what is not built yet:

``` r

file.show(system.file("notes", "geocoding-status.md", package = "cangeocode"))
file.show(system.file("notes", "road-network-file.md", package = "cangeocode"))
file.show(system.file("notes", "address-normalization-status.md",
                      package = "cangeocode"))
```
