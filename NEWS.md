# cangeocode 0.2.0

The package went one direction only in 0.1.0: coordinates to addresses. This
release adds the other direction, and with it a second thing the package now
does in its own right — **normalizing** free-text addresses into structured
components, which is what most address work actually needs. There is also one
external geocoder to check the results against.

**Rebuild your database.** The import schema is now version 5. Existing
databases keep working for reverse geocoding, but address normalization and
`geocode()`'s `mun` argument both need the new gazetteer tables:

``` r
nar_connection(refresh = TRUE)
```

## Forward geocoding

* New `geocode()` turns free-text Canadian addresses into coordinates, one row
  per input in input order, carrying the parsed components alongside the
  result. On 5,000 Corporations Canada addresses it places 89.1%, taking 0.9s
  for the whole batch — so batch rather than loop, since the street-name join
  costs about as much for 5 addresses as for 200.

* Every result says **how** it was found, in `match_method`, and what that
  method costs, in `uncertainty_m`. The latter is the 90th-percentile error the
  package adds relative to NAR's own point: `0` for a building match, 176 m for
  a blockface one, half the flanking span for an interpolated one. Both
  non-zero figures are measured rather than assumed.

  `uncertainty_m` says nothing about NAR's *own* error, which is a separate
  quantity — `0` means "this package added nothing", not "this point is exact".

* Addresses NAR does not carry are placed by interpolation between their
  neighbours, using **only civics of the same parity** (median error 4.2 m
  against 35.2 m for both sides pooled, by leave-one-out over all 10.6M NAR
  civic points). Interpolation **refuses to extrapolate** past the last known
  civic on a side rather than returning a number that looks like the others.

* `prov`, `mun` and `within` constrain the search and are **authoritative**:
  they override whatever the address string said, and the override lands on the
  returned row too. `mun` resolves through NAR's alias set, so `"Toronto"`
  reaches the addresses NAR files under `SCARBOROUGH`.

* `method` names the tiers to try and the order to try them in — any of
  `"nar"`, `"nar_interpolate"` and `"bc"`. Each tier is offered only the rows
  its predecessors left without a position, so the order is the priority. The
  default `c("nar", "nar_interpolate")` is the offline pair.

## Address normalization

This is a step inside `geocode()` and also an end in its own right: matching
two address lists to each other needs the parse and never needs a coordinate.

* New `normalize_address()` parses address strings into the components NAR is
  keyed on, by rules first and then against a NAR gazetteer. New `Streets`,
  `MunAlias` and `PostalMun` tables are built at import time to support it.
  Measured on 5,000 real filings nobody cleaned, it extracts a civic number and
  street name from 98.8% and resolves 86.0% to an address NAR actually holds.

* Supplying `con` resolves the parse against NAR's streets, which corrects
  misspellings no rule could reach — `29 HPCKING AVE, SAULT STE. MARIE` comes
  back as `Hocking` — and restores NAR's own spelling, accents and periods
  included. `parse_source` reports which rows cleared the gazetteer and which
  are the parser's unconfirmed reading.

* Canonicalization is **conditioned on the province**, because there is no
  single right abbreviation in Canada: NAR writes `AVE` in Ontario and `AV` in
  Quebec, `W` against `O`. `prov` therefore chooses a vocabulary rather than
  offering a hint.

* New `address_pattern()` sorts a parse into one of twelve shapes. Two of them,
  `po_box` and `rural_route`, exist to say *this will never resolve*: NAR
  contains neither, so they separate "this address is wrong" from "this address
  was never going to be in the gazetteer".

## British Columbia

* New `bc_geocode()` binds the Province of BC's public Address Geocoder. No API
  key required, and BC only.

* New `bc_validate()` compares an existing `geocode()` result against BC's
  answer in metres. This is the only independent positional source currently
  wired up, and it gives the first read on NAR's own error: a median of 19.8 m
  between a `nar_building` point and BC's parcel point over 224 addresses.

* `geocode(method = c("nar", "nar_interpolate", "bc"))` adds the service as a
  last-resort tier. On 600 BC addresses the NAR pathway gave up on 76 and the
  BC tier resolved 75 of them, 31 at address level.

* **A response from this service is not a match.** It always answers — garbage
  input returns the centre of the locality with a low score rather than an
  error — so `match_method` is derived from its precision vocabulary and
  `min_score` rejects what it scored badly. Rejected rows keep their `bc_score`
  and `bc_faults`.

  The `bc_*` `uncertainty_m` figures are the one set of numbers in this package
  that were chosen rather than measured; BC publishes only a categorical
  accuracy. Treat them as a ranking safe to filter on.

* `httr2` is in `Suggests`, and nothing contacts the network unless one of
  these is called by name.

## Documentation

* Two longer notes ship with the package and record what does *not* work yet,
  with the measurements behind each claim:
  `system.file("notes", "geocoding-status.md", package = "cangeocode")` and
  `system.file("notes", "address-normalization-status.md", package = "cangeocode")`.

* New `vignette("geocoding")` and `vignette("address-normalization")`, one for
  each of the two things the package does.

# cangeocode 0.1.0

Initial development version: `reverse_geocode()`, the NAR import into DuckDB,
and `collect_nar()` for getting query results back as `sf`.
