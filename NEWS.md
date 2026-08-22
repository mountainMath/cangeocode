# cangeocode 0.2.0

The package went one direction only in 0.1.0: coordinates to addresses. This
release adds the other direction, and with it a second thing the package now
does in its own right — **normalizing** free-text addresses into structured
components, which is what most address work actually needs. There is also one
external geocoder to check the results against.

**Rebuild your database.** The import schema is now version 6. Existing
databases keep working for reverse geocoding, but address normalization and
`geocode()`'s `mun` argument both need the new gazetteer tables:

``` r
nar_connection(refresh = TRUE)
```

## Downloading only the provinces you need

* `nar_connection()` gained a **`provinces`** argument. The StatCan release is
  one 1.7 GB zip whose members are split by province, and the server honours
  HTTP range requests, so the package can read the archive's own index for a
  few kilobytes and then fetch only the members a province needs.
  `nar_connection(provinces = "PE")` is 10 MB and about 40 seconds for a
  working Prince Edward Island geocoder; British Columbia is 192 MB, Ontario
  552 MB, the country 1,666 MB.

* The addresses are the same NAR rows either way, so a partial database
  geocodes its own provinces **exactly as well** as a national one does — same
  `ADDR_GUID`, same coordinates. It simply holds nothing outside them.

* Coverage is recorded in the database and checked before anything is
  downloaded. A national database satisfies every request; asking for a
  province a partial database lacks **adds** just that province rather than
  rebuilding; and `refresh = TRUE` rebuilds the coverage a database already
  has rather than silently widening or narrowing it. New `nar_provinces()`
  reports what a connection holds.

* In an interactive session, a first call that names no provinces now asks,
  showing what each choice actually costs in megabytes. Non-interactively it
  downloads the whole country, as before.

* `geocode()` answers **`match_method = "not_covered"`** for an address that
  parsed to a province the database does not hold. That is deliberately
  distinct from `none`: the address may be perfectly good, and only a partial
  import ever produces it.

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
  street name from 98.8% and resolves 86.5% to an address NAR actually holds.

* Supplying `con` resolves the parse against NAR's streets, which corrects
  misspellings no rule could reach — `29 HPCKING AVE, SAULT STE. MARIE` comes
  back as `Hocking` — and restores NAR's own spelling, accents and periods
  included. `parse_source` reports which rows cleared the gazetteer and which
  are the parser's unconfirmed reading.

* The gazetteer **answers with a municipality when NAR determines one** — that
  is, when exactly one municipality in the country carries a street of that
  name. Where two or more do, `MUN_NAME` stays `NA` rather than naming the
  largest, because that would be a guess and a wrong municipality joins two
  different buildings. Together with a name match that now recognises a
  single-keystroke typo and a word the parser swallowed (`772` for
  `Route 772`), this recovers 215 more fields per 5,000 rendered NAR addresses
  and loses none.

* Canonicalization is **conditioned on the province**, because there is no
  single right abbreviation in Canada: NAR writes `AVE` in Ontario and `AV` in
  Quebec, `W` against `O`. `prov` therefore chooses a vocabulary rather than
  offering a hint.

* New `address_pattern()` sorts a parse into one of twelve shapes. Two of them,
  `po_box` and `rural_route`, exist to say *this will never resolve*: NAR
  contains neither, so they separate "this address is wrong" from "this address
  was never going to be in the gazetteer".

* New `address_key()` collapses a parse into a single string that two spellings
  of the same address share, which is what a join or a deduplication needs.
  Components are folded past case, accents and the punctuation NAR and the
  parser disagree on, so `St. John's` and `SAINT JOHNS` key alike. The unit is
  left out by default, keying a building rather than a tenant; `unit = TRUE`
  keys the tenant. A row with no street name keys to `NA` rather than to an
  empty string, so unparseable rows cannot all join to each other.

* New `format_address()` writes the components back out as one readable line,
  with the unit hyphenated onto the civic number and the postal code spaced.
  The street type is placed by language rather than by province, so a `Rue` in
  Ottawa still reads correctly. Output parses back to the same `address_key()`,
  so a cleaned column still joins to the column it was cleaned from.

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
