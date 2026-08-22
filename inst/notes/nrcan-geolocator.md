# What NRCan's geolocator actually does

`nrcan_geocode()` and the `"nrcan"` tier query
`https://geolocator.api.geo.ca/geolocation/en/locate?q=`, and everything the package believes
about that service was, until now, inferred from its answers. **The service is open source**,
at
[`Canadian-Geospatial-Platform/geoview-api-geolocator`](https://github.com/Canadian-Geospatial-Platform/geoview-api-geolocator),
and this note records what reading it settled.

For what the tier is *worth* — coverage, error distributions, why `uncertainty_m` is 150 — see
the NRCan section of [`geocoding-status.md`](geocoding-status.md). For why
`R/geocode_nrcan.R` is shaped the way it is, see `.claude/geocoding.md`. This note is about the
thing on the other end of the wire.

**Read at upstream commit `e1aed67` (2026-02-25), verified live 2026-08-22.** The service is
under active development and was rewritten wholesale some time before Dec 2025; anything below
that is not marked *verified live* is read from source and may drift.

## The repository is the service, and the file names mislead

`backend/api-geolocation-mock/` is not a mock. Despite the directory name and the module
docstring ("Lambda that mimics the old Geolocation API"), it is what answers `locate` in
production. What settles it: the official client passes `expand=component` as a static
parameter (`backend/geolocator-bucket-content/services/locate-schema.json`), the code in that
directory ignores the parameter entirely, and so does the live service — the response to
`?q=1155+Robson+St,+Vancouver,+BC&expand=component` is byte-identical to the response without
it. Every rejection case documented in `R/geocode_nrcan.R` also reproduces against it exactly.

`expand=component` is worth naming because it is the one thing that would change the design
here. In the retired `geogratis` service it returned a *structured* breakdown of the answer —
street name, number, municipality, province as separate fields — which is precisely what
`nar_nrcan_agreement()` reconstructs by re-parsing the title. It is dead. The parameter is
still in the schema file only because nothing removed it.

There are three other backends in the repo. `backend/api-lambda/` is the aggregator behind
`https://geolocator.api.geo.ca/?q=&keys=`, which fans out to `locate` plus four other sources;
`api-forward-sortation-area` and `api-nts-grid` are two of those sources. The package talks to
`locate` directly, which is the right call — the aggregator imposes a **3-second timeout** on
each upstream (`url_methods.py`, `url_request`) and silently substitutes a
`{'key': 'unsuccess'}` record for anything slower.

## `locate` is a fuzzy text match over one string

This is the finding that matters most, because it explains the failure mode the whole floor
exists to catch.

The index (`backend/jupyter-notebooks/geolocation-indexing.ipynb`) holds one OpenSearch
document per feature, and the only searchable field is **`title`** — a flat string built as
`", ".join([street, place, province])`, e.g. `Robson Street, Vancouver, British Columbia`. The
query (`lambda_function.py`) is a single `multi_match` over `title^2.75` and `type` with:

```
"type": "best_fields", "operator": "OR", "minimum_should_match": 1,
"fuzziness": "AUTO" if len(q) <= 10 else 0
```

`operator: OR` with `minimum_should_match: 1` means **one token matching anywhere in the
title is enough to return the document**. There is no structured parse of the query, no
municipality constraint, no notion that `Montreal` is a locality rather than another word in
the string. `function_score` then reweights by feature type — Street 2.2, Intersection 2.15,
Geoname 1.8 with per-feature-class multipliers — and 25 results come back.

Three consequences the package already assumes, now with a mechanism:

* **There is no confidence field because there is nothing to compute one from.** The service
  publishes no score, and the relevance score it does have internally is a text-match score
  against a concatenated string. A score, had it been published, would not have separated
  `330 Spadina Avenue` from `330 Spadina Road`.
* **A response is not a match.** Asking for an address it does not hold returns whatever
  scored highest on partial token overlap. `1155 Robson **Ave**, Vancouver` returns Robson
  **Street** — the street type contributes nothing to the match.
* **Re-parsing the returned title is the only floor available**, and comparing per component
  rather than as a string is not fussiness. It is the only place the query's structure re-enters.

Note also that `fuzziness` is disabled for any query over 10 characters. Every address the
package sends is longer than that, so no typo tolerance is in play for this caller; the
matching is exact-token, and its looseness comes entirely from `OR`.

## Interpolation, and what `INTERPOLATED_POSITION` certifies

When the top-level search returns a `Street` document, the handler tries to place the civic
number on it (`address_interpolator.py`):

1. `extract_house_number_from_query` takes the **first** `\b\d{1,5}\b` in the query string.
2. `find_address_ranges_for_street` looks up ranges on that `street_id` filtered to
   `min_house_number <= n <= max_house_number`.
3. `interpolate_from_range_v2` takes `ratio = (n - min) / (max - min)`, flips it if
   `digitizing_direction == 33`, clamps to `[0, 1]`, and walks that fraction of the segment's
   haversine length along the linestring.
4. The returned title is fabricated as `f"{house_number} {orig_title}"` and the qualifier is
   set to `INTERPOLATED_POSITION`.

**So `INTERPOLATED_POSITION` certifies exactly one thing: the number fell inside a real
address range on the street the text match selected.** That is a genuine signal and it is the
reason the qualifier floor works.

**It also means the civic-number comparison in `nar_nrcan_agreement()` is vacuous.** The
number in the title is the number the package sent — step 4 pastes it back verbatim — so
`CIVIC_NO` can never disagree for a row that got this far. The check is harmless and worth
keeping as a guard against a future response shape, but it verifies nothing today, and the
roxygen crediting it as one of the two must-be-present-and-equal components overstates it.
The components doing real work are the street name, the municipality, the street type and the
direction.

Two hard limits fall straight out of the regex in step 1, both verified:

* **A civic suffix defeated the tier entirely, and this is now fixed.** `\b\d{1,5}\b` does
  not match `990A` — there is no word boundary between `0` and `A` — and finding no number
  anywhere in the string, the handler skips interpolation and returns the street centroid,
  which the floor then rejects. `nar_address_string()` glues the suffix to the number
  deliberately (`R/geocode_bc.R`), which is right for BC and was structurally fatal here.

  `nar_address_string()` now takes `suffix =`, and `nrcan_geocode()` passes `FALSE`. Measured
  over 20 suffixed NAR building points sampled nationally: **0 of 20 placed with the suffix,
  16 of 20 without**, median error 32 m and max 153 m. The rejected 20 come back as
  `Geoname/LOCATION` (13) or a street centroid (7) — the service does not degrade gracefully,
  it answers a different question. Nothing is laundered by the strip: `CIVIC_NO` never carried
  the suffix, so the floor compares exactly what it did before, and the returned `input` column
  still echoes the address as it was given. Roughly 189k NAR building points carry a suffix, so
  this is ~1% of addresses going from never-placed to usually-placed rather than a shift in the
  headline rate.
* **A civic number of six or more digits is invisible** for the same reason: `123456` matches
  neither `\b\d{1,5}\b` at any offset.

### For the road network file tier

[`geocoding-status.md`](geocoding-status.md) sizes an RNF interpolation tier but does not build
it. `address_interpolator.py` is a working reference for that tier, and its shortcomings are
the more useful half of it:

* **No side-of-street offset and no end setback.** The point lands on the road centreline, and
  a house at the start of a range lands exactly on the intersection node. That is most of the
  33 m median measured against NAR building points — it is a systematic bias, not scatter, and
  a tier that applies a perpendicular offset and a setback should beat it on the same data.
* **No parity handling.** `find_address_ranges_for_street` filters on the numeric range only,
  and takes the **first** hit out of an unsorted `size: 100` result set. Where left and right
  sides are separate range documents with overlapping numbers, the side is chosen arbitrarily.
  `numbering_method` — the odd/even/mixed flag — is indexed in the address-range mapping and
  never read.
* **`digitizing_direction` is 32/33 and the repo does not agree with itself about which way
  round.** `interpolate_from_range` flips the ratio on `32`; `interpolate_from_range_v2` flips
  on `33` and carries a comment asserting `32 = DO NOT FLIP`. Only `_v2` is called. Do not
  take the constant on faith — validate it against known addresses at both ends of a range.

The index is built from five extracts, named in the indexing notebook:
`road_segment_centroide`, `road_segment_intersection`, `toponym`, `nts_all_scales` and
`postal_code_centroid`, with the address ranges in a second index keyed by `street_id` and
carrying `bdg_id`, `zt_id`, `min_house_number`, `max_house_number`, `digitizing_direction`,
`numbering_method` and the segment geometry. `INTERPOLATED_CENTROID` on a `Street` is not
computed at query time at all — it is the qualifier baked into the `road_segment_centroide`
documents, which is why it comes back whenever interpolation is skipped or finds no range.

## Only the first `INTERPOLATED_POSITION` is hoisted, and the rest are still there

`move_first_interpolated_to_top()` (`bbox_handler.py`) moves **the first** result whose
qualifier is `INTERPOLATED_POSITION` to position 0 and `break`s. Every other interpolated
result keeps its relevance-order position among the 25.

`nar_nrcan_top()` used to read only `resp[[1]]`, on the reasoning — recorded in its own
roxygen — that "a correct answer further down is not distinguishable from a wrong one, the
ranking is the only signal the service gives." **That reasoning predated the floor and was
wrong.** The floor is a signal, it is independent of rank, and the correct answer is
frequently sitting below a wrong one. Verified live, with `nar_nrcan_agreement()` run over the
full lists:

```
1 Rue Notre-Dame Ouest, Montreal, QC      330 Spadina Rd, Toronto, ON
 0  … Lorrainville, Quebec      reject     0  330 Spadina Avenue, City Of Toronto   reject
 6  … Montréal, Quebec          ACCEPT     6  330 Spadina Road, City Of Toronto     ACCEPT
 7  … Victoriaville, Quebec     reject
20  … Trois-Pistoles, Quebec    reject
```

In both cases the floor accepts exactly one row of the 25 and rejects every other — and both
of these are the examples the package's own documentation uses for what the tier throws away.
Scanning the full list for the first row that passes costs **no additional HTTP request**; the
results are already in the response body being discarded.

**This shipped.** `nar_nrcan_top()` is now `nar_nrcan_candidates()`, which returns every
result in rank order, and `nar_nrcan_floors(cand, q, idx)` puts all of them through both floors
and returns the best-ranked survivor. Two things followed from it:

* `nar_geocode_tier_nrcan()` used to set `n_matches` to a hardcoded `1L` with a comment
  explaining there were no alternatives to count. Now there are: the number of rows that pass
  the floor is a real ambiguity count, and more than one survivor is a genuine signal (the same
  street name in two municipalities that both pass containment).
* `nrcan_reject` now reports why the *best* candidate failed rather than why the top one did —
  the highest-ranked interpolated position if there was one, otherwise the class of the
  highest-ranked usable result — and the wording changed from `top result is …` to
  `best result is …` to say so.

**Measured, and smaller than the two flagship cases suggest.** Over a 250-address national
`REPEATABLE (42)` sample, applying the same floors to the top result versus to the whole list:

| | placed | p50 | p90 | max |
| --- | --- | --- | --- | --- |
| top result only | 134/250 = 53.6% | 34 m | 145 m | 432 km |
| whole list | 137/250 = 54.8% | 33 m | 144 m | 432 km |

**+1.2 points, three addresses, at 21 m, 22 m and 25 m** — all three were rejected as
`street name … != …` where the top result was a different street with a prefix in common
(`ELGIN MEADOWS` answered with `ELGIN`, `LAKE` with `1`, `DE LA COULEE` with `DE LA VENDEE`).
One address in 250 had more than one survivor. The distribution does not move, which is the
point: scanning only ever *adds* a candidate that cleared the same floor, so it cannot make the
tail worse, and the 432 km outlier is a top-result survivor that predates the change.

Montréal and Spadina are not typical — they are the ambiguous-name cases, which are
over-represented in hand-picked examples precisely because they are what makes a good example.
The honest summary is that this buys about a point of recall for free, and makes the two cases
the package documents as its own worst behaviour go away.

Re-run `data-raw/probe_geolocator.R` to re-measure; the harness calls the shipped functions.

## The service drops about one request in twelve, and it is worth re-sending them

Measured over 300 national addresses at 5 requests/second: **24 came back HTTP 500** — a clean
status code, not the 500-in-a-200 above — and they **fast-fail at a 0.23 s median against
0.59 s for a real answer**, so they are not timeouts.

Every one of the 24 succeeded when re-sent, 23 of them on the very first attempt with no
delay:

| retried | immediately | +1 s | +3 s | +8 s |
| --- | --- | --- | --- | --- |
| recovered | 21/24 | 22/24 | 22/24 | 22/24 |

**24 of 24 recovered on at least one of the four.** One query is the proof that this is not a
property of the query: `388 South Carriage WAY, LONDON, ON` succeeded on the immediate retry
and then failed the next three. Twelve queries that had succeeded were re-asked and all twelve
succeeded again. It is server-side flakiness in the Lambda/API-Gateway stack, uncorrelated with
the address.

Left unretried this was **silently costing about 8% of recall**, and worse, it was invisible:
a lost request produced zero candidates, which the floors reported as `no answer` — the same
thing the service says when it genuinely has nothing. A coverage number computed that way
blames the geolocator for the transport.

**`nrcan_geocode(retries = 3)` is the fix**, via `httr2::req_retry()` with
[`nar_nrcan_transient()`](../../R/geocode_nrcan.R) as the `is_transient` predicate: any 5xx,
`429`, or a `200` whose body is a JSON object. An empty array is deliberately **not**
transient — that is the service answering "nothing", and a lost answer is indistinguishable
from it anyway. Two details that are easy to get wrong:

* `req_error(is_error = \(resp) FALSE)` does **not** disable the retry. `is_transient` is
  consulted independently of `is_error`; verified with a counting predicate. Only the final
  answer comes back as data rather than as a condition, which is what the per-address loop
  wants.
* The **circuit breaker was considered and rejected.** `failure_threshold` is compared against
  the attempt counter *within a single request* (`i > threshold` in `retry_check_breaker`), not
  against failures accumulated across a batch, so with a small `max_tries` it either can never
  fire or fires on ordinary noise. It does not do what a batch geocoder would want it for.

Measured end to end over the same 300 addresses: **requests lost 14 → 2, placed 154 → 164**.
Over 100 addresses the wall clock went from 0.65 s to 0.74 s each — httr2 waits
`runif(1, 2^tries)` seconds between attempts, which is more patience than the evidence calls
for, but backing off is the right thing to do to a service that is telling you it is
struggling. Rows that exhaust their retries now report `request failed` rather than
`no answer`, so the two are separable in any future measurement.

**The BC geocoder does not need this** and was not given it: 150 consecutive requests, 150
HTTP 200s.

## Their query normalizer, and why spelling made no difference

`normalize_query` rewrites query tokens through `synonyms.json` — 302 canonical forms, roughly
the same ground as `R/normalize_variants.R`, covering street types, provinces, directions, and
ordinals (`1st`, `first`, `1er`, `première` all fold to `1`).

**The reverse map is built last-wins and collides badly.** Canonical entries claim the same
abbreviation more than once, and the loop that inverts them overwrites silently:

| token | folds to | because |
| --- | --- | --- |
| `st` | `saint` | claimed by `rue`, then by `saint` |
| `ne` | `nova scotia` | claimed by `northeast`, then by `nova scotia` |
| `no` | `northwest` | claimed by `northwest` (also a French `numéro`) |
| `bl` | `boulevard` | claimed by `bluff`, then by `boulevard` |
| `rd` | `route` | `route` claims `road`, `rd`, `chemin`, `ch` |

So `100 Water St, Charlottetown, PE` reaches the index as
`100 water saint, charlottetown, prince edward island`.

**This is the mechanism behind a measurement already recorded** in
[`geocoding-status.md`](geocoding-status.md): sending `ST` versus `Street` changes the outcome
for one address in 139. It changes almost nothing because `operator: OR` needs only `water` to
match, so mangling the type token costs nothing that the type token was contributing anyway.
That measurement stands, and now has a reason rather than only an observation. The tier should
keep sending NAR's abbreviations.

## Things to know that are not about accuracy

**Every query is logged.** `lambda_function.py` writes each request to an OpenSearch analytics
index before searching: the query string itself, the language, the user agent, the referrer,
and the caller's IP resolved through an `ip2geo` processor. The code comments say IPs are not
themselves retained, only aggregated. This is ordinary for a public service, but a package
whose users may geocode client lists, patient addresses or survey respondents should say so
next to the licence note in `nrcan_geocode()`'s roxygen — the addresses leave the machine and
are retained by a third party.

**The HTTP 200 carrying `{"message": "Internal server error"}` belongs to the old backend.**
Both queries documented as reproducing it — `100 Water St, Charlottetown, PE` and
`1155 Robson Street, Vancouver, BC` — now return clean result arrays (verified live). The
guard in `nar_nrcan_candidates()` that treats a JSON *object* as an absent answer is still
worth keeping: the current handler returns `{"statusCode": 500, "body": ...}` from its exception
path, and whether that surfaces as a real 500 or as an object inside a 200 depends on the API
Gateway integration, which is not visible from outside. The two example queries in the roxygen
are stale and should be dropped rather than replaced — no current query is known to trigger it.

**Coverage of the address-range index is patchy in a way the qualifier reveals.** Verified
live: `990 Bute St, Vancouver, BC` and `1155 Robson St, Vancouver, BC` both return
`INTERPOLATED_CENTROID`, while `100 Water St, Charlottetown, PE` interpolates. A centroid is
not always the service declining a hard address — sometimes it is a whole city's ranges
missing. This tempers the expected gain from the civic-suffix fix above, and it is a caution
for the street-centroid tier: the 116-of-423 centroid bucket is a mix of "found the street,
declined the number" and "have no ranges here at all", which the response does not distinguish.

## The sibling worth a separate look: `maps.canada.ca/nominatim`

`backend/geolocator-bucket-content/services/nominatim-schema.json` points at
**`https://maps.canada.ca/nominatim/search`** — a Canada-hosted Nominatim instance, no key
required, which the aggregator queries for the `nominatim` key. It is structurally better
suited to this package than `locate` is, and is **not** currently bound:

* It takes **structured input** — `street=`, `city=`, `state=`, `postalcode=`, `countrycodes=CA`
  — so the query's structure never has to be flattened into a string and recovered.
* It returns **structured output** — `addressdetails=1` gives `house_number`, `road`, `city`,
  `state`, `postcode` as fields — so the agreement floor becomes a field comparison instead of
  a re-parse of a title.
* It carries `place_rank` and `addresstype`, where rank 30 / `building` is a building-level
  match, which is a qualifier floor with more resolution than `INTERPOLATED_POSITION`.
* It has a **`/reverse` endpoint**, which no other online source in this package does.

Verified live: `990 Bute St, Vancouver` returns the building (`The Berkeley`, rank 30) where
`locate` returns a street centroid, and `1155 Robson St, Vancouver` likewise.

Three things must be settled before it could be a tier, and none of them is technical:

1. **Licence.** Results are OpenStreetMap data under **ODbL** — attribution and share-alike —
   which is a materially different obligation from the OGL covering every other source here,
   and it attaches to derived databases. This is the blocker, not the network work.
2. **Rate limits and standing.** The instance exists to serve GeoView. Nothing published says
   what bulk use is acceptable, and the aggregator's own 3-second timeout suggests it is not
   provisioned generously. `geo@nrcan-rncan.gc.ca` is the contact the README names for bulk use.
3. **Coverage.** Both probes were downtown Vancouver, which is the easy case. OSM's Canadian
   address coverage is uneven and concentrated in cities with municipal open-data imports; it
   would need the same `probe_geolocator.R` treatment on a national sample before any figure
   could be claimed.

## Not our finding to use

`backend/geolocator-bucket-content/services/geocode-schema.json` has a live **Google Maps
Geocoding API key** committed as a static parameter in a public repository. It is not used by
this package and must not be. It is recorded here only so that the next person to read that
file knows it was seen and deliberately left alone.
