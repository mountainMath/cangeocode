# Forward geocoding

> Component note for `cangeocode`. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md);
> `geocode()` parses with `normalize_address()` first, so [`normalization.md`](normalization.md)
> is upstream of everything here. Measurements, the tier ceiling and what is not built yet:
> [`../inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md).

## `R/geocode.R` — the forward query layer

`geocode()` parses with `normalize_address()` and then runs the tiers named in **`method`**,
**in the order given** — that order *is* the priority, since each tier is offered only the
rows its predecessors left without a position. `match_method` reports which one answered and
`uncertainty_m` what that method costs. On the 5,000 Corporations Canada addresses the eval
draws, the exact tier places 84.9% and interpolation lifts that to **89.1%**, in 0.9s for
the whole batch.

The vocabulary is `"nar"` (exact lookup), `"nar_interpolate"`, `"bc"` and `"nrcan"`, defaulting
to `c("nar", "nar_interpolate")` — the offline pair. **`method` replaced the earlier `source`,
`interpolate` and `fallback` arguments**, which were three ways of saying the same thing and
could not express an ordering. `nar_geocode_methods()` validates it; exact matches beat
prefixes in `pmatch()`, so `"nar"` is unambiguous against `"nar_interpolate"`.

**"Unplaced" is `is.na(out$x)`, not `match_method == "none"`.** That single definition is
what sends a `nar_no_geometry` row on to the next tier — NAR holds the record but no
coordinates, and withholding a position its neighbours can supply would be perverse — while
the `ADDR_GUID` the exact tier found survives whichever tier ends up placing it. The reverse
is a real cost and is documented: a tier that never runs reports nothing, so putting `"nar"`
last leaves interpolated rows with no `ADDR_GUID`.

| `match_method` | meaning | `uncertainty_m` |
| --- | --- | --- |
| `nar_building` | the civic number is in NAR with its own building point | 0 |
| `nar_blockface` | in NAR, but only a blockface centroid | 176 |
| `nar_interpolated` | not in NAR; placed between the flanking civics | `0.5 * span` |
| `nar_no_geometry` | in NAR (`ADDR_GUID` is set) but unplaceable | `NA` |
| `nrcan` | interpolated by NRCan's geolocator, past both floors | 150 |
| `not_covered` | parsed to a province this (partial) database does not hold | `NA` |
| `none` | not found | `NA` |

`uncertainty_m` is defined as the **90th-percentile error this package adds relative to
NAR's own building point**, and deliberately says nothing about NAR's own error, which is
neither published nor consistent — the User Guide admits a building point may be the
driveway. So `0` means "this package added nothing", not "this point is exact". The two
non-zero figures are measured: 176 m is the p90 building→blockface separation over the
1.85M addresses carrying both (p50 50, p95 332), and the interpolation figure comes from
the error/span ratio being **scale-invariant** — its p90 is 0.50 in every span bucket from
under 50 m to over 2 km (0.496–0.522), so half the flanking span is the p90 error whatever
the scale.

**Extrapolation is refused rather than flagged.** Past the last known civic on a side there
is no second point, and continuing the run's spacing scores a 15.1 m median but a 237 m p90
— barely better than the nearest neighbour it would displace. 7.3% of NAR civics sit at the
end of a run. Interpolation is same-parity only (4.2 m median against 35.2 m pooling both
sides, and 16.9 m for nearest-known-civic), and takes only `geom_source = 'building'` flanks,
since compounding a 176 m blockface error at each end would be presented as precision.

**`...` has to serve two online services with different vocabularies**, and they are not
interchangeable: `min_score` is BC's and would be an unused-argument error at the geolocator.
BC keeps receiving all of `...` — forwarding unknown names to its service as query parameters
is a documented feature of that binding — while `nar_nrcan_dots()` passes on only the names
matching `nrcan_geocode()`'s own formals, minus the five the tier supplies itself. It reads
those from `formals()` rather than a literal list, so adding an argument to `nrcan_geocode()`
routes it automatically. That is also why `nrcan_geocode()` deliberately has **no `...` of its
own**: its formals have to be a closed set for the filter to mean anything.

`prov`, `mun` and `within` are **authoritative** — they override whatever the address string
said, and the override lands on the returned row too, so a result never reports a province
next to a point constrained to a different one. `mun` goes through `MunAlias` rather than
straight at `MAIL_MUN_NAME`, because it is a name a person typed: constraining to `TORONTO`
by mailing city would drop everything NAR files under `SCARBOROUGH`. The parsed municipality
keeps the direct comparison, the gazetteer having already turned it into NAR's own string.
`within` densifies its outline **with the CRS temporarily unset** before reprojecting —
`st_segmentize()` on a geographic geometry needs `lwgeom`, which is not a dependency, and
planar interpolation is what is wanted anyway.

**The one performance trap, and it is a 99x one.** Both name families must be matched, and
writing that as `OFFICIAL = x OR MAIL = x` leaves the join with no equijoin key, so DuckDB
nested-loops the 17.4M-row table: the interpolation tier took **15.87s** that way against
**0.16s** as a `UNION` of two single-column equijoins, for byte-identical results. The exact
tier hid it, `CIVIC_NO = p.civic` having handed the planner a hash key of its own. That is
why `nar_geocode_candidates()` exists and why both tiers go through it. Otherwise no index
is needed: the folded street-key join costs 0.05s for a 5-row probe and 0.08s for a 200-row
one, so **batch into one call rather than looping**.

Street type and direction are compared through `upper()` on the NAR side. NAR stores
`OFFICIAL_STREET_NAME` Mixed Case against the `MAIL_*` family's UPPERCASE, and the gazetteer
hands back the `OFFICIAL_*` spelling — without the fold, `24 Sussex Dr, Ottawa` matches nothing.
(The type and direction columns are uppercase in both families, so the fold is redundant there
and kept for uniformity.)

Coordinates are built in `sf` from the returned `x`/`y` rather than through `collect_nar()`,
because these are freshly computed values rather than a stored geometry column; the storage
CRS is still read from the database with `nar_crs()`, and `sf` handles the axis order that
`collect_nar()` needs `always_xy` for.

## `R/geocode_bc.R` — the provincial geocoder

A binding to the Province of British Columbia's [Address Geocoder]. `bc_geocode()` is the
client, `nar_geocode_tier_bc()` is the `"bc"` tier `method` can name, and `bc_validate()`
compares an existing result against BC's answer in metres. BC only: asked about an Ontario
address the service answers with whatever BC place shares the name, so the tier filters on
`PROV_ABVN == "BC"` before sending anything.

**The service always answers, so a response is not a match.**
`1234 Nonexistentzzz Rd, Victoria, BC` comes back as the centre of Victoria with a score of
48 — a point, not an error. Two independent floors decide: `nar_bc_precision()` maps
`matchPrecision` onto a `bc_*` method, and `min_score` (default 60) rejects what the service
itself scored badly. A rejected row keeps its `bc_score` and `bc_faults` and loses only its
`uncertainty_m`, so what was thrown away stays readable.

**The `bc_*` uncertainty figures are the only numbers in this package that were not
measured.** BC publishes `locationPositionalAccuracy` as the categorical
`high`/`medium`/`low`/`coarse` and no distance at all, so `nar_bc_precision()` translates its
precision vocabulary into deliberately pessimistic order-of-magnitude metres. Treat them as a
ranking safe to filter on, not as an error bar comparable to the NAR tiers'. Calibrating them
is named as the next step in the note.

**Both online tiers rebuild the query string from the components rather than forwarding
`input`.** `prov`/`mun` are authoritative and overwrite the parsed columns, so forwarding the
original string would silently discard the caller's constraint the moment a row fell through.
`within` is enforced too, in R — the SQL predicate cannot reach a point that came from another
service, so a fallback point outside the bounds is discarded rather than returned.

**Throttling needs `capacity`, not `rate`.** `httr2::req_throttle(rate = 5)` builds a
`5 * 60 = 300`-token bucket and lets the first 300 requests go at once. `capacity = rate,
fill_time_s = 1` is the actual cap, with the realm named explicitly so a URL-derived realm
cannot give every address its own pool.

`httr2` is in `Suggests` and nothing reaches the network unless one of these functions is
called. The tests run entirely against responses captured from the live service into
`tests/testthat/fixtures/bc-*.json`, which is also the only way the parser stays checkable
once BC changes its scoring; `nar_bc_feature()` takes parsed JSON rather than a response
object precisely so that is possible.

[Address Geocoder]: https://geocoder.api.gov.bc.ca/

## `R/geocode_nrcan.R` — the national geocoder

A binding to NRCan's [geolocator], `https://geolocator.api.geo.ca/geolocation/en/locate?q=`:
keyless, national, and needing no local database, which is what makes it the tier that can
answer before anything has been downloaded. `nrcan_geocode()` is the client and
`nar_geocode_tier_nrcan()` the `"nrcan"` tier. Unlike BC it is **not filtered by province** —
it covers the country — but it is still the tier to name last, because it is the only one
whose accuracy is a percentile on a long tail rather than a bound.

**The service is open source, and reading it settled several things this file used to
infer.** What `INTERPOLATED_POSITION` actually certifies, why the fuzzy match answers a
different question, and which of the floor's component checks do real work are recorded in
[`../inst/notes/nrcan-geolocator.md`](../inst/notes/nrcan-geolocator.md). Read it before
changing `nar_nrcan_candidates()`, `nar_address_agreement()` or `nar_nrcan_floors()`.

**There is no reverse geocoding here, and not for want of looking.** `locate?lat=&lon=`
returns `{"error": "Missing query parameter 'q'"}`, `reverse` and `reverse-geocode` are 404,
and the retired `geogratis.gc.ca/services/geolocation` host redirects to this same `q`-only
endpoint. Reverse geocoding stays NAR-backed and local.

**The service always answers, and it answers plausibly**, which is worse than BC's failure
mode: no score comes back to disbelieve. `1 Rue Notre-Dame Ouest, Montreal, QC` returns a real
`INTERPOLATED_POSITION` for a real Rue Notre-Dame Ouest — in Lorrainville, 500 km away, with
nothing in the response saying so. So the accuracy question here is entirely a *filtering*
question, and two floors do all the work:

1. a result must be `Street` / `INTERPOLATED_POSITION`. `INTERPOLATED_CENTROID` means
   "found the street, not the civic number", and a `Geoname` means the address degraded to a
   populated place — `Zzzzqqq nowhere at all` ranks a village first.
2. its `title` must re-parse, **component by component**, to the address that was sent
   (`nar_address_agreement()`).

**Both floors run over every result in the response, not just the top one, and the reason is
that the floor is independent of rank.** This used to read `resp[[1]]` only, on the argument
that scanning for a better-agreeing result further down would be picking the answer to fit the
question. It is not, because the floor is not a similarity score being maximized — it is a
pass/fail test that the answer re-parses to the address that was sent, and a candidate at rank
7 that passes it is verified exactly as strictly as one at rank 0 that passes it. The service
returns 25 results in one response and hoists only the *first* `INTERPOLATED_POSITION` to the
top, so the rest are already paid for. `1 Rue Notre-Dame Ouest, Montreal, QC` and
`330 Spadina Rd, Toronto` — the two examples this file uses for a confident wrong answer — both
carry the right answer at rank 6, and in both the floor accepts exactly one of the 25.

Two consequences. `n_matches` is now the count of candidates that passed rather than a
hardcoded `1L`: more than one means the same street name in two municipalities that both
satisfy containment, which is a real ambiguity. And `nrcan_reject` reports why the **best**
candidate failed — the highest-ranked interpolated position, or failing that the class of the
highest-ranked usable result — hence `best result is …` rather than `top result is …`.

**The civic-number suffix is stripped from the query and only from the query.** The service
locates the house number with `\b(\d{1,5})\b`, which has no word boundary to find inside
`990A`, so a suffixed civic never reached its interpolator and always came back as a centroid.
`nar_address_string(suffix = FALSE)` is the fix and it is NRCan-only; BC wants the suffix. It
launders nothing, because the floor compares `CIVIC_NO`, which never carried the suffix.

**Comparing the title as a whole string does not work**, and each failure is a separate
mechanism: the municipality migrating into the street name (`28 Silver ST, CORNER BROOK` →
`28 Brook Street, Corner Brook` — `Brook` is in both), a silently substituted street type
(`330 Spadina RD` → `330 Spadina Avenue`, 3 km), and a wholly different street of the right
shape (`61 Oakridge BLVD, OAK BLUFF, MB` → `61 Oak Bluff Road, Brandon`, 190 km). Field-wise
comparison catches all three and is a **strict improvement**, not a precision-for-recall
trade: over 423 addresses it removes 27 answers the substring test kept (median 1615 m off,
16 of them past a kilometre) and recovers 7 it wrongly rejected (28–215 m), where the service
returned an incorporated or parent municipality — `City Of St. Catharines`, `Montréal` for
`MONTRÉAL-NORD`. Measured, not assumed.

The rules within `nar_address_agreement()` are deliberately asymmetric. `""` is *absent*, and an
absent component cannot contradict, so a street type the query never carried is not evidence
against an answer that has one. The two exceptions are the **street name and the civic
number**: those are what was being asked, so a missing one means nothing was verified rather
than nothing disagreed. The municipality is compared by **whole-word containment** in either
direction, because the service returns incorporated forms (`City Of Toronto` for `TORONTO`)
and NAR returns the bare name; `\b` is safe there only because `nar_key_fold()` has already
left nothing but A–Z, 0–9 and spaces.

**The title is parsed without the gazetteer, and that is load-bearing.** Passing `con` to
`normalize_address()` for the title makes the floor launder the very error it exists to
catch: `105 Pouch Cove LINE, BAULINE, NL` was answered with `Pouch Cove` 4.6 km away, and the
gazetteer resolves `Pouch Cove` to `BAULINE` (adjacent NL communities share one NAR
municipality), rewriting the answer into the question. Nothing is lost by dropping it —
incorporated names are handled by the containment rule and accents by folding, neither of
which needs a database.

The **query** side, by contrast, *is* gazetteer-resolved, since it is whatever `geocode()`
parsed. So the floor verifies that the service answered what the package asked, not what the
user typed: `12 Main St, Moncton, NB` is sent as `12 Martin ST` (NAR has no Main Street in
Moncton and the similarity gate resolves it) and the returned `12 Martin Street` agrees. That
is correct — the rewriting is the normalizer's documented job and is visible in the result's
own columns — but it means a `nrcan` row inherits the normalizer's confidence, not only the
service's.

**`uncertainty_m` is 150**, and it is **not comparable to `nar_blockface`'s 176** despite
being the smaller number. Both are p90s, but a blockface error is *bounded* by the length of
the block, while this one is a percentile on a tail that runs to 2.7 km. 150 is the
conservative of the two p90s measured (115 m over n=204, 152 m over n=88).

Same `capacity`/`fill_time_s` throttling trap as BC, same `req_error(is_error = ~FALSE)` —
**a failed lookup is data, not an exception**. One quirk is this service's own: some queries
return HTTP 200 with a body of `{"message": "Internal server error"}` instead of the results
array. That one is tied to the query, and `nar_nrcan_candidates()` detects it structurally —
a parsed body with names is an object, hence this; an unnamed one is the array. Reading it as
a result would take `message` for a title.

**The service also drops about one request in twelve with a plain HTTP 500, and that one is
not about the query** — measured, one address succeeded and then failed three times after.
`retries = 3` re-sends them via `req_retry(is_transient = nar_nrcan_transient)`, which is
worth about 8 points of coverage. Three things about it are not guessable:
`req_error(is_error = ~FALSE)` does **not** suppress `is_transient`, which is consulted
independently; an empty array is deliberately *not* transient, because that is the service
answering "nothing"; and `failure_threshold` is compared against attempts *within one request*,
so the circuit breaker cannot see a batch and was skipped. A row whose retries ran out reports
`request failed` rather than `no answer` — both are zero candidates, but only one of them is
about the address, and folding them together understates the tier. That is what the `failed`
argument to `nar_nrcan_floors()` carries, and why it beats every other rejection reason: a
failed row has no candidates for the other rules to look at.

**The known weakness is that the street-name rule is equality.** On addresses NAR could not
place — the ones this tier exists for — half its street-name rejections are the service
answering correctly through a dirty parse (`ATHLONE AVENIUE`, `CHEM DE HARDWOOD FLAT`,
`50TH` against `50 Street`) and being refused. Relaxing it is the ranked next step and is not
free: containment is precisely what let `28 Silver ST, CORNER BROOK` through. The sizing is
in the status note.

Tests run against `tests/testthat/fixtures/nrcan-*.json` captured from the live service, and
four of the six fixtures are wrong answers a naive binding would accept.
`data-raw/probe_geolocator.R` measures the tier by calling `nar_nrcan_candidates()` and
`nar_nrcan_floors()` themselves, so the harness measures the shipped code rather than a
restatement of it. It scores against NAR's building points, which are a **reference and not
ground truth** — NAR has its own bad records, so a large distance means the two disagree, not
that the geolocator is wrong. The numbers it produced are in
[`../inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md).

## `R/geocode_qc.R` — the Quebec geocoder

A binding to the Ministère des Ressources naturelles et des Forêts' Esri [GeocodeServer] over
the *Répertoire québécois des adresses*, published CC-BY. `qc_geocode()` is the client,
`nar_geocode_tier_qc()` is the `"qc"` tier `method` can name, `qc_validate()` compares an
existing result against Quebec's answer in metres, and `qc_reverse_geocode()` is **the only
online reverse geocoder in the package** — the other three services do forward only, so the
sentence "reverse geocoding is NAR-backed and local" is no longer true of everything here.
Quebec only, so the tier filters on `PROV_ABVN == "QC"` before sending anything.

**The query has to be spelled French-canonical, and this is not cosmetic.** The locator's
reference strings are `Rue Notre-Dame Ouest`; `nar_address_string()` renders NAR's own
`NOTRE-DAME RUE O`, and sent that the locator does not degrade, it stops matching — 31.5%
civic matches against 95.5%. `nar_qc_query()` exists for this and is the reason the tier is
worth having at all. Spelling out the street type is worth 37 points, the direction 26, the
word order under one; the measured table is in that function's roxygen and reproduced by
`data-raw/probe_qc.R`. The failure is silent in the worst way: `1 RUE NOTRE-DAME O, MONTREAL`
comes back as a *street centroid scoring 92.4* where the correct civic point scores 82.5, so
the abbreviated form does not lose the address, it replaces it with a confident wrong answer.

**`Loc_name` is the precision field. `Addr_type` is not, and `Score` is not a ranking.**
`Addr_type` is `Feature` for both locators and separates nothing. The score measures how much
of the string *sent* was consumed: correlation with distance-from-NAR is Spearman 0.018, and
street-only answers score *higher* than civic ones (median 87.0 against 83.0). This is why
`min_score` defaults to 0 and should stay there — a threshold here removes correct addresses
before it removes street centroids, which is the opposite of what `min_score` does for BC.

**Four things about the response would each produce silently wrong code.** They are captured
in `tests/testthat/fixtures/qc-*.json` rather than described, so they stay checkable:

- **The batch answers out of order.** Send three addresses, the `locations` come back 3, 1, 2.
  `nar_qc_locations()` places rows by `ResultID`, and an id the service dropped stays an
  unmatched row rather than shifting everything after it.
- **The coordinates come from `location`, never from the `Latitude`/`Longitude` attributes.**
  Those are rendered in the service's French locale with a comma decimal mark
  (`"45,5061613986714"`), which `as.numeric()` turns into `NA` on a good day, and they are
  empty for a street-level match whose `location` is populated.
- **Only the batch endpoint populates `Loc_name`.** `findAddressCandidates` returns it empty,
  which would leave a civic match and a street centroid indistinguishable — so the batch
  endpoint is used even for one address.
- **The reverse endpoint reports no distance.** `qc_reverse_geocode()` measures it itself in
  EPSG:3347, because a caller wants to know how far the answer is before believing it.

**The service is not independent of NAR, and `qc_validate()` says so in its own
documentation.** The locators are named `RQA_Adresse` and `RQA_Rue`: it serves the Répertoire,
which is also what NAR's Quebec records are built from. Measured over 400 NAR Quebec building
points the median disagreement is **0.9 m** (p90 13.3 m, nothing over 500 m). That is evidence
of shared lineage, not of accuracy — the same trap `.claude/CLAUDE.md` records for the BC
geocoder, only more so. Use it as a fallback, not as a check.

As a fallback it is worth about what the geolocator is: on 600 Quebec corporations addresses
it takes coverage from 81.0% to 83.3%, recovering 12.3% of what NAR left unplaced.

The floor is the shared one: `nar_address_agreement()` re-parses the returned title and
compares it component by component against what was asked for, **without the gazetteer** —
the same rule as the geolocator's, and for the same reason. It catches what the locator floor
cannot, including `12 RUE SAINT-JEAN, GATINEAU` answered as `12 Rue Saint-Jean-Bosco,
Gatineau` with `RQA_Adresse`: right municipality, right number, wrong street.

`httr2` and `jsonlite` are in `Suggests` and nothing reaches the network unless one of these
functions is called. `nar_qc_locations()` and `nar_qc_reverse_row()` take parsed JSON rather
than a response object so the whole response layer is testable offline.

[GeocodeServer]: https://servicescarto.mrnf.gouv.qc.ca/pes/rest/services/Territoire/Adresse_Geocodage/GeocodeServer

## `R/geocode_osm.R` — the OpenStreetMap geocoder

A binding to **`https://maps.canada.ca/nominatim/search`** — the Nominatim instance the
Government of Canada hosts, and the one NRCan's own aggregator queries under its `nominatim`
service key. **It is not `nominatim.openstreetmap.org`**, whose usage policy forbids exactly
this: bulk geocoding against the volunteer-funded instance is a violation, and pointing
`nar_osm_url()` there would make every user of this package one. The GC instance is keyless,
national, and under no such restriction. `osm_geocode()` is the client.

**It is exported and deliberately not a `geocode()` tier.** Two reasons, and the licence is
the one that decides it: OSM data is **ODbL**, a share-alike licence whose obligations attach
to a derived *database*, while NAR, BC and NRCan are all Open Government Licence. Mixing a
handful of ODbL rows into a result table changes what the caller may do with the whole table,
and a default tier would do that silently. So the licence text the service returns is carried
through as `osm_licence` on every row, and choosing this service is an explicit call. The
second reason is that `nar_osm_uncertainty_m()` returns `NA_real_` — the accuracy has not been
measured against NAR yet, and inventing a constant to make the tier tidy would be asserting
something unmeasured.

**This service refuses, which the other two do not.** BC answers a nonexistent address with a
city centroid and the geolocator answers it with a confident wrong street; Nominatim returns
`[]`. It also returns the road itself at `place_rank` 26 when it has the street but not the
number, which is a legible "found the street, not the address" rather than a point. That
changes what the floor is for — most of the loss here is coverage, not rejection — but does
not make it unnecessary:

1. `place_rank >= 30` **and** a `house_number` of its own. Rank 30 alone is not enough:
   `24 Sussex Dr, Ottawa` comes back at rank 30 with no house number at all, so the belt and
   the braces are both load-bearing.
2. the separated fields must agree with the address that was sent, through the same
   `nar_address_agreement()` the geolocator's floor uses.

**The comparison function is shared, and lives in `R/geocode.R` rather than in either
binding.** The rules — `""` is absent and cannot contradict, the street name and civic number
are what was asked so a missing one is a failure, the municipality matches by whole-word
containment in either direction — are properties of comparing two Canadian addresses, not of
either service. Only the way the answer is *obtained* differs, and that is the interesting
asymmetry: the geolocator hands back one string that has to be re-parsed, while Nominatim
already separated `house_number`, `road`, `city` and `ISO3166-2-lvl4`.

**So the road is parsed and the municipality is not.** `road` is a full street line
(`Bute Street`, `Rue Notre-Dame Ouest`) and has to go through `normalize_address()` to be
compared component by component; `city`/`town`/`village` and the ISO code are already the
fields, and are written straight onto the parsed frame. Parsing the `display_name` instead
would mean getting past a building name and two sub-municipal localities —
`The Berkeley, 990, Bute Street, Davie Village, West End, Vancouver, …` — to reach what the
service already handed over separately. `nar_osm_mun()` coalesces `city`, `town`, `village`,
`municipality` and `hamlet` and **deliberately stops there**: `suburb`, `neighbourhood`,
`quarter` and `city_district` are below the municipality and would match a query's
municipality against something smaller than one.

**That parse gets no gazetteer, for the same reason the geolocator's title does not** — a
`con` would let the gazetteer rewrite the answer into the question and launder the error the
floor exists to catch. See the geolocator section above; the mechanism is identical.

**`n_matches` counts distinct addresses, not results.** `1155 Robson St, Vancouver` comes back
as two OSM objects — the building and an office inside it, 8 m apart — with identical house
number, road and city. Counting results would report an ambiguity that does not exist, so the
survivors are deduplicated on `address_key()` of the parsed answer. More than one still means
what it means at the geolocator: the same street in two places that both pass.

**The query is not NAR's canonical order, and this is measured.** `nar_address_string()`
produces `1 NOTRE-DAME RUE O`, which returns **nothing**; `1 Rue Notre-Dame O` also returns
nothing; `1 Rue Notre-Dame Ouest` returns the address. Both the word order and the unexpanded
direction are at fault, and only for French — `100 Queen St W` works as-is, because Nominatim
expands `W` and has no idea what `O` is. `nar_osm_street()` therefore uses `nar_type_leads()`
to put the type where the language puts it and spells out `N`/`S`/`E`/`O` only where it leads.
Accents need no handling; the service folds them. This is the one place in the package where
the *query string* is shaped for a specific service rather than being NAR's own spelling.

**A supplied structured parameter is a requirement, not a hint.** `street=`, `city=` and
`state=` narrow the search, so an empty one is a filter nothing can satisfy — `nar_osm_query()`
drops absent elements rather than sending them blank. `structured = FALSE` collapses the same
parts into a single `q=`, which is looser and is kept as a knob for the probe rather than as a
recommendation.

Small things that are easy to get wrong: `lat` and `lon` arrive as **strings** and are
converted; the province comes from `ISO3166-2-lvl4` (`CA-BC`) with the `CA-` stripped, not
from the prose `British Columbia`; Nominatim's usage policy asks a bulk caller to identify
itself, so `req_user_agent()` here is a condition of use rather than a courtesy, and the
default `rate` is **1 request a second** for the same reason. `nar_osm_transient()` mirrors the
geolocator's predicate and is **precautionary** — this service has not been seen to drop
requests, but an HTTP 400 with an `{"error": …}` body is *not* retried, since that is the
service labelling a malformed query and re-sending it would only get the same answer three
times.

Tests run against `tests/testthat/fixtures/osm-*.json` captured from the live service, and
unlike the geolocator's fixtures half of them are the service correctly declining.
`data-raw/probe_osm.R` measures accuracy the way `data-raw/probe_geolocator.R` does — calling
`nar_osm_candidates()` and `nar_osm_floors()` themselves, over the **same** `REPEATABLE (42)`
sample, so the two services are comparable. It has not been run at scale yet, which is why
there is no accuracy paragraph here and why `uncertainty_m` is `NA`.

[geolocator]: https://geolocator.api.geo.ca/
