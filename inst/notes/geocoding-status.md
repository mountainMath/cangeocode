# Forward geocoding: what resolves, what does not, and what is not built yet

`geocode()` (`R/geocode.R`) turns an address string into a coordinate by parsing it with
`normalize_address()` and resolving the result against NAR, optionally falling through to an
online service. This note records what it
currently reaches, how the accuracy figures in its documentation were measured, and the
pathways that were sized but not built.

For *why the code is shaped the way it is*, see the `R/geocode.R` section of
`.claude/geocoding.md`. For the parser's own failure modes — which cap everything here, since
an address that does not parse cannot be resolved — see
[`address-normalization-status.md`](address-normalization-status.md).

## Reproducing the numbers

Three independent measurements, all against NAR 2026-06.

**Tier coverage** comes from the same 5,000 Corporations Canada addresses that Part B of
`data-raw/eval_normalize.R` draws — same file, same filter, same `set.seed(20260821)` — so
the figures line up with the normalizer's. That seed is load-bearing; see the eval harness
section of the normalization note.

**Interpolation accuracy** comes from a leave-one-out sweep over NAR itself: for every
distinct civic point, drop it, interpolate from its same-side neighbours, and measure the
error against the point that was dropped. 10,559,271 distinct civic points, of which
9,225,453 (92.7%) have same-side flanking neighbours at all.

**Agreement with the BC geocoder** comes from the same corporations file filtered to BC,
`set.seed(20260821)`, 600 rows for the coverage figures and a further `set.seed(1)` draw of
250 already-placed rows for the distance comparison. It makes one network request per
address, so it is not part of any test or check.

**Agreement with NRCan's geolocator** comes from `data-raw/probe_geolocator.R`, which draws
`USING SAMPLE reservoir(n ROWS) REPEATABLE (42)` from NAR's own building-point addresses and
asks the service for each one. Figures below are the 423-address run. It queries the live
service and is likewise not part of any test or check.

**NAR is the reference here, and a reference is not ground truth.** NAR is accurate in
general, but it has its own poor and outright wrong records, so a large distance in these
tables is a *disagreement* and the geolocator is sometimes the one that is right. This matters
most in the tails: the multi-kilometre survivors are as likely to be bad NAR records as bad
geolocator answers, and the p50/p90 figures are safe only because a systematic bias would have
to be shared by both sources to survive at the median.

## Where it stands

5,000 Corporations Canada addresses, 0.9s for the whole batch:

| `match_method` | n | share |
| --- | ---: | ---: |
| `nar_building` | 3,952 | 79.0% |
| `nar_blockface` | 292 | 5.8% |
| `nar_interpolated` | 210 | 4.2% |
| `nar_no_geometry` | 7 | 0.1% |
| `none` | 539 | 10.8% |
| **placed** | **4,454** | **89.1%** |

Without the interpolation tier, 84.9%. Every exact-tier match agreed with the postal code
the filer wrote — 100% of 4,244 — which is worth stating because nothing in the query uses
the postal code, so it is a free independent check rather than a tautology.

Interpolated results are honest but coarse: `uncertainty_m` has a median of 69 m and a p90
of 380 m, with 26% at or under 25 m and 43% at or under 50 m. That spread is the point of
reporting it — filter on `uncertainty_m` rather than treating the tier as uniformly good.

## Why interpolation is same-parity, and why it refuses to extrapolate

Leave-one-out over all 10.6M distinct civic points:

| method | p50 | p90 | p95 | within 50 m |
| --- | ---: | ---: | ---: | ---: |
| interpolate, same side | **4.2 m** | 41.1 | 87.1 | 91.6% |
| nearest known civic | 16.9 m | 71.3 | 139.7 | 85.5% |
| both sides pooled | 35.2 m | 87.0 | 140.1 | 69.2% |
| extrapolate past end of run | 15.1 m | 237.2 | 641.5 | 72.9% |
| nearest civic, at end of run | 26.0 m | 223.6 | 548.0 | 69.0% |

Same-side interpolation beats every alternative by a wide margin. Extrapolation does not:
its median looks respectable but its p90 is 237 m against the 224 m of simply taking the
nearest known civic, so it buys almost nothing over the answer it would displace while
looking exactly like a real interpolation in the output. It is refused outright.

Error by flanking span, and by the gap in civic numbering:

| flanking span | p50 err | within 50 m | | civic gap | p50 err |
| --- | ---: | ---: | --- | --- | ---: |
| < 50 m | 1.9 m | 98.8% | | ≤ 4 | 1.7 m |
| 50–150 m | 10.9 m | 92.3% | | 5–10 | 3.2 m |
| 150–500 m | 31.9 m | 64.3% | | 11–30 | 6.0 m |
| 500–2000 m | 81.6 m | 37.2% | | > 30 | 18.8 m |
| > 2000 m | 325.1 m | 17.3% | | | |

**The uncertainty model falls out of this.** The ratio of error to flanking span is
scale-invariant: its 90th percentile is 0.50 in every one of those span buckets
(0.496–0.522), with p50 ≈ 0.10 and p95 between 0.67 and 0.80. So `uncertainty_m =
0.5 × span_m` is the p90 error at any scale, which is what `geocode()` reports.

## What `uncertainty_m` does not include

It is the error *this package introduces relative to NAR's own building point*. NAR's own
positional error is not in it and is not estimated, because it is not consistently
knowable: the StatCan NAR User Guide says a building representative point "may not
correspond exactly to the physical center of the building structure itself" and may be the
road access point or the driveway, and publishes no accuracy figure. `uncertainty_m = 0`
therefore means "this package added nothing", not "this point is exact".

If a consistent estimate ever becomes available — StatCan publishing one, or a comparison
against an independent high-accuracy source — it should be added as a separate column
rather than folded into this one. The two are different quantities and a caller measuring
reproducibility against NAR wants the first alone.

`bc_validate()` now gives the first measurement of it, for one province. See below.

## How far NAR's points sit from a second source

250 BC addresses drawn from the same corporations file, geocoded through NAR and then again
through the BC Address Geocoder, keeping the rows BC resolved to `bc_site` or `bc_civic`
(its parcel-level precisions). Distances in metres between the two answers:

| NAR tier | n | p50 | p75 | p90 | p95 | ≤25 m |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `nar_building` | 224 | 19.8 | 57.3 | 118.8 | 195.5 | 55% |
| `nar_blockface` | 5 | 22.6 | 59.3 | 71.2 | 75.2 | 60% |
| `nar_interpolated` | 14 | 26.7 | 111.1 | 265.2 | 2685.5 | 50% |

**This is disagreement, not error.** It contains both sources' positional error *and* their
definitional difference: NAR's `BG_X` is a representative point that the User Guide says may
be the road access point or the driveway, while BC's is a parcel point. It is an upper bound
on NAR's own error rather than an estimate of it — but a median of ~20 m on the tier that
`uncertainty_m` calls 0 is the number to keep in mind when reading that 0.

The `nar_interpolated` row is the interesting one: leave-one-out against NAR's own points put
its median at 4.2 m, and against BC it is 26.7 m. The difference is almost exactly the
~20 m floor the building tier shows, which is what a genuinely small interpolation error
sitting on top of a source disagreement looks like. The 2.7 km p95 is two rows, both
long rural blocks.

**The two sources are not independent, so this cannot be read as a benchmark of NAR.** BC's
geocoder and NAR's BC records plausibly share upstream data, so agreement between them is
partly agreement with themselves and the disagreement above is a *lower* bound on how far
apart two genuinely independent sources would be. Where they do differ, **BC's answer is the
more reliable of the two** — it is a parcel-level provincial authority, and NAR is a national
compilation of what the provinces and municipalities supplied. Benchmarking NAR properly needs
a source that shares nothing with it, which the package does not currently have.

Not enough rows per province, and one province only, to generalize. Growing this into a real
calibration — and into a defensible `uncertainty_m` for the `bc_*` tiers, which is currently
the one set of numbers in this package that was not measured — is the obvious next piece of
work.

## The 10.8% that does not resolve

Decomposed on the same draw, as a share of all 5,000:

- **3.7%** — the street does not exist in NAR anywhere in the province. Nothing in the NAR
  pathway can fix these; this is what the road network file is for.
- **3.8%** — the street exists but the address could not be reached even with
  interpolation, mostly end-of-run refusals and streets whose known civics are all the
  other parity.
- **1.4%** — never parsed at all: no street name or no civic number was extracted.
- the remainder — the street exists in NAR under a municipality that did not match.

Combined ceiling for the NAR-only pathway is therefore around 93%.

The `"nrcan"` tier reaches very little of this: appending it recovers 8.1% of the unplaced,
for the reason given in its own section below — the addresses NAR cannot place are largely
the ones no national compilation has. The road network file remains the pathway that would
actually move this number.

## Measured and deliberately not done

**Blockface points are not used as interpolation flanks.** They would have recovered some
of the end-of-run refusals — `24 Sussex Dr, Ottawa` is the worked example, where civic 50
is a blockface centroid and the nearest even building point is 140 — but a flank carrying
176 m of its own error at each end produces a result that looks like a 4 m interpolation.
Doing this properly means propagating flank error into `uncertainty_m` rather than assuming
the flanks are exact, which the current model does. Worth doing; not free.

**No spatial or btree index is used for the street join, and this was measured.** The
folded street-key join costs 0.05s for a 5-row probe and 0.08s for a 200-row probe — the
scan is the entire cost and every probe row shares it. Batching is what matters, not
indexing. (Callers should pass many addresses in one `geocode()` call rather than looping.)

## The BC geocoder binding

`bc_geocode()`, `bc_validate()`, and the `"bc"` tier `geocode(method = )` can name. Notes on what it turned out
to be:

**The service always answers.** `1234 Nonexistentzzz Rd, Victoria, BC` returns the centre of
Victoria with a score of 48 — a point, not an error — so a response is not a match. Two
independent floors decide: `matchPrecision` must be an address-level precision, and `score`
must clear `min_score` (default 60). Both the score and the faults survive a rejection, so
what was thrown away stays visible.

**As a fallback it is worth a lot in BC.** On 600 BC addresses from the corporations file the
NAR pathway placed 524 (87.3%) and returned `none` for 76. With
`method = c("nar", "nar_interpolate", "bc")`, 75 of those 76 resolved: 31 at address level (`bc_site` 5, `bc_civic` 26), 18 interpolated along a
block (`bc_block`), 26 only to a street (`bc_street`). So roughly **half the NAR failures in
BC are real addresses BC knows about**, and the rest at least get a street.

**The `bc_*` uncertainty figures are not measured.** BC publishes
`locationPositionalAccuracy` as the categorical `high`/`medium`/`low`/`coarse` and no
distance at all, so `nar_bc_precision()` translates its precision vocabulary into
deliberately pessimistic order-of-magnitude metres. They are a ranking that is safe to filter
on, not an error bar comparable to the NAR tiers'. This is the one place in the package where
a number was chosen rather than measured, and it is flagged as such in the function's own
documentation.

**Throttling needs `capacity`, not `rate`.** `httr2::req_throttle(rate = 5)` builds a
`5 * 60 = 300`-token bucket, so the first 300 requests go out at once — a burst allowance,
not a throttle. `capacity = rate, fill_time_s = 1` is what actually caps it. The realm is
named explicitly too, since a realm derived from the URL would put every address in its own
pool.

No API key is needed for modest use; `api_key` is there for jobs that warrant registering.

## The NRCan geolocator binding

This section is about what the tier is *worth*. What the service on the other end actually
does — it is open source, and reading it corrected several things assumed here — is in
[`nrcan-geolocator.md`](nrcan-geolocator.md).

`nrcan_geocode()` and the `"nrcan"` tier `geocode(method = )` can name, over
`https://geolocator.api.geo.ca/geolocation/en/locate?q=`. Keyless, national, and needing no
local database, which is the whole reason to want it: it is the only tier that can answer
before a NAR release has been downloaded, and the only one that covers provinces a partial
import does not hold.

**It does not reverse geocode, and the alternatives were checked rather than assumed.**
`locate?lat=&lon=` returns `{"error": "Missing query parameter 'q'"}`; `reverse`,
`reverse/en` and `reverse-geocode` are all 404; the retired
`geogratis.gc.ca/services/geolocation` host redirects to this same endpoint and still demands
`q`. There is no reverse capability to bind. `reverse_geocode()` stays NAR-backed and local.

**It always answers, and it answers plausibly**, which is a harder problem than BC's. BC at
least returns a score to disbelieve; this service returns none. `1 Rue Notre-Dame Ouest,
Montreal, QC` comes back as a real `INTERPOLATED_POSITION` on a real Rue Notre-Dame Ouest —
in Lorrainville, 500 km away — with nothing in the response marking it as a substitution. So
the accuracy question is a filtering question, and the numbers below are all about which
floor is applied.

**What the floors are worth.** Of 423 addresses, 383 produced a usable answer, and 204
(**48.2% of everything queried**) survived both floors. Distances are to NAR's own building
point, with the caveat above — this is agreement with NAR, not accuracy:

| | n | p50 | p90 | p95 | max | >1 km |
| --- | --- | --- | --- | --- | --- | --- |
| `Street` + `INTERPOLATED_POSITION` only | 267 | — | 1044 m | — | 2728 km | — |
| + province and municipality as substrings of the title | 224 | — | 264 m | — | 190 km | 17 |
| + component agreement (**shipped**) | 204 | 33 m | 115 m | 212 m | 2733 m | 1 |

The 2728 km in the first row is not a typo: without the second floor the service will answer
a Quebec address with a Yukon street of the same name.

**The component floor is a strict improvement over comparing the title as a string**, not a
recall-for-precision trade. It removes 27 answers the substring floor kept — median **1615 m**
off, 16 of them over a kilometre, worst `61 Oakridge BLVD, OAK BLUFF, MB` →
`61 Oak Bluff Road, Brandon` at 190 km — and it *recovers* 7 the substring floor rejected, all
of them right (28–215 m), where the service returned an incorporated or parent municipality
name: `City Of St. Catharines` for `ST CATHARINES`, `Montréal` for `MONTRÉAL-NORD`,
`Saint-Simon` for `SAINT-SIMON-DE-BAGOT`. Whole-word containment on the municipality field is
what buys those back.

**Why answers were rejected**, over the 423:

| reason | n |
| --- | --- |
| best result was not `Street`/`INTERPOLATED_POSITION` | 116 |
| no usable answer at all | 40 |
| municipality disagreed | 30 |
| street name disagreed | 28 |
| street type disagreed | 5 |

The largest bucket is the service declining to resolve the civic number — an
`INTERPOLATED_CENTROID` says it found the street but not the number. Those are not wrong
answers, they are refusals, and a **street-centroid tier would be the way to use them** (see
below); today they are dropped.

### Three changes since that measurement

The 423-address numbers above were taken when the tier read only the result the service ranked
first, when the query carried the civic-number suffix, and when a request the service dropped
was simply lost. Reading the service's own source
(see [`nrcan-geolocator.md`](nrcan-geolocator.md)) showed the first two were costing recall for
nothing, and measuring the wire showed the third was. All three have shipped. The tables above
are **not** re-measured; what each change is worth was measured separately. Every coverage
figure in this note predates the retry and is therefore several points low.

**The whole result list is now put through the floors, not just the top result.** The response
carries up to 25 results, the floors are independent of rank, and the correct answer is often
below a wrong one — `1 Rue Notre-Dame Ouest, Montreal, QC` is answered with Lorrainville at
rank 0 and Montréal at rank 6, and `330 Spadina Rd, Toronto` with Spadina Avenue at rank 0 and
Spadina Road at rank 6. In both, the floor accepts exactly one of the 25. Over a 250-address
national sample, applying the same floors to the top result versus to the whole list moved
placement from **53.6% to 54.8%** — three addresses, at 21 m, 22 m and 25 m — with p50, p90 and
max unchanged. It costs no extra request and cannot widen the tail, since a scanned candidate
had to clear the same floor as a top one. The two flagship cases are much better than the
average because ambiguous street names are exactly what makes a memorable example.

Because more than one candidate can now survive, the tier reports a real `n_matches` instead of
a hardcoded `1L`; two survivors means the same street name in two municipalities that both pass
whole-word containment. One address in the 250 had one.

**The civic-number suffix is dropped from the query, and only from the query.** The service
finds the house number with `\b(\d{1,5})\b`, which cannot match `990A`, so a suffixed civic
never reached its interpolator at all and came back as a centroid this tier then rejected.
Over 20 suffixed NAR building points sampled nationally: **0 of 20 placed with the suffix, 16
of 20 without**, median 32 m, max 153 m. Nothing is hidden from the floor, which compares
`CIVIC_NO` and never saw the suffix; `nar_address_string()`'s default is still to keep it,
because that is what the address is, and the BC tier wants it. About 189k NAR building points
carry a suffix, so this is roughly 1% of addresses going from never-placed to usually-placed
rather than a change in the headline rate.

**Dropped requests are retried, and a request the service lost is no longer reported as an
address it had nothing for.** About one request in twelve comes back a clean HTTP 500 —
24 of 300 measured, fast-failing at a 0.23 s median against 0.59 s for a real answer — and every
one of the 24 succeeded when re-sent, 23 on the first retry. One query succeeded and then failed
three times afterwards, which is what rules the failure out as a property of the query. End to
end over those 300 addresses, `retries = 1` lost 14 requests and placed 154; `retries = 3` lost
2 and placed **164**. That is about 8 points of coverage that earlier measurements in this note
silently charged to the geolocator, and roughly 14% more wall clock to recover. Exhausted rows
now say `request failed` in `nrcan_reject` rather than `no answer`, so the transport and the
service are separable in the next measurement. The BC geocoder was measured clean over 150
consecutive requests and was not given a retry.

**The 6 survivors past 300 m are all legitimate long rural roads** where interpolation over a
sparse civic range is genuinely uncertain — and where, NAR not being ground truth, some of the
gap may be NAR's — `23 Lakeshore RD E, ORO-MEDONTE, ON` at 2733 m,
`3330 Prospect RD, PROSPECT, NS` at 738 m. Only one survivor in 204 exceeds a kilometre. That
is the residual the floors cannot reach, because the answer is on the right street in the
right town.

**`uncertainty_m` is 150, and it is not comparable to `nar_blockface`'s 176** even though it
is the smaller number. Both are p90s, but a blockface error is bounded by the length of a
block, while this is a percentile on a tail running to 2.7 km. 150 is the conservative of the
two p90s measured (115 m at n=204, 152 m at n=88 on the earlier sample), and the function's
own documentation says so.

**Query spelling was an open question and is now closed.** Sending `ST` or `Street`, `NB` or
`New Brunswick`, changes the outcome for one address in 139 over the same sample, so the tier
sends NAR's own abbreviations. The knob survives in the probe harness (`PROBE_EXPAND`) only
to re-check that. It is not entirely cosmetic in one respect: some queries return HTTP 200
with a body of `{"message": "Internal server error"}` instead of a results array, and which
spelling triggers it varies — `100 Water St, Charlottetown, PE` fails where the spelled-out
form works, and `1155 Robson Street, Vancouver, BC` fails the other way round. That one is
tied to the query, unlike the plain HTTP 500 above, which is not. Both are retried anyway;
retrying a reproducible failure costs three requests and settles it.

**As a fallback for NAR's tail it is worth much less than BC's geocoder, and this is the
number to know before reaching for it.** On the 5,000 corporations addresses,
`c("nar", "nar_interpolate")` left 495 unplaced (9.9%); the geolocator placed **40 of them —
8.1%** — lifting overall coverage from 90.1% to 90.9%. BC's service, by comparison, recovers
75 of 76. The reason is coverage rather than independence: the addresses NAR cannot place
are largely the ones no national compilation has, so the geolocator fails on the same rows,
whereas BC's provincial records genuinely hold addresses NAR's BC extract does not. Of the 495 it was asked, 127 produced no answer at all and
149 came back as a street centroid.

**Roughly half of its street-name rejections on that tail are the floor's fault, not the
service's.** 103 of the 495 were rejected on the street name, and inspecting them: only 4
involve the gazetteer rewriting the query. The rest are the *parser* handing over a dirty
street name and the service quietly cleaning it up —

| query sent | title returned | verdict |
| --- | --- | --- |
| `KING ST W SCOTIA` | `40 King Street West, City Of Toronto` | right, rejected (building name in the name) |
| `CHEM DE HARDWOOD FLAT` | `836 Chemin De Hardwood Flat, Bury` | right, rejected (type left in the name) |
| `ATHLONE AVENIUE` | `49 Athlone Avenue, City Of Brampton` | right, rejected (misspelled type) |
| `50TH` | `4943 50 Street, Red Deer` | right, rejected (ordinal vs cardinal) |
| `ST-VALLIER` | `571 Rue Saint-Vallier Ouest, Québec` | right, rejected (`ST` not expanded to `SAINT`) |

44 of the 103 have the returned name as a **whole-word subset** of the query name, 7 the
reverse, and 11 differ only by `ST`/`SAINT` or by an ordinal suffix; 52 are genuinely
unrelated streets that the floor is right to reject. So the tier's own tolerance for junk —
which is exactly what would make it valuable on the addresses the parser mangles — is being
thrown away by an equality test. **This is the ranked next step for this binding**, and it has
to be done carefully: relaxing to containment is what let `28 Silver ST, CORNER BROOK` through
in the first place, so any relaxation needs the same before/after distance measurement the
current floor got. Some of it belongs upstream in the parser instead (`ST` → `SAINT` is a
normalizer question, see the normalization note).

**Where it sits in a `method` chain.** After everything else, always. It is slower than the
local tiers by three orders of magnitude, it is the only tier whose uncertainty is unbounded,
and roughly half of what it is asked is rejected. What it is genuinely good for is not
NAR's tail but the case where there is **no NAR to fall back from**: a fresh install with
nothing downloaded, or the `not_covered` rows a single-province import produces.

## The OpenStreetMap binding

`osm_geocode()` queries **`https://maps.canada.ca/nominatim/search`**, the Nominatim instance
the Government of Canada hosts — not the volunteer-funded `nominatim.openstreetmap.org`, whose
usage policy forbids bulk geocoding. It is exported and is **not** a `geocode()` tier.

**Nothing in this section is measured yet, and that is the point of the section.**
`data-raw/probe_osm.R` exists and runs over the same `REPEATABLE (42)` sample as
`data-raw/probe_geolocator.R`, so the two services are directly comparable, but it has not been
run at scale. `nar_osm_uncertainty_m()` returns `NA_real_` rather than a plausible constant,
and `uncertainty_m` on an `osm` row is `NA` accordingly. Until that probe runs there is no
coverage figure, no p90, and no basis for placing it in a `method` chain.

**Why it is not a tier is a licence question, not an accuracy one.** OSM data is ODbL —
attribution plus share-alike, with the obligation attaching to a derived *database* — where
NAR, the BC geocoder and the geolocator are all Open Government Licence. A default tier would
fold a handful of ODbL rows into a result table and change what the caller may do with the
whole of it, silently. So the service's own licence string rides along as `osm_licence` on
every row, and using it is an explicit call. If the probe shows it recovers a useful part of
NAR's tail, the decision to make is about the licence, and it is the user's to make per
project rather than this package's to make by default.

**What the first live runs did show**, on a handful of addresses rather than a sample:

* It **refuses**, which neither other service does. Empty array for an address it does not
  have; the road at `place_rank` 26 when it has the street but not the number. Both confident
  wrong answers this note uses as examples of the geolocator's failure mode —
  `1 Rue Notre-Dame Ouest, Montreal` placed 500 km away, `28 Silver ST, CORNER BROOK` placed on
  a different street — come back correct or refused here. So the floor rejects far less than
  the geolocator's, and the number to watch when the probe runs is the **answer rate**.
* Coverage is the open question, and it is uneven by construction: OSM's Canadian addresses
  are concentrated in cities with municipal open-data imports. Downtown Vancouver, Montreal and
  Toronto all answer at building level; nothing rural has been tried.
* The French word order matters and is now handled — `1 NOTRE-DAME RUE O` returns nothing where
  `1 Rue Notre-Dame Ouest` returns the address. See the design note; it is the only place in
  the package where a query is spelled for a particular service.

At the default one request a second, a 150-address probe takes about three minutes. The rate
limit is deliberate: the instance is keyless, unmetered and exists to serve GeoView, and
nothing published says what bulk use is acceptable. `geo@nrcan-rncan.gc.ca` is the contact the
geolocator README names for bulk use, and asking is the honest step before any large run.

## Not built yet, in the order the measurements justify

### 1. Statistics Canada Road Network File (RNF)

Product **92-500-X**, annual, reference date 2025-01-01, latest release 2025-06-18,
available as shp/gml/gdb/gpkg under the Statistics Canada Open Licence. Its CRS is
**EPSG:3347 — identical to this package's storage CRS**, so no reprojection is needed at
import.

The address-range fields are `AFL_VAL`/`ATL_VAL` (from/to, left) and `AFR_VAL`/`ATR_VAL`
(right), alongside `NGD_UID, NAME, TYPE, DIR, CSDUID_L/R, CSDNAME_L/R, PRUID_L/R, RANK,
CLASS`. This would address the 3.7% of addresses whose street is absent from NAR entirely.

**The catch, and it is the design problem:** the ranges are partial — observed for some
segments, imputed for others, absent for the rest — and **the record layout carries no
provenance flag**, so a range cannot be told apart from a guess by reading the file. The
proposed remedy is to validate RNF ranges against NAR building points at import time and
store the outcome, which turns an unknowable into a measured one and gives the tier an
uncertainty figure comparable to the ones above. DuckDB's spatial extension has
`ST_LineInterpolatePoint`, `ST_LineSubstring`, `ST_LineLocatePoint`, `ST_Length`, `ST_Read`
and `ST_Azimuth`; `ST_OffsetCurve` is absent, so the left/right offset from the centreline
has to be done with azimuth plus trigonometry.

Version discovery would need an `rvest` scraper of the same shape as
`available_nar_versions()`. Guessing the download URL does not work — both
`lrnf000r25a_e.zip` and the `.gpkg.zip` form redirect to an HTML landing page.

### 2. Street or municipality centroid as a last resort

The tier below RNF: when nothing places the civic number but the street is known, return
its centroid with an uncertainty of half its length. Cheap to build on the existing
`Streets` table, and it converts a `none` into a coarse but honest answer. Deliberately
last, because it is only worth having once the tiers above it have taken everything they
can.

The geolocator measurement sharpened the case for this one: its single largest rejection
bucket, 116 of 423, is `INTERPOLATED_CENTROID` — the service found the street and declined
the civic number. That is exactly the answer this tier would return, so the same
classification would let `nar_nrcan_floors()` accept those as a `nrcan_street` method instead
of dropping them, once there is an `uncertainty_m` convention for a street centroid to
report.
