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

Four independent measurements against NAR 2026-06, and a fifth against a source that is not NAR.

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

**Everything about the Quebec geocoder** comes from `data-raw/probe_qc.R`, which draws its NAR
samples `REPEATABLE (42)` stratified on whether the address carries a street direction, and its
corporations sample with the same `set.seed(20260821)` as above. It queries the live service and
is not part of any test or check.

**Agreement with NRCan's geolocator** comes from `data-raw/probe_geolocator.R`, which draws
`USING SAMPLE reservoir(n ROWS) REPEATABLE (42)` from NAR's own building-point addresses and
asks the service for each one. Figures below are the 423-address run. It queries the live
service and is likewise not part of any test or check.

**What `geocode_accept()`'s bars cost and buy** comes from stage 6 of `data-raw/probe_pvsc.R`
— `PVSC_STAGES=6`, opt-in because it re-geocodes the 40,000-address Nova Scotia sample with
`keep_refused = TRUE` rather than reusing stage 2's cached bench. It is measured there rather
than on the corporations draw for one reason: a bar that decides which answers to distrust
cannot be scored against NAR, which is the source it exists to catch mistakes about. PVSC is
the only reference in this package established to be independent of NAR; see
[`nova-scotia-pvsc.md`](nova-scotia-pvsc.md), stage 0.

**NAR is the reference here, and a reference is not ground truth.** NAR is accurate in
general, but it has its own poor and outright wrong records, so a large distance in these
tables is a *disagreement* and the geolocator is sometimes the one that is right. This matters
most in the tails: the multi-kilometre survivors are as likely to be bad NAR records as bad
geolocator answers, and the p50/p90 figures are safe only because a systematic bias would have
to be shared by both sources to survive at the median.

## Where it stands

5,000 Corporations Canada addresses. Re-measured 2026-08-24, after the parser changes the
normalization note records and with the `"rnf"` tier appended; the default offline pair is the
first column and is what `geocode()` does out of the box.

| `match_method` | `c("nar", "nar_interpolate")` | share | + `"rnf"` | share |
| --- | ---: | ---: | ---: | ---: |
| `nar_building` | 4,100 | 82.0% | 4,100 | 82.0% |
| `nar_blockface` | 297 | 5.9% | 297 | 5.9% |
| `nar_interpolated` | 224 | 4.5% | 224 | 4.5% |
| `rnf_interpolated` | — | — | 93 | 1.9% |
| `rnf_ambiguous` | — | — | 7 | 0.1% |
| `nar_no_geometry` | 4 | 0.1% | 4 | 0.1% |
| `none` | 375 | 7.5% | 275 | 5.5% |
| **placed** | **4,621** | **92.4%** | **4,714** | **94.3%** |

The offline pair was 89.1% when this note was first written; the 3.3 points since are the
parser, not the geocoder — the match fold, the leading-prose strip and the comma-free
segmentation, none of which changed a single line of `R/geocode.R`. That is worth keeping in
view when reading any tier's *marginal* contribution below: several of them were measured
against a larger residual than the one that exists now, and a tier's share of the residual falls
as the parser improves even though the tier is unchanged.

Measured on the earlier draw and not re-measured: without the interpolation tier, 84.9%, and
every exact-tier match agreed with the postal code the filer wrote — 100% of 4,244 — which is
worth stating because nothing in the query uses the postal code, so it is a free independent
check rather than a tautology.

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

### The definitional difference was tested, and it is not the parcel-versus-access-point one

The paragraph above attributes part of the ~20 m to a definition mismatch: NAR's building
point "may be the road access point or the driveway", BC's is a parcel point. BC's geocoder
exposes `locationDescriptor` — `parcelPoint`, `rooftopPoint`, `frontDoorPoint`, `accessPoint`,
`routingPoint`, or `any` — so the hypothesis is directly testable by asking for the access
point instead and seeing whether the gap closes. `data-raw/probe_bc.R` does that: 400 NAR BC
building points sampled deterministically (`hash(ADDR_GUID || '20260824')`), each queried
under all six descriptors, 378 clean under every one. Measured 2026-08-24 against the 2026-06
NAR.

**It does not close. It widens.** Distance from NAR's building point:

| requested | p25 | p50 | p75 | p90 | ≤10 m | ≤25 m |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `any` / `parcelPoint` / `frontDoorPoint` / `rooftopPoint` | 1.6 | **20.2** | 50.7 | 99.1 | **38%** | **57%** |
| `accessPoint` | 16.6 | 28.9 | 52.9 | 98.7 | 11% | 43% |
| `routingPoint` | 19.1 | 31.6 | 54.7 | 99.7 | 6% | 36% |

Paired per address, `accessPoint` is a median 5.3 m further from NAR than the default and
`routingPoint` 7.7 m further. **The package's existing default is already the closest of the
three**, so no change to what is requested is warranted, and the ~20 m stands as measured.

**Three descriptors are not distinct requests at all.** `frontDoorPoint`, `rooftopPoint` and
`parcelPoint` each returned a point *identical* to `any` on **100%** of addresses. The service
does not go looking for the kind of point named; it returns whatever main location it holds —
a parcel point for 359 of the 400, a rooftop for 19, and the access point for the 20 where it
holds no main location. Only `accessPoint` and `routingPoint` are separate points. So
`nar_bc_feature()` now reports `bc_descriptor` and `bc_accuracy` — what actually came back,
and BC's own categorical accuracy class — because asking does not mean getting and a caller
who requests `rooftopPoint` needs to know they got a parcel centroid.

**The per-address picture refutes the aggregate reading in the other direction, though.** The
default is closest on only **58%** of addresses; `accessPoint` wins on 29% and `routingPoint`
on 13%. NAR's BC building points are a *mixture* of point definitions, which is exactly what
the User Guide's hedge says and is not something a single descriptor choice can fix. There is
no BC point that NAR is trying to be.

And the p25 of 1.6 m is the lineage showing through: on a quarter of addresses the two sources
agree to within a metre and a half, which is not two independent readings converging.

## The residual that does not resolve

**7.5% now, and this decomposition was taken when it was 10.8%** — the parser changes above
have since removed a third of it, and which of the four buckets they came out of has not been
re-measured. The shares below are of the 5,000 as it stood then, and the ranking is what to
trust rather than the numbers:

- **3.7%** — the street does not exist in NAR anywhere in the province. Nothing in the NAR
  pathway can fix these; this is what the road network file is for.
- **3.8%** — the street exists but the address could not be reached even with
  interpolation, mostly end-of-run refusals and streets whose known civics are all the
  other parity.
- **1.4%** — never parsed at all: no street name or no civic number was extracted.
- the remainder — the street exists in NAR under a municipality that did not match.

Combined ceiling for the NAR-only pathway was therefore put at around 93%. The offline pair
now reaches 92.4%, so that ceiling is essentially met and the remaining headroom is in the tiers
that reach outside NAR rather than in the NAR pathway itself.

The `"nrcan"` tier reaches very little of this: appending it recovers 8.1% of the unplaced,
for the reason given in its own section below — the addresses NAR cannot place are largely
the ones no national compilation has. The road network file was the pathway that would
actually move this number, and now does: `"rnf"` recovers 24.5% of the unplaced, three times
what any online tier offers.

## What the acceptance bar costs and buys

`geocode_accept()` (`R/geocode_accept.R`) applies a bar to a result that already exists, and
the seven tests are deliberately separate rather than one `strictness` scalar — collapsing
three incommensurable failures into one number would mean inventing exchange rates. It does not
follow that the exchange rates cannot be *measured*, only that they are not constants, and
until now they had not been. This is what each test is worth.

Measured on the 40,000-address Nova Scotia sample of `probe_pvsc.R`, geocoded with the shipped
offline pair and `keep_refused = TRUE`, distances against **PVSC's own coordinate** rather than
NAR's. Baseline places 86.7%, of which 149 rows are matches the gazetteer threshold refused;
7.80% had the municipality remapped and a third of those on no attestation.

| bar | placed | p50 | ≤100 m | >1 km | >5 km | of the rows it withdrew, >1 km |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| *(no bar)* | 86.7% | 10.8 m | 92.5% | 0.91% | 0.56% | — |
| `refused = FALSE` | 86.3% | 10.8 m | 92.6% | 0.80% | 0.46% | **26.2%** |
| `attested_only` | 84.5% | 10.6 m | 92.8% | 0.69% | 0.35% | 9.3% |
| `unambiguous` | 81.7% | 10.2 m | 93.9% | 0.40% | 0.17% | 9.2% |
| `postal_code` | 86.7% | 10.8 m | 92.5% | 0.91% | 0.56% | — |
| `max_uncertainty = 100` | 82.0% | 10.2 m | 93.9% | 0.42% | 0.18% | 9.3% |
| `min_confidence = 0.9` | 83.1% | 10.5 m | 93.0% | 0.63% | 0.35% | 7.3% |
| `method = "nar_building"` | 81.6% | 10.2 m | 93.9% | 0.40% | 0.18% | 9.1% |
| all seven | 78.0% | 9.9 m | 94.3% | **0.28%** | **0.09%** | 6.6% |

The last column is the one to read first, because it is the only one that says whether a test
is *aimed* at anything. The base rate is 0.91%, so a test whose withdrawn rows are 9% gross
error is finding the tail nine times better than chance — and `refused = FALSE` finds it
**twenty-nine times better than chance**.

**`refused = FALSE` is the best value here by a wide margin, and that is a result about the
gazetteer's threshold rather than about the bar.** It spends 149 rows to remove 39 errors past
a kilometre and 35 past five — 3.8 rows per gross error against 10.7 to 13.7 for every other
test that fires. What that measures is that the threshold `keep_refused = TRUE` reaches past is
well placed: the matches it turns away really are a quarter gross error. So the pair is not a
round trip. Resolving with `keep_refused = TRUE` and then taking one pass with `refused = FALSE`
and one without is exactly the workflow the vignette recommends, and this is the number that
justifies it — the refused rows are worth *looking at*, and worth dropping if nobody will.

**`postal_code` never fires on this corpus, and that is a property of the input.** PVSC renders
no postal code, so `POSTAL_CODE` is `NA` on every row and the test is silent by design rather
than vacuous by defect. It prices nothing on an address list that does not state postal codes,
which is most of them; it is the one test here whose value cannot be read off this table.

**Three of the tests are largely the same test.** `unambiguous`, `max_uncertainty = 100` and
`method = "nar_building"` each spend about 2,000 rows and each land the >1 km count within ten
of 130 — because in Nova Scotia the rows that are ambiguous, the rows carrying uncertainty over
100 m and the rows below the exact tier are heavily the same rows. They are not coextensive
(stacking `unambiguous` with `max_uncertainty = 100` costs about two points more than
either alone and buys a tenth of a point of tail), but a caller who stacks all three is paying three times for
most of one thing. Pick one.

| combination | placed | p50 | ≤100 m | >1 km | >5 km |
| --- | ---: | ---: | ---: | ---: | ---: |
| `refused` + `attested_only` | 84.3% | 10.6 m | 92.8% | 0.67% | 0.34% |
| `refused` + `unambiguous` | 81.4% | 10.2 m | 93.9% | 0.38% | 0.14% |
| `refused` + `attested_only` + `unambiguous` | 79.5% | 10.1 m | 94.1% | 0.33% | 0.10% |
| `unambiguous` + `max_uncertainty = 100` | 79.6% | 10.0 m | 94.1% | 0.31% | 0.09% |
| all seven | 78.0% | 9.9 m | 94.3% | 0.28% | 0.09% |

The seven individual spends sum to 8,376 rows and the seven together cost 3,461, so the tests
overlap more than half. **`refused` + `attested_only` + `unambiguous` reaches 79.5% placed and
0.33% / 0.10% — within a rounding error of what all seven buy, for 1.5 points more coverage.**
That is the combination to recommend when one has to be recommended.

**What the whole dial is worth, and what it is not.** Turning everything on trades 8.7 points
of coverage for a 3.2× cut in errors past a kilometre and a 6.2× cut past five. That is a real
instrument and it is not a guarantee: **87 rows in the sample are more than a kilometre from
PVSC's point after failing none of the seven tests**, and 28 are more than five. Every caveat
that applies to `n_matches` applies to the bar as a whole — these tests describe the candidates
that were found and the evidence the parse rested on, and none of them can ask whether the
search looked in the right place. The bar narrows the tail; nothing here removes it.

**The exchange rates, stated as rates so they are read as measurements and not as constants.**
Rows withdrawn per error past a kilometre removed, from the baseline: `refused = FALSE` 3.8,
`attested_only` 10.7, `max_uncertainty = 100` 10.8, `unambiguous` 10.9, `method` 11.0,
`min_confidence = 0.9` 13.7, all seven 15.2. These are Nova Scotia, this sample, this NAR
release. The ordering is what to carry elsewhere; the numbers are not.

A caution on reading the table to the row: two runs of stage 6 differed by one row in 34,680,
so the placed counts are reproducible to about a hundredth of a point and not exactly. Nothing
below the second decimal in the percentage columns is meaningful.

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

**Which reference point comes back is now reported, not assumed.** `bc_descriptor` and
`bc_accuracy` ride along on every row. BC honours a `locationDescriptor` request only for
`accessPoint` and `routingPoint`; the other three name a point it may not hold and it answers
with its main location instead. See the descriptor measurement above.

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

## The Quebec register tier

`rqa_import()` and the `"rqa"` tier. Offline, Quebec-only, and **not in the default
`method`** — the tables it reads exist only if the import was run, and a tier that appears
or disappears depending on that would be worse than an explicit one. `geocode()` checks for
them up front rather than when the tier is first reached, because whether a tier runs at all
depends on what its predecessors left unplaced: a missing import would otherwise surface on
one batch of addresses and not the next.

The register is Quebec's *Répertoire québécois des adresses*, the same source NAR's Quebec
rows are derived from and the same one `qc_geocode()` queries over the wire — but published
in full, and about 750,000 certified addresses larger than NAR's Quebec extract. What the
gap is made of, why the import is a separate table rather than a merge, and how the numbers
below were produced: [`quebec-addresses.md`](quebec-addresses.md).

4,000 Corporations Canada filings with a Quebec address, seed 20260821, NAR 2026-06:

| | placed | placed on a *register* point |
| --- | ---: | ---: |
| `c("nar", "nar_interpolate")` | 88.5% | 82.7% |
| `c("nar", "rqa", "nar_interpolate")` | **90.1%** | **89.1%** |

**The second column is the result, not the first.** The tier places 258 filings; only 62 of
them were unplaced before. The other 196 were being interpolated between two neighbours, and
the tier replaces that guess with the register's own coordinate — a median of 26 m away
(p90 102 m). It costs nothing measurable: 10.0s against 10.1s for the batch, since it only
ever sees the rows NAR left behind.

**Where it sits in a `method` chain.** Below `"nar"` and above `"nar_interpolate"` — a
register point, however coarse, beats an interpolated one, and loses to a NAR building
point. It is not a fallback in the sense the online tiers are.

`match_method` carries the register's own positional class — `rqa_building`, `rqa_geocoded`,
`rqa_uncertain`, `rqa_lot`, `rqa_other` — and `uncertainty_m` is filled in only for
`rqa_building`. Nothing here has measured what `Géocodée` or `Incertaine` are worth on the
ground; the two non-zero figures in the table at the top of this note *were* measured, and
an invented third would be indistinguishable from them. The 26 m median above is a
disagreement with interpolation, not an accuracy figure — neither point is ground truth.

## The Quebec geocoder binding

`qc_geocode()`, `qc_reverse_geocode()`, `qc_validate()`, and the `"qc"` tier
`geocode(method = )` can name. A binding to the MRNF's Esri `GeocodeServer` over the
*Répertoire québécois des adresses*, published CC-BY, keyless, with a 1000-address batch
endpoint. All figures below come from `data-raw/probe_qc.R`; run it with `PROBE_PART=render`,
`agree` or `tier`.

**How the query is spelled decides whether the service works at all.** This is the single
largest effect measured anywhere in this package. The locator's reference strings are
French-canonical — `Rue Notre-Dame Ouest` — and `nar_address_string()` renders NAR's own
`NOTRE-DAME RUE O`. Over 400 NAR Quebec addresses with building points, half of them carrying
a direction:

| rendering | civic | street only | unmatched |
| --- | --- | --- | --- |
| `NOTRE-DAME RUE O` — `nar_address_string()` | 31.5% | 4.0% | 64.5% |
| NAR order, direction spelled out | 58.0% | 3.0% | 39.0% |
| FR order, direction spelled out | 58.8% | 14.2% | 27.0% |
| NAR order, type and direction spelled out | 95.0% | 3.5% | 1.5% |
| **FR order, type and direction** — `nar_qc_query()` | **95.5%** | **3.5%** | **1.0%** |

So the *abbreviations* are what break it and the word order barely matters: the direction is
worth 26 points, the street type another 37, the order under one. The order is used anyway
because it is free and it is the form the service answers in, which keeps the floor comparing
like with like.

**And the failure is silent.** `1 RUE NOTRE-DAME O, MONTREAL` does not come back empty; it
comes back as a *street centroid scoring 92.4* where the correct civic point scores 82.5. A
binding that read the score and skipped the spelling would report high confidence in an answer
several hundred metres away, on 68% of Quebec.

**`Score` carries no positional information.** Over the same 400 addresses the correlation
between score and distance from NAR's building point is **Spearman 0.018**. Civic matches ran
75.9–86.2 with a median of 83.0; street-only answers ran 75.8–95.2 with a median of **87.0** —
higher. A `min_score` threshold here removes correct addresses before it removes street
centroids, which is the opposite of what it does for BC, so `min_score` defaults to 0 and the
parameter documentation says to leave it there. `Loc_name` (`RQA_Adresse` vs `RQA_Rue`) is the
precision field; `Addr_type` is `Feature` for both and separates nothing.

**It agrees with NAR to within a metre, and that is not good news.** On the 382 of 400
addresses it resolved to a civic point, the median distance from NAR's own building point is
**0.9 m**, p90 13.3 m, p99 61.0 m, nothing over 500 m. Compare the BC geocoder's `nar_building`
p50 of 19.8 m and the geolocator's tails. The explanation is in the locator names: `RQA_*` — it
is serving the Répertoire, which is also what NAR's Quebec records are built from. **This is
shared lineage, not accuracy**, and it means the Quebec service cannot settle NAR's Quebec
accuracy any more than the BC geocoder can settle BC's. `qc_validate()` says so in its own
documentation. It remains useful as a *fallback*, where sharing an upstream is harmless. The
lineage has since been measured directly against the Répertoire, over 2.5 million paired
addresses rather than 400 — median 21 cm, and NAR carrying RQA's coordinate verbatim on the
classes RQA interpolated. See [`quebec-addresses.md`](quebec-addresses.md).

**As a fallback it is worth roughly what the geolocator is.** On 600 Quebec addresses from the
corporations file the NAR pathway placed 81.0%; with `method = c("nar", "nar_interpolate",
"qc")` that goes to **83.3%** — 12.3% of what NAR left unplaced, all of it at civic level
(`qc_address`), none at street level. That is well short of BC's near-half recovery, which
fits: Quebec's residual is dominated by the *parser*, not by NAR coverage. Those two
percentages were measured before the gazetteer's match fold, which took Québec's Part B join
rate from 68.2% to 75.5% — the NAR pathway's share is now higher and this tier's marginal
recovery correspondingly smaller. Re-run it before quoting the 12.3%. NAR's Quebec records
carry building points on 99.8% of rows, and the Part B join rate for Québec is 75.5% against
88.3% nationally — see the normalization note. **Fixing the Quebec parse is worth several times
what this tier is**, and the tier does not substitute for it.

**Four response-shape traps**, each captured as a fixture rather than described, in
`tests/testthat/fixtures/qc-*.json`: the batch answers out of order (`ResultID`, not position);
the `Latitude`/`Longitude` attributes are French-locale strings with comma decimal marks and
are empty for street matches, so coordinates come from `location`; only the batch endpoint
populates `Loc_name`, so it is used even for one address; and the reverse endpoint reports no
distance, so `qc_reverse_geocode()` measures it in EPSG:3347.

**This is the only online reverse geocoder in the package.** BC's, the geolocator's and
Nominatim's bindings here are forward only.

## The OpenStreetMap binding

`osm_geocode()` queries **`https://maps.canada.ca/nominatim/search`**, the Nominatim instance
Natural Resources Canada hosts — not the volunteer-funded `nominatim.openstreetmap.org`, whose
usage policy forbids bulk geocoding. It is exported and is **not** a `geocode()` tier.

**Nothing in this section is measured yet, and that is the point of the section.**
`data-raw/probe_osm.R` exists and runs over the same `REPEATABLE (42)` sample as
`data-raw/probe_geolocator.R`, so the two services are directly comparable, but it has not been
run at scale. `nar_osm_uncertainty_m()` returns `NA_real_` rather than a plausible constant,
and `uncertainty_m` on an `osm` row is `NA` accordingly. Until that probe runs there is no
coverage figure, no p90, and no basis for placing it in a `method` chain.

**Why it is not a tier is a licence question, not an accuracy one.** OSM data is ODbL —
attribution plus share-alike, with the obligation attaching to a derived *database* — where
NAR and the road network file are the Statistics Canada Open Licence, and the BC geocoder and
the geolocator the Open Government Licence. A default tier would
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

### 1. Statistics Canada Road Network File (RNF) — **built and measured as shipped, 2026-08-24**

`rnf_import()` and the `"rnf"` tier ship in `R/rnf.R`. Everything below is the measurement that
justified building it and the shape the recommendation took; the tier implements both
conditions. **The shipped tier has now been measured end to end** — `RNF_STAGES=5` runs it
through `geocode()` rather than through the probe's own SQL, which matters because the two are
not the same query. It delivers the design target:

| | design target | delivered |
| --- | --- | --- |
| coverage on the 5,000-filing draw | 92.4% → 94.3% | 92.4% → **94.3%** |
| recovered, of the 379 unplaced | 87 (23.0%) | **93 (24.5%)** |
| p50 / p90 from `nar_building` | 25.9 m / 107.8 m | **26.0 m / 107.9 m** |
| `uncertainty_m` coverage | 91.7% | **91.8%** |

The tier recovers six rows *more* than the design target, because it joins on the match fold and
compares the CSD directly where the probe did neither, and it costs 13.4 s against the offline
pair's 14.0 s on the same batch. Two findings in that run do not appear below and are in
[`road-network-file.md`](road-network-file.md): the overlap-versus-residual gap, now decomposed
against a third baseline into what the tier costs (43 → 60 m) and what the residual costs
(60 → 149 m); and the fact that **the ambiguity refusal did not remove the gross-error tail on
the recovered rows** — all three survivors past 2 km have `n_matches == 1`, two of them are the
postal-code check accusing a filer whose city and postal code disagree, and the third is a bad
parse the tier placed faithfully.

Product **92-500-X**, annual, reference date 2025-01-01, latest release 2025-06-18, under the
Statistics Canada Open Licence. Its CRS is **EPSG:3347 — identical to this package's storage
CRS**, so no reprojection is needed at import. The address-range fields are `AFL_VAL`/`ATL_VAL`
(from/to, left) and `AFR_VAL`/`ATR_VAL` (right). This is the pathway for the 3.7% of addresses
whose street is absent from NAR entirely.

The catch was that the ranges are partial — observed for some segments, imputed for others,
absent for the rest — and **the record layout carries no provenance flag**, so a range cannot be
told apart from a guess by reading the file. **That has now been measured against NAR rather
than reasoned about**, and the file, the tier's coverage, its accuracy and its uncertainty figure
are all written up in
[`road-network-file.md`](road-network-file.md) with `data-raw/probe_rnf.R` to reproduce them.
The headline numbers:

* **89.7%** of NAR civic numbers fall inside the range RNF claims for the side the house is
  actually on — the provenance proxy the flag does not provide. The geometrically derived side
  agrees with the range's parity 94.2% of the time against 7% for the other side, so RNF's
  `L`/`R` and its digitizing direction need no flip.
* **71.7% of named segments carry a range**, from NS at 89.2% down to SK at 36.6%.
* Interpolating with a 5% end setback and a 13 m side offset lands **p50 24.3 m / p90 93.3 m**
  from NAR's own building point — about six times worse than `nar_interpolate` at the median, so
  the tier belongs below it and above `nar_blockface`. The setback and offset are worth ~10 m at
  the median, which is the correction `nrcan-geolocator.md` predicted would be most of the
  geolocator's own 33 m.
* On the same 5,000-filing draw the residual above is decomposed from, **the pathway places 96 of
  the 379 unplaced filings — 25.3% of the residual, 92.4% → 94.3%**, against the `"nrcan"` tier's
  8.1%. The largest recovery any tier has offered.
* **But accuracy on the overlap does not transfer to the residual.** Checked against the filing's
  own postal code — which the pathway never reads — the recovered rows sit p50 151 m from their
  FSA centroid against 41 m for rows NAR also placed, 85% within 500 m against 97%, and three of
  those 46 urban-FSA rows are placed >2 km wrong. (Rural FSAs are excluded from that comparison,
  and the *baseline* is why: rows NAR placed itself sit p50 2,503 m from a rural centroid, so the
  measure has no resolution there.) The cause is **ambiguity, not imputation**:
  rows where more than one same-named segment in the CSD contains the number run p90 1678 m with
  11.7% over 1 km, against p90 108 m and 0.1% for the rest.

**It was built with two conditions**, both of which the measurement forced: refuse when
`n_matches > 1` (which costs 9 of the 96 rows and removes the entire gross-error tail — the tier
labels those `rnf_ambiguous` and returns the count without a coordinate),
and report `uncertainty_m = max(95, 0.35 × len_m)`, which covers 91.7% overall and 93.1% of
segments over 600 m where a flat 110 m covers 90.4% and 67.2%. Per-segment validation against NAR
stored at import time — the original proposal — remains the better filter than any global rate.

Two import facts that cost time to establish: the download URL *can* be constructed, as
`…/2011/geo/RNF-FRR/files-fichiers/lrnf000r<YY><t>_e.zip`, but **only the shapefile (`a`) is
published for every release** — the GeoPackage resolves for 2025 alone, so an importer that
prefers it breaks on the archive. And **13 features are CircularStrings in the GeoPackage**,
which DuckDB's spatial extension refuses in a way that fails the whole read — but the shapefile
format cannot express one, so it spells the same 2,251,726 features as plain LINESTRINGs and no
WKB-dropping workaround is needed. Two independent reasons for the same choice. Version
discovery was expected to want an `rvest` scraper of
`www150.statcan.gc.ca/n1/en/catalogue/92-500-X`, which lists the issues as `92-500-X<year>001`;
`rnf_latest_release()` HEAD-probes the constructed URLs backwards from the current year instead,
which needs no HTML and cannot be broken by a page redesign.

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
