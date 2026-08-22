# Forward geocoding: what resolves, what does not, and what is not built yet

`geocode()` (`R/geocode.R`) turns an address string into a coordinate by parsing it with
`normalize_address()` and resolving the result against NAR. This note records what it
currently reaches, how the accuracy figures in its documentation were measured, and the
pathways that were sized but not built.

For *why the code is shaped the way it is*, see the `R/geocode.R` section of
`.claude/CLAUDE.md`. For the parser's own failure modes — which cap everything here, since
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

## How far NAR's points sit from an independent source

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
