# The road network file, measured

Statistics Canada's **Road Network File** (product 92-500-X) carries an address range on each
side of each segment: `AFL_VAL`/`ATL_VAL` on the left, `AFR_VAL`/`ATR_VAL` on the right. Those
four fields are the only national source of street geometry with numbers attached that is not
NAR, and they are why `geocoding-status.md` has listed an RNF interpolation tier as the first
unbuilt pathway: **3.7% of all filings in the standing evaluation fail because the street does
not exist in NAR anywhere in the province**, and a street that exists on the ground is in the
road network file whether or not anyone has geocoded a house on it.

The blocker was never the geometry. It was this, from the same note:

> the ranges are partial — observed for some segments, imputed for others, absent for the rest —
> and the record layout carries no provenance flag, so a range cannot be told apart from a guess
> by reading the file.

**That is still true of the file, and it is no longer true of the data.** NAR's building points
are an independent second reading of the same streets, so asking how often a civic number NAR
holds falls inside the range RNF claims for its side turns the unknowable into a measured one.
It does. The answer is **89.7%**, and the rest of this note is what that does and does not buy.

Reproduce with `Rscript data-raw/probe_rnf.R` (needs `NAR_CACHE_PATH`; the stages are
selectable with `RNF_STAGES`). Measured against the 2025 RNF and the 2026-06 NAR,
2026-08-23. Stages 1-4 are the decision, and the decision they recommend is under
[Recommendation](#recommendation). **The tier was then built, and stage 5 measures the shipped
code rather than this file's own SQL** — see
[Delivered](#delivered-the-shipped-tier-2026-08-24), which is where the numbers to quote now
live, and where one claim below does not survive.

## Getting the file: the shapefile, and only the shapefile

The standing note said guessing the URL does not work. It does, but not under the path the 2021
geography pages suggest. The download form at
`https://www12.statcan.gc.ca/census-recensement/2011/geo/RNF-FRR/index-s-eng.cfm` POSTs
`lang`/`year`/`type` and answers `302` with

```
/census-recensement/2011/geo/RNF-FRR/files-fichiers/lrnf000r<YY><t>_e.zip
```

`<t>` is `a` shapefile, `g` GML, `f` file geodatabase, `p` GeoPackage. **Only `a` is published
for every release.** Releases 20, 22, 23, 24 and 25 all serve the shapefile; `p` resolves for 25
alone. An importer that reaches for the GeoPackage — which is the nicer file, one 880 MB
container instead of a shapefile's four — works this year and 404s on the archive, so the
shapefile is the contract. The zip is ~340 MB.

Version discovery still wants a scraper, but of a different page than assumed:
`https://www150.statcan.gc.ca/n1/en/catalogue/92-500-X` lists issues as `92-500-X<year>001`, back
to 2005; 2025 was released June 18, 2025. That is a cleaner surface than the geography index and
would mirror how `available_nar_versions()` already works.

An empty-body `POST` to the form returns a 344-byte body that is easy to misread as a rejection.
It is `411 — the request must be chunked or have a content length`; the redirect is in the
headers, not the body, and following it with `-L` hides the one line worth having.

## What is in it

2,251,726 features, 1,174,367 km of centreline, `NGD_UID` unique, 20 fields, EPSG:3347 — which
is already `nar_storage_crs()`, so nothing is reprojected on the way in.

**13 features are CircularStrings in the GeoPackage** and DuckDB's spatial extension refuses
them outright (`Unsupported geometry type in WKB`), failing the whole read rather than the rows.
Reading with `keep_wkb = true` and dropping WKB type code `0109000000` is what makes *that* load
work at all, and every count in this note is off the GeoPackage, so it is 2,251,713 usable below.
**The shapefile does not have this problem**: the format cannot express a CircularString, so
those 13 arrive as ordinary `LINESTRING`s along with everything else and `ST_Read` returns all
2,251,726 features. This is a second reason to take the shapefile — the first, that it is the
only format published for every release, is above — and it means the importer needs no
WKB-dropping workaround at all.

There is no provenance flag. There is also no field that could be pressed into service as one:
`CLASS` and `RANK` describe the road, not the range.

## Range presence: 71.7% of named segments

13% of segments are unnamed (ramps, service roads, unnamed rural allowances) and are not
candidates for anything. Of the rest, **71.7% carry a range on at least one side**; 62.4% of the
whole file. The spread by province is the widest number in this note — and the two denominators
diverge, because the share of segments that are unnamed varies by province too:

| province | of named segments | of all segments |
| --- | --- | --- |
| Nova Scotia (12), the best | 89.2% | 81.8% |
| Ontario (35) | 81.4% | 75.2% |
| Quebec (24) | 76.2% | 73.5% |
| British Columbia (59) | 66.9% | 59.4% |
| Manitoba (46) | 52.5% | 43.2% |
| Saskatchewan (47), the worst | 36.6% | 18.7% |

Saskatchewan is not a coverage failure to be fixed downstream — an RNF tier simply will not
serve rural Saskatchewan, and the province ordering here is close enough to NAR's own that the
tier is thinnest exactly where NAR is thinnest. **13% of present sides are degenerate**
(`from == to`, a range of one number), which the interpolator has to treat as a midpoint rather
than divide by zero.

Parity is clean but not guaranteed: left sides are mostly odd, right mostly even, and only ~0.4%
of sides mix parity between their endpoints. Parity is usable as a tie-break, not as a filter.

## Validity: 89.7%, and the side is real

200,000 NAR **building** points (reservoir sample, seed 42), each matched to the nearest
same-named RNF segment within 150 m. 98.4% find one; median 21.1 m from the centreline, p90
47.1 m — that offset is a house's setback plus half a road allowance, and it is the floor on
everything below.

Which side of the centreline the house sits on is derived geometrically, from the **local**
direction of travel (`ST_LineSubstring` at f±0.02, then the sign of the 2-D cross product), not
from the segment's endpoints — a curved block puts houses on the wrong side of a chord drawn end
to end. DuckDB's spatial extension has no `ST_OffsetCurve`, so this is done by hand.

- **95.9%** of matched points have a range on the side they are actually on.
- **89.7% of civic numbers fall inside that side's range.** This is the provenance proxy. It is
  the number that was missing.
- The derived side agrees with the range's parity **94.2%** of the time, against **7%** for the
  other side. RNF's `L`/`R` and its digitizing direction are internally consistent and need no
  flip — which is worth stating plainly, because `nrcan-geolocator.md` records the geolocator's
  own source contradicting itself on the digitizing constant (32 in one place, 33 in another) and
  warns against taking it on faith. Taken on measurement, it holds.

10.3% is the honest error rate on the fields, and it is a floor on the tier's own: a range that
does not contain a number NAR holds either was imputed, or is stale, or belongs to a different
segment of the same name. The file does not say which, and after this measurement it still does
not — but a tier can now price it.

### A mismatch is not evidence that the range is wrong

The 5.8% of parity disagreements and the 10.3% of out-of-range numbers are read above as
failures of the *range*, and that reading is only ever partly right. Three other things produce
exactly the same measurement, and none of them is a defect in the field being tested:

- **The road is not drawn in enough detail.** A divided road with two carriageways is often one
  generalized centreline, so the houses on both sides of the median sit on whichever side of the
  single line the geometry puts them, regardless of which carriageway they address. The same
  happens on a service road drawn coincident with the arterial it parallels, and anywhere the
  file's generalization drops a jog that the addressing follows.
- **The address point is inaccurate.** NAR's point can be a parcel centroid, a rooftop off the
  frontage, or a rural point far from the road it is addressed on; a house nearer the side street
  than the street it is numbered on lands on the wrong side of the right line. The 21.1 m median
  and 47.1 m p90 offsets above are the scale of this, and a narrow street is inside them.
- **The civic number is misfiled.** Parity is an addressing *convention*, and municipalities
  break it — infill, renumbering, and one-sided blocks all leave even numbers legitimately in an
  odd range.

Which is why the tier **uses parity to choose between two sides and never to veto one**. An even
number inside an odd range on the only side that carries a range is still placed there: refusing
would drop a real address to avoid an error the width of a street, and the evidence does not
support attributing the mismatch to the range in the first place. The same argument does not
extend to containment, which *is* filtered on — a number outside every range on the segment is
not off by a street width, it is a claim the segment never made.

## Accuracy: p50 24.3 m, p90 93.3 m

Interpolating along the segment and measuring to NAR's own building point, on the same sample:

| placement | p50 | |
| --- | --- | --- |
| segment midpoint (no interpolation) | 49.3 m | what you get for ignoring the range |
| plain interpolation | 34.5 m | |
| + 5% end setback | 32.1 m | keeps a first-in-range house off the intersection node |
| + 13 m perpendicular offset | **24.3 m** | p90 **93.3 m**, p95 140.1 m |

The setback and the side offset are the two corrections `nrcan-geolocator.md` observed the
geolocator's interpolator does *not* apply, and estimated at "most of a 33 m median". Applied
here they are worth ~10 m at the median — the estimate was the right size.

Error grows sharply with segment length, and **not proportionally** — which is the fact the
uncertainty model below turns on:

| segment length | n | p50 | p90 |
| --- | --- | --- | --- |
| < 100 m | 30,053 | 18.4 m | 49.2 m |
| 100–250 m | 76,615 | 21.3 m | 66.2 m |
| 250–500 m | 43,242 | 24.6 m | 93.0 m |
| 0.5–1 km | 10,666 | 44.4 m | 179.5 m |
| > 1 km | 8,681 | 113.9 m | 602.9 m |

A segment 2.5 times longer than another buys 3 m of extra median error, not 2.5 times as much:
the first two buckets are 18.4 m and 21.3 m. The floor is the offset from the centreline, not
the interpolation.

Against `nar_interpolate`'s leave-one-out p50 4.2 m / p90 41.1 m, **RNF is about six times worse
at the median.** It belongs strictly below NAR interpolation in `method` order, which is where
`geocoding-status.md` always assumed it would go. It is better than `nar_blockface`
(`uncertainty_m` 176) and better than `nrcan` (150).

## Coverage: 8.3% of RNF's ranged streets are not in NAR at all

RNF resolves 4,933 CSD keys to NAR `Streets`' 4,170, sharing 3,601; the RNF-only keys are rural
municipalities and reserves. Of 397,396 named-and-ranged RNF street/CSD pairs, **32,820 (8.3%)
have no counterpart in NAR** under either name family. RNF's `PRUID_L:CSDTYPE_L:CSDNAME_L` is
spelled in the same vocabulary as NAR's `MUN_KEY`, so the two join on
`strip_accents(upper(...))` with no crosswalk — the one piece of luck in this whole exercise.

That 8.3% is the population the tier exists for. **It is not the recovery figure**, and the
distance between the two is the point of the next two sections.

## Recovery: 96 of 379, on the addresses the package actually fails

Measured on the same 5,000-filing Corporations Canada draw `data-raw/eval_normalize.R` and
`geocoding-status.md` use — same file, same filter, same seed — so the number is comparable to
the residual that note decomposes.

`c("nar", "nar_interpolate")` leaves **379 filings unplaced (7.6%)**. The RNF pathway places
**96 of them: 25.3% of the residual, 1.9% of all filings, taking placement from 92.4% to
94.3%.** For scale, appending the `"nrcan"` tier recovers 8.1% of the same residual.

The 96 are genuine. Spot-reading them gives Ryan Reynolds Way, Celebration Dr, Frank Tompa Dr,
Grosbeak Trl — new subdivisions, streets built after NAR's last release, which is exactly the
failure mode RNF was hypothesised to cover. They are not parser errors being laundered into
coordinates by a looser matcher, which is the thing a fallback tier most often turns out to be
doing.

**This is the largest single recovery any tier has offered.** It is also where the note has to
stop being encouraging.

## The correction: accuracy on the overlap does not transfer to the residual

This is the mistake the Quebec register work had to correct after the fact — reading a quality
figure off the rows a source *shares* with NAR and assuming it describes the rows where NAR is
silent. It does not, and here it demonstrably does not.

On the 4,541 filings NAR also placed, the RNF point sits p50 28.6 m / p90 143.6 m away
(p50 27.0 m / p90 134.8 m against `nar_building` alone) — consistent with the 200,000-point
sample above, and reassuring in exactly the way that does not generalise.

The 96 recovered rows have no NAR point to compare to, so they are checked against **the filing's
own postal code**, which nothing in the RNF pathway reads. 55 of the 96 have a postal code NAR can
place a centroid for. That check is independent, and it says the residual is worse:

| | n | p50 from own postal code's NAR centroid | within 500 m |
| --- | --- | --- | --- |
| rows NAR also placed | 4,327 | 44 m | 93% |
| the 96 recovered rows | 55 | **209 m** | **73%** |

**But that pooled comparison overstates the gap, and the reason is worth recording**, because it
is the same class of error one level down. A rural postal code covers a large area, so distance
from its centroid measures the FSA and not the geocoder — and the baseline proves it: rows NAR
placed *itself*, which are right, sit p50 2,503 m from a rural-FSA centroid and only 9% within
500 m. Rural FSAs carry no signal at all. The recovered set is 16% rural (9 of 55) against the
baseline's 4.7%, so part of the pooled difference is that composition rather than any error.

Split, the like-for-like comparison is the urban one:

| | n | p50 | within 500 m | within 2 km |
| --- | --- | --- | --- | --- |
| NAR-placed, urban FSA | 4,124 | 41 m | 97% | 99% |
| **recovered, urban FSA** | 46 | **151 m** | **85%** | **93%** |
| NAR-placed, rural FSA | 203 | 2,503 m | 9% | 41% |
| recovered, rural FSA | 9 | 2,514 m | 11% | 44% |

The rural rows are indistinguishable from correct ones and prove nothing either way. The urban
rows are the finding: **still clearly worse than the overlap — 151 m against 41 m, 85% within
500 m against 97% — and three of the 46 are placed more than 2 km wrong, at 23.7 km, 16.9 km and
10.2 km.** A 24 m median with a 20 km tail is not a 24 m tier.

## The cause is visible, and it is ambiguity — not the ranges

The gross errors are not imputed ranges misfiring. They are the *right* kind of range on the
*wrong* segment. Defining a row as **ambiguous** when more than one segment of that name in that
CSD has a range containing the number:

| | share | p50 | p90 | over 1 km |
| --- | --- | --- | --- | --- |
| unambiguous (n = 1) | 84% | 25.9 m | 107.8 m | **0.1%** |
| ambiguous (n > 1) | 16% | 40.0 m | 1678.5 m | **11.7%** |

`28 Nelson St, Toronto M1J 2V3` is the archetype: the CSD "Toronto" holds many Nelson Streets,
several ranges contain 28, and containment picked one of them. The median barely moves; the tail
is entirely in the ambiguous half.

**`geocode()` already reports `n_matches`.** Refusing to answer when it exceeds 1 keeps **87 of
the 96 — still 23.0% of the residual, 1.7% of all filings** — and removes essentially the whole
gross-error tail. Nine rows are given up to make the other 87 trustworthy. That is the cheapest
trade in this note.

## `uncertainty_m`

`nar_interpolate` reports `0.5 × span_m`, which is scale-invariant because its error genuinely
is proportional to the block it interpolates across. **RNF's is not** — see the length table
above, where halving the segment barely moves the median because the floor is the ~20 m offset
from the centreline rather than the interpolation. A purely proportional model would report
absurd confidence on a 40 m block.

On unambiguous rows, against the 90th-percentile convention the other tiers use:

| model | covered | segments > 600 m | median reported |
| --- | --- | --- | --- |
| flat 110 m | 90.4% | 67.2% | 110 m |
| **`max(95, 0.35 × len_m)`** | **91.7%** | **93.1%** | 95 m |

The flat value hits the headline target and lies about long segments, which is the failure mode
`uncertainty_m` exists to prevent. **`max(95, 0.35 × len_m)`** is the recommendation.

Capping the tier at a maximum segment length was tested and **rejected**: refusing segments over
1 km keeps 97.7% of rows and moves p90 only from 134.8 m to 125.2 m. The length is already
reported through `uncertainty_m`; refusing on it as well pays rows for a number the caller
already has.

## Recommendation

Build it, with two conditions that are not optional.

1. **Refuse when `n_matches > 1`.** Without it the tier ships a 20 km error in a package whose
   worst honest tier is 176 m, and the cost is 9 rows in 5,000.
2. **`uncertainty_m = max(95, 0.35 × len_m)`**, and place the tier below `nar_interpolate` and
   above `nar_blockface` in `method` order.

One more thing the import has to do, established above rather than guessed: take the
**shapefile** — the GeoPackage exists for one release only, and it is also the format that
carries the 13 CircularStrings DuckDB refuses. The shapefile has neither problem and needs no
WKB-dropping workaround.

The validation this note performs is worth doing **at import time and storing**, which is what
`geocoding-status.md` proposed. Done per segment rather than per sample it would give each
segment its own in-range/out-of-range count against NAR's points, and a segment that NAR
contradicts is a segment the tier should decline before it is asked — a better filter than any
global rate, and one the file itself will never provide.

## Delivered: the shipped tier, 2026-08-24

`R/rnf.R` was built to both conditions, and it does **not** run the query stages 1-4 ran. It
joins on `MATCH_FOLD` rather than the plain name fold, compares the municipality against RNF's
own CSD name *as well as* through `MunAlias`, constrains the street type and direction wherever
both sides carry one, and refuses `n_matches > 1` outright instead of picking the shortest
candidate. Each of those moves the recovery figure, and not all in the same direction, so what
was delivered had to be measured separately from what was designed. `RNF_STAGES=5` does it,
through `geocode()`; it needs only a database `rnf_import()` has been run against, so unlike
stages 1-4 it costs no download.

| | design target (stages 1-4) | delivered (stage 5) |
| --- | --- | --- |
| coverage on the 5,000-filing draw | 92.4% → 94.3% | 92.4% → **94.3%** |
| recovered, of the 379 unplaced | 87 after the refusal (23.0%) | **93 (24.5%)** |
| refused as `rnf_ambiguous` | 9 | 7 |
| p50 / p90 from `nar_building`, where both answer | 25.9 m / 107.8 m | **26.0 m / 107.9 m** |
| `uncertainty_m` coverage | 91.7% | **91.8%** |
| … on segments over 600 m | 93.1% | **92.6%** |

**The tier delivers the design target and recovers six rows more than it.** The looser fold and
the direct CSD comparison are what buy them — both reach rows the probe's plain-fold,
`MunAlias`-only join never generated a candidate for — and the type and direction constraints
cost less than that. The agreement figures are for every row the tier answers rather than for an
unambiguous subset, because the shipped tier *has* no ambiguous answers to exclude; the refusal
happens before the coordinate is computed.

It costs nothing measurable: 13.4 s for `c("nar", "nar_interpolate", "rnf")` against 14.0 s for
the offline pair on the same 5,000, since it only ever sees what its predecessors left.

### The overlap-versus-residual correction survives, and is now decomposed

Stage 5 adds a third group to the postal-code check, and the third group is the point of it.
Urban FSAs only, since the rural ones carry no signal either way:

| | n | p50 | within 500 m | within 2 km |
| --- | ---: | ---: | ---: | ---: |
| NAR's own answer, on rows the tier also placed | 3,749 | 43 m | 97% | 99% |
| **the tier**, on those same rows | 3,749 | 60 m | 97% | 99% |
| **the tier**, on the rows only it placed | 46 | **149 m** | **87%** | **93%** |

The middle row holds the tier's own error constant, which is what the earlier comparison could
not do. Reading down: placing an address the tier has a *checkable* answer for costs 43 → 60 m,
and being an address NAR could not place costs a further 60 → 149 m. **The residual is harder by
more than twice what the tier's own error is worth**, which is the correction surviving contact
with the shipped code rather than an artefact of the probe's SQL.

### The refusal did not remove the gross-error tail here, and the reason matters

The section above says the ambiguous half holds essentially the whole gross-error tail, and on
the rows NAR can check that is what the split shows. **It does not carry over to the residual.**
All three of the recovered urban rows past 2 km have `n_matches == 1`, so the refusal never saw
them. Read individually they are three different things, and only one is the tier's fault:

| row | placed | cause |
| --- | ---: | --- |
| `185 Deerfield Rd, Newmarket, ON L4C 0Z1` | 23.7 km from the FSA centroid | **the check is wrong.** `L4C` is Richmond Hill; the filer's city and postal code name different municipalities. RNF put it on Deerfield in Newmarket, on a segment whose range is 165–195, which is where the filer said it was. |
| `1006 Theatre Rd, Gravenhurst, ON P1P 1R3` | 10.2 km | **the check is wrong**, the same way. `P1P` is Bracebridge. The segment is in Gravenhurst with a range of 1001–1155. |
| `10272 County Road 2, Cobourg, ON K9A 4J8` | 23.5 km | **the parse is wrong, and the tier laundered it.** The gazetteer resolves `County Road 2` to street `28`, type `HWY`, in `BAILIEBORO`; RNF then placed 10272 faithfully on that. |

Three things follow. **The postal-code check is weaker than it was treated as** — two of its
three worst accusations are a filer whose own two fields disagree, which is a thing corporate
filings do, so `p50 149 m` is a pessimistic reading of the residual and not a generous one.
**The refusal is still right and is still not sufficient**: it removes the ambiguity tail that
the overlap exposes, and nothing about it addresses a confidently wrong parse. And **the one
real error is upstream**. This is the failure mode the recovery section said the tier was not
exhibiting — a parser error laundered into a coordinate by a looser matcher — and it is
exhibiting it, at roughly 1 row in 93. A tier that reaches streets NAR does not have cannot
sanity-check its input against NAR, which is precisely what makes it the tier where a bad parse
survives. `numbered rural roads carry no street type at all` is the standing note in
`.claude/normalization.md`, and `County Road 2` is that rule meeting a gazetteer entry it should
have lost to.

## What this does not settle

- **The 89.7% is against NAR, and NAR is not ground truth.** Where the two disagree, RNF is
  sometimes the one that is right; a disagreement is a disagreement. What survives that caveat is
  the *shape* — which rows are safe, which are not, and that the split is ambiguity rather than
  imputation.
- **The postal-code check is a check, not a benchmark.** 55 rows, 9 of them rural and therefore
  uninformative, one FSA centroid each. It is enough to refute "the residual behaves like the
  overlap". It is not enough to put a number on how the residual does behave.
- **Quebec is untested separately here.** RQA now serves Quebec offline, and whether RNF adds
  anything on top of it is a question this sample is too thin to answer — see
  [`quebec-addresses.md`](quebec-addresses.md) for why a Quebec coverage share read off NAR's
  residual is the specific thing not to do.
- **The 3.7% "street not in NAR" residual and the 8.3% "pair not in NAR" coverage figure are not
  the same population**, and neither is the 24.5%. Only the last one was measured on addresses
  the package actually fails.
- **How often a bad parse reaches this tier is not measured.** One of 93 recovered rows is a
  confidently wrong parse placed confidently, found by hand rather than by a method that would
  find the others. Nothing counts them, and the postal-code check cannot: it accused two correct
  rows for every real one it caught.
