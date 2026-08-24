# Nova Scotia's PVSC addresses: an independent witness, and what NAR's points are made of

Every accuracy figure elsewhere in `inst/notes/` is measured against NAR, and
every one of them therefore carries the same disclaimer: NAR is not ground
truth, so a gap is a *disagreement* and NAR is sometimes the side that is wrong.
[`nar-consistency.md`](nar-consistency.md) escaped that once by finding a
contradiction *inside* a NAR row — its postal code against its own coordinate —
but that reaches only the rows where the two disagree, and it has no third side
to appeal to.

Nova Scotia's Property Valuation Services Corporation publishes, through the
province's DataZONE portal, a point and a **pre-split** address for every
improved property it assesses. PVSC assesses property; it does not maintain the
province's civic addressing. That distinction is the whole question, because an
assessment body is exactly the kind of organisation Statistics Canada draws on
when it builds NAR — so before any measurement below means anything, PVSC has to
be shown *not* to be one of NAR's own sources.

The section on provenance does that, and the answer turned out to be worth more
than the question. PVSC is independent of NAR. But **NAR's Nova Scotia
coordinates are the province's E911 file re-datumed**, which rules that file out
as a check on NAR in exactly the way [`quebec-addresses.md`](quebec-addresses.md)
rules out RQA for Quebec.

This note records what the comparison says. `data-raw/probe_pvsc.R` reproduces
all of it.

## The source

| | rows | no coordinate | no civic number |
| --- | ---: | ---: | ---: |
| [Residential Dwelling Characteristics](https://www.thedatazone.ca/d/a859-xvcs) (`a859-xvcs`) | 386,186 | 26,902 (7.0%) | 4,556 |
| [Commercial Building Characteristics](https://www.thedatazone.ca/d/9ac6-zg6i) (`9ac6-zg6i`) | 42,382 | 3,874 (9.1%) | 11,276 |

428,568 rows in total; 384,752 carry a civic number, a street and a coordinate;
**372,878 distinct address+point pairs** after de-duplication. Compare NAR's
537,686 Nova Scotia addresses, of which 99.4% are building points — the highest
building share of any province, which is what makes NS the right place to run
this comparison at all.

The de-duplication is load-bearing and not cosmetic. PVSC emits one row per
**living unit**, not per address — its own description warns that a multi-unit
parcel repeats the assessment account — so counting rows would weight an
apartment block by its unit count and quietly turn every figure below into a
statement about Halifax.

Two things the file is not. It covers **improved** properties only, so vacant
civic addresses are absent by construction; and its coordinate is a per-record
point rather than a parcel centroid — 347,306 of the 359,284 residential rows
that carry a coordinate hold a point no other row shares, and the duplicates are
the multi-unit rows.

### Licence

The Socrata metadata carries no `licenseId`; the licence hides in
`metadata.custom_fields.["License/Attribution"]`. It is the **Open Data &
Information Government Licence — PVSC & Participating Municipalities** v1.0
([text](https://www.pvsc.ca/en/home/datazone/datazone-license.aspx)), an OGL
variant: worldwide, royalty-free, perpetual, **commercial use allowed**, the
only condition being attribution.

That matters for one specific reason. `osm_geocode()` is exported but is not a
tier, and the reason is the ODbL licence rather than accuracy. PVSC's licence
composes with the OGL and CC-BY sources already here, so **nothing in the
licence would stop PVSC becoming a tier**. Whether it should is a question about
value, taken up at the end.

## Is PVSC one of NAR's sources?

A fair suspicion, and it has to be settled before anything else here counts.
Property assessment is the same role BC Assessment plays in British Columbia,
and where a register turns out to be carrying its donor's coordinates the
comparison collapses: [`quebec-addresses.md`](quebec-addresses.md) found exactly
that for Quebec, where NAR sits **0.21 m** from RQA across 2.5 million
addresses, 72.3% of them inside a metre. That is not two sources agreeing; it is
one coordinate round-tripped through a projection.

### What Statistics Canada documents

NAR's user guide says its addresses "are extracted from Statistics Canada's
**Building Register** and were validated by two independent data sources". The
[Statistical Building Register](https://www23.statcan.gc.ca/imdb/p2SV.pl?Function=getSurvey&SDDS=5380)
is where the sources are named, and it splits them into two roles:

> The national Canada Post Point-of-Call data file along with the provincial 911
> emergency data files represent the main administrative data files that are
> used for the **universe** of both buildings and building units. Other types of
> data files such as property assessment roles and land registries,
> hydro-electricity companies files, and provincial driver's license data files
> are used to **complement** information from the main sources.

So property assessment rolls — PVSC in Nova Scotia, BC Assessment in British
Columbia — *do* feed NAR, but as attribute sources, populating "building type,
building name and building unit usage". `BU_USE` and `BU_N_CIVIC_ADD` are those
attributes, and they are 100% populated in every province. The **universe and
the geocoding** come from Canada Post and the provincial 911 file, and the same
page notes each source is separately "geocoded to the spatial data
infrastructure" by StatCan rather than carried over at the donor's coordinate.

In Nova Scotia the 911 file is the **Nova Scotia Civic Address File**
([NSCAF](https://data.novascotia.ca/Municipalities/Nova-Scotia-Civic-Address-File-Civic-Points/tntn-er5g),
484,178 civic points, Nova Scotia OGL), which GeoNOVA calls the province's only
addressing authority.

### What the coordinates say

Documentation is not evidence, so both candidates were measured the way
`quebec-addresses.md` measured RQA: joined on civic number + street name +
street type, keys required unique province-wide on **both** sides, which
sidesteps the municipality — the one field NS is known to disagree on.

| pair | n | p50 | < 1 m | exactly equal |
| --- | ---: | ---: | ---: | ---: |
| NAR ↔ **NSCAF** | 361,204 | **1.04 m** | 2.1% | 0 |
| NAR ↔ PVSC | 306,268 | 11.50 m | 1.21% | 0 |
| NSCAF ↔ PVSC | 326,697 | 11.88 m | 1.31% | 0 |
| *(Quebec: NAR ↔ RQA, for scale)* | 2,512,836 | 0.21 m | 72.3% | — |

The NAR/NSCAF row is not a distance distribution at all. **95.2% of the 361,204
pairs land in a single 1–2 m bucket**; p10 is 1.029 m and p75 is 1.060 m, an
interquartile range of three centimetres. Subtract one constant vector — NAR
sits **0.156 m east and 1.004 m north** of NSCAF, magnitude 1.016 m — and the
residual is p50 **3.5 cm**, p95
10.4 cm, while the magnitude drifts smoothly from 0.990 m in the south of the
province to 1.034 m in the north. A constant offset with a latitude gradient and
centimetre residuals is a **datum transformation between NAD83 realizations**,
not two organisations placing a point on the same house.

> **NAR's Nova Scotia geometry is NSCAF's geometry.** This is the Quebec result
> again with a different donor, and it carries the same warning: NSCAF cannot
> check a NAR coordinate in Nova Scotia, because it *is* the NAR coordinate.

PVSC is the opposite case. It sits ~11.5 m from NAR and ~11.9 m from NSCAF — the
same distance from both, which is what a genuine third reading looks like — with
no spike at zero anywhere in its distribution.

The address inventories corroborate that, though only in one direction, and the
asymmetry is worth stating rather than glossing. Of PVSC's 352,393 distinct
civic keys only 8.3% are absent from NAR, which on its own is equally consistent
with PVSC being a donor and with NAR simply being comprehensive. The direction
that discriminates is the other one: of NAR's 455,609 keys, **29.1% are absent
from PVSC**, and a donor cannot leave three tenths of the recipient unexplained.
Even so, the set overlap is corroboration; the coordinates are the evidence.

So the suspicion was right about the mechanism and wrong about the source. PVSC
does contribute to NAR — its assessment attributes are in the file — but not the
address list and not the point, and the measurements below stand.

## What it says about location

The headline, and the one number in this package not measured against NAR.
Geocoding 40,000 sampled PVSC addresses through the shipped pipeline and
measuring to PVSC's own point, over the 32,757 that resolve to an **exact,
unambiguous NAR building point**:

| p50 | p75 | p90 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 10.3 m | 23.9 m | 58.9 m | 125.8 m | 650 m |

| ≤10 m | ≤25 m | ≤50 m | ≤100 m | ≤500 m | ≤1 km |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 48.9% | 76.1% | 88.2% | 93.9% | 98.6% | 99.45% |

**This is the strongest accuracy result in the package**, and the provenance
section above is what licenses the claim. Two independently produced building
points for the same house agree to within 10 m half the time and within 50 m in
seven cases out of eight. Whatever else is wrong with NAR in Nova Scotia, its
building points are not systematically displaced — and because PVSC is a third
reading rather than a donor, that conclusion rests neither on NAR agreeing with
itself nor on NAR agreeing with the file it was built from.

Residential and commercial behave almost identically (p50 10.2 m and 12.7 m;
≤50 m 88.3% and 86.1%), which is worth stating because it is the one place a
difference would have been easy to believe.

The tiers below the exact one are much worse, and the gap is larger than their
descriptions suggest: blockface matches (n=63) run p50 108.7 m, and interpolated
matches (n=1,944) p50 38.2 m but **p90 2.3 km**. A blockface distance is not
comparable to a building one — see [`nar-database.md`](../../.claude/nar-database.md) —
but the interpolation tail is not a units problem.

## What it says about the tail, which is the finding

0.55% of those "exact, unambiguous" matches are more than a kilometre from
PVSC's point, and 0.31% are more than five kilometres. In a province 575 km
long, the worst is 386 km — Cape Breton answering for Yarmouth.

Splitting on whether the gazetteer **kept** the community name PVSC supplied or
**remapped** it to a different mailing municipality separates the population
almost perfectly:

| | n | p50 | p99 | >1 km | >5 km |
| --- | ---: | ---: | ---: | ---: | ---: |
| community name kept | 29,931 | 10.3 m | 527 m | 0.28% | **0.05%** |
| community name remapped | 2,826 | 11.1 m | 31.9 km | 3.47% | **3.01%** |

**85% of every error beyond 5 km is a remap.** The remap is not
straightforwardly wrong — its median is 11.1 m, so most of the time it is doing
exactly the job it exists for, routing a rural community to the mailing
municipality NAR files it under. It is the *tail* that is poisoned: when the
community name fails to resolve, the search widens, finds the only `Wildwood Dr`
in the province, and returns it as a unique match.

Which is the sharp edge here:

> `n_matches == 1` is not a safety guarantee. It means one candidate was found,
> not that the right one was among the candidates searched.

[`road-network-file.md`](road-network-file.md) already established that refusing
on `n_matches > 1` is *necessary and not sufficient*. This is the same lesson
arriving from the opposite direction, and for the first time with a rate on it:
in Nova Scotia, **one exact unambiguous match in 180 is more than a kilometre
wrong**, and the parse — not the coordinate — is where the evidence for that
lives.

`uncertainty_m` does not catch it. It reports **0 m for all 32,754** of these,
including the 386 km one. That is not a bug so much as a mis-description: the
field reports the *spread of the candidates found*, and where exactly one was
found the spread is genuinely zero. It says nothing about whether the search
looked in the right place. Ambiguous matches, by contrast, do get a large
uncertainty and the field works as intended there.

## What it says about the parser

PVSC's split components are **labels**, which makes this a corpus in the sense
[`deepparse.md`](deepparse.md) uses the word. Over 25,000 sampled addresses:

| civic no | suffix | street name | street type | direction | all five |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 99.94% | 99.94% | 97.12% | 98.86% | 98.70% | 96.37% |

Two cautions before anyone quotes those. The string being parsed is rendered
from the same fields being scored, so this measures whether the parser can
invert *PVSC's* conventions — a real test, since they differ from NAR's (PVSC
writes `HIGHWAY 1` where NAR writes `1` + `HWY`, and Cape Breton writes the same
road as `NO 3` + `HWY`; both are folded onto the gold side before scoring, or
the parser loses four points to a spelling difference) — but a weaker one than
the held-out corpora in `deepparse.md`.

More importantly: **the 2.88% street-name "disagreement" is not an error rate.**
Most of it is the gazetteer doing its job, correcting PVSC's spelling to NAR's —
`FERGUSONS` → `FERGUSON`, `MCDOUGAL` → `MCDOUGALL`, `PARRISH` → `PARISH`,
`BORGALDS POINT` → `BORGELS POINT`. Counting those as parser failures would
punish the feature.

### The two families that are real defects

Both are visible in the parse alone, without looking at a coordinate:

**Municipality truncated** (2.32% of rows). A multi-word community name is
split, the leading tokens spill into the street or the type, and only the tail
survives as the municipality. Nova Scotia is disproportionately exposed because
its community names are so often multi-word:

| input | parsed street | parsed type | parsed municipality |
| --- | --- | --- | --- |
| `1 LAYTON DR, HOWIE CENTRE` | `LAYTON` | `DR` | `CENTRE` |
| `4 OCEANVIEW DR, PORT LORNE` | `OCEANVIEW DR` | `PORT` | `LORNE` |
| `7006 HIGHWAY 105, UPPER RIVER DENYS` | `105` | `HWY` | `RIVER DENYS` |
| `1236 HIGHWAY 3, MIDDLE EAST PUBNICO` | `3 MIDDLE` | `HWY` | `PUBNICO` |

**Spurious street direction** (1.14%). A street whose *name* genuinely begins or
ends with a compass word has it stripped into `STREET_DIR`. NAR itself carries
these with the compass word inside the name — `West Jeddore RD` with an empty
direction, 206 addresses — so the strip breaks the join outright:

| input | parsed name | parsed dir | NAR holds |
| --- | --- | --- | --- |
| `805 WEST BROOK RD` | `BROOK` | `W` | `West Brook` + `RD` |
| `1772 WEST JEDDORE RD` | `JEDDORE` | `W` | `West Jeddore` + `RD` |
| `211 EAST GREEN HARBOUR RD` | `GREEN HARBOUR` | `E` | — |
| `2733 OHIO EAST RD` | `Ohio` | `E` | — |

**What they cost is matches, not metres**, and the distinction took a correction
to get right. The vivid examples — `459 NORTH RIVER RD` landing 165 km away —
made it look as though these families drove the gross-error tail. They do not:
they account for 2% of errors beyond 1 km and **none at all** beyond 5 km. The
far tail is the remap, measured above. What these families actually do is stop
the address resolving:

| | placed |
| --- | ---: |
| clean (n=38,768) | 88.4% |
| municipality truncated (n=927) | 51.0% |
| spurious direction (n=456) | **18.4%** |

Together, 3.08% of rows, and **13.0% of everything the pipeline fails to place
in Nova Scotia**. A spurious direction is close to fatal — five rows in six go
unplaced.

## What it says about coverage

Stripping the community name and asking whether NAR carries that street and
number *anywhere* in the province separates "NAR does not have it" from "the
gazetteer could not route it":

- **90.6%** — NAR carries the street and number somewhere in NS (8.7% of those ambiguously)
- **9.4%** — no counterpart anywhere in NAR
- of those absent, RNF at least knows the street for **43.1%**

So roughly **one PVSC address in eleven has no NAR counterpart at all** — some
new subdivisions (`75 CHIPSTONE CLO, HALIFAX`), some renamed streets
(`5221 CORNWALLIS ST, HALIFAX`, renamed since NAR's snapshot), a good deal of
rural Cape Breton.

Against the same 40,000-row sample, tier by tier:

| tier | places | p50 | ≤100 m | >1 km |
| --- | ---: | ---: | ---: | ---: |
| `nar` exact | 82.2% | 10.3 m | 93.9% | 0.55% |
| `nar_interpolate` | 27.3% of the misses | 38.3 m | 71.0% | 11.05% |
| `rnf` | 34.8% of the misses | 68.0 m | 61.8% | 8.62% |

RNF reaches 934 rows interpolation cannot, at p50 75.7 m — consistent with
[`road-network-file.md`](road-network-file.md)'s finding that it is the largest
recovery any tier offers, now confirmed against a reference that is not NAR.

## What to do about it

Ranked, and the order is deliberate — the first two are worth more than the
third.

1. **Fix the spurious direction.** 1.14% of Nova Scotia rows, 82% of them
   unplaced, and the fix is bounded: do not strip a leading or trailing compass
   word into `STREET_DIR` when NAR (or RNF) carries a street whose *name*
   contains it in that municipality. The evidence is already in the database.
   See [`normalization.md`](../../.claude/normalization.md) before touching it.

2. **Stop reporting `uncertainty_m` as 0 for a unique match whose municipality
   was remapped.** The remap is recoverable information — `normalize_address()`
   already knows it happened — and it multiplies the >5 km rate by 60. It does
   not need a new measurement, only for the number already computed to be
   carried through to the output.

3. **Municipality truncation** (2.32%, half of them unplaced) is the harder of
   the three, because the fix is a gazetteer question rather than a rule
   question: NS community names are multi-word far more often than the
   inventory anticipates.

4. **A PVSC tier is not obviously worth building.** The licence permits it and
   it would reach the 9.4% NAR lacks, but it is one province, it covers improved
   properties only, and 7–9% of its own rows have no coordinate. The better use
   of this file is the one made here — as a **validation source** for Nova
   Scotia, the role `bc_validate()` and `qc_validate()` play elsewhere. That
   would be a `ns_validate()`, and it is a much smaller piece of work than a
   tier. It must be built on PVSC and **not** on NSCAF, for the reason the
   provenance section gives: NSCAF is the bigger and more authoritative file,
   and it is the one that cannot validate anything here because NAR already
   contains it.

5. **NSCAF is worth importing as a tier even though it cannot validate.** The
   two roles come apart. It carries 484,178 civic points against PVSC's 372,878,
   it includes vacant civic addresses PVSC omits by construction, and it is the
   province's addressing authority under an OGL that composes. Being NAR's own
   donor makes it useless as a second opinion and says nothing against it as a
   source of addresses NAR's snapshot missed — though the 28.5% of NAR keys
   absent from PVSC suggests measuring that gap against NSCAF directly before
   building anything.

## The standing caveat, restated

PVSC is independent of NAR; it is not correct. Where the two disagree this note
does not establish which is right, and a 386 km disagreement is a statement
about the *pipeline*, not about either source's coordinate. What the
independence buys is narrower and still worth having: the disagreement is real
rather than an artefact of measuring NAR against itself, and both failure
families it isolates are detectable **in the parse, before any coordinate is
consulted** — which is what makes them fixable.

The independence is specific to PVSC, and the natural mistake is now on the
record above. NSCAF looks like the better reference in every visible respect —
it is the province's addressing authority, it is larger than PVSC, and it covers
the vacant civic addresses PVSC omits — and it is the one Nova Scotia file that
cannot serve as a reference at all, because NAR was built from it. The general
form of that trap is worth carrying to the next province: **the more
authoritative a provincial address file is, the more likely NAR already
contains it**, and the useful witness is the one collected for some other
purpose entirely.
