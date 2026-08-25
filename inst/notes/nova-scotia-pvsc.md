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

That independence has since been spent on a second question this file is the
only place in the package equipped to answer: **what each of
`geocode_accept()`'s bars costs and buys**. A bar that decides which answers to
distrust cannot be scored against NAR, since NAR is the source it exists to
catch mistakes about. Stage 6 (`PVSC_STAGES=6`, opt-in) measures it, and the
table is in
[`geocoding-status.md`](geocoding-status.md#what-the-acceptance-bar-costs-and-buys).
The headline: `refused = FALSE` withdraws rows that are 26.2% gross error
against a 0.91% base rate, all seven tests together trade 8.7 points of coverage
for a 3.2x cut past a kilometre — and 87 rows are still more than a kilometre
out after passing every one of them.

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

## What was done about it

Both halves of the tail finding are now in the package: an uncorroborated remap
is **fined** at scoring time, and what survives is **reported** — not just as a
flag, but as the reason the substitution was allowed to stand. Measured on the
same 40,000-address sample, `NAR_CACHE_PATH/pvsc/bench.rds`, seed 1.

Every figure below is from the current code. The unpenalised baseline is 32,886
exact unambiguous building matches carrying **98 errors past 5 km**, 170 past
1 km — one in 193.

### Where the widening comes from

`MunAlias` keys a written name to a **census subdivision**, not to a community.
So `MILFORD, NS` does not restrict candidates to Milford: it restricts them to
Halifax Regional Municipality, which is 166 mailing communities spread over
127 km. Inside that set a street in the wrong community can outscore the right
one on the evidence, because agreement on the street *type* is worth 0.10 and a
single Damerau-Levenshtein step costs 0.072 at the name gate. `12 Wildwood Dr,
Milford` resolves to `Windwood Dr, Middle Sackville` at 0.952, and beats the
`Wildwood Ave` in Halifax that scores 0.900 for the type it does not match.

The widening is not the defect. Without it a misspelt street in a small
community resolves to nothing at all, and 1,685 of the 2,894 remaps in this
sample are the feature working. The defect is that nothing distinguished them.

### Two signals, learned from NAR rather than assumed

**A shared full postal code.** Two mailing municipalities that appear on the same
six-character postal code are two names for one delivery geography, whoever else
disagrees. `HOWIE CENTER` and `SYDNEY` share three; `MILFORD` and
`MIDDLE SACKVILLE` share none. Nationally that is 32,216 directed pairs, built as
a TEMP table in 0.2 s (`nar_mun_copostal()`) — no schema bump, no re-import. It
has to be the full code and not the FSA: an FSA in rural Nova Scotia covers most
of a county, so the FSA-keyed `PostalMun` already in the schema would attest
nearly every pair in the province and the test would never fire.

**The census subdivision the street already sits in.** Amalgamations and legacy
names do not produce shared postal codes — the merger did not merge the delivery
names — so `Bathurst St, Toronto` reaching a street NAR still mails to
`NORTH YORK` is a swap no postal evidence will ever attest. `Streets.CSD_ENG_NAME`
carries the relationship directly, and comparing against it is the second
attestation. It is small in Nova Scotia (91 rows) but it is *clean*: zero of them
past a kilometre.

Both are read out of NAR. **There is no curated alias list in either direction**,
which is what makes the test portable to provinces nobody has looked at.

Splitting the 2,894 remaps by which arm answered, before any penalty:

| `mun_evidence` | n | p50 | p90 | p95 | >1 km | >5 km |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `copostal` — shares a postal code | 1,601 | 7.4 m | 49 m | 121 m | 0.94% | **0.62%** |
| `csd` — amalgamation or legacy name | 84 | 14.7 m | 98 m | 214 m | 0.00% | **0.00%** |
| **`unattested`** | 1,025 | 18.6 m | 316 m | **12.5 km** | 7.22% | **6.83%** |
| `untestable` — not a mailing name | 184 | 32.0 m | 328 m | 740 m | 3.26% | 1.63% |

A hundredfold separation at p95, from signals that cost 0.2 s. The unattested
class is 35% of the remaps and carries 85% of their gross errors.

> **The comparison has to be anchored on the baseline reading.** The parser emits
> several candidate readings per string and the gazetteer scores all of them; an
> alternative reading may re-segment the trailing run and hand back a *shorter*
> municipality. `HOWIE CENTRE` read as `CENTRE` — itself a Nova Scotia
> municipality, and one that shares a postal code with `LUNENBURG`. Scoring the
> swap against that reading lets a truncation manufacture its own attestation.
> This was live long enough to be measured wrong: it laundered 184 rows into
> `copostal`, which is the entire `untestable` row above.

### The penalty, and what it costs

A candidate whose mailing municipality is neither the one written nor attested by
either arm has its whole score multiplied by `mun_swap_penalty`, default **0.88**.
Against the 0.85 acceptance threshold that means an unattested swap must score
0.966 unpenalised — an exact or one-keystroke street name *and* agreement on
everything else the string supplied. Two uncertainties at once is one too many.

The value is the knee of a curve, not a preference:

| penalty | exact matches | matches lost | past 5 km | errors removed | matches per error |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1.00 | 32,886 | — | 98 | — | — |
| 0.95 | 32,821 | 65 | 82 | 16 | 4 |
| 0.92 | 32,829 | 57 | 69 | 29 | 2 |
| 0.90 | 32,818 | 68 | 69 | 29 | 2 |
| **0.88** | **32,513** | **373** | **42** | **56** | **7** |
| 0.86 | 32,454 | 432 | 41 | 57 | 8 |
| 0.85 | 31,958 | 928 | 32 | 66 | 14 |

Read the last column marginally rather than cumulatively: 0.90 → 0.88 buys 27
gross errors for 305 matches, 11 apiece; 0.88 → 0.86 buys **one** for 59; 0.86 →
0.85 buys 9 for 496, 55 apiece. 0.88 is the last step that is cheap.

Below 0.85 the penalty stops discriminating — every unattested swap is refused
whatever else it got right, and **85% of the matches that costs were within 100 m
of PVSC's own point.** That is the trade that makes refusing the whole class the
wrong answer: the class is 100× enriched in gross error and still overwhelmingly
correct.

### What it did

| `mun_evidence` | n | p50 | p90 | p95 | >1 km | >5 km | floor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `kept` — not remapped | 30,044 | 10.2 m | 57 m | 119 m | 0.26% | 0.05% | — |
| `copostal` | 1,632 | 7.5 m | 50 m | 127 m | 1.10% | 0.80% | 0 |
| `csd` | 91 | 14.5 m | 87 m | 203 m | 0.00% | 0.00% | 0 |
| `unattested` | 561 | 12.9 m | 83 m | 195 m | 1.78% | 1.78% | 118 |
| `untestable` | 184 | 32.0 m | 328 m | 740 m | 3.26% | 1.63% | 118 |
| `inferred` | 0 here | — | — | — | — | — | 118 |

Errors past 5 km over the whole exact unambiguous population fall from 98 to 42,
and the rate from one in 193 more than a kilometre wrong to one in 288. What it
costs is 1.1 points of overall placement, 87.44% → 86.32%, almost all of it rows
that go unplaced rather than rows that move.

The `unattested` class is what the penalty acts on and it shrank by 45%, from
1,025 rows to 561; the ones that stayed are 4× less likely to be kilometres out
than the ones that left. `untestable` is untouched by construction — an absence
of evidence about a name NAR has never seen is not evidence of a bad swap, and
fining it was measured: it refuses 119 matches to remove 2 gross errors, 60 apiece
against the swap arm's 11 at its knee.

### What survives, and why it is priced rather than refused

The reproduction that opened this section is one of the survivors, and it is
worth being exact about what changed:

```
36 LAKEVIEW DR, HOWIE CENTRE, NS
   before  ->  Lakeview Cir, CONQUERALL MILLS   conf 0.900   nar_building   388 km out
                 mun_remapped  (absent)         uncertainty_m 0
   after   ->  Lakeview Cir, CONQUERALL MILLS   conf 0.900   nar_building   388 km out
                 mun_evidence  untestable       uncertainty_m 118
```

It is **not refused**, and the penalty never touches it. NAR files no
postal-coded mail under `HOWIE CENTRE` — it spells the community `HOWIE CENTER` —
so there is nothing to test the swap against and the untestable exemption carries
it through. What changed is that the row no longer claims 0 m.

`25 River Rd, Moser River` is the harder case: it still answers with the
`River Rd` in Halifax, 118 km away, and that swap **is** attested — `MOSER RIVER`
and `HALIFAX` do share a postal code, because `HALIFAX` is a mailing name
covering the whole regional municipality. So it gets no floor at all. That is the
copostal class's 0.80% past 5 km showing up as a single row, and it is the reason
the flag exists separately from the metres.

Refusing either would mean `mun_swap_penalty = 0.85` and worse, and would cost
the 928 matches above. So they are priced instead.

`normalize_address()` returns `mun_remapped` and `mun_evidence`, and
`geocode()` floors `uncertainty_m` at **118 m** — the pooled 90th percentile of
the three unverified classes, over 745 rows — wherever the evidence is not an
attestation. Three honest things and one dishonest one to avoid:

* the floor stops `uncertainty_m` reporting **0 m** on the population where 0 m
  is least true, which was recommendation 2 below;
* **an attested remap is not floored at all**, and that is the measurement rather
  than a concession: the attested classes pool to p90 **52 m** over 1,723 rows,
  *below* the 57 m of rows whose municipality was never touched. A swap a postal
  code or a census subdivision vouches for is as good as no swap. `inferred` is
  grouped with the unverified classes on the argument and not on a measurement —
  PVSC always carries a city, so the class cannot occur in this sample at all;
* 118 m is a *disagreement*, not an error budget: much of it is NAR's own
  distance from PVSC rather than anything the remap added;
* and it does **not** describe the tail. The unverified classes run 1.6–1.8% past
  5 km against 0.05% for untouched rows — thirty times the rate, at a distance no
  90th percentile of either population reports, and the Moser River row shows an
  *attested* one can be 118 km out. A caller who cannot tolerate a
  kilometre-scale error should filter on `mun_remapped` itself. A metre value
  cannot express a bimodal distribution, and pretending otherwise would be the
  same mis-description as reporting 0.

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

2. ~~**Stop reporting `uncertainty_m` as 0 for a unique match whose
   municipality was remapped.**~~ **Done**, 2026-08-24, and it grew two further
   halves. The remap is *fined* where nothing attests it — errors past 5 km fall
   from 98 to 42, and one exact unambiguous match in 288 is now more than a
   kilometre wrong against one in 193 — and what survives is reported as
   `mun_remapped` plus `mun_evidence`, with `uncertainty_m` floored at 118 m only
   where the evidence is not an attestation. See
   [What was done about it](#what-was-done-about-it) above. Two of the four
   attestation arms had to be built rather than assumed, and the anchor bug
   documented there was found by disbelieving the reproduction.

3. **Municipality truncation** (2.32%, half of them unplaced) is the harder of
   the three, because the fix is a gazetteer question rather than a rule
   question: NS community names are multi-word far more often than the
   inventory anticipates. It is also worth more now than it was: a truncated
   municipality is a *wrong* name, so the swap penalty fines the answer it would
   otherwise have found, and some of the 373 matches that penalty costs are
   truncations rather than genuine swaps.

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
