# Quebec addresses: NAR against the source register

Quebec is the one province where the address data in this package has a
published upstream that can be read directly. The **Répertoire québécois des
adresses** (RQA) is maintained by the Ministère des Ressources naturelles et
des Forêts, published under CC-BY 4.0, and is the register NAR's Quebec rows
are derived from. It is also what the `qc` geocoding tier queries — the
locator's own reference names are `RQA_*` — so this note is what establishes
that the tier, the register and NAR are three views of one dataset rather than
three sources.

Everything below comes from two harnesses, both run against NAR 2026-06 and the
RQA release dated 2026-08-01. Re-run them after importing a new NAR release; the
numbers move.

```
RQA_PART=all Rscript data-raw/compare_rqa.R      # register against register
RQA_PART=all Rscript data-raw/diagnose_quebec.R  # a Québec eval sample against both
```

`compare_rqa.R` compares the two registers directly. `diagnose_quebec.R` takes the
Québec slice of the Part B eval corpus — Corporations Canada filings, addresses
nobody cleaned — and asks of every failure whether the address is in NAR, in RQA,
in both or in neither, which is what turns a join rate into a diagnosis. It takes
`RQA_PART` of `split`, `gain`, `interp` or `all`.

The bulk download is `https://diffusion.mern.gouv.qc.ca/Diffusion/RQA/RQA_CSV.zip`
(778 MB, extracting to a 3.08 GB `RQA.csv` of 5,322,997 rows plus a 24 MB
`Odonymes_renvois.csv`). RQA rows carry an `etat`; all counts here are
restricted to `Certifiée`, which drops about 7,500 retired rows.

## How much address there is on each side

| | NAR Quebec | RQA |
| --- | ---: | ---: |
| rows | 4,568,811 | 5,315,435 |
| with a unit designator | 1,460,540 | 1,665,467 |
| distinct civic addresses | 3,236,571 | 3,652,473 |
| rows flagged as a building point | 4,559,562 (99.8%) | 1,429,139 (26.9%) |

RQA holds **415,902 more distinct civic addresses than NAR**, about 12.9% on
top of what NAR carries. Keyed on postal code plus civic number — the coarsest
key that survives the two registers spelling street names differently — NAR has
2,744,951 keys, RQA 2,957,686, and 2,732,411 are shared. That is 99.5% of NAR
inside RQA, and **225,275 RQA keys NAR does not have**. They are ordinary
addresses, not noise:

```
210 B Rue Heriot, Drummondville J2C1J8
32 Montée des Chevaliers, Val-des-Monts J8N4C5
3231 Chemin de la Claire-Fontaine, Saint-Placide J0V2B0
1736 Grand Rang, Saint-Tite G0X3H0
1846 4e Rang, Saint-Côme–Linière G0M1J0
77 Boulevard du Souvenir, Laval H7N4G1
```

Rural and semi-rural, weighted towards the address shapes the parser already
finds hardest — numbered rangs, `Montée`, long hyphenated specifics. RQA's own
quality flag on the missing keys: Géocodée 92,638, Incertaine 67,981, Bâtiment
45,693, Centre lot 13,484, Front lot 5,465.

## The positional-quality field NAR does not carry

RQA classifies every row by how its point was placed. NAR has no analogue.

| `qualite_positionnement_geometrique` | rows | % |
| --- | ---: | ---: |
| Géocodée | 2,086,835 | 39.3 |
| Incertaine | 1,674,182 | 31.5 |
| Bâtiment | 1,429,797 | 26.9 |
| Centre lot | 73,303 | 1.4 |
| Front lot | 58,856 | 1.1 |
| Site / Accès propriété | 24 | 0.0 |

This matters for how NAR's Quebec coverage reads. NAR reports `geom_source =
'building'` on **99.8%** of its Quebec rows, and elsewhere in this package that
flag is treated as the good case — the alternative is a blockface point, which
is a different kind of measurement and not comparable (see
[`nar-database.md`](../../.claude/nar-database.md)). In Quebec it is not a
quality statement at all. It says which NAR file the row arrived in, and for
Quebec that file is carrying RQA points of which RQA itself calls **31.5%
`Incertaine`** and only 26.9% building-placed. Do not read Quebec's 99.8%
building coverage as 99.8% building-accurate geometry.

## Point-to-point disagreement

2,512,836 addresses that both registers carry, on keys unique on both sides,
NAR's building point against RQA's point:

| n | p50 | p90 | p99 | < 1 m | > 100 m |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2,512,836 | 0.21 m | 8.0 m | 55.9 m | 72.3% | 0.5% |

A median of 21 cm over two and a half million addresses is not agreement
between two sources. It is the same coordinate, round-tripped through a
projection. Split by RQA's flag, what is actually going on becomes visible:

| RQA quality | n | % | p50 | p90 | < 1 m |
| --- | ---: | ---: | ---: | ---: | ---: |
| Géocodée | 1,343,276 | 53.5 | 0.16 m | 0.4 m | 97.1% |
| Bâtiment | 754,416 | 30.0 | 4.25 m | 16.0 m | 18.0% |
| Incertaine | 367,289 | 14.6 | 0.14 m | 0.3 m | 98.2% |
| Front lot | 28,476 | 1.1 | 14.86 m | 29.4 m | 2.2% |
| Centre lot | 19,379 | 0.8 | 0.21 m | 31.5 m | 75.6% |

Where RQA interpolated or was unsure, NAR has RQA's coordinate to within
centimetres — 97% and 98% inside a metre. Where RQA has a *building*-placed
point, NAR has something else, a few metres away. The offset is scatter and not
a shift: mean `dx`/`dy` is within 0.05 m of zero in every class, with a standard
deviation of 27 m in the Bâtiment class against 13 m in the Géocodée one, so it
is not a datum or transform error on either side. The reading — an inference,
not something either register states — is that NAR takes a building centroid
from its own building layer where one is available and falls through to RQA's
coordinate otherwise, which would put the two points at opposite ends of the
same structure.

The practical consequence is the one that matters here:

> **NAR's Quebec geometry is not independent of RQA, and neither is the `qc`
> geocoding tier.** Checking a NAR Quebec point against `qc_geocode()`, against
> `qc_validate()`, or against RQA directly measures how well the address parsed,
> not whether the coordinate is right. There is no second opinion available for
> Quebec inside this package.

`qc_validate()` says this in its own documentation, and
[`geocoding.md`](../../.claude/geocoding.md) records it for the tier. The
250,000-address BC comparison in
[`geocoding-status.md`](geocoding-status.md) is a genuinely partial second
opinion because the BC Geocoder maintains its own civic register; Quebec has no
equivalent.

## What is left of Québec, re-diagnosed

The gazetteer's match fold took Québec's postal-confirmed Part B rate from 68.2%
to 75.5%, and the two street-type surfaces added alongside this note took it to
77.5%, so the diagnosis that used to sit here was stale. This is the re-run:
`RQA_PART=split`, 4,000 Québec filings drawn QC-only, seed 20260821, scored with
the same two joins Part B uses. **79.8% confirm against NAR through the
municipality, 81.8% confirm one way or the other, 727 fail.** (The 79.8% and the
77.5% are the same measure on different draws — Part B's QC figure is whatever
slice of a national 5,000 happens to be Québec, a few hundred rows, so the two
are not comparable at a point and only the split below is worth reading.) Each
failure is looked up in both registers and put in exactly one class:

| class | n | % of failures | whose problem |
| --- | ---: | ---: | --- |
| `spelling` | 97 | 13.3 | ours — NAR has the address, under a name the parse disagrees with |
| `no_civic` | 37 | 5.1 | ours — no civic number or no street came out of the string |
| `parse` | 109 | 15.0 | ours — RQA has the address, NAR does not confirm it |
| `postal` | 100 | 13.8 | the filer's — NAR has the address, at a different postal code |
| `coverage` | 300 | 41.3 | NAR's — RQA has the address and NAR does not |
| `neither` | 84 | 11.6 | the filer's — no register holds it |

**Ours 33.4%, NAR's coverage 41.3%, the filing's own 25.3%.**

The classification uses two lookup levels, and the distinction is the whole
reason the old split was wrong. A **key** is the full postal code plus the civic
number; an **address** is the forward sortation area, the civic number and a
street name matched under the same fold the gazetteer uses. The old split keyed
on the full postal code only, so an address both registers hold at a postal code
the filer typed wrong landed in `neither` and read as a bad filing.
`1255 Rue Peel, Montréal H3B 2T6` is the worked example: both registers carry
1255 Peel, at H3B 2T9 and H3B 4V4, and neither is what the filer wrote. That
class is now `postal`, and isolating it moved a hundred rows out of `neither` and
`parse`.

So the headline finding of the old diagnosis survives and gets larger:
**the single biggest block of Québec's remaining shortfall is addresses NAR does
not carry, and it is now 41.3% rather than 26.4%.** The parser's own share fell
from 33.7% to 33.4% while the total shrank, which is the fold and the lexicon
rows doing what they were supposed to.

Inside the classes that are ours, the dominant residue is a street type the
lexicon does not know: 49 of the 243 come back with no `STREET_TYPE` at all.
Before the lexicon rows added with this note it was 120 of 316, `CHEM.` (41) and
`BD` (18) between them accounting for 59 failures on two missing surface forms.
What is left of that tail is single rows of `BOU.`, `BV`, and — a different bug —
`BOUL.DES`, where the input period strip glues a period-abbreviated type onto the
word after it.

## What RQA is actually worth here

Not as a geometry source. Two things, and the ranking between them has flipped.

**The 225,275 addresses NAR lacks** — the next section prices them, because the
re-diagnosis above makes them the largest single thing standing between this
package and a better Québec number.

**The odonyme decomposition**, which used to be the larger prize. RQA does not
store a street name as a string. It stores it decomposed, with a stable
identifier:

| générique | particule | spécifique | cardinal | recomposé (normal) | recomposé (court) |
| --- | --- | --- | --- | --- | --- |
| Boulevard | de la | Côte-Vertu | | Boulevard de la Côte-Vertu | boul. de la Côte-Vertu |
| Avenue | | Élie-Beauregard | | Avenue Élie-Beauregard | av. Élie-Beauregard |
| Rang | | Saint-Ange | | Rang Saint-Ange | rang Saint-Ange |
| Rue | des | Violettes | | Rue des Violettes | rue des Violettes |

115,352 distinct odonymes over 43 génériques and 16 particules, with a particule
present on **27.8%** of rows, four recomposed surface forms per odonyme, and
551,160 rows carrying a `renvoi_seqodo` cross-reference to another odonyme —
the alternative and former names, expanded in `Odonymes_renvois.csv`.

That is a labelled decomposition of every street name in Quebec, in the exact
shape `normalize_address()` is trying to produce, together with the register's
own alternative spellings. It is worth less than it looks, and the reason is
worth recording: the classes it was going to fix — `ST-`/`STE-` left unexpanded,
a dropped leading particule, hyphen-versus-space — are the classes the match fold
now handles for free. What the decomposition can still buy is the part folding
cannot reach: former and alternative names via the renvois, and génériques that
belong to the name rather than the type. One caution if it is ever loaded: six of
RQA's génériques (`Domaine`, `Traverse`, `Descente`, `Chaussée`, `Trait-carré`,
`Carrefour`) do not appear anywhere in NAR's observed street types, so promoting
them to canonical types would make those addresses parse cleanly and then join
nothing.

## Should the missing addresses be imported?

`RQA_PART=gain` and `RQA_PART=interp` size both halves of that question. They
answer differently, and the answer depends on which of this package's two
objectives is being served.

### How large the gap really is

The postal-plus-civic key says 225,275. The coarser **address key** — forward
sortation area, civic number, street name — says more, but only after one
correction that has to be made or the number is nonsense. NAR stores a leading
particule inside `OFFICIAL_STREET_NAME` (`de la Côte-de-Liesse`) while RQA keeps
it in a column of its own and `specifique_odonyme` has none, so comparing the two
raw counts every particule in the province as a miss: 1,265,940. Stripping the
particule off both sides first:

| | keys |
| --- | ---: |
| NAR Québec | 3,105,074 |
| RQA | 3,400,293 |
| **in RQA, not in NAR** | **357,723** |
| in NAR, not in RQA — the noise floor | 62,504 |

Two corrections come off the 357,723. 49,278 (13.8%) are addresses NAR does hold
under a name that contains, or is contained by, RQA's; 43,174 sit at a civic
number NAR has no row for on any street. **Net, the real gap is about 308,000
addresses, roughly 9% on top of NAR's Québec.** The 13.8% containment estimate is
independently confirmed below: of 4,000 sampled gap addresses geocoded against
NAR, 12.5% resolved straight to a NAR *building* point.

What they are matters as much as how many. RQA's own positional flag on the
missing postal-plus-civic keys is **worse** than on what NAR already carries —
Géocodée 41.3%, Incertaine 30.0%, Bâtiment 20.3%, Centre lot 6.0%, Front lot
2.4%, against 26.9% building-placed register-wide. By region they are everywhere,
not concentrated: Montréal 35,927, Montérégie 24,793, Chaudière-Appalaches
20,235, Laurentides 18,726, Estrie 17,475, Capitale-Nationale 17,172, Outaouais
15,368, Saguenay–Lac-Saint-Jean 12,494, Centre-du-Québec 10,503, Mauricie 9,885.
Spot-checked by hand and genuinely absent from NAR: `5510 Saint-Jacques`,
`1650 Chabanel`, `1370 Beauharnois` and `365 Rue Sainte-Catherine Est` in
Montréal, `431 Courtemanche` in Montréal-Est, `45 Gamelin` in Gatineau.

### For geocoding: it buys precision, and reach for a third of them

`RQA_PART=interp` takes 4,000 addresses NAR does not carry, renders them the way
a user would type them, and runs `geocode()` on the local tiers only:

| `match_method` | n | share |
| --- | ---: | ---: |
| `nar_interpolated` | 2,247 | 56.2% |
| `none` | 1,252 | 31.3% |
| `nar_building` | 498 | 12.5% |
| `nar_blockface` / `nar_no_geometry` | 3 | 0.1% |

and measures how far the point it returns falls from the coordinate RQA holds for
that address:

| method | n | p50 | p90 | p99 | < 50 m | > 500 m |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `nar_interpolated` | 2,247 | 23.5 m | 323.7 m | 21,230 m | 65.7% | 7.2% |
| `nar_building` | 498 | 0.1 m | 7.2 m | 9,926 m | 94.0% | 5.4% |

The `nar_building` row is the containment correction showing up again: those are
not missing addresses, they are the same address spelled differently, and NAR
answers to a tenth of a metre. Of the genuine gap, **two thirds already get an
interpolated point with a median error of 23 m**, and **a third get nothing at
all** — 1,252 rows where NAR has no usable geometry for the street. Those are not
parse failures: every one of them yielded a civic number and a street name, and
the gazetteer resolved 73.8% of them, so the string was understood and NAR simply
had nowhere to put it.

So importing would give roughly 110,000 Québec addresses a coordinate they get
none for today, and replace an interpolated guess with a register point for
another 200,000 — cutting a 23 m median to nothing and removing a tail where 7%
of interpolations land more than half a kilometre out and the worst are tens of
kilometres out. That is real, but it is precision on addresses that mostly
already resolve, and the `qc` tier already catches some of the rest online: of
six hand-confirmed missing addresses, NAR alone placed four by interpolation, the
`qc` tier placed a fifth, and only `431 Courtemanche` stayed unplaced.

### For address normalization: it is the only thing that helps

The other objective has no online fallback. Matching two address lists against
each other needs the parse to resolve against a register, and if the register
does not carry the address there is nothing a better parser can do. That is
exactly the 41.3% `coverage` class above. Anti-joining the gap in and re-scoring
the Québec sample puts the ceiling at **81.8% → 88.3%** for a postal-plus-civic
import and **89.3%** for a street-level merge. Nothing else on the next-steps
list is worth six points.

### The recommendation

**Import it, as a separate table and a separate tier — not merged into NAR.** The
coverage is worth having and the licence permits it: CC-BY 4.0 against NAR's OGL,
both attribution licences, compatible in the way ODbL is not (which is why
`osm_geocode()` is bound but is not a tier — see
[`geocoding.md`](../../.claude/geocoding.md)). Three reasons the merge is the
wrong shape:

1. **Merging destroys the only instrument Québec has.** Every measurement in this
   note exists because NAR and RQA are separately readable. Once NAR contains RQA
   there is no way left to ask what NAR is missing, and Québec — which already has
   no independent geometry check — would lose its coverage check too.
2. **The added rows are positionally weaker than what NAR carries** (20.3%
   building-placed against 26.9%, 30.0% `Incertaine`), so a merged table would
   quietly degrade the meaning of Québec's `geom_source = 'building'`, which this
   note has already had to warn about once.
3. **A merged table stops being NAR.** `nar_provinces()`, the row counts quoted in
   the vignettes, the coverage tables in
   [`geocoding-status.md`](geocoding-status.md) and `nar_schema_version()` all
   describe a StatCan release. A local table with its own provenance keeps every
   one of those honest.

Sequenced: the normalization gain is the one that justifies the work, so the first
build is the lookup — the gap rows loaded as a table `normalize_address()` and the
Part B harness can join against. The geocoding tier is a smaller and later win,
and should slot in below `nar_building` and above `nar_interpolated`, since a
register point beats an interpolated one and loses to a NAR building point on the
evidence above.

### Built, 2026-08-23: `rqa_import()` and the `"rqa"` tier

The import exists (`R/rqa.R`), in the shape argued for above. `rqa_import()` writes
two tables into the same `.duckdb` the NAR release lives in — `RqaAddresses`, one
row per certified register address, and `RqaStreets`, a gazetteer grouped by
odonyme and municipality — and never touches `Addresses`. Over the 2026-06 release
it takes **29 seconds** end to end from an extracted CSV.

The **whole certified register is imported, not just the gap**, and each row
carries `IN_NAR`. The gap is a property of the *pair*: a subset built against one
NAR release would be silently wrong against the next, and computing `IN_NAR`
inside the release's own file is the only way it stays correct by construction.
`RqaStreets.N_NOT_IN_NAR` then counts, per street, how many of its addresses NAR
has no row for — which is the street-level version of the same question this note
has been asking throughout.

`IN_NAR` is **fold equality on (FSA, civic number, folded street name)**, not
containment — containment has no equijoin key and would turn a scan into a
product. It therefore over-reports the gap by roughly 14%, the figure the `interp`
run measured independently. What it reproduces:

| | rows | distinct address keys |
| --- | --- | --- |
| certified register | 5,315,435 | 3,400,913 |
| **not in NAR** | **475,294** | **356,089** |

356,089 against the 357,723 this note measured out-of-band — the same number, from
a different code path, which is the check that mattered. Of the gap rows, 40.7%
are `Géocodée`, 31.0% `Incertaine` and 20.3% `Bâtiment`, matching the split above.

**What the tier is worth.** Same 4,000-filing Québec sample, seed 20260821, NAR
2026-06:

| | placed | placed on a *register* point |
| --- | --- | --- |
| `c("nar", "nar_interpolate")` | 88.5% | 82.7% |
| `c("nar", "rqa", "nar_interpolate")` | **90.1%** | **89.1%** |

The headline is the second column, not the first. 258 filings get an RQA point;
only 62 of them were unplaced before. The other 196 were already being
interpolated, and the tier replaces a guess between two neighbours with the
register's own coordinate — a median of **26 m** away (p75 51 m, p90 102 m, p95
195 m). Cost: nothing measurable — 10.0s against 10.1s for the batch, because the
tier only ever sees the rows NAR left unplaced.

`match_method` reports the register's own positional class (`rqa_building`,
`rqa_geocoded`, `rqa_uncertain`, `rqa_lot`, `rqa_other`) rather than one label,
and **`uncertainty_m` is filled in only for `rqa_building`**, where 0 means what it
means for NAR: this package added nothing. Nothing here has measured what
`Géocodée` or `Incertaine` are worth on the ground, and a plausible invented
number would be indistinguishable from the two that were measured.

### Built, 2026-08-23: the normalization pass, and what the 81.8% → 88.3% actually was

`RqaStreets` is now wired into `normalize_address()` as a **second gazetteer pass**: Québec only,
over the rows the NAR pass could not settle, labelled `parse_source = "rqa"`. The eval harness was
run before and after, as this note asked. It does not support the projection this note made, and
the correction matters more than the feature.

On the same 5,000-filing Part B sample (942 Québec rows):

| | before | after |
| --- | --- | --- |
| Québec confirmed against NAR | 77.5% | 77.5% |
| Québec confirmed against NAR **or** RQA | — | **83.0%** |
| rows the `rqa` gazetteer pass answered | — | **4** |

**The 81.8% → 88.3% projected above was a confirmation-set effect, not a parser gain**, and this
note wrote it up as the latter. It was computed by asking how many Québec failures are addresses
RQA holds and NAR does not — a question about *the judge*, not about the parse. Importing RQA does
make the judge better, and by 5.5 points rather than 6.5, which is close enough. What it does not
do is make `normalize_address()` place more Québec strings: it placed 4 more.

The reason is that this note's 41.3% coverage share was measured over **NAR's** residual — Québec
filings that fail to *join* NAR — and then read as if it were the parser's residual. Those sets
overlap much less than assumed. The gazetteer's fuzzy branch already answers most coverage-class
rows with a near neighbour, correctly; what it leaves behind is dominated by strings the parser
cannot read at all. Inspecting the 88 Québec filings the NAR pass still fails on shows them almost
entirely mistyped rather than uncovered: `20-110 boul. de Mortagne, Bouceherville`,
`1116 ST.CATHERINE ST.WEST`, `1052 N.P. LAPIERRE`, `1603 - 3410, rue Peel`,
`13 place Jason Roxboro`, `4150 SteCatherine Ouest`. A second register cannot read a misspelling.

The pass itself is correct and worth keeping. On a 3,000-address sample drawn from the gap
population — what RQA holds and NAR does not — it answers **8.9%** of rows, all of them exactly
right, where the NAR pass answers 81.9% correctly and the rules layer 0.6%. That is the population
it exists for; Corporations Canada filings are simply not drawn from it, because a registered
office is a business address in a settled municipality and the gap is rural, new and cottage
addresses.

`data-raw/eval_normalize.R` now judges Québec against both registers on separate lines, so the two
effects can never be confused again in this harness. **Do not quote the "either" figure as a
parser result.**

One real bug fell out of building it: `nar_match_fold_sql()` folded `-` where its R twin folded
the en and em dash as well, because stringi's transliteration in `nar_fold()` converts them and
DuckDB's `strip_accents()` does not. NAR carries zero en dashes (it writes `--`, 2,134 addresses);
RQA keeps them in 11 street names over 2,472 addresses, so the two spellings of
`du Bord-du-Lac–Lakeshore` never met. Fixed, with a test that folds SQL-side from the raw name —
the existing parity test folds in R first, which is the step that hides the character.
