# Address normalization — what still fails, and what to do next

Companion to `.claude/normalization.md`, which documents *why the code is shaped the way it is*.
Lives in `inst/notes/`, so it installs with the package and
`system.file("notes", package = "cangeocode")` finds it.
This file documents *where it currently falls short*. Every number here is measured, not
estimated; each section says which measurement produced it so it can be re-run and disputed.

**As of:** 2026-08-21, commit `6cde886`, NAR release `2026-06` (17,362,476 addresses).
Layers 1 (rules) and 2 (NAR gazetteer) are implemented. Layer 3 (LLM fallback) is not — see
*Deferred* at the end.

## Reproducing the numbers

```sh
R_ENVIRON_USER=/dev/null NAR_CACHE_PATH=/Users/jens/data/nar \
  EVAL_N=5000 Rscript data-raw/eval_normalize.R          # both parts, ~3 min
```

`EVAL_PARTS=A` or `=B` runs one part. `EVAL_N` sets the sample size.

**Both parts are seeded, and both seeds are load-bearing.** Part A draws with
`USING SAMPLE reservoir(N ROWS) REPEATABLE (...)` because DuckDB's sampler does **not** take R's
`set.seed()` — without `REPEATABLE`, every run draws a different sample and a before/after
comparison measures the sampler rather than the change. At N = 5,000 a rate near 0.95 carries
roughly 0.6 points of noise, wide enough to hide or invent most of the effects worth chasing.
Part A's SQL is an `sprintf` template, so a literal percent sign in it has to be doubled.
Part B re-seeds R before drawing, or `EVAL_PARTS=B` and `EVAL_PARTS=AB` sample different
corporations — Part A consumes the RNG stream on its way past.

The LLM measurements in *What a local LLM adds* come from a second harness:

```sh
R_ENVIRON_USER=/dev/null NAR_CACHE_PATH=/Users/jens/data/nar \
  LLM_MODEL=gemma4:e2b Rscript data-raw/eval_llm.R       # ~6 min with Ollama up
```

It prints the residual ceilings with no model at all, and runs the two shortlist experiments only
if `httr2` is installed and `LLM_HOST` answers.

Two consecutive full runs are now byte-identical apart from timing. **Any change to the parser
should be evaluated by running the harness before and after on the same seed**, not by comparing
against a number written down here.

## Where it stands

**Part A** — 5,000 real NAR rows rendered into noisy surface forms and parsed back:

| field | exact | | recovered when the surface form dropped it |
| --- | --- | --- | --- |
| `CIVIC_NO` | 99.9% | `STREET_TYPE` | 88.2% |
| `STREET_NAME` | 98.0% | `STREET_DIR` | 96.8% |
| `STREET_TYPE` | 97.5% | `MUN_NAME` | **63.3%** |
| `STREET_DIR` | 99.3% | `PROV_ABVN` | 95.3% |
| `MUN_NAME` | 94.4% | `POSTAL_CODE` | 0.3% |
| **ALL** | **97.3%** | **CORE** (civic + name) | **98.0%** |

**Part B** — 5,000 Corporations Canada registered offices, i.e. addresses nobody cleaned:

| | |
| --- | --- |
| street name and civic number extracted | 98.8% |
| joins a real NAR address (civic + name + municipality + province) | 86.6% |
| ... and the filer's postal code confirms it | 81.7% |

### How to read these

- **`POSTAL_CODE` 55.8% / 0.3% is not a failure.** The normalizer never invents a postal code.
  The 55.8% is simply the share of rendered forms that carried one; the 0.3% is rounding noise on
  those it did not. Ignore both.
- **Part B's postal-confirmed figure is a lower bound, not an accuracy.** "Confirmed" demands that the filer's exact
  six-character postal code appear on a NAR row for that civic number and street. Corporate
  registry postal codes are frequently stale or simply wrong, and NAR often carries a different
  one for the same building. Of six hand-checked "unresolved" rows, four parsed correctly *and*
  exist in NAR — `1321 Matheson Blvd E, Mississauga` (NAR has it at `L4W1R1`, the filer wrote
  `L4W0C2`), `239 Temby Private, Ottawa` (`K1T2W6` vs `K1T2V6`), `11 Crossbill Rd, Brampton`, and
  `1123 Leslie Street, Toronto`.
- **Part A's `MUN_NAME` 63.3% is still the smallest number on the page** that means anything, and
  it is mostly not a parser problem. See *Failure mode 1*. It was 46.3% until the gazetteer began
  answering with a municipality it had no way to get wrong.
- **Part A and Part B do not always move together, and that is the harness working.** The
  `SUITE 800-666` fix below lifted Part B's join rate by 0.4 points and left every Part A figure
  byte-identical, because the noise grammar never renders a designator in front of a hyphenated
  unit-civic pair. Part A measures the mess we imagined; Part B measures whether we imagined the
  right mess. A Part B gain with no Part A movement is a gap in the grammar, not a fluke.

## Failure modes, ranked

Ranking is by measured size. The categorisation comes from running Part A's sample and grouping
the ~500 misses by which fields disagreed (`scratchpad/miss.R` pattern; the top combinations were
`mun` 234, `name` 56, `mun+prov` 49, `type+mun` 38, `name+mun` 32, `name+type` 29).

### 1. The municipality was never in the string and cannot be inferred — ~52% of Part A misses

`27 Feagan Dr, Ontario` → we return `MUN_NAME = NA`; the answer was `SCARBOROUGH`.
`2340 SHORE RD, NS` → `CARLETON VILLAGE`. `2 richard street` → `SYDNEY MINES`.

When the surface form drops the municipality, the gazetteer can only recover it if the street name
is unique enough within the province to identify one. It manages this 63.3% of the time, up from
46.3% since the exact branch started answering where the answer is **determined** — exactly one
municipality in NAR carries a street of that name, so there is nothing left to guess. The
remainder are genuinely ambiguous: there are many `Richard St`s in Nova Scotia and nothing in the
input distinguishes them.

**What is left is a ceiling, not a bug.** Raising it further means either accepting a
most-populous-match heuristic (which would return confident wrong answers) or requiring a postal
code. The right response is probably to keep returning `NA` and let `confidence` carry the
ambiguity — but the alternative worth measuring is *returning all candidates* when the street name
resolves to a small number of municipalities, which the `output = "multiple"` convention in
`reverse_geocode()` already models. Measured on the residual *after* the determined case was
answered: of the 191 remaining misses where street name and province are already right, 36 have
2–5 candidate municipalities, 55 have 6–20 and 66 have more than 20. So a shortlist would be short
enough to be worth showing in about a fifth of them, and the rest are the ceiling.

### 2. A leading direction word that belongs to the street name — ~100k addresses (0.58%)

```
544 North Park Blvd, Oakville, ON   ->  name PARK,  dir N   (want: name NORTH PARK, dir none)
96 North Point Rd, Heart's Content  ->  name POINT, dir N   (want: name NORTH POINT)
```

The direction step takes a leading `NORTH`/`SOUTH`/`EAST`/`WEST` unconditionally, and the
gazetteer does not rescue it. **This fires whether or not the street type is present**, which makes
it the highest-severity pure parser bug on the list.

3,228 NAR streets across 100,337 addresses have a name beginning with a direction word:
`NORTH PARK` (3,537 addresses), `EAST LIBERTY` (3,323), `SOUTH PARK` (3,053), `NORTH SHORE`
(3,038), `NORTH SERVICE` (1,740), `SOUTH MILLWAY`, `NORTH RIVER`, `WEST SAANICH`.

**Fix:** a leading direction should be provisional, not committed — if the gazetteer finds no
street under the stripped reading but does find one with the direction word back in the name, take
the second reading. The candidate framework this needs now exists (`R/normalize_variants.R`);
what is left is a strategy that emits the restored-direction reading, and a gate saying when it is
worth emitting. Same shape as the fix for mode 3, and the two should still be built together.

### 3. A name-final type word eaten as the type, when the real type is missing — ~586k addresses (3.4%) at risk

```
3 Aspen Cove Rd, Fort McMurray, AB  ->  name ASPEN COVE, type RD   correct
3 Aspen Cove, Fort McMurray, AB     ->  name ASPEN,      type COVE  wrong
```

Note the pairing: **this only bites when the input omits the street type.** With the type present
the longest-match is unambiguous and `Wharton Glen Way` and `Park Lawn Rd` both parse correctly.
That confines the damage to the 13% of type-dropped forms the harness measures at 86.9%.

21,589 NAR streets across 586,346 addresses end their *name* in a word that is also a street type:
`PARK` (72,457 addresses), `HILL` (49,672), `RIDGE` (38,150), `BAY` (31,656), `POINT` (29,419),
`VIEW`, `HEIGHTS`, `GROVE`, `GLEN`, `BEACH`, `COVE`, `CENTRE`.

**Fix:** same arbitration as mode 2 — when stripping a trailing type leaves a name the gazetteer
cannot find, retry with the word restored and no type. **Read the gate finding in *Fixed* first:**
a restored-name candidate that happens to exist somewhere will outscore the baseline on the
gazetteer's own score, so the retry has to be gated on the baseline failing rather than offered
alongside it.

### 4. Keyboard typos in the street name — 92.1% vs 98.6% clean

Was 76.2% against 98.3%. The gap closed when the name gate stopped being a pure similarity
threshold: `Yinge` → `Yonge` and `NAPLE RD` → `Maple` both used to survive as typed, because
Jaro-Winkler pays a prefix bonus and so scores the *same* one-key slip 0.89 in `NARTIN`/`MARTIN`
and 0.83 in `QALL`/`WALL`. They were never mis-ranked — the correct street was already the nearest
candidate — they were rejected by the 0.90 gate. See *Fixed, and worth keeping fixed*.

The plan earmarked this tail for Layer 3, on the argument that a language model has priors about
Canadian street names that Jaro-Winkler does not. That argument was tested and did not survive:
see *What a local LLM adds, measured*. What remains after the gate change is mostly corruptions
that land on another real street name, where nothing in the string can arbitrate.

### 5. Street-name periods in the exact branch — 104,272 addresses (0.6%), unhandled by design

`298 ST. SIMON, BELLE RIVER` parses to `SIMON ST` — the period-bearing name `St. Simon` cannot meet
its fold key. `nar_gazetteer_sql()` folds periods out of both sides on the municipality joins and
the fuzzy street comparisons, but deliberately **not** on the exact-branch `Streets.NAME_FOLD`
join, which would cost the `str_name_idx` index. See `.claude/normalization.md` for the rule.

**Fix, if it is ever worth it:** materialise a second period-folded column on `Streets` and index
that, rather than folding at query time. Costs a schema version.

### 6. A doubled province reads as a municipality — small, unmeasured

`77 Progress Avenue, Toronto, Ontario, ON M1P 2Y7` → `MUN_NAME = ONTARIO`. The province step
consumes the trailing `ON` and the municipality step then takes `Ontario` at face value. Cheap
to fix: a municipality candidate that is itself a province name should be rejected when a
province has already been resolved.

### 7. Patterns that exist to say "this will never resolve"

`po_box` and `rural_route` are delivery instructions and **NAR contains neither**; `street_only`
has no civic number to join on and confirms at 0.0% in Part B by construction. These buckets are
working as intended — they separate "this address is wrong" from "this address was never going to
be in the gazetteer". Their Part B confirmation rates are quoted with sample sizes in single or
double digits and should not be read as trends.

## Fixed, and worth keeping fixed

**A unit designator in front of a hyphenated unit-civic pair.** `SUITE 800-666 BURRARD ST` is the
standard Canadian office form. `800-666` on its own split correctly, but the spelled-out
designator took the whole hyphenated token as the unit value and returned **no civic number at
all** — so the designator made the parse worse than omitting it, and a row with no civic number
does not join anything downstream. `nar_take_leading_unit()` now routes the designator's value
through the same `nar_split_unit_civic()` the `#` branch always used.

**A hyphen left standing as its own token.** `nar_norm_text()` joins `302 - 1055` because a bare
number follows it, and declines on `1688 - 152nd` because `152ND` is not one. The lone `-` then
survived tokenization and became the first word of the street name (`- 152ND`). `nar_tokens()`
drops it; hyphens *inside* a token, which is most of Quebec, are untouched.

Together: Part B 85.6% → 86.0% joined, 98.6% → 98.8% civic-and-name extracted. Both are
regression-tested in `test-normalize.R`.

**`STE` guarded on only one of the two unit paths.** `nar_take_unit_segments()` has required an
ambiguous designator's value to look like a unit number since the Sainte collision was first found,
but `nar_take_trailing_unit()` did not, and in a comma-less string the municipality is not a
segment of its own. So `123 Main St Sault Ste Marie ON` reached the trailing rule with `STE`
second-from-last and parsed as a unit called `MARIE` on a street in `SAULT` — the same failure the
guard exists to prevent, through the other door. The test is now the shared `nar_is_unit_value()`
and both paths call it. Also fixes `12 Ste Anne St Ste Anne MB` and `1 Rue Sainte-Catherine Ste Foy
QC`. Part A is byte-identical and Part B moves one row between pattern buckets — the harness cannot
see this, because Part A renders its municipalities out of NAR into a comma-delimited form and Part
B's filings are mostly comma-delimited too. That is the same gap in the noise grammar the
hyphenated-unit fix exposed, and it is the reason both of these were found by hand rather than by
the eval.

### The parser produces candidate readings, and evidence chooses between them

`R/normalize_variants.R`, and the framework modes 2 and 3 below have been waiting for. One string
now yields several readings; the municipality inventory arbitrates when parsing is rules-only, the
street gazetteer when a connection is available, and the baseline reading is candidate 1 and wins
every tie. The design is in [`.claude/normalization.md`](../../.claude/normalization.md).

The first strategy built on it is **municipality anchoring**: match the trailing token run against
an inventory of the 9,748 distinct `MunAlias` names, longest first, then parse the remainder with
the municipality already decided.

```
100 Main St TH25, Vancouver  ->  unit TH25, MAIN ST, VANCOUVER   already worked
100 Main St TH25 Vancouver   ->  unit TH25, MAIN ST, VANCOUVER   was: mun "TH25 VANCOUVER"
100 Main #25 Vancouver       ->  unit 25,   MAIN,    VANCOUVER   was: no unit, no municipality
```

The comma was carrying the parse. Anchoring reaches the same remainder without it, which is the
point: no local rule can separate `TH25 VANCOUVER` from `100 MILE HOUSE`, because the difference
is not in the tokens — it is in whether the place exists. The inventory ships in `R/sysdata.rda`
as `nar_lex_muns`, so this works with no connection at all.

Two smaller fixes rode along, both found by the first: `nar_take_trailing_unit()` had an
unreachable branch for a hash on its own token, so `# 25` fell through to the street name while
`#25` resolved; and a lone leftover token is now taken as a unit only when it mixes digits and
letters (`TH25`, `PH2`, `4B`), because the first version ate the numbers off `Rte 12` and
`Highway 20`.

**The finding worth keeping.** Generating an alternative reading unconditionally *costs* rows.
`80 rue Albanel, QC` names no municipality, Albanel is a real one, and the anchored reading leaves
a street called `RUE`; likewise `de la Durantaye`, `de Nantes`, `l'Assomption` and `Fesroches
Trail`. **The gazetteer cannot arbitrate these back** — a match restricted to a real municipality
outscores an unrestricted one by construction, so the worse parse wins on a score that was never
meant to compare two parses of the same string. Arbitration cannot repair a candidate that should
not have been offered, so the gate belongs at generation: an alternative is offered only when the
baseline proposes a municipality that is *not a place*, or a street name containing a `#`. A
baseline proposing no municipality is not defective — that is mode 1, a ceiling, and inventing an
answer for it is strictly worse than `NA`.

Anything built on this framework needs the same discipline. Modes 2 and 3 want to retry a *street*
reading, and the same asymmetry applies to them.

> Part A **exactly at parity** — 0 rows gained, 0 lost, which is what the gate bought; the
> ungated version lost 4 street names on the rows above. Part B 86.5% → 86.6% joined, 81.6% →
> 81.7% postal-confirmed, Quebec 67.8% → **68.2%**, rules-only fallbacks 374 → 371. The rules
> layer costs ~9% throughput for the defect check.
>
> One Part A row flipped `Castleglen WAY` → `Castleglen RD`: Calgary has both, with **identical**
> address counts, so the gazetteer's window function had no tie-break left and DuckDB's arbitrary
> choice moved when the probe table's shape changed. `STREET_TYPE` is now the final key in that
> `QUALIFY` clause. The row is a loss either way; what the change bought is that it is the *same*
> loss on every run, which a before/after harness requires.

### The name gate stopped being a pure similarity threshold

Three changes to `nar_gazetteer_sql()`, made together because the measurement that motivated them
was one measurement. All three are in the `scored` CTE's name-similarity block or the `exact` CTE;
`R/normalize_gazetteer.R` carries the reasoning inline.

**A single edit counts as a match, at length 3 and above.** 77 of the 82 rows where the correct
street was *already the nearest candidate* were being rejected by the 0.90 `name_threshold`, and 69
of those sat exactly one Damerau-Levenshtein step from the input. Jaro-Winkler cannot see this
because it pays a prefix bonus: the same one-key slip scores 0.89 in `NARTIN`/`MARTIN` and 0.83 in
`QALL`/`WALL`. The length floor is load-bearing rather than tidiness — at two characters one edit
is the whole word, and `5W` against `5E` is a different street, not a typo.

**Whole-word containment counts as a match.** This catches the words a parse rule ate — `5` for
`NO. 5`, `772` for `ROUTE 772`, `PARK` for `PARK LAWN` — which similarity ranks nowhere near the
top (679th, in the first of those). It cannot displace a street actually called `PARK`, which
scores an exact 1.0 and wins.

Both are worth a flat 0.90 rather than a branch of their own, so `name_threshold` keeps meaning one
thing and raising it above 0.90 turns both off, which is what asking for stricter should do.

**The exact branch answers with a municipality when NAR determines it.** One municipality carrying
the only street of that name has *determined* it; withholding that was its own kind of wrong
answer. Two or more and it stays `NA` — the busiest city with a street of this name is a guess, not
a resolution. The province comes with it, so a string that never named one can still resolve to
both.

Each branch needs a prefilter to stay affordable, and both were measured rather than assumed. Edit
distance is only asked about candidates already scoring `jw_sim >= 0.70` (one edit cannot drag
Jaro-Winkler below that: the worst case is a substituted first character of a three-letter word, at
0.778), and containment only about candidates longer than the probe (a shorter one can only contain
it by equalling it, which similarity already scores 1.0). Without the pair the query runs 3.5x
slower for byte-identical answers; with them the cost is 7.9s → 11.1s over 4,982 rows.

**Measured, per row, against the same 5,000-row Part A sample before and after: 215 rows gained, 0
lost.**

| field | gained | lost |
| --- | --- | --- |
| STREET_NAME | 97 | 0 |
| STREET_TYPE | 13 | 0 |
| STREET_DIR | 19 | 0 |
| MUN_NAME | 127 | 0 |
| PROV_ABVN | 45 | 0 |
| CIVIC_NO | 0 | 0 |

Disabling each branch in turn attributes it: **edit +82 rows, determined municipality +117,
containment +16** (they overlap, so they do not sum to 215). At the harness level, Part A ALL
95.2% → 97.3% and CORE 96.0% → 98.0%; typo'd street names 76.2% → 92.1%; a dropped municipality
recovered 46.3% → 63.3%; rows falling through to `rules` 204 → 105. Part B joins 86.0% → 86.5%.

### Ordinal-suffix repair, which turned out to need no code

`9YH Ave` for `9TH Ave` was ranked as its own fix — the corrupted suffix is a closed vocabulary and
a rule could rewrite it. It was measured first: **96 ordinal-named streets in the Part A sample, 96
street names recovered, 0 still wrong.** The edit branch above already takes the whole class, so
the rule would fire never. It is recorded here rather than shipped as dead code, and the general
point is worth keeping: a repair rule that a gazetteer match already covers is a guess wearing a
rule's clothes, since without the gazetteer nothing confirms `9YH` was meant to be `9TH` rather
than a real suffix.

## Measured and deliberately not done

Recording these so they are not re-litigated:

- **Joining on `CSD_ENG_NAME` as well as `MAIL_MUN_NAME` buys 0.2 points.** The amalgamation story
  is real — NAR files `1123 Leslie St` under `NORTH YORK` while the filer writes `Toronto` — but
  `MunAlias` already absorbs nearly all of it. Measured on Part B's sample: municipality agreement
  85.6%, adding CSD name 85.9%.
- **Relaxing the municipality to an FSA match buys 2.5 points** (85.6% → 88.1%), but an FSA is a
  much weaker claim than a municipality and would inflate the metric rather than the accuracy.
  Useful as a diagnostic, not as a join.
- **The true ceiling on Part B is 89.7%**: that is the share of corp addresses whose civic number
  and street name match *anything* in the right province. Only 1.4 points of the remaining 10.3%
  are cases where we failed to extract a name or a civic number at all. The rest is NAR coverage
  and genuinely bad input.

## What a local LLM adds, measured

The approved plan's Layer 3 was an LLM fallback for the rows Layers 1–2 flag. The residual now
exists and can be measured directly, so this is the measurement rather than the argument.

**These numbers describe the residual as it stood *before* the gate changes in *Fixed, and worth
keeping fixed*** — which is the point, since this measurement is what identified them. Rerunning
`data-raw/eval_llm.R` today reports a smaller residual (strict recovery 88.4% → **92.7%**, 580
misses → 365) because the deterministic work took the part of it the models were competing for.
The comparisons below are left at their original denominators so they stay internally consistent;
the conclusion only gets stronger.

**Scope of the claim.** These are *foundation models used off the shelf* — `gemma4:e2b` (5.1B,
Q4_K_M) and `qwen3:8b`, via Ollama, JSON-schema-constrained, `think: false`, `temperature: 0`.
None of them has been trained on this task. The plan's actual proposal was a **fine-tune**, on
data NAR generates for free, and nothing below tests that. What follows bounds the off-the-shelf
option and re-ranks the work queue; it does not close the fine-tune question.

Both experiments are **pick-from-shortlist**: the model never emits a string, it chooses among
real NAR rows. That is the shape that neutralizes the structural failures measured during
planning (a model that writes `civic_no: "1234A-990"` cannot do so if it is picking).

### The residual a model would work on

Strict Part A recovery is **88.4%** (580 misses in 4,982) — counting a field the surface form
dropped as a miss, which is the right denominator here, since recovering what is *not in the
string* is exactly what a model would be for. That is a different measure from the headline
95.2% ALL, which excludes dropped fields; neither contradicts the other.

**Municipality** — 286 misses where street name and province are already right. How much is even
knowable is set by NAR, not by the parser:

| municipalities in NAR carrying that street | rows |
| --- | --- |
| exactly 1 — determined | 66 |
| 2–5 | 56 |
| 6–20 | 66 |
| more than 20 | 67 |

**Street name** — 151 misses with a municipality to restrict on, median 990 candidate streets
each. The truth is already the top Jaro-Winkler candidate in **82 of 151 (54%)**, in the top 10 in 114
(75%), absent from the top 20 in 7.

### Experiment 1 — pick the municipality from a 2–20 shortlist

| | correct | median ms/call |
| --- | --- | --- |
| current pipeline | 0 of 122 (returns `NA`) | — |
| chance | 21.7% | — |
| `gemma4:e2b` | 37 / 122 = **30.3%** | 792 |
| `qwen3:8b` | 37 / 122 = **30.3%** | 1731 |

Four times the parameters, the same score, twice the latency. And 85 of 122 come back a
*confidently wrong* municipality, which for `address_key()` is strictly worse than `NA`: a wrong
municipality joins two different buildings, a missing one joins nothing.

### Experiment 2 — pick the street name from the 10 nearest real streets

| | correct |
| --- | --- |
| take the top Jaro-Winkler candidate (free, already in the package) | 82 / 144 = **56.9%** |
| `gemma4:e2b` | 79 / 144 = 54.9% |
| `qwen3:8b` | 65 / 144 = **45.1%** |
| oracle ceiling — truth present in the shortlist at all | 114 / 144 = 79.2% |

Both models score below a scorer the package already runs, and the larger model is markedly
worse: it reasons its way off the obvious answer.

### Why those 82 rows fail today — the decisive part

Of the 82 where the truth was *already* ranked first, **77 (94%) are rejected by the 0.90
`name_threshold` gate**, not mis-ranked. Ordered by similarity: `772`→`ROUTE 772` (0.00),
`7 & 8`→`RANG 7 & 8` (0.53), `5W`→`5E` (0.67), `9YH`→`9TH` (0.80), `2BD`→`2ND` (0.80),
`1SY`→`1ST` (0.82). Nothing is being ranked wrong. A threshold is discarding correct answers,
mostly because Jaro-Winkler is close to meaningless on 2–3 character tokens.

Splitting the 151 street-name misses by shape shows where the work actually is:

| class | n | truth already rank 1 |
| --- | --- | --- |
| garbled ordinal (`9YH`→`9TH`) | 10 | 90% |
| the truth is our answer **plus a word** (`5`→`NO. 5`, `772`→`ROUTE 772`, `PARK`→`PARK LAWN`) | 55 | 22% |
| some other keyboard typo | 76 | 80% |
| everything else | 10 | 0% |

The 55-row class is a rule eating a word, and similarity ranks the truth 679th for `5`→`NO. 5` —
so it is outside any shortlist a model would ever be shown. A **whole-word containment** search
over the streets in that municipality finds the truth in **54 of 151**, as the *only* candidate
in 24, and produces a wrong candidate in 6. Milliseconds, no model.

### What this settles

For the measured residual, an off-the-shelf local model adds nothing that the gazetteer does not
already have, and the pick-from-shortlist framing was its best case. The four deterministic items
it displaced have since been done — three of them shipped, the fourth measured as already covered;
see *Fixed, and worth keeping fixed*. Cost, for scale: 0.8–1.7 s **per row** against 0.05–0.08 s
for a *whole batch* of gazetteer joins.

Two things this explicitly does **not** settle:

- **The fine-tune.** Untested. Its case is now narrower than the plan assumed — with those four
  done, the remaining tail is mostly underdetermined (`5W` vs `5E`; a municipality with 20+
  candidates) and no model size fixes an underdetermined problem.
- **Segmentation.** Splitting messy multi-line or POI-prefixed input *before* the parser runs is
  a routing job, not a knowledge job, and no measurement here touches it. Part A's noise grammar
  is comma-delimited and cannot generate that class — the same blind spot that hid the
  `Sault Ste. Marie` bug from both halves of the harness.

## Next steps, in the order the measurements justify

The first four items of the previous list came out of *What a local LLM adds* and are done — see
*Fixed, and worth keeping fixed* for what they bought. What is left is ordered the same way, by
rows recovered per unit of effort.

1. **Candidate readings for the direction and type steps** (modes 2 and 3). The framework and the
   arbitration now exist; what is missing is the two strategies and their gates. One mechanism
   fixes both: when a stripped reading finds nothing in the gazetteer, retry with the token
   restored to the name. Affects ~686k addresses' worth of street forms; the direction half fires
   even on clean input. The name gate now recovers some of this incidentally — whole-word
   containment catches a type the parser ate whenever the gazetteer has the fuller name — so
   re-measure the remaining size before building it.
2. **Reject a province name as a municipality** (mode 6). An afternoon.
3. **Decide what `MUN_NAME = NA` should mean** for the still-ambiguous rows (mode 1). The
   determined case is now answered; this is the 157 rows with 2 or more candidates. Either
   document `NA` as the honest answer or return candidates. A design decision, not a bug fix.
4. **Period-folded street index** (mode 5), only if it is by then the largest remaining item.

## Deferred

`R/normalize_llm.R` and the `data-raw/finetune/` track from the approved plan are not built. The
plan's sequencing said the eval decides whether they are warranted. The eval now exists, and
*What a local LLM adds* measures the off-the-shelf half of that question: on this residual a
foundation model scores below the gazetteer's own scorer, so Layer 3 as an off-the-shelf
component is not warranted and is not merely deferred.

The **fine-tune** is a separate claim and remains untested. Revisit it after the remaining *Next
steps*, against whatever residual is left — which is where the honest case for it has to be made, since the
classes those items remove are the ones it would otherwise be credited with.

Also noted in the plan and still outstanding, unrelated to normalization: `reverse_geocode()`
builds its `address` string from `MAIL_*` columns, and `MAIL_STREET_NAME` is empty for 957,307
addresses, so those results return an address with no street. A `coalesce(MAIL_*, OFFICIAL_*)`
would fix it.
