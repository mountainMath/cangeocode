# Address normalization — what still fails, and what to do next

Companion to `.claude/CLAUDE.md`, which documents *why the code is shaped the way it is*.
Lives in `inst/notes/`, so it installs with the package and
`system.file("notes", package = "cangeocode")` finds it.
This file documents *where it currently falls short*. Every number here is measured, not
estimated; each section says which measurement produced it so it can be re-run and disputed.

**As of:** 2026-08-21, commit `8694726`, NAR release `2026-06` (17,362,476 addresses).
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

Two consecutive full runs are now byte-identical apart from timing. **Any change to the parser
should be evaluated by running the harness before and after on the same seed**, not by comparing
against a number written down here.

## Where it stands

**Part A** — 5,000 real NAR rows rendered into noisy surface forms and parsed back:

| field | exact | | recovered when the surface form dropped it |
| --- | --- | --- | --- |
| `CIVIC_NO` | 99.9% | `STREET_TYPE` | 86.9% |
| `STREET_NAME` | 96.0% | `STREET_DIR` | 96.1% |
| `STREET_TYPE` | 97.2% | `MUN_NAME` | **46.3%** |
| `STREET_DIR` | 98.9% | `PROV_ABVN` | 91.7% |
| `MUN_NAME` | 91.8% | `POSTAL_CODE` | 0.3% |
| **ALL** | **95.2%** | **CORE** (civic + name) | **96.0%** |

**Part B** — 5,000 Corporations Canada registered offices, i.e. addresses nobody cleaned:

| | |
| --- | --- |
| street name and civic number extracted | 98.8% |
| joins a real NAR address (civic + name + municipality + province) | 86.0% |
| ... and the filer's postal code confirms it | 81.1% |

### How to read these

- **`POSTAL_CODE` 55.8% / 0.3% is not a failure.** The normalizer never invents a postal code.
  The 55.8% is simply the share of rendered forms that carried one; the 0.3% is rounding noise on
  those it did not. Ignore both.
- **Part B's 80.8% is a lower bound, not an accuracy.** "Confirmed" demands that the filer's exact
  six-character postal code appear on a NAR row for that civic number and street. Corporate
  registry postal codes are frequently stale or simply wrong, and NAR often carries a different
  one for the same building. Of six hand-checked "unresolved" rows, four parsed correctly *and*
  exist in NAR — `1321 Matheson Blvd E, Mississauga` (NAR has it at `L4W1R1`, the filer wrote
  `L4W0C2`), `239 Temby Private, Ottawa` (`K1T2W6` vs `K1T2V6`), `11 Crossbill Rd, Brampton`, and
  `1123 Leslie Street, Toronto`.
- **Part A's `MUN_NAME` 46.3% is the single biggest number on the page**, and it is mostly not a
  parser problem. See *Failure mode 1*.
- **Part A and Part B do not always move together, and that is the harness working.** The
  `SUITE 800-666` fix below lifted Part B's join rate by 0.4 points and left every Part A figure
  byte-identical, because the noise grammar never renders a designator in front of a hyphenated
  unit-civic pair. Part A measures the mess we imagined; Part B measures whether we imagined the
  right mess. A Part B gain with no Part A movement is a gap in the grammar, not a fluke.

## Failure modes, ranked

Ranking is by measured size. The categorisation comes from running Part A's sample and grouping
the ~500 misses by which fields disagreed (`scratchpad/miss.R` pattern; the top combinations were
`mun` 234, `name` 56, `mun+prov` 49, `type+mun` 38, `name+mun` 32, `name+type` 29).

### 1. The municipality was never in the string and cannot be inferred — ~60% of Part A misses

`27 Feagan Dr, Ontario` → we return `MUN_NAME = NA`; the answer was `SCARBOROUGH`.
`2340 SHORE RD, NS` → `CARLETON VILLAGE`. `2 richard street` → `SYDNEY MINES`.

When the surface form drops the municipality, the gazetteer can only recover it if the street name
is unique enough within the province to identify one. It manages this 46.3% of the time. The
remainder are genuinely ambiguous: there are many `Richard St`s in Nova Scotia and nothing in the
input distinguishes them.

**This is a ceiling, not a bug.** Raising it means either accepting a most-populous-match heuristic
(which would return confident wrong answers) or requiring a postal code. The right response is
probably to keep returning `NA` and let `confidence` carry the ambiguity — but the alternative
worth measuring is *returning all candidates* when the street name resolves to a small number of
municipalities, which the `output = "multiple"` convention in `reverse_geocode()` already models.

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
the second reading. The two-reading arbitration is the same shape as the fix for mode 3, and the
two should be built together.

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

**Fix:** same two-reading arbitration as mode 2 — when stripping a trailing type leaves a name the
gazetteer cannot find, retry with the word restored and no type.

### 4. Keyboard typos in the street name — 76.2% vs 98.3% clean

The fuzzy branch recovers most single-character corruptions (`bsnk street` → `Bank`,
`Srthur Rd` → `Arthur`, `Hlory Court` → `Glory`). It fails where the corruption lands on another
plausible token: `Yinge` → we keep `YINGE`, the answer was `Yonge`; `NAPLE RD` stays `NAPLE`,
the answer was `Maple`.

This is the tail the plan earmarked for Layer 3, and it is the clearest case for it: a language
model has priors about Canadian street names that Jaro-Winkler does not.

### 5. Street-name periods in the exact branch — 104,272 addresses (0.6%), unhandled by design

`298 ST. SIMON, BELLE RIVER` parses to `SIMON ST` — the period-bearing name `St. Simon` cannot meet
its fold key. `nar_gazetteer_sql()` folds periods out of both sides on the municipality joins and
the fuzzy street comparisons, but deliberately **not** on the exact-branch `Streets.NAME_FOLD`
join, which would cost the `str_name_idx` index. See CLAUDE.md for the rule.

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

## Next steps, in the order the measurements justify

1. **Two-reading arbitration for the direction and type steps** (modes 2 and 3). One mechanism
   fixes both: when a stripped reading finds nothing in the gazetteer, retry with the token
   restored to the name. Affects ~686k addresses' worth of street forms; the direction half fires
   even on clean input.
2. **Reject a province name as a municipality** (mode 6). An afternoon.
3. **Decide what `MUN_NAME = NA` should mean** (mode 1). Either document it as the honest answer
   or return candidates. This is a design decision, not a bug fix, and it is worth taking to the
   user before building anything.
4. **Re-measure, then decide on Layer 3.** The plan's gate was: Layer 1 above 95% on rendered NAR
   inputs means the LLM's job is a small tail. It is at 95.2% ALL / 96.0% CORE. After items 1–2
   the tail that remains is mode 4 (typos) and mode 1 (unrecoverable municipality) — the first is
   a good fit for a model, the second is not a model problem at all. **That is an argument for a
   narrow typo-correction fallback rather than a full parse-with-an-LLM layer**, and it changes
   what the fine-tune would be trained on.
5. **Period-folded street index** (mode 5), only if item 4 leaves it as the largest remaining item.

## Deferred

`R/normalize_llm.R` and the `data-raw/finetune/` track from the approved plan are not built. The
plan's sequencing said the eval decides whether they are warranted; the eval now exists and says
the remaining tail is narrower and differently-shaped than the plan assumed. Revisit after item 4.

Also noted in the plan and still outstanding, unrelated to normalization: `reverse_geocode()`
builds its `address` string from `MAIL_*` columns, and `MAIL_STREET_NAME` is empty for 957,307
addresses, so those results return an address with no street. A `coalesce(MAIL_*, OFFICIAL_*)`
would fix it.
