# Address normalization — what still fails, and what to do next

Companion to `.claude/normalization.md`, which documents *why the code is shaped the way it is*.
Lives in `inst/notes/`, so it installs with the package and
`system.file("notes", package = "cangeocode")` finds it.
This file documents *where it currently falls short*. Every number here is measured, not
estimated; each section says which measurement produced it so it can be re-run and disputed.

**As of:** 2026-08-23, commit `4180d4f`, NAR release `2026-06` (17,362,476 addresses).
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

A third harness measures this parser against a purpose-built neural tagger on two corpora it was
never tuned on — a generated dirty corpus and StatCan's healthcare-facility free text. It is the
one that found the segmentation gap, and it has its own note,
[`deepparse.md`](deepparse.md):

```sh
R_ENVIRON_USER=/dev/null NAR_CACHE_PATH=/Users/jens/data/nar \
  Rscript data-raw/dirty_corpus.R                        # ~35 min, once, needs Ollama
R_ENVIRON_USER=/dev/null NAR_CACHE_PATH=/Users/jens/data/nar \
  EVAL_N=5000 DP_MODEL=fasttext Rscript data-raw/eval_deepparse.R
```

Unlike the two above it is **not** fully reproducible: the corpus is a local model's output, and
regenerating it will move the numbers. `<EVAL_CACHE>/dirty_corpus.csv` is the corpus of record.

Two consecutive full runs of `eval_normalize.R` are byte-identical apart from timing. **Any change to the parser
should be evaluated by running the harness before and after on the same seed**, not by comparing
against a number written down here.

## Where it stands

**Part A** — 5,000 real NAR rows rendered into noisy surface forms and parsed back:

| field | exact | | recovered when the surface form dropped it |
| --- | --- | --- | --- |
| `CIVIC_NO` | 99.9% | `STREET_TYPE` | 88.2% |
| `STREET_NAME` | 98.1% | `STREET_DIR` | 96.8% |
| `STREET_TYPE` | 97.5% | `MUN_NAME` | **63.4%** |
| `STREET_DIR` | 99.3% | `PROV_ABVN` | 95.3% |
| `MUN_NAME` | 94.5% | `POSTAL_CODE` | 0.3% |
| **ALL** | **97.4%** | **CORE** (civic + name) | **98.1%** |

**Part B** — 5,000 Corporations Canada registered offices, i.e. addresses nobody cleaned:

| | |
| --- | --- |
| street name and civic number extracted | 98.9% |
| joins a real NAR address (civic + name + municipality + province) | 88.8% |
| ... and the filer's postal code confirms it | 83.8% |

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
- **Part A's `MUN_NAME` 63.4% is still the smallest number on the page** that means anything, and
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

### 2. A leading direction word in the street name — fixed, and nothing left in it is about directions

Fixed on 2026-08-25 in two steps: `nar_dir_lead_variant()` offers the unstripped reading, and a
tie-break in `nar_gazetteer_winner()` settles the case where both readings are perfect. The full
before/after for each is under *A stripped leading compass word, offered back as a parallel
candidate* and *A tie broken by the municipality the reading kept*.

On the 2,500-address probe the parser now keeps the compass word in the name **2,473** times and
loses it **27** — down from 453 before either step. What is left has been read row by row, and
**not one of the 27 is a failure of the direction step**. Two causes account for all of them.

**Mode 3 wearing mode 2's clothes — 22 rows.** A name-final word that is also a NAR street type is
eaten as the type before the direction step is ever reached, so there is no leading compass word
left to restore and the restored reading is nonsense:

```
4250 West Hill AV, MONTRÉAL, QC   ->  name AV,   dir O     (HILL is a NAR type)
1072 East Centre, SASKATOON, SK   ->  name EAST            (CENTRE is a NAR type)
50 West Crest, ANCASTER, ON       ->  name West, dir N     (CREST is a NAR type)
```

Seventeen of the 22 are Montréal's `West Hill AV`, and they fail *benignly*: nothing matches, they
fall through to `rules`, and they end up unplaced rather than confidently placed on another
street. This is failure mode 3 and it is counted there too — the direction fix cannot reach it,
and building mode 3's candidate reading would take these with it.

**NAR files the same city both ways — 3 rows.** Not a parse failure at all. Lethbridge carries
`Parkside DR S` (2801–3329, 28 addresses) *and* `South Parkside DR` (2618–4012, 55); Simcoe
carries `Main ST N` (56–121, 13) *and* `North Main ST` (21–196, 54). Where the civic ranges
overlap, both readings score 1.0, both kept the municipality, and there is nothing left to
arbitrate on — the ambiguity is in the register, not in the string. Where they do not overlap the
score already settles it correctly: `151 North Main ST, SIMCOE` resolves to `North Main` because
151 is outside `Main ST N`'s range and the civic-in-range term is worth 0.12, while `100 North
Main ST` is inside both and goes to the baseline. This is the same duplication the *source-nar*
vignette documents under *The same street, filed two ways*, met from the other side, and no
tie-break can help: the tie is real.

The remaining 2 are single rows with no shared cause — `695 South Shore RD, NAPANEE` resolving to
a longer street of a similar name, and `11 South Country RD, RM OF DUNDURN` losing the word
outright.

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

**Fix:** the same mechanism mode 2 now uses — offer the reading in which the word was *not* taken
as the type, and let the gazetteer choose between them. Whether it needs a gate is the open
question and mode 2 does not settle it: there both readings named the same municipality, which is
what made an ungated parallel candidate safe. **Read the gate finding in *Fixed* first** — a
restored-name candidate that happens to exist *somewhere else* will outscore the baseline on the
gazetteer's own score.

### 4. Keyboard typos in the street name — 91.9% vs 98.6% clean

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

**Two street-type surfaces the lexicon never had.** Re-diagnosing Québec
(see [`quebec-addresses.md`](quebec-addresses.md)) found 120 of 316 parser-side failures coming
back with no `STREET_TYPE` at all, and the token sitting where the type should be was `CHEM.` on
41 of them and `BD` on 18 — 59 failures on a 4,000-address sample from two abbreviations absent
from `data-raw/street_types.csv`. Adding `CHEM,CH,fr` and `BD,BOUL,fr` and rebuilding
`R/sysdata.rda` took that sample from 78.3% to 79.8% confirmed and Part B nationally from 88.4%
to 88.8% joined, 83.3% to 83.7% confirmed, Québec 75.5% to 77.5%. Part A is byte-identical and
so are both deepparse corpora, which is the expected shape: Part A renders its surface forms out
of NAR's own type vocabulary, so a surface NAR never writes cannot appear there. **This is the
cheapest class of fix in the package and the harnesses cannot find it** — only a residual
inspection that tabulates the token following the civic number can, and
`data-raw/diagnose_quebec.R` now prints that table. What is left of the tail is single rows
(`BOU.`, `BV`) plus a different bug: `nar_norm_text()` strips every period at input, so
`BOUL.DES GRANDES PRAIRIES` becomes `BOULDES ...` and the type is glued to the word after it.

### A stripped leading compass word, offered back as a parallel candidate

`nar_parse_one()` took a leading `NORTH`/`SOUTH`/`EAST`/`WEST` into `STREET_DIR`
unconditionally. For some 92,000 NAR addresses that word is the *name* — `East Uniacke Rd`,
`West Beaver Creek Rd`, `North Park St` — and NAR says so by leaving the direction column empty on
both name families.

**The measurement that decided the shape of the fix.** Draw 2,500 of those addresses and write
each one exactly as NAR spells it (`929 North River RD, OTTAWA, ON`). The parser kept the word for
2,047 and lost it for 453 — and only **68** of the 453 were the unplaced case the rule was assumed
to cause. The other **385**, 381 of them at `parse_source = "gazetteer"`, were resolved onto a
*different* street, usually its mirror image, with a clean score and nothing in the output saying
so:

```
90 South Edgely Ave, Scarborough, ON       ->  North Edgely
125 East Beaver Creek Rd, Richmond Hill    ->  West Beaver Creek
2085 North Orr Lake Rd, Elmvale, ON        ->  South Orr Lake
161 West 19th St, Hamilton, ON             ->  East 19th
135 East Liberty St, Toronto, ON           ->  Liberty
```

The arithmetic is in `nar_gazetteer_sql()` and it is not close. Strip `East` off `East Beaver
Creek` and the probe is `BEAVER CREEK`, which whole-word containment scores 0.90 against *both*
halves of the pair; direction agreement is worth 0.06 and the stripped reading has no direction
left in the name to agree with, so tie-breaks separate them and the wrong one wins about as often
as the right one. Restore the word and the probe is `EAST BEAVER CREEK`, which matches one exactly
at 1.0 and the other not at all.

**That is why it is a parallel candidate and not a fallback.** The prescription here until
2026-08-25 was "retry with the token restored when the stripped reading finds nothing", which
repairs 68 of the 453 and leaves 385 confidently wrong. `nar_dir_lead_variant()` offers both
readings to the gazetteer and lets 1.0 beat 0.868.

**It needs no gate, which is the one thing that distinguishes it from the anchored readings.**
`nar_baseline_is_defective()` exists because offering a municipality variant unconditionally costs
rows. Neither of its two reasons applies here: both readings carry the same municipality, so it is
a like-for-like comparison of two street names in one place and the restricted-beats-unrestricted
asymmetry never arises; and a street genuinely called `Park` is still matched exactly at 1.0 by
the baseline while the restored `NORTH PARK` matches nothing. There is also nothing for a gate to
*see* — `125 East Beaver Creek Rd, Richmond Hill, ON` names a real municipality, splits on commas
and parses cleanly.

**The word travels as the token that arrived.** `dir` is canonicalized to an abbreviation and `E`
never meets `East Uniacke`: the match fold does not expand it. So `nar_parse_one()` carries the
original token out as an attribute — a column would have to be added to every one of its
`return()` paths and `rbind()` over the readings drops it anyway. Abbreviations are restored too;
`W GEORGIA` is not a name NAR carries so that candidate simply loses, and the ~2,000 addresses
whose NAR name really does open with an abbreviated compass word are what it is there for. Only
the *leading* word: a trailing direction and one sitting between the type and the municipality
have not been measured.

> **Measured.** On the probe, losses **453 → 45** of 2,500, and the confident-wrong-street class
> **385 → 6**. Rows keeping the word place 99.5% of the time; rows losing it now place 35.6%,
> down from 68.4%, which is the point — what is left of the loss set is unplaceable rather than
> plausibly misplaced. Eval harness, same seeds: Part A ALL **97.3% → 97.4%**, CORE
> **98.0% → 98.1%**, `STREET_NAME` **98.0% → 98.1%**, NS **98.0% → 98.7%**, ON
> **97.6% → 97.9%**, one more row resolved at the gazetteer layer and one fewer falling back to
> rules. Part B postal-confirmed **85.0% → 85.1%**, coverage failure **15.0% → 14.9%**. Part A's
> `grid` and `numeric_street` pattern buckets shed rows to `civic_street`, which is the
> reclassification working: `West 19th St` in Hamilton is a street with a name, not a grid address.

`data-raw/probe_direction.R` reproduces all of it in about two minutes, and now also tabulates the
residual whole. NAR is a fair reference here, which is unusual in this package: every accuracy
measurement elsewhere carries the not-ground-truth caveat because it compares a *coordinate* to
NAR's. This asks whether the parser reproduces the decomposition NAR itself records — name here,
direction there — and NAR is by definition authoritative about its own columns. See also
`## The same street, filed two ways` in `vignette("source-nar")`, where the 92,167-address
national inventory and NAR's own 162 self-contradictions are reported.

The 45 that remained were diagnosed here and turned out not to be a direction problem at all,
which is what the next entry fixes.

### A tie broken by the municipality the reading kept

The second half of the item above, and it started as a diagnosis rather than an idea. Of the 45
losses `nar_dir_lead_variant()` left, 39 had the restored reading offered, matched **exactly**,
and lose a tie:

```
170 North Park ST, BRANTFORD, ON  ->  Park ST N, HAMILTON       (mun_evidence "csd")
125 East Main ST, WELLAND, ON     ->  Main ST E, PORT COLBORNE  (mun_evidence "copostal")
```

Brantford has `North Park ST` (455 addresses) and no plain `Park`; Welland has `East Main ST`
(327) and no plain `Main`. So the restored reading scores 1.0. But the baseline reaches 1.0 too,
by **leaving the municipality the string named** — and the 0.88 swap penalty that would have
separated them exempts an *attested* swap, by CSD in the first case and by a shared full postal
code in the second. Two readings of one string, both perfect, and the baseline wins ties by rule.

**The exemption is right and the tie is not.** Not fining an attested swap is a measured decision
with its own entry below: those swaps place well, and refusing them costs 13 matches per error
avoided. That reasoning is about a swap competing against *nothing* — it is the only reading on
offer, and the question is whether to accept it. It says nothing about a swap competing against a
reading that stayed put and scored the same. Score has already declined to separate them and the
penalty has already declined to; the only question left is which reading did what it was told.

So: **on equal scores, prefer the candidate whose answer is in the municipality the string
wrote.** One term in the `order()` inside `nar_gazetteer_winner()`, reading the `mun_kept` column
all three gazetteer queries already emit.

**Where it cannot fire, which is most places.** It is inert on a row with one candidate, which is
the overwhelming majority. It is inert where the string named no municipality: `mun_kept` compares
against the *baseline* reading's municipality, so with nothing written it is `FALSE` for every
reading of the row. And that same anchoring makes it inert on the municipality-anchored variants —
those exist to re-segment a trailing run into a town the baseline missed, so it is *their* answers
that differ from the baseline's, and the term points back at the baseline they were generated to
challenge. It bites in one place, which is the place it was measured on.

> **Measured.** Probe losses **45 → 27** of 2,500. The whole of the change is in the placed-wrong
> class: rows that lose the word placed 35.6% of the time before and **7.4%** after, because the
> 18 rows recovered were precisely the ones being confidently placed in the wrong city. Eval
> harness, same seeds, both parts: **byte-identical output on 10,000 rows** — every figure in
> Part A and Part B unchanged. That is the expected result and it is the safety evidence: the tie
> needs a compass-led street *and* a municipality with an attested-swap partner, which no row in
> either random sample happens to have. The mechanism argument above is what covers the rest, and
> `tests/testthat/test-normalize.R` pins all four arms of the `order()` directly.

What it does not reach is a tie where both readings kept the municipality, because NAR files the
city both ways — Lethbridge's `Parkside DR S` alongside `South Parkside DR`. That tie is real and
no rule about municipalities can break it; see failure mode 2.

### Québec's own register, as a second gazetteer pass

`rqa_import()` loads the *Répertoire québécois des adresses* into the same DuckDB file as its own
tables, and `nar_resolve_gazetteer()` now runs a second pass against them — Québec only, over the
rows the NAR pass could not settle, labelled `parse_source = "rqa"`. It is what the item at the
top of the previous *Next steps* list asked for. It is also the clearest case in this note of a
projection that did not survive being built, and the reason is worth more than the feature.

**What was projected:** a Québec ceiling of 81.8% → 88.3%, "six points, and nothing else on this
list is worth six points."

**What was measured**, on the same 5,000-filing Part B sample (942 Québec rows):

| | |
| --- | --- |
| Québec confirmed against NAR | 77.5% |
| Québec confirmed against NAR **or** RQA | 83.0% |
| ... rows only RQA confirms | 5.5% |
| rows the `rqa` gazetteer pass answered | **4** |

Those are two different effects and the projection conflated them.

- **The 5.5 points are a confirmation-set effect.** They arrived the moment `RqaAddresses`
  existed for the harness to judge against, and most of the parses they newly confirm were the
  NAR pass's all along. Nothing about the parser changed to earn them. `data-raw/eval_normalize.R`
  now reports the two registers on separate lines for exactly this reason: judging Québec against
  NAR alone scores ~475,000 real addresses as parse failures, but folding the second judge into
  the headline number would make a better judge and a better parser indistinguishable.
- **The parser gain is 4 rows in 942.** The pass works — on a 3,000-address sample drawn from
  what NAR is missing and RQA holds, it answers **8.9%** of rows and every one of them exactly.
  The Québec filings NAR cannot settle are simply not those addresses. They are *mistyped*:
  `Bouceherville`, `ST.CATHERINE ST.WEST`, `1052 N.P. LAPIERRE`, `1603 - 3410, rue Peel`,
  `13 place Jason Roxboro`. A second register cannot read a misspelling.

**The lesson for the rest of this list.** The 41.3% coverage share in
[`quebec-addresses.md`](quebec-addresses.md) was measured over *NAR's* residual — addresses that
fail to join. It was read as if it were the parser's residual. Those two sets overlap far less
than the note assumed, because the gazetteer's fuzzy branch already answers most coverage-class
rows with a near neighbour, correctly, and what it leaves behind is dominated by input the
parser cannot read at all. Before promoting any item here on a coverage argument, check whether
the rows it names are ones the parser currently *fails*, not merely ones NAR currently *lacks*.

Keeping it is still right: it costs nothing outside Québec, it cannot displace an answer the NAR
pass gave, and it is correct on the population it exists for. It is filed here rather than under
*Measured and deliberately not done* on those grounds, not on the strength of the number.

**One real bug came out of building it.** `nar_match_fold_sql()` replaced `-` where its R twin
replaced `-` *and* the en and em dash, because stringi's `Latin-ASCII` transliteration inside
`nar_fold()` had already converted them on the R side and DuckDB's `strip_accents()` had not.
NAR carries zero en dashes — it transliterates them to `--` in 2,134 addresses — so nothing
surfaced this while NAR was the only gazetteer. RQA keeps the en dash in 11 street names over
2,472 addresses, `du Bord-du-Lac–Lakeshore` among them, and the two registers' spellings of the
same street folded apart and never met. The existing parity test could not catch it: it folds its
inputs in R first, which is the step that hides the character.

### A comma-free string, segmented on the municipality inventory

The other thing the deepparse benchmark found, and the last one it was still winning. ODHF's
custodians write the whole address as one unpunctuated run — `8512 164th st surrey bc v4n 1e5`,
2,241 rows of it — and nothing in the string marks where the street stops. The parser inferred
the boundary from the street type and got it wrong in three distinct ways, all of them invisible
to a corpus that uses commas:

* the place name's first word was taken for the street's direction. `3908 loraine ave north
  vancouver` leaves `VANCOUVER`, which is a real municipality, so nothing downstream saw a
  problem.
* the place name's last word was taken for the street type. `RIDGE`, `ISLAND`, `BAY` and `BEACH`
  are all NAR street types, so `maple ridge`, `bowen island`, `brentwood bay` and `qualicum
  beach` each ate their own first word into the street name and left no municipality at all.
  `4830 scott ave terrace` is the degenerate case: the whole place name is a street type.
* the street named no type, so there was no boundary to find. `27830 swensson abbotsford`,
  `1818 kingsway vancouver`.

The fix is one more condition on `nar_baseline_is_defective()`: offer the anchored readings when
a **longer trailing run than the baseline claimed also names a municipality**. That is evidence
from the inventory rather than a rule about token shapes, which matters, because the shapes are
identical to the ones that must not move — `100 MILE HOUSE` and `MILE HOUSE`, `NORTH BAY` and
`BAY`. Two new guards keep it honest: a residue that is nothing but particules or a bare street
type is not a street name (`nar_is_street_name()`), and a run that *is* a street type is only
free to be the municipality when the street still names one of its own, which is the whole
difference between `4830 scott ave terrace` and `82 Fesroches Trail`.

`odhf_full` goes **57.5% → 62.3%** postal-confirmed, past deepparse-as-segmenter's 61.3%;
municipalities missing from its failures fall 214 → 55. **Part A and Part B are both exactly
unchanged** — 97.9% CORE, 94.4% MUN, 98.9% / 88.4% / 83.3% — and structurally so rather than by
luck: the run scan reaches back at most one token short of the last comma segment, so a
comma-delimited municipality is longer than anything it can propose and the condition cannot
fire. The generated `llm` corpus trades 0.2 CORE points for 2.1 municipality points.

What it does **not** do is undo a direction without a connection. `NORTH VANCOUVER` and
`VANCOUVER` are both real, both readings score the same, and the baseline wins ties by rule, so
rules-only keeps `VANCOUVER` and only the gazetteer moves it. See
[`normalization.md`](../../.claude/normalization.md) for why a longest-match tie-break would be
the wrong repair.

### A prose prefix in front of the address, cut before anything else reads the string

Found by the deepparse benchmark, not by this harness, and it is the largest effect measured on
input the parser was not tuned against. Every civic-number rule anchors on a number at the *front*
of the string, so `located at 41 Cultus Rd, Clear Creek, ON` did not parse badly — it parsed as a
street called `LOCATED AT 41 CULTUS` with no civic number at all. `nar_strip_lead_prose()` cuts to
the first digit-initial token at the top of `nar_parse_rules()`, behind four guards that each hold
back a real address form (`Highway 7`, `Apt 4B-1234`, `PH12, 2160 Terry-Fox Av`,
`Chemin du 4e Rang`) and with delivery lines exempt. The generated dirty corpus goes 70.9% →
**93.2%** CORE; `careof` 18.6% → 92.4%, `verbose` 0.0% → 86.6%.

**Part A does not move at all, and Part B moves 0.1 point.** The rule touches 0 of Part A's 4,982
rows and 6 of Part B's 5,000, because both corpora put the civic number first — one more instance
of the gap this section keeps documenting: the noise grammar cannot render a class it was not told
about. What made this one visible was building a corpus specifically to escape it. See
[`deepparse.md`](deepparse.md) for the measurement and
[`normalization.md`](../../.claude/normalization.md) for what each guard is holding back.

### The gazetteer compares on a folded name, and Quebec stops failing at the door

The largest single gain the parser has had, and it is three characters wide. The fuzzy branch used
to compare NAR's spelling to the input having settled only case, accents and periods.
[`nar_match_fold()`](../../.claude/normalization.md) also folds the hyphen and the apostrophe to a
space and spells `ST`/`STE` out to `SAINT`/`SAINTE`, on **both** sides — probe and gazetteer — and
the same fold now runs on the municipality names in `MunAlias` and `PostalMun`.

Three failures were riding on this, all of them Quebec's ordinary spelling rather than anything
noisy:

* **The particule nobody writes.** NAR files `du Curé-Labelle`, `du Square-Victoria`,
  `du Président-Kennedy`, `de Senneville`. People write `CURE LABELLE`, `VICTORIA`, `KENNEDY`,
  `SENNEVILLE`. Whole-word containment is the rule that should catch exactly this — and it could
  not fire, because with the hyphen in place `SQUARE-VICTORIA` is a single word and the probe is
  not inside it.
* **Saint abbreviated.** `ST-JACQUES` against `Saint-Jacques` is six edits on thirteen characters:
  past the name gate, past the single-edit rule, and nowhere near the top of a similarity ranking.
* **Saint abbreviated in the *municipality*,** which was the more expensive half. `ST-LAURENT`
  never resolved through `MunAlias`, and the municipality is what restricts the candidate set —
  so the street had no candidates to be matched against at all, whatever its own spelling.

Measured on the standing 5,000-address harness:

| | before | after |
| --- | ---: | ---: |
| Part B, joins a real NAR address | 86.6% | **88.3%** |
| Part B, confirmed by the postal code | 81.7% | **83.3%** |
| Part B, **Québec** | 68.2% | **75.5%** |
| Part B, `french_street` pattern | 80.8% | **83.7%** |
| Part B, Ontario | 86.5% | 86.7% |
| Part A, STREET_NAME | 98.0% | 97.9% |
| throughput | 399/s | **460/s** |

Québec gains **7.3 points**, and about three quarters of that is the municipality half rather than
the street half. Nothing regresses except one row of Part A, and it is worth naming because it is
the shape of the risk: `19 sr arnaud st, guelph, on` is a rendered typo for `St Arnaud`, a real
Guelph street where `St` genuinely is Saint. Unfolded, `SR ARNAUD` sat one edit from `ST ARNAUD`
and matched; folded, the gazetteer side became `SAINT ARNAUD` and a typo inside the abbreviation
can no longer reach it. One row in 4,982, against 69 Québec rows gained.

The throughput went **up**, which was not the intent. Folding is cheap but the edit distance is
not, and folding the hyphen out moved far more pairs past the 0.70 similarity prefilter that
guards it — `COTE-DES-NEIGES` and `COTE DES NEIGES` are near-identical strings. That first cost
45% of the normalizer's speed, and the fix is a length gate: one Damerau-Levenshtein step cannot
bridge a length difference greater than one, so pairs that fail an integer comparison never reach
the distance. It rejects nothing the distance would have accepted, and it made the query 6.9x
faster than the folded version and 15% faster than the code before any of this.

`StreetFold` exists for the same reason at one remove: the fold is computed once per connection
over all 511,848 gazetteer names rather than once per candidate per probe row.

### The parser produces candidate readings, and evidence chooses between them

`R/normalize_variants.R`, and the framework mode 2 was built on and mode 3 is still waiting for. One string
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

### A municipality swap is fined unless something in NAR attests it

The gazetteer's fuzzy branch restricts candidates to a municipality, but it does so through
`MunAlias`, which keys on the **census subdivision**. In a regional municipality that is a very
wide door: `MILFORD, NS` admits 166 communities spread over 127 km, all of them filed under
Halifax Regional Municipality. Within a set that wide a near-miss on the street name can beat the
right answer — `12 WILDWOOD DR, MILFORD` scored `Windwood DR, MIDDLE SACKVILLE` at 0.952 over
`Wildwood AVE, HALIFAX` at 0.900, because agreeing on the type is worth 0.10 and one transposed
letter costs 0.072. The parse looks clean and the street is real; it is simply somewhere else.

The correction is two signals read out of NAR rather than assumed. **Two mailing municipality names
that appear on the same full six-character postal code** are two labels for one delivery geography;
`nar_mun_copostal()` builds the directed pairs — 32,216 of them, in 0.2 s. The FSA is not enough —
in rural Nova Scotia one FSA covers most of the errors this exists to catch. And **the census
subdivision the street already sits in** catches what a postal code never can: an amalgamation did
not merge the delivery names, so `Bathurst St, Toronto` reaching a street NAR still mails to
`NORTH YORK` has no shared postal code and never will. `Streets.CSD_ENG_NAME` carries that
relationship directly. Neither signal is a curated alias list, in either direction.

`nar_gazetteer_sql()` multiplies the score by `mun_swap_penalty` (0.88) when the resolved
municipality differs from the one written, unless one of those two arms attests it, or the written
name is one NAR has never seen on any postal-coded row (in which case there was nothing to test).
The same `CASE` is emitted as `mun_evidence`, so which arm answered is an output column and not
just an internal decision.

Measured against PVSC's independent Nova Scotia points (see
[`nova-scotia-pvsc.md`](nova-scotia-pvsc.md)), the signals separate the classes by about two
orders of magnitude before any penalty is applied: attested swaps sit at p95 121 m and 0.62%
beyond 5 km; unattested swaps at p95 12.5 km and 6.83%. The penalty at 0.88 then takes exact
unambiguous building matches from p95 127.6 m to 122.2 m and errors past 5 km from 98 to 42, for
373 lost matches out of 32,886. 0.88 is the knee: 0.90 → 0.88 buys 27 gross errors for 305 matches,
0.88 → 0.86 buys 1 for 59, and 0.86 → 0.85 buys 9 for 496. Refusing the whole unattested class
costs 928 matches, 85% of which were within 100 m of PVSC's point. That last figure is the reason
the swap is fined and not forbidden.

**The swap is scored against the baseline reading, not the reading being scored.** The parser emits
several candidates per string, and an alternative reading may re-segment the trailing run into a
shorter municipality — `HOWIE CENTRE` read as `CENTRE`, itself a Nova Scotia municipality sharing a
postal code with `LUNENBURG`. Scoring the swap against that lets a truncation manufacture its own
attestation. This was live long enough to be measured wrong; it laundered 184 rows into an attested
class, and fixing it bought 60 exact matches at the cost of one gross error.

`normalize_address()` now returns `mun_remapped` and `mun_evidence`, so the class is visible rather
than merely smaller; the residual is bimodal and a `confidence` number cannot express that. The RQA
pass is deliberately left unpenalised — see [`normalization.md`](../../.claude/normalization.md).

**The exemption for an attested swap is a decision about a swap that competes against nothing.**
The measurements above weigh accepting the swap against refusing it, which is the right question
when it is the only reading on offer. It is not the right question when a second reading of the
same string stayed in the municipality that was written and scored the same — there the exemption
is what lets the wrong one through, and the answer is the tie-break under *A tie broken by the
municipality the reading kept*, not a change to the penalty.

## Measured and deliberately not done

Recording these so they are not re-litigated:

- **Joining on `CSD_ENG_NAME` as well as `MAIL_MUN_NAME` buys 0.2 points.** The amalgamation story
  is real — NAR files `1123 Leslie St` under `NORTH YORK` while the filer writes `Toronto` — but
  `MunAlias` already absorbs nearly all of it. Measured on Part B's sample: municipality agreement
  85.6%, adding CSD name 85.9%. **Still true as a join, and reversed as an attestation**: the swap
  penalty introduced a question the join never asked — not *can this street be found* but *may this
  substitution stand unfined* — and there `CSD_ENG_NAME` is the only arm that can speak for an
  amalgamation, because the merger did not merge the postal codes.
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
  **This one has since been measured, it was the larger of the two, and it is now fixed.** See
  [`deepparse.md`](deepparse.md): on a corpus built specifically to escape Part A's grammar, a
  leading prose prefix took this parser from 98.0% to **0.0%** on the affected class, a neural
  tagger used purely as a segmenter recovered 12.5 points of it, and `nar_strip_lead_prose()` —
  shipped — recovers 22.3 at no cost, which reverses the comparison. The fine-tune question
  below is answered there.

## Next steps, in the order the measurements justify

The first four items of the previous list came out of *What a local LLM adds* and are done — see
*Fixed, and worth keeping fixed* for what they bought. So have the two the deepparse benchmark
added, the prefix strip and comma-free segmentation, and with them nothing in that benchmark
still beats the parser on a corpus it was never tuned on. So has the Québec re-diagnosis that
led the last list, **and so is the Québec import that re-diagnosis put at the top of it** — built,
measured, and worth 4 rows in 942 rather than the six points projected. What that cost and what it
taught is under *Québec's own register, as a second gazetteer pass*; the short version is that a
coverage share measured over NAR's residual is not a coverage share of the parser's residual, and
every remaining item should be checked against that before it is promoted.

What is left is ordered the same way, by rows recovered per unit of effort — with the caveat that
the largest number on this list has now twice been the one that moved least.

1. **The Québec odonyme decomposition, now that the import has spoken.** It outlived the item it
   used to rank below. RQA publishes every street name in the province already split into
   générique, particule, spécifique and point cardinal — 115,352 odonymes, a particule on 27.8% of
   rows — plus 551,160 cross-references to alternative and former names in `Odonymes_renvois.csv`.
   The match fold captured the cheap part of what that data was going to buy (containment now sees
   through the particule and the hyphen), so what is left is the part folding cannot do: former
   names, and génériques that are part of the name rather than the type. Six of RQA's génériques
   have no counterpart in NAR's observed types, so they must not be promoted to canonical types.
2. **A candidate reading for the type step** (mode 3). The direction half of this mechanism is
   built and measured twice over; the type half is not, and it is the same move — offer the
   reading in which the name-final word was *not* taken as the type, and let the gazetteer choose.
   ~586k addresses' worth of street forms at risk. It has also become the *only* thing left in
   failure mode 2: 22 of the 27 remaining direction losses are a type word eaten early, 17 of them
   Montréal's `West Hill AV`, so this item would take them with it. The name gate recovers some of
   it incidentally — whole-word containment catches a type the parser ate whenever the gazetteer
   has the fuller name — so re-measure the remaining size before building it. Note the one thing
   it will not fix: where NAR files the same city both ways with overlapping civic ranges, the tie
   is in the register and no reading can break it.
3. **Reject a province name as a municipality** (mode 6). An afternoon.
4. **Decide what `MUN_NAME = NA` should mean** for the still-ambiguous rows (mode 1). The
   determined case is now answered; this is the 157 rows with 2 or more candidates. Either
   document `NA` as the honest answer or return candidates. A design decision, not a bug fix.
5. **A period-abbreviated type glued to the next word.** `nar_norm_text()` strips every period at
   input, so `BOUL.DES GRANDES PRAIRIES` arrives as one token `BOULDES` and no type is found.
   Single-digit row counts in the Québec residual, and probably the same anywhere French
   abbreviations are typed without a space; splitting on a period *before* stripping it, where the
   left side is a known type surface, would cover it. Cheap, and worth doing whenever the file is
   open for another reason.
6. **Period-folded street index** (mode 5), only if it is by then the largest remaining item.

## Deferred

`R/normalize_llm.R` and the `data-raw/finetune/` track from the approved plan are not built. The
plan's sequencing said the eval decides whether they are warranted. The eval now exists, and
*What a local LLM adds* measures the off-the-shelf half of that question: on this residual a
foundation model scores below the gazetteer's own scorer, so Layer 3 as an off-the-shelf
component is not warranted and is not merely deferred.

The **fine-tune** is a separate claim, and it has now been tested at one remove.
[`deepparse.md`](deepparse.md) measures the strongest off-the-shelf *purpose-built* neural
address tagger — trained on Canadian data, not a general foundation model — on four corpora,
two of which this parser was never tuned against. It loses to the gazetteer on both tuned
corpora and on six of eight generated classes, because it carries no register and a fine-tune
would have to acquire one. It won on exactly one thing, segmentation, and rules won that by
more. **Neither a fine-tune nor a from-scratch model is warranted on the evidence.**

That harness has been re-run twice since, once for each rule the benchmark produced, which was
the condition set for reopening the case. It does not reopen. After `nar_strip_lead_prose()`,
`cangeocode` led `dp -> norm` on the generated corpus under both models and the tagger's only
remaining win was `odhf_full`, by 3.8 points on 2,241 rows of comma-free text. After the
comma-free segmentation above, that reverses too: **62.3% against 61.3%**. What the tagger still
leads on is `odhf_street` — 78.6% against 75.9% — which is not a segmentation result at all,
since those rows have their municipality appended behind a comma. It is a different question,
and one to diagnose before it is a reason to run this harness again.

Also noted in the plan, unrelated to normalization, and **done, 2026-08-25**:
`reverse_geocode()` built its `address` string from `MAIL_*` columns alone, and
`MAIL_STREET_NAME` is empty for 957,307 of NAR 2026-06's 17.4M addresses, so those results came
back with no street -- `242, WINTERTON A0B3M0`. The fallback is now in `nar_row_address()`, and
measuring it first changed its shape twice:

* It swaps the **whole name family**, not field by field. `MAIL_STREET_TYPE` is empty on every
  one of those 957,307 rows, so a per-field `coalesce()` would have paired an official name with
  a mail type it was never spelled against -- and `MAIL_STREET_DIR` survives on 11 of them,
  which is exactly the hybrid that would produce. 957,213 rows carry an official name; 94 have
  no street under either family.
* It fixes a second, unnamed half. `MAIL_MUN_NAME` is empty for 39,691 rows and
  `MAIL_POSTAL_CODE` for 57,154, and those were pasted in with `paste0()`, which spells an `NA`
  `"NA"` -- so the string read `9 Bowdring RD, NA NA`. Missing parts are now dropped, and
  `CSD_ENG_NAME` stands in for the city on 39,620 of the 39,691. That substitution is defensible
  here and would not be everywhere: the CSD is not the mailing city and the two do not nest, but
  `MunAlias` already treats it as a surface for the municipality, and it is derived from the
  coordinate -- which is the thing a reverse geocode was asked about. Nothing stands in for the
  postal code.

The assembly moved out of `reverse_geocode()` into `nar_row_address()` so it could be tested
without a database: every row the test fixture carries has both name families populated, and
adding one that does not would move every row count in `test-import.R`.
