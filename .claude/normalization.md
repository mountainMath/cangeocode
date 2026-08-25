# Address normalization

> Component note for `cangeocode`. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md).
> Known failure modes, the eval harness, and what to fix next live in
> [`../inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md)
> — read it before changing the parser or the gazetteer, and re-run the eval harness
> before and after any such change.

## Numbered rural roads and the pattern recognizer

`R/normalize_address.R` has a `nar_take_numbered_road()` step, hooked in **after the civic number
and before the direction/type steps**, that handles the roads NAR files with *no street type at
all*: `OFFICIAL_STREET_NAME = "Range Road 272"`, `OFFICIAL_STREET_TYPE` empty. A hit returns
immediately and skips the direction and type steps entirely — measured, these roads carry a
direction 113 times against 99,556 blanks. Left to the ordinary path, `RANGE ROAD 272` reads as
name `RANGE`, type `RD`, plus a stray `272` nobody claims.

The crosswalk is `nar_lex_numbered_roads` in `data-raw/build_lexicons.R`, sized from the `Streets`
table: Range Road (AB 65,464 / SK 258), Route (NB 51,000), Township Road (AB 33,599), Concession
(ON 7,490), Mun (MB 963), County Road (550), Concession Road (442), Regional Road (249). Plus one
open family in the function itself — `<Word> {ROAD|SIDEROAD|CONCESSION} <number>`, which is what
Bruce Road 3 and Southgate Sideroad 21 are.

**Two collisions define the shape of this, and both are load-bearing:**

- **`HIGHWAY` is deliberately not in the lexicon.** `Highway 7` is filed the *ordinary* way, as
  name `7` with type `HWY` (115,175 rows). Adding it would break the commonest numbered road in
  the country to fix none of the others.
- **`ROUTE` is province-gated to NB.** New Brunswick writes typeless `Route 105` (51,000 rows);
  Quebec files `Route 132` as name `132`, type `ROUTE` (113,827 rows). Same five characters,
  opposite parses. Lexicon rows carry a `prov` that is empty for everyone else, and **a row with a
  `prov` set does not fire when the province is unknown** — that leaves the commoner reading in
  place rather than guessing.

Every match requires a **trailing bare number** (one optional letter, `212A`). That requirement is
what keeps `CONCESSION`, `SIDEROAD` and `ROUTE` — all real street types — from being stolen: a
type-bearing street never ends in a loose number.

Two numbers in front of the phrase means **the first is the civic number and the second belongs to
the name**: NAR really does have a street called `53222 Range Road 272`, whose addresses carry
their own small civic numbers. One number means it is just the civic number.

`R/normalize_pattern.R` sorts each parse into one of twelve buckets, exported as
`address_pattern()` and carried as the `pattern` column on `normalize_address()`. Buckets are
assigned by **priority, most specific first** (`nar_address_patterns()` is the order; the
assignments in `nar_address_pattern()` run backwards through it so earlier tests overwrite later
ones). The regional forms are checked before the ordinary ones so `grid`, `french_street` and
`numbered_road` describe the genuinely unusual addresses instead of being swamped by the
`civic_street` majority they overlap.

Two of the buckets exist to say *this will never resolve*: `po_box` and `rural_route` are delivery
instructions and **NAR contains neither**, and they confirm against NAR at a fraction of
`civic_street`'s rate — that is the point, they separate "this address is wrong" from "this address
was never going to be in the gazetteer". `nar_delivery_marks()` anchors `BOX` to the start of a
comma segment *and* requires a number after it, or Markham's Box Grove Bypass becomes a post
office box.

Traits (`numbered_road`, `type_leads`, `intersection`) are accumulated **during** the parse and
threaded out on `nar_parse_one()`'s `traits` column, because they record *how* a string parsed and
several forms end in identical columns. The pattern is computed once, in `nar_parse_rules()`,
before the gazetteer runs — it describes the parse, not the corrected result.

## The leading-prose strip

`nar_strip_lead_prose()` runs at the very top of `nar_parse_rules()`, before the postal code
comes out, and cuts everything in front of the first digit-initial token: `located at 41 Cultus
Rd` becomes `41 Cultus Rd`. It exists because **every civic-number rule in this parser anchors on
a number at the front of the string**, so a prose prefix does not degrade the parse, it collapses
it — prefix and civic number are read as one street name and the pattern falls to `street_only`.
Adding ten characters to the front of an address that otherwise parses perfectly took `CORE` from
98% to 0% on the generated `verbose` class. Measured, the rule takes the generated dirty corpus
from **70.9% to 93.2%** `CORE`, with `careof` going 18.6% → 92.4% and `verbose` 0.0% → 86.6%; see
[`../inst/notes/deepparse.md`](../inst/notes/deepparse.md), which is where the failure was found.

**It is deliberately near-inert everywhere else, and that is not a sign it is switched off.** It
touches 0 of Part A's 4,982 rows and 6 of Part B's 5,000, because both of those corpora put the
civic number first. Do not "fix" its low hit rate on the tuned corpora.

**Four guards, and every one of them is holding back a real address form.** They were arrived at
by running the rule over all four corpora and reading what changed, so removing one is not a
simplification:

- **At most one comma may be crossed.** One comma is a care-of line or a building name
  (`Sarah Steele Building, 609 Steele Street`); two would be a municipality, and eating that
  loses more than the prefix costs.
- **A number that closes its comma segment is not a civic number.** It is the tail of a street
  name — `Highway 7`, `Line 5`, `Rang 9` — or a unit written ahead of the address,
  `Suite 200, 119 Markham St`.
- **A unit designator anywhere in the dropped run, or a digit inside any dropped token.** The
  first is `Apt 4B-1234 Bloor St W` and `# 5 100 Main St`. The second is the *undesignated* unit,
  which is what comma-crossing exposes: `PH12, 2160 Terry-Fox Av`, `E10, 20 Palace St`,
  `Suite-1606, 80 Alton Towers Cir`. Prose does not carry digits, so the test is cheap and it is
  the guard that makes crossing a comma safe at all.
- **A street type or numbered-road word governing the number**, after peeling the French
  particules that sit between them: `Range Road 272`, `County Road 21 North`, `Chemin du 4e Rang`,
  `Avenue du 8 Mai`. Only the run *after* the last dropped comma is examined — a type separated
  from the number by a comma cannot be governing it, which is what keeps
  `Sunnybrook Health Sciences Centre, 2075 Bayview Ave` strippable. `nar_road_tail_words()` exists
  because `nar_is_street_type()` claims most numbered-road words but not `MUN`.

**The caller applies the fifth guard, not the function.** Strings carrying a `nar_delivery_marks()`
hit are exempt: a PO box or rural route line is an instruction, its number is not a civic number,
and `Wabana, PO Box 580 Bell Island` would otherwise be cut down to `580 Bell Island`.

## Two collisions between the parser's vocabulary and real place names

**`STE` is Suite and it is also Sainte.** Left unguarded, `Sault Ste. Marie` parses as a unit
called `SAULT MARIE` and the municipality is lost outright — 36,711 NAR addresses' worth.
`nar_is_unit_value()` therefore requires a designator's *value* to look like a unit number (a
digit, or a lone letter) before accepting it. That requirement is confined to
`nar_lex_unit_ambiguous`, which is `STE` and nothing else, and **must not be widened to every
designator**: `APT BSMT` and `APT TRLR` are real units whose value is a word, and applying the
rule to `APT` collapses the whole run into the street name and drops the civic number with it.
Both directions are regression-tested.

**All three unit paths must apply it.** The guard first went in on
`nar_take_unit_segments()` alone, which was enough only for comma-delimited input: without commas
the municipality is not a segment of its own, so `123 Main St Sault Ste Marie ON` reached
`nar_take_trailing_unit()` instead and lost the city through the other door. `nar_is_unit_value()`
exists so the three callers — segments, leading, trailing — cannot drift apart again. Note that
neither half of the eval harness catches this: Part A renders its municipalities out of NAR into a
comma-delimited form, so a comma-less place name is a form the noise grammar never produces.

**NAR keeps periods in municipality names; `nar_norm_text()` strips them from input.** `ST.
JOHN'S` (54,129 addresses), `SAULT STE. MARIE` (36,711) and `ST. ALBERT` (29,097) can therefore
never match a parsed fold key. `nar_gazetteer_sql()` folds periods out of *both* sides with
`replace(..., '.', '')` — on the `MunAlias` join, the `PostalMun` subquery, `mun_exact`, and the
two fuzzy street comparisons. It deliberately does **not** do so on the exact-branch
`Streets.NAME_FOLD` join, which would cost the `str_name_idx` index — so street-name periods stay
unhandled there by design.

## `R/normalize_variants.R` — candidate parses and the arbitration between them

A single left-to-right walk has to commit to a reading before it has any evidence that the
reading exists, and some of those commitments cannot be undone downstream: once `TH25 VANCOUVER`
is the municipality, nothing puts the unit back. So the parser produces *readings*, and something
with evidence picks — the municipality inventory when parsing is rules-only, the street gazetteer
when a connection is available.

**The baseline reading is always candidate 1 and wins a tie on everything but one term.** A
candidate displaces it only on evidence, never on preference — and the single term ahead of
`.cand` in `nar_gazetteer_winner()`'s `order()` is evidence too: on equal scores, the reading
whose answer is in the municipality the string *wrote* wins. See **A tie is broken by the
municipality that survived** below for why that had to be added and where it cannot fire.

**`nar_baseline_is_defective()` is the load-bearing part of the file, and it is a gate on
*generating* alternatives rather than on choosing between them.** Offering a second reading
unconditionally *costs* rows, which is the thing that had to be measured to be believed:
`80 rue Albanel, QC` names no municipality, Albanel is a real one, and anchoring it leaves a
street called `RUE`. Same for `de la Durantaye`, `de Nantes`, `l'Assomption`, `Trail` — place
names doing duty as street names in strings that never named a place.

**The gazetteer cannot arbitrate those back.** A match restricted to a real municipality
outscores an unrestricted one *by construction*, so the worse parse wins on a score that was
never meant to compare two different parses of the same string. Arbitration cannot repair a
candidate that should not have been offered — which is why the fix belongs at generation.

**The gate governs the anchored readings only.** `nar_dir_lead_variant()` is the second source
of candidates and it deliberately sits outside it — see below for why a gate would be the wrong
shape there. Three conditions open the gate, and nothing else may be added without re-running the
harness:

- the proposed municipality **is not a place**. `TH25 VANCOUVER` is not, `100 MILE HOUSE` is, and
  no rule about token shapes tells them apart — which is the whole reason the inventory exists.
- the proposed street name **contains a `#`**, which `nar_norm_text()` guarantees introduces a
  unit and which no street name can contain. That is the signature of a string nothing split.
- **a longer trailing run than the one it claimed is also a place.** This is the comma-free case,
  and the only one of the three that can fire on a baseline with nothing visibly wrong with it.

**The third condition is what segments an undelimited string, and its whole content is that the
inventory saw something the walk did not.** `3908 loraine ave north vancouver` reads `NORTH` as
the street's direction and leaves `VANCOUVER`, a real municipality, so neither of the other two
tests sees anything — but `NORTH VANCOUVER` is a real municipality too, and that is the entire
evidence for offering the other reading. It also covers the baselines that proposed *no*
municipality because a street type inside the place name ate the boundary: `maple ridge`, `bowen
island`, `brentwood bay` and `qualicum beach` all end in a NAR street type, `4830 scott ave
terrace` ends in one that is the whole name, and `27830 swensson abbotsford` trails a street that
names no type at all so there is no boundary to find.

**A baseline proposing no municipality where the string holds no run that names one is still not
defective.** The string did not carry a place, `NA` is the right answer, and recovering it is a
gazetteer question — failure mode 1 in the status note, and a ceiling rather than a bug.

**The scan cannot fire on a comma-delimited municipality, and that is structural rather than a
guard.** `nar_mun_anchor_runs()` reaches back at most to one token short of the last comma
segment, so what a comma already gave the baseline is longer than anything the scan is allowed to
propose. The third condition is therefore confined to strings the writer never delimited, which
is what makes it safe to open at all — Part A and Part B are comma-delimited end to end and both
came back byte-identical.

`nar_mun_anchor_variants()` tries the longest trailing run first, and offers *every* length that
names a place: `NORTH BAY` and `BAY` are both municipalities, so neither may be assumed. Four
guards, and the last two are what let the gate open this wide:

- a run may not reach back past the last comma (a municipality never spans a comma the writer
  put in);
- a candidate is dropped unless a street name survives the remainder, which is what stops
  `123 Kingston` from resolving to the city with no street in it;
- **a residue that is not a street name counts as no street name** (`nar_is_street_name()`).
  Every place name that also does duty as a street name fails *here* rather than at the gate:
  `135 de Nantes` anchors Nantes and leaves `DE`, `22 avenue de la Durantaye` leaves `DE LA`,
  `80 rue Albanel` leaves `RUE`. Particules are not a name, and neither is a street type standing
  alone — both tested after the particules come off, so `RUE DE LA` fails as surely as `RUE`.
- **a run that is a street type has to be one the street can spare.** `TRAIL` is a municipality in
  Ontario and a street type everywhere. `82 Fesroches Trail` is the second and `4830 scott ave
  terrace` is the first, and the only thing separating them is whether a type survives in the
  remainder. Without this guard the gate's third condition turns every street ending in a type
  that is also a place into an address with no type and a municipality it never named.

**A leading compass word gets a second reading unconditionally, and that is not an exception to
the gate — it is a different situation.** `nar_dir_lead_variant()` puts the stripped word back
into the street name, and the whole point of the defect is that the baseline looks perfectly
well-formed: `125 East Beaver Creek Rd, Richmond Hill, ON` names a real municipality, splits on
commas, and parses cleanly to `BEAVER CREEK` + `E`. There is nothing for a gate to see. NAR spells
some 92,000 addresses with the compass word inside the name and no direction on either name
family, and the parse commits to the other reading before it has any evidence.

**What made it urgent is that the failure is not an unplaced row.** Strip `East` off
`East Beaver Creek` and the probe is `BEAVER CREEK`, which whole-word containment scores 0.90
against *both* halves of the pair; direction agreement is worth 0.06 and the stripped reading has
no direction left in the name to agree with, so the mirror image wins about as often as the street
does — and it wins with a clean score. Measured on 2,500 NAR-spelled compass-led addresses, 385 of
453 losses were a confident wrong street and only 68 were unplaced, which is why this is a
*parallel candidate* and not a fallback fired when the stripped reading finds nothing. A fallback
repairs the 68.

**The two reasons it needs no gate are the two reasons the anchored readings do.** Both readings
carry the same municipality, so it is a like-for-like comparison of two street names in one place
and the restricted-beats-unrestricted asymmetry never arises. And it cannot displace a correct
reading: a street genuinely called `Park` is matched exactly at 1.0 by the baseline probe, while
the restored `NORTH PARK` matches nothing and — where neither exists — falls under the name gate
and is refused, leaving today's answer untouched.

**The word is restored verbatim, which is the part that is easy to get wrong.** `nar_parse_one()`
canonicalizes the direction to an abbreviation, and `E` is no use to a probe that has to meet
`East Uniacke` in NAR's own spelling — the match fold does not expand it. So the original token
has to travel out of the parse, and it does so as an attribute (`dir_lead`) rather than a column,
because every other `return()` path in `nar_parse_one()` would otherwise have to carry it and
`rbind()` over the readings drops it anyway. Abbreviations are restored too: `W GEORGIA` is not a
name NAR carries so that candidate simply loses, and the ~2,000 addresses whose NAR name really
does open with an abbreviated compass word are what it is there for.

**Only the *leading* word.** A trailing direction (`100 Queen St West`) and one sitting between the
type and the municipality get no second reading — those shapes have not been measured, and NAR's
own inventory of them is a different size.

**Rules alone will not undo a direction.** `3908 loraine ave north vancouver` yields two readings
that both name a real municipality, both score the same completeness, and the baseline wins ties
by rule — so without a connection the answer stays `VANCOUVER`. Only the gazetteer, which knows
Loraine Avenue is in one of them and not the other, moves it. That is the arbitration order
working as designed and not a shortfall in the gate; do not add a tie-break that prefers the
longer name, because `100 MILE HOUSE` and `MILE HOUSE` are the same shape with the opposite
answer. The one tie-break that *was* added reads evidence out of the gazetteer's answer rather
than a property of the string, which is what makes it safe where a shape rule is not.

**A known hole this exposed but does not cause.** With the municipality already fixed,
`nar_parse_one()` silently drops whatever trails the street type — `802 11 rue Victoria, La Baie`
picks `RUE`, strands `VICTORIA` and loses it. Reverting the type pick when it strands a token was
tried and **rejected**: anchoring deliberately bites into the last comma segment, so the tokens
it strands are as often municipality debris (`330 Spadina Road, City Of Toronto` leaves
`CITY OF`) as street words, and the revert cost more than it bought on every corpus.

**The municipality inventory ships in `R/sysdata.rda` as `nar_lex_muns`** — 9,748 distinct
`MunAlias` names with province and address count, rebuilt by `data-raw/observe_municipalities.R`
through `data-raw/build_lexicons.R`. It is what lets arbitration work with no connection at all.
Lookup is province-qualified first and then bare, deliberately: the province is itself parsed and
may be the thing that is wrong.

**Adding a candidate needs no gazetteer SQL change.** `nar_gazetteer_sql()` already ends in
`QUALIFY row_number() OVER (PARTITION BY row_id ...)`, so k candidates are k probe rows and one
extra max-score pick in R. What it does need is the final `STREET_TYPE` tie-break in that window:
`Castleglen RD NE` and `Castleglen WAY NE` in Calgary have **identical** address counts, so the
old ordering left the winner to DuckDB, and merely changing the shape of the probe table flipped
it. A before/after harness run cannot tolerate a coin flip.

> Measured effect of candidate readings, when they went in: **Part A exactly at parity**
> (0 rows gained, 0 lost — the gate is what bought that), Part B **86.5% → 86.6%** joining a real
> NAR address, postal-confirmed **81.6% → 81.7%**, Quebec **67.8% → 68.2%**, rules-only fallbacks
> 374 → 371. The rules layer costs about 9% throughput for the defect check; both the
> single-candidate paths in `nar_parse_variants()` and `nar_arbitrate_rules()` are pure
> short-circuits and were verified output-identical.
>
> Measured effect of the third gate condition, on the same samples: **Part A and Part B both
> exactly unchanged** — CORE 97.9%, MUN 94.4%, and 98.9% / 88.4% / 83.3% to the decimal, because
> neither corpus contains an undelimited string. `odhf_full`, which is 2,241 of them, went
> **57.5% → 62.3%** postal-confirmed, passing deepparse-as-segmenter's 61.3%; municipalities
> missing from its failures fell 214 → 55. The generated `llm` corpus gained 2.1 points of
> municipality (73.0% → 75.1%) for 0.2 of CORE (93.2% → 93.0%) — two rows whose baseline was
> already junk that the containment street test was crediting anyway.

## `R/normalize_gazetteer.R` — matching the parse against NAR

`nar_gazetteer_sql()` builds one query with **`{probe}` / `{name_threshold}` placeholders
substituted by `gsub(fixed = TRUE)`, not `sprintf`**. The template is past `sprintf`'s 8192-byte
format limit — the inline comments alone push it there — and as a bonus a literal `%` in a `LIKE`
pattern needs no doubling.

**`name_sim` is not a similarity.** It is `greatest()` over Jaro-Winkler and two flat 0.90 awards
for kinds of evidence Jaro-Winkler structurally cannot express. Keeping them at exactly the
default `name_threshold`, rather than giving each a branch of its own, is what makes
`name_threshold` still mean one thing: raising it above 0.90 turns both extra rules off, which is
what asking for stricter should do.

- **One Damerau-Levenshtein edit, at `length(name_fold) >= 3`.** Jaro-Winkler pays a prefix bonus,
  so it scores the same one-key slip 0.89 in `NARTIN`/`MARTIN` and 0.83 in `QALL`/`WALL` — 77 of
  the correct answers the 0.90 gate discarded were *already ranked first*. **The length floor is
  load-bearing**: at two characters one edit is the whole word, and `5W` against `5E` is a
  different street, not a typo.
- **Whole-word containment**, matched as `' ' || name || ' ' LIKE '% ' || probe || ' %'`. This
  catches the words a parse rule ate — `5` for `NO. 5`, `772` for `ROUTE 772`, `PARK` for
  `PARK LAWN` — which similarity ranks nowhere near the top (679th, for the first). It cannot
  displace a street actually called `PARK`, which scores an exact 1.0 and wins.

**Both prefilters are required, and both were measured.** Edit distance is asked only of
candidates already at `jw_sim >= 0.70` (one edit cannot go below that: worst case is a substituted
first character of a three-letter word, 0.778), and containment only of candidates *longer* than
the probe (a shorter one can only contain it by equalling it, already scored 1.0). Without the
pair the query is **3.5x slower for byte-identical answers**. `jw_sim` is a lateral column alias
reused by the guard, which is why the final union needs `SELECT * EXCLUDE (jw_sim, s_fold, s_mail_fold) FROM scored` —
the `exact` branch has no such column and a `UNION` lines the branches up by position.

**Both sides are compared on a *match fold*, not on `NAME_FOLD`.** `nar_match_fold()` takes
`nar_fold()`'s case-and-accent folding two steps further: periods and apostrophes vanish, hyphens
become spaces, and a standalone `ST`/`STE` is spelled out to `SAINT`/`SAINTE`. It is applied to
the probe and to the gazetteer, and it is why the fuzzy branch can see Quebec at all.

- **The hyphen is not punctuation here, it is a word boundary.** NAR writes `du Square-Victoria`
  and `Côte-des-Neiges`; people write `VICTORIA` and `COTE DES NEIGES`. Unfolded, the whole-word
  containment rule above cannot fire on either — `SQUARE-VICTORIA` is *one* word — and the edit
  distance sees `COTE-DES-NEIGES` and `COTE DES NEIGES` as different strings for two characters
  it should not be counting.
- **`ST` is the same problem the period is, one step further on.** `ST-JACQUES` against
  `Saint-Jacques` is six edits on thirteen characters. No similarity threshold and no single-edit
  rule reaches that, and it is the ordinary way Quebec street and municipality names are written.
- **The apostrophe goes to a space, not to nothing**, so `de l'Orme` folds to `DE L ORME` and
  containment can find `ORME` inside it.

The R half returns early on a zero-length input, and that guard is load-bearing rather than
defensive: the fold pads with `paste0(" ", x, " ")`, and `paste0()` given a zero-length argument
returns **one** element, not none. So an empty query folded to a one-row vector, and the caller
building a data frame around it failed on a length mismatch that said nothing about addresses —
which is how `geocode("49321, BRAZEAU COUNTY, AB")` came to error instead of reporting `none`.

`nar_match_fold_sql()` is the DuckDB twin and **must stay in step with the R function character for
character** — the probe is folded in R and the gazetteer in SQL, and a rule that exists on only one
side silently stops matching rather than erroring. `test-normalize.R` pins the two against each
other over the shapes that distinguish them.

**The dash class is the one place the two halves can drift apart without any test noticing, and it
did.** R's half never sees an en or em dash, because stringi's `Latin-ASCII` transliteration inside
`nar_fold()` has already turned it into a hyphen; DuckDB's `strip_accents()` leaves both exactly
where they were. Nothing surfaced this while NAR was the only gazetteer — NAR carries **zero** en
dashes, having transliterated them to `--` in 2,134 addresses. Quebec's register does not: 11
street names over 2,472 addresses keep the en dash, `du Bord-du-Lac–Lakeshore` among them, so the
two registers' spellings of the same street folded apart and never met. The SQL half therefore
replaces `[-–—]`, not `-`. The parity test cannot catch this on its own, because it folds its
inputs in R first, which is the step that hides the character; the test beside it folds SQL-side
from the raw name, the way `rqa_build_tables()` does.

The gazetteer side is folded **once per connection** into the `StreetFold` TEMP table by
`nar_street_fold()`, keyed on `rowid`, not per candidate per probe row. The alternative — a stored
column and a schema bump — would make every database built before it slower rather than merely
different; this costs nothing at import and needs no re-import.

**Folding forced a length gate in front of the edit distance, and the gate made the whole thing
faster than it was unfolded.** Removing the hyphen moves far more pairs past the `jw_sim >= 0.70`
prefilter, because `COTE-DES-NEIGES` and `COTE DES NEIGES` *are* near-identical strings — the first
version of this change cost 45% of the normalizer's throughput. One Damerau-Levenshtein step cannot
bridge a length difference greater than one, so an integer comparison rejects those pairs before
the distance runs, and rejects nothing the distance would have accepted. Query time 11.8s → 1.7s,
and 15% faster end to end than before the fold existed.

**The municipality is folded on the same path, and that half is where most of the gain is.**
`MunAlias` and the `PostalMun` fallback both compare through `nar_match_fold_sql()`, so `ST-LAURENT`
resolves. It matters more than the street half because a municipality that fails to resolve takes
the street with it: the municipality is what restricts the candidate set, so an unresolved one
leaves the street with nothing to be matched against, however it is spelled.

The `exact` branch is deliberately **not** folded. It is an indexed equality on `NAME_FOLD` and
exists to be fast when there is no locality to restrict by; folding it would mean either an index
it cannot use or the stored column just rejected.

**The `exact` branch answers with a municipality only when NAR determines it** —
`CASE WHEN count(DISTINCT MAIL_MUN_NAME) = 1 THEN any_value(...) END`. One municipality carrying
the only street of that name has *determined* it; withholding that is its own wrong answer. Two or
more and it stays `NULL`, because the busiest city with a street of this name is a guess, not a
resolution. The province follows the same rule, after the caller's `prov` if given, so a string
that named neither can still resolve to both. `test-normalize.R` pins both halves against a
fixture carrying one street in two cities and another in one.

> Measured effect of the three together on the eval's 5,000-row Part A sample: **215 rows gained,
> 0 lost**. Attribution and the harness-level deltas are in
> [`inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md).

### The municipality swap penalty

**`MunAlias` keys on the census subdivision, so the fuzzy branch's "locality" is a whole regional
municipality.** `MILFORD, NS` does not restrict candidates to Milford; it restricts them to Halifax
Regional Municipality — 166 mailing communities over 127 km. Inside that set the scoring can prefer
a street in the wrong community, because type agreement is worth 0.10 and one Damerau-Levenshtein
step costs 0.072 at the gate: `12 Wildwood Dr, Milford` scored `Windwood Dr, MIDDLE SACKVILLE` at
0.952 over the `Wildwood Ave` in Halifax at 0.900, and `geocode()` then found exactly one civic
number there and reported `n_matches == 1`, `uncertainty_m == 0`, 60 km away.

**The widening is not the defect** — 1,601 of 2,795 remaps in the PVSC sample are it working. The
defect was that nothing separated those from the rest.

**Two mailing municipalities sharing a full postal code is the separator, and it is read out of NAR
rather than curated.** `nar_mun_copostal()` builds `MunCoPostal` (32,216 directed pairs, 0.2 s) and
`MunMail` as TEMP tables, the `nar_street_fold()` pattern and for the same reason — no schema bump,
no re-import. Three things about it are not guessable:

- **The full postal code, never the FSA.** `PostalMun` is already there and is FSA-keyed, and an
  FSA in rural Nova Scotia covers most of a county — it would attest nearly every pair in the
  province and the penalty would never fire.
- **`MunMail` exists so that an absence of evidence can be told from an untestable name.** A name
  NAR files no postal-coded mail under shares no postal code with anything, and that is not
  evidence of a bad swap. It is read off exactly the same rows as `MunCoPostal` for that reason: a
  name that could never have appeared in the pair table must not be scored as one that could have
  and did not.
- **The pairs are directed** (`MN_A` written, `MN_B` candidate), so the scoring join needs no `OR`
  and stays a hash join.

**The penalty multiplies the whole score, and the operative rule is its product with the
threshold.** At the default `mun_swap_penalty = 0.88` against `threshold = 0.85`, an unattested
swap has to reach 0.966 unpenalised — an exact or one-keystroke name *and* agreement on everything
else the string gave. It **reorders as well as refuses**, which is half its value: the Wildwood
case now answers with the Halifax street it always scored second. Below 0.85 the penalty stops
discriminating and simply refuses every unattested swap; `1` is the old behaviour. The knee at 0.88
is calibrated on one province and the calibration table is in
[`inst/notes/nova-scotia-pvsc.md`](../inst/notes/nova-scotia-pvsc.md); the mechanism it rests on —
a fuzzy name compounding an unattested municipality — is not provincial.

**A second attestation, for amalgamations and legacy names: the census subdivision.** A swap whose
candidate sits in the census subdivision the string named — or whose own `CSD_ENG_NAME` is the name
the string wrote — is not fined. `Bathurst St, Toronto` reaches a street NAR still mails to
`NORTH YORK`; they share no postal code and never will, because the amalgamation did not merge the
delivery names. `Streets.CSD_ENG_NAME` already carries the relationship, so this is read out of NAR
for the same reason the postal pairs are: **no curated alias list, in either direction.** It is
worth 32 exact matches in the PVSC sample at zero errors past a kilometre, which is why it is an
exemption and not a discount.

**Four exemptions, and each is holding back a real address form.** A candidate whose municipality
equals the written one is not a swap; a string that named no municipality at all had its locality
supplied by the postal code and has nothing to have swapped; a census-subdivision match is the
amalgamation arm above; and an untestable name is exempt per `MunMail` above. The order of the
`CASE` branches is the argument — `copostal` is tested before `csd` so the stronger evidence is the
one reported, and `untestable` last so it cannot mask either.

**The exemptions are also the output.** The same `CASE`, run a second time over the same
predicates, emits `mun_evidence` — `kept` / `inferred` / `copostal` / `csd` / `untestable` /
`unattested` — which is how the gazetteer's private reasoning about *why* a swap was allowed
becomes something `geocode()` can price. The two `CASE`s must stay in step; they are adjacent in
`nar_gazetteer_sql()` for that reason.

**The swap is scored against the *baseline reading*, not against the reading being scored.** The
parser emits several candidate readings per string and the gazetteer scores all of them; an
alternative reading may re-segment the trailing run and hand back a shorter municipality.
`HOWIE CENTRE` read as `CENTRE` — itself a Nova Scotia municipality, and one that shares a postal
code with `LUNENBURG`. Scoring the swap against *that* lets a truncation manufacture its own
attestation: NAR files no mail to `HOWIE CENTRE`, so the honest verdict is `untestable`, but the
reading that broke the name gets credited with a `copostal` match. `nar_gazetteer_pass()` therefore
anchors `mun_input` on the first candidate by `.cand` — the reading that took the string's own
delimiters at face value — and only `mun_use` / `mun_match` stay on the per-candidate name. This
was a live defect: it laundered 184 rows into a falsely attested class, and fixing it cost one
gross error and bought 60 exact matches.

**The RQA pass is deliberately unpenalised.** RQA files under census subdivisions — `Montréal`,
never `Verdun` — so changing the municipality is what that pass *does*, and every one of its
answers would be fined for it.

**`mun_exact` and `mun_kept` are different questions and the query carries both.** `mun_exact`
compares against `mun_use`, which may have come from the postal code, and orders ties *inside* the
window that picks one street row per probe. `mun_kept` compares against `mun_input`, the
municipality the **baseline** reading took out of the string, and is what leaves the query as
`mun_remapped` — the flag `normalize_address()` returns and `geocode()` prices per
`mun_evidence`. Reusing either for the other's job silently changes what the flag means.

**A tie is broken by the municipality that survived.** `mun_kept` has a second consumer:
`nar_gazetteer_winner()` orders on it between `score` and `.cand`. It is there because the swap
penalty exempts an *attested* swap, and an attested swap that competes against a reading which
stayed put is the one case that exemption gets wrong — `170 North Park St, Brantford` resolves at
1.0 to `Park Rd N` in HAMILTON (attested by CSD name) and at 1.0 to `North Park St` in Brantford
itself, so score cannot separate them and the penalty has already declined to. This is why
`mun_kept` anchors on `mun_input` and not on the reading being scored: the term has to compare
every candidate against *one* municipality — the string's own — or it decides nothing. That
anchoring also makes it inert exactly where it should be: on a single-candidate row, on a string
that named no municipality (`mun_kept` is then `FALSE` for every reading alike), and on the
municipality-anchored variants, whose whole purpose is to answer somewhere the baseline did not.
Measured: probe losses 45 → 27, the placed-wrong share of them 35.6% → 7.4%, and the eval harness
byte-identical on 10,000 rows.

### `keep_refused` — reporting what the threshold turned away

A match scoring below the combined `threshold` is normally discarded, and the row comes back
parsed but unresolved. From the outside that is **indistinguishable from the street not
existing** — no rejected answer, no score, no evidence class. It is a false negative with
nothing to read, and the caller who could overrule it (someone holding locality evidence the
package does not have) is exactly the one given nothing to overrule it with.

`keep_refused = TRUE` adopts the best sub-threshold match anyway, `confidence` carrying the
sub-threshold score, and adds a `refused_for` column naming the gate. Notes:

* **Two reasons, and only one of them is worth a name.** `"mun_swap"` means the score cleared
  `threshold` *before* the swap multiplier and not after — the street matched and the
  municipality did not, which is a different failure from a weak name. It is recoverable in R
  as `score / mun_swap_penalty >= threshold` **only because the penalty applies to exactly one
  evidence class** (`unattested`); the R side checks that class explicitly rather than
  inverting the multiplier blind. If a second class ever gets fined, the query must return the
  pre-penalty score instead — and that means a new column in *both* branches of the
  `UNION ALL`, which lines up by position.
* **Only what cleared `name_threshold` can be reported at all.** That gate lives inside the
  query, so a name too far from every candidate never comes back. This is a documented limit,
  not something to work around by relaxing the gate — the gate is what stops type and direction
  credit from carrying a wrong street over the line.
* **A NAR refusal outranks an RQA one, and an RQA *match* outranks a NAR refusal.** The refused
  write-back only targets rows still at `parse_source == "rules"`, while the accepted one is
  unconditional and clears `refused_for`. So a row NAR could only refuse still gets Quebec's
  register offered to it properly, and a row both passes only refused keeps NAR's answer —
  the same running-order priority the accepted matches follow.
* **The column is created in `nar_resolve_gazetteer()`, before `out_cols` is taken.** That
  function cuts the result back to the column list it captured at entry, so a column a pass
  added would otherwise be silently dropped on the way out.
* `res$refused_for` is tested with `"refused_for" %in% names(res)` and not `is.null()`:
  `res` is a tibble, and `$` on an absent column warns.

`geocode_accept(refused = FALSE)` is the other half — take one pass with the refusals and one
without, and the difference is what the threshold is buying.

### The second pass, over Quebec's own register

`nar_resolve_gazetteer()` runs `nar_gazetteer_pass()` twice: once against NAR, and — only where
[`rqa_import()`](rqa.md) has been run — once against `RqaStreets`. The shared machinery is the
probe build, the temp-table write, the `score >= threshold` filter, the one-winner-per-`.row`
selection and the write-back; only the eligible rows, the query and the `parse_source` label
differ.

**The second pass sees only what the first left**, and only Quebec. That is the same rule as
`geocode()`'s tiers — priority is running order — and it is what makes importing RQA unable to
change an answer that already worked. It can only fill in one that did not. The `.row`-level
`parse_source != "gazetteer"` test is what carries this; `parse_source` starts at `"rules"` and is
never `NA`, so the comparison is safe.

**A match comes back as `parse_source = "rqa"`, not `"gazetteer"`.** It is a real confirmation
against an authoritative register, but the caller has to be able to tell the difference: a row
carrying it will still fail a join against `Addresses`, because NAR does not hold that address.
Encoding that in a fudged `confidence` instead would have destroyed the one thing `confidence`
means — the weights are identical across the two passes precisely so it keeps meaning it.

`nar_rqa_gazetteer_sql()` differs from `nar_gazetteer_sql()` in three ways, each forced:

- **No `exact` branch.** RQA covers one province. An unrestricted name match here would assert
  Quebec about a string that never said so, so a row with no municipality and no FSA simply drops
  out of the `muns` CTE.
- **One name family**, compared on `MATCH_FOLD` and never on `NAME_FOLD` — RQA keeps the particule
  in a column of its own where NAR keeps it inside the street name, so the plain folds of the same
  street are not the same string.
- **The municipality resolves through NAR's `MunAlias`, restricted to `PROV_ABVN = 'QC'`, and out
  through `split_part(MUN_KEY, ':', 3)`.** `MUN_KEY` is `prov:type:CSD name`, and that CSD name is
  exactly what RQA files under, so `ANJOU`, `LASALLE`, `SAINT-LAURENT` and `VERDUN` all reach
  `Montréal` without RQA needing an alias table — or its `BOROUGH` column — of its own. The two
  routes to a municipality are a `UNION`, not an `OR`, for the reason recorded in
  [`geocoding.md`](geocoding.md).

**What it is worth, measured.** On the 3,000-address sample of what NAR is missing and RQA holds,
the second pass answers **8.9%** of rows, all of them exactly right. On Corporations Canada — real
typed filings — it answers **4 of 942 Quebec rows**. Both numbers are correct and the gap between
them is the finding: the Quebec filings NAR cannot settle are overwhelmingly *mistyped*
(`Bouceherville`, `ST.CATHERINE ST.WEST`, `1603 - 3410, rue Peel`), not *uncovered*, and a second
register cannot read a misspelling. Do not quote the 5.5-point Quebec gain in the eval as this
pass's doing: that is a **confirmation-set** effect, delivered by `RqaAddresses` merely existing
for the harness to judge against, and most of the parses it newly confirms were NAR's all along.

## `R/address_format.R` — putting the components back together

`normalize_address()` takes an address apart; `address_key()` and `format_address()` put it back,
once for a machine and once for a person. Both accept **either** a `normalize_address()` result or
the raw strings, resolved by `nar_as_components()`, so a caller who only wants the output never has
to name the columns. Passing `prov`/`con` alongside an already-parsed data frame is an **error**
rather than silently ignored — those arguments change the parse, and dropping a constraint the
caller asked for is worse than refusing it.

`address_key()` folds through `nar_key_fold()`, which is `nar_fold()` plus two rules that exist for
the same reason the gazetteer's `replace(..., '.', '')` does: **periods and apostrophes vanish
outright** (NAR keeps them in `ST. JOHN'S`, the parser strips them, and a key has to see past that),
while every other separator becomes a space, so `NOTRE-DAME` keys as `NOTRE DAME` rather than
`NOTREDAME`. Fields run **broad to narrow** — province, municipality, street, civic — so sorting
keys clusters a street, and a missing field leaves an empty slot rather than shifting the rest
along.

**A row with no street name keys to `NA`, not to an empty string.** Otherwise every unparseable row
in a file joins to every other unparseable row, which is the worst failure a match key has. Note
this does not fully protect the join: `dplyr` matches `NA` to `NA` by default, which the
documentation says explicitly.

The unit is **out of the key by default**, so the key is a building. Including it keys a tenant, at
the cost that the unit is the least reliably parsed component — the tradeoff is documented rather
than decided.

`format_address()` places the street type **by language, not by province**: it leads only when the
canonical type exists in French alone, so `Rue` in Ottawa still reads correctly and the three
canonicals both vocabularies share (`RTE`, `CONC`, `PK`) stay in English order. That is a
refinement of the rule in `data-raw/render_address.R`, which keys on `prov == "QC"` because it is
rendering *from* NAR rows and knows the province is the truth there. A civic suffix is glued on
(`990A`) unless it carries punctuation, in which case it is a fraction and gets a space (`12 1/2`,
not `121/2`).

Component **case is left exactly as parsed**, which for a gazetteer row means NAR's own
convention: names mixed case, types and directions and municipalities in capitals (`Burrard ST`).
Measured on the 2026-06 release, `OFFICIAL_STREET_NAME` is mixed case on 16.09M rows while
`OFFICIAL_STREET_TYPE` and `OFFICIAL_STREET_DIR` are uppercase on **all** of them. The uneven look
is therefore NAR's, and imposing a house style would mean re-casing `McTavish`.

`test-address-format.R` asserts the round trip: `address_key(format_address(x)) == address_key(x)`.
A formatter that emitted something the parser could not read back would silently break the one
workflow it exists for.

