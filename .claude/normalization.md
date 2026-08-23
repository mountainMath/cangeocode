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

**The baseline reading is always candidate 1 and always wins a tie.** A candidate displaces it
only on evidence, never on preference.

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

So exactly two conditions open the gate, and nothing else may be added without re-running the
harness:

- the proposed municipality **is not a place**. `TH25 VANCOUVER` is not, `100 MILE HOUSE` is, and
  no rule about token shapes tells them apart — which is the whole reason the inventory exists.
- the proposed street name **contains a `#`**, which `nar_norm_text()` guarantees introduces a
  unit and which no street name can contain. That is the signature of a string nothing split.

**A baseline proposing no municipality at all is not defective.** The string did not carry one,
`NA` is the right answer, and recovering it is a gazetteer question — failure mode 1 in the
status note, and a ceiling rather than a bug.

`nar_mun_anchor_variants()` tries the longest trailing run first, and offers *every* length that
names a place: `NORTH BAY` and `BAY` are both municipalities, so neither may be assumed. Two
further guards: a run may not reach back past the last comma (a municipality never spans a comma
the writer put in), and a candidate is dropped unless a street name survives the remainder, which
is what stops `123 Kingston` from resolving to the city with no street in it.

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

> Measured effect on the eval's 5,000-row samples: **Part A exactly at parity** (0 rows gained,
> 0 lost — the gate is what bought that), Part B **86.5% → 86.6%** joining a real NAR address,
> postal-confirmed **81.6% → 81.7%**, Quebec **67.8% → 68.2%**, rules-only fallbacks 374 → 371.
> The rules layer costs about 9% throughput for the defect check; both the single-candidate
> paths in `nar_parse_variants()` and `nar_arbitrate_rules()` are pure short-circuits and were
> verified output-identical.

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

`nar_match_fold_sql()` is the DuckDB twin and **must stay in step with the R function character for
character** — the probe is folded in R and the gazetteer in SQL, and a rule that exists on only one
side silently stops matching rather than erroring. `test-normalize.R` pins the two against each
other over the shapes that distinguish them.

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

