# Per-address input and set-valued constraints

> Component note for `cangeocode`. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md); the query
> layer this changes is [`geocoding.md`](geocoding.md), and the parser it threads through is
> [`normalization.md`](normalization.md).

**Status: proposed. None of this is built.** It records a design decision and the traps found
while sizing it, so that the next attempt starts from here rather than from the same three
false starts. Everything in the present tense below describes code that exists *today*;
everything proposed is marked as such.

## The problem

An address and what the caller already knows about it arrive in **two arguments that must be
kept the same length**: `x` carries the strings and `known` carries a named list of vectors,
each length 1 or length `n`, recycled row-wise by `nar_known()`. That shape has two costs.

It is awkward to build — a caller with a data frame of addresses has to tear it into a vector
plus a parallel list and keep the row order intact by hand.

And it **blocks set values outright**. `known$MUN_NAME` of length `n` already means one value
per row. For a three-address batch, `c("Victoria", "Saanich", "Oak Bay")` means row 1 is
Victoria, row 2 Saanich, row 3 Oak Bay. There is no room left for "any of these": the two
readings collide exactly when `length(v) == n`, which is the case that matters.

`geocode()` already accepts a data frame, so the record-per-address shape half exists. That
path is the *post-parse* one — it requires `CIVIC_NO` and `STREET_NAME` and has no way to carry
a raw string — which is precisely the gap.

## The container: a frame with an `input` column

**Proposed.** Accept a data frame carrying an `input` column beside optional `known` columns,
where a **list-column means "any of these"**:

```r
geocode(tibble(
  input     = c("123 Main St", "45 Oak Ave"),
  MUN_NAME  = list(c("Victoria", "Saanich", "Oak Bay"), "Kelowna"),
  PROV_ABVN = "BC"
))
```

A list of per-address lists was considered and rejected. Three reasons, and the first is the
one that decides it:

* **A list-column is R's native set-per-row.** It is the OR with no new syntax, no wrapper
  class, and no per-row walk. A list of lists would need all three.
* It stays vectorized, subsettable and joinable, so a caller builds it with `mutate()` rather
  than `Map()`.
* `input` **round-trips**. It is already the name of the column `normalize_address()` emits
  (`R/normalize_address.R`), which `geocode()` carries through and `geocode_matches()` rebuilds,
  so a result can be edited and handed straight back in. `q` was the other candidate name and
  does not have this property.

If the list-of-lists spelling is wanted anyway, it should be a thin adapter that converts to
the frame at entry, never a second internal representation.

### `input`'s presence is the discriminator, and no new argument is needed

The frame path already keys on **`parse_source`** to tell a `normalize_address()` result from a
frame the caller built — the distinction [`nar_known_csd()`](../R/known.R) exists for, since
`CSD_NAME` is a *report* on the first and a *constraint* on the second. Adding `input` makes
that a three-way, and it stays readable:

| the frame carries | what it is | `CSD_NAME` means |
| --- | --- | --- |
| `input` | strings to parse | constraint |
| `parse_source` | a parse | a report, and does **not** constrain |
| neither | hand-built components | constraint |

### `known` stays

`known = list(PROV_ABVN = "BC")` must not require building a frame to say one thing about a
whole batch. Where a frame column and `known` name the same component, **the column wins per
row and `known` fills where it is `NA`** — one rule, and it has to be documented, because the
silent alternative is a constraint that does not bind, which is the failure `known` exists to
prevent.

## What a set means

A set-valued key is **resolved, not asserted**, and that is a genuine change to the contract.
Every key today is authoritative: `nar_known_apply()` writes it onto the returned row without
anything having matched it, which is why an asserted `CSD_NAME = "Toronto"` comes back
`TORONTO` where a resolved one comes back NAR's `Toronto`. A set has no single value to write.
So a set-valued key **constrains only**, the way `CSD_NAME` already does by travelling on the
`nar_csd_constraint` attribute rather than on a column.

Once it resolves, the existing machinery answers "pick a unique city" with no new logic:

| the address exists in… | outcome |
| --- | --- |
| exactly one listed city | resolves; `MUN_NAME` reports it in **NAR's** spelling; `n_matches = 1` |
| two or more listed cities | `n_matches >= 2`, `uncertainty_m` widens, `geocode_matches()` shows the set |
| none of them | unplaced, as any constraint that does not bind |

**Ambiguity is reported, not refused.** Turning row 2 into a refusal belongs in
`geocode_accept()`: the precision/recall dial is deliberately at *report* time and deliberately
not a `strictness` argument on `geocode()`. A set widening the candidate pool is exactly the
situation `n_matches` was built to describe.

### `mun_evidence` needs a new class, and the swap penalty must not fire

The first instinct — a set suppresses the municipality-swap penalty the way a single asserted
`MUN_NAME` does, via `nar_known_has_mun()` forcing `mun_evidence = "kept"` — is wrong, and so
is the opposite instinct of fining it normally.

* **Not fined.** The 0.88 penalty fines a substitution the caller did not sanction. If the
  caller listed five municipalities and the match landed in one of them, nothing unsanctioned
  happened.
* **Not `"kept"`.** The string may have said Victoria while the match landed in Saanich. That is
  not the municipality the string wrote, and `"kept"` is what the tie-break in
  `nar_gazetteer_winner()` reads.

So it wants its own class — **`"known_set"`** — which also keeps `uncertainty_m` honest, since
its floor is keyed on `mun_evidence` and not on `mun_remapped`.

## Runtime: this is not the 99x trap, and it is bounded above

The `OR` that costs 99x is `OFFICIAL = x OR MAIL = x` across the two NAR **name families**: it
destroys the equijoin key, so DuckDB falls back to a nested loop over 17.4M rows, which is why
those two branches are a `UNION`.

A municipality set is a different `OR`. The join key stays `p.name_fold =
strip_accents(upper(a.<family>))` and the municipality is a **filter inside the same `AND`
chain** in `nar_geocode_street_key()`. Widening a filter admits more rows; it does not change
the join strategy.

Better, **the cost has a ceiling that already ships**: constraining to {A, B, C} can never be
slower than constraining to nothing, and "a string that never named a municipality is resolved
against the whole province" is supported behaviour today — it is the `inferred` evidence class.
The same bound holds in the gazetteer, where `p.mun_input = ''` is the existing unconstrained
path. Expect a modest, bounded slowdown; do not expect a cliff.

## Where the work is

| area | change | size |
| --- | --- | --- |
| `nar_known()`, frame entry | accept `input`, accept list-columns, the overlap rule | small |
| `nar_known_apply()`, `nar_known_clear_mun()` | skip set keys when writing; still count as asserted for clearing | small |
| probe, `nar_geocode_street_key()` | `IN (...)` batch-wide, or a side table plus `EXISTS` per row | small–moderate |
| **`nar_gazetteer_sql()`** | `{fold_smun} = p.mun_input AS mun_kept` and the `mun_evidence` `CASE` become membership tests | **moderate, highest risk** |
| **online-tier floors** | `nar_address_agreement()` compares the municipality by scalar whole-word containment; needs `any()` over the set | **moderate** |
| parser | a set cannot disambiguate a parse; inert, and must be documented | small |

The two moderate rows are where this goes wrong if it goes wrong.

**The gazetteer**, because the fuzzy branch compares on a match fold whose R and SQL halves must
stay byte-identical, and this touches the SQL half. **The online floors**, because BC, NRCan and
Quebec always answer — the floor is the only thing separating an answer from a substitution, so
a membership test that is too loose silently converts rejections into confident wrong
coordinates. That is the failure mode with no symptom.

`nar_geocode_street_key()` is shared by both NAR tiers on purpose: an interpolation that
selected its flanking civics from a different street than the exact tier searched would be a
silent error. Any change here goes in the shared fragment, never in one caller.

## The first slice

Frame-with-`input` plus list-columns, **sets on `MUN_NAME` and `CSD_NAME` only**, **NAR-side
tiers only**, and the online tiers **erroring** on a set rather than quietly ignoring it — an
ignored constraint is the confident wrong answer this whole argument exists to prevent.

That covers the motivating case, keeps the diff out of both risky areas, and gets ambiguity
reporting free from `n_matches`. Per-row sets on arbitrary keys, and membership in the online
floors, follow once the semantics have shipped.

## Open

* Whether `known` should accept sets too, or only the frame. Sets in `known` are batch-wide by
  construction, which is a simpler `IN (...)` and may be all that is wanted.
* Whether an empty set is `NULL` (do not constrain) or a refusal. It should probably be an
  error: neither reading is obviously right, and guessing is how a constraint stops binding.
