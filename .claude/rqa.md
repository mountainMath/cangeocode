# Quebec's address register

> Component note for `cangeocode`, covering `R/rqa.R` — the `rqa_import()` path and the
> `"rqa"` tier. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md); the tier runs inside the
> machinery in [`geocoding.md`](geocoding.md) and joins on folds defined in
> [`normalization.md`](normalization.md). What the register is, what the gap is made of and
> every number quoted here: [`../inst/notes/quebec-addresses.md`](../inst/notes/quebec-addresses.md).

One file does two things — an import and a geocoding tier — because they are the same
decision. The tier exists *because* the import is separate; if RQA were merged into
`Addresses` there would be no tier, just more NAR.

## Why it is beside NAR and not in it

The *Répertoire québécois des adresses* is the register StatCan's Quebec rows are derived
from, published in full, carrying about **308,000 net civic addresses NAR does not have**.
Merging is the obvious move and is the wrong one:

* **It spends the instrument to buy the rows.** Everything this repo knows about what NAR is
  missing in Quebec is known because the two registers are separately readable. A merged
  table can no longer be asked the question — and Quebec already has no independent geometry
  check, since `qc_validate()` queries the same upstream.
* **The added rows are positionally weaker**: 20.3% building-placed against 26.9%
  register-wide, 30.0% flagged `Incertaine` by RQA itself. Merging degrades what
  `geom_source = 'building'` means for Quebec, which is already the misleading number
  `quebec-addresses.md` had to warn about.
* **A merged table stops being NAR.** `nar_provinces()`, the vignette row counts and
  `nar_schema_version()` all claim to describe a StatCan release.

`nar_schema_version()` is **deliberately not bumped** for this. A bump forces every existing
user to re-download a multi-hundred-MB release, and these tables are optional and additive:
absence is the normal state, not a stale one. `nar_has_rqa()` is the test, and it demands
**both** tables: the import writes `RqaAddresses` first and `RqaStreets` second, so a run that
died partway reads as having no RQA rather than as having half of it.

## The import

`rqa_import(version, refresh, csv)` writes `RqaAddresses` (one row per certified address) and
`RqaStreets` (a gazetteer grouped by odonyme and municipality, mirroring `Streets`), plus
`rqa_*` keys in `nar_metadata`. 29 seconds over the 2026-06 release from an extracted CSV.

**The CSV is resolved before the write connection is opened.** The download is the long,
failure-prone step and DuckDB gives one writer an exclusive lock on the whole file — holding
it across a 780 MB download would block every reader for the duration. `options(rqa_csv = )`
skips the download entirely and is what the fixtures and any import-shaping work should use.

**Metadata is written last**, like the coverage marker on an appended NAR province: an
interrupted import then reads as absent rather than as present and incomplete.

**`all_varchar = true` on `read_csv` is not optional.** Civic numbers, postal codes and unit
numbers all carry values DuckDB's sniffer will type as numeric and then lose leading zeroes
from; every cast is explicit and `TRY_CAST` where the register is allowed to be empty.

### The reshape, which is the whole point

The two registers spell a street differently, and the difference is not cosmetic:

| | NAR | RQA |
| --- | --- | --- |
| `de la Côte-de-Liesse` | all of it in `OFFICIAL_STREET_NAME` | `particule_odonyme` + `specifique_odonyme` |
| `Boulevard` | `STREET_TYPE = BOUL` | `generique_odonyme = Boulevard` |
| `Ouest` | `STREET_DIR = O` | `point_cardinal_odonyme = Ouest` |

So `STREET_NAME` here is **particule plus specifique**, the générique becomes `STREET_TYPE`
through the same French lexicon `normalize_address()` uses (`nar_lex_lookup(..., "fr")` —
`AVENUE` → `AV`, not `AVE`), and the cardinal becomes `STREET_DIR`. Comparing the raw columns
instead reads **1,265,940** missing addresses where there are 357,723; that factor of 3.5 is
what the reshape is for.

**An unknown générique keeps its own folded spelling and is not promoted to a canonical
type.** Six of RQA's — Domaine, Traverse, Descente, Chaussée, Trait-carré, Carrefour — have no
counterpart anywhere in NAR, so a canonical for them would parse cleanly and then join
nothing. The raw générique is kept in `STREET_GENERIC` alongside, so the reshape is always
reversible.

### `IN_NAR`

Every row records which side of the gap it fell on **for the release it was imported into**,
which is sound only because the tables live inside that release's own `.duckdb` file. This is
why the *whole* certified register is imported rather than just the gap: the gap is a property
of the pair, has to stay recomputable, and a subset built against one release would be
silently wrong against the next.

The test is **fold equality on (FSA, civic number, folded street name)**, not containment.
Containment has no equijoin key and turns a scan into a product. It over-reports the gap by
about 14% — a figure measured independently, and the reason `RqaStreets.N_NOT_IN_NAR` is a
ranking aid rather than a count to quote. Over 2026-06: 475,294 rows / 356,089 distinct keys,
against the 357,723 measured out-of-band by a different code path.

## The tier

`nar_geocode_tier_rqa()`, dispatched from `nar_geocode_match()`. Gated on `prov == "QC"`
exactly as the `"qc"` tier is. Not in the default `method`.

**It joins on `MATCH_FOLD`, not `NAME_FOLD`, and the shared probe carries a `match_fold`
column only for this.** The addresses this tier exists for are precisely the ones NAR does not
carry — so the gazetteer could not resolve them, and the parser hands back *the user's*
spelling rather than NAR's. The plain fold is an exact-spelling key and would miss most of
them. The match fold (spelling `ST` out to `SAINT`, hyphen to word boundary) is what survives
that, and its R and SQL halves must stay identical — see
[`normalization.md`](normalization.md). The NAR tiers keep `name_fold`, which is indexed.

**`match_method` carries the register's own positional class** — `rqa_building`,
`rqa_geocoded`, `rqa_uncertain`, `rqa_lot`, `rqa_other` — rather than one flat label, and
**`uncertainty_m` is filled in only for `rqa_building`**, where 0 means what it means for NAR:
this package added nothing. Nothing here has measured what `Géocodée` or `Incertaine` are
worth on the ground. The two non-zero figures in `geocoding.md`'s table *were* measured, and
an invented third would be indistinguishable from them at the call site.

When several register rows answer, the winner is chosen by positional class — Bâtiment,
Géocodée, then anything else, then Incertaine last — and ties by `RQA_ID` so the answer is
stable across runs. `n_matches` and `spread_m` report what was collapsed.

**The database check is up front in `geocode()`, not in the tier.** Whether a tier runs at all
depends on what its predecessors left unplaced, so a missing import would otherwise surface on
one batch of addresses and not the next. The tier keeps its own guard as a backstop for
`nar_geocode_match()` being called directly.

## The normalization pass

`nar_resolve_gazetteer()` offers `RqaStreets` the rows NAR could not settle, in Quebec only, and
labels a match `parse_source = "rqa"`. The design and what it is worth are in
[`normalization.md`](normalization.md) — read that before touching
`nar_rqa_gazetteer_sql()`. Three things belong here:

**It reuses NAR's `MunAlias` rather than an alias table of its own**, joining out through
`split_part(MUN_KEY, ':', 3)` — the CSD name, which is exactly what RQA files under. This is why
the `BOROUGH` column the import carries is unnecessary for normalization even though the tier
needs it: `MunAlias` already maps `ANJOU`, `LASALLE`, `SAINT-LAURENT` and `VERDUN` onto
`24:V:Montréal`.

**The register's spelling is returned as-is, except the municipality.** NAR's own
`OFFICIAL_STREET_NAME` is title case with the accents kept — because its Quebec rows came from
this register in the first place — so RQA's spelling already *is* the convention. `MAIL_MUN_NAME`
is the one NAR upper-cases, so the query does too. An `upper()` on the street name here was wrong
and shipped for exactly as long as it took to look at `Streets`.

**The measured gain is small and the harness number is not it.** The pass answers 8.9% of the
addresses NAR is missing, and 4 of 942 real Quebec filings. The eval's 5.5-point Quebec
improvement is a *confirmation-set* effect from `RqaAddresses` existing, not a parser gain — the
two are reported as separate lines in `data-raw/eval_normalize.R` for that reason.

## Licence

**CC-BY 4.0, where everything else here is an open government licence.** That is compatible in
the way ODbL is not — which is why this is a tier and `osm_geocode()` is not — but it is a
condition, not a courtesy: anything published from these points carries the attribution, and
`rqa_attribution()` is the exported string. `rqa_import()` prints it on success.

## Tests

`tests/testthat/helper-rqa.R` writes a five-row miniature register and runs the genuine import
over it; `local_rqa_connection()` imports NAR, **disconnects**, imports RQA, and reopens
read-only, because DuckDB will not give a second writer the lock. `local_nar_fixture(qc =
TRUE)` adds the one Quebec address both fixtures share, which is what makes `IN_NAR` testable
at all. Each register row carries one thing the import has to get right — a leading particule,
a cardinal to canonicalize, a borough the municipality name does not name, a retired row
`etat` must drop — so a failure names its own cause.

The normalization tests key off the same five rows: Rue Courtemanche in Montréal-Est is the one
address NAR does not carry, so it is what the second pass has to answer; Rue Peel is in both, so
it is what proves NAR still wins — and the two registers spell its municipality differently, which
is what makes the winner visible in the output.
