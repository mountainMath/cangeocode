# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cangeocode` is an R package (MIT, R >= 4.1) with **two objectives, both first-class**:
geocoding Canadian addresses in both directions, and **normalizing** free-text addresses into
structured components. Normalization is a step inside `geocode()` and also an end in itself —
matching two address lists to each other needs the parse and never needs a coordinate — so it
carries its own vignette, its own README section, its own pkgdown group and its own status
note. Do not fold it back into the geocoding story.

Everything offline lives in one local **DuckDB** database (`spatial` extension) built from
Statistics Canada's **NAR** (National Address Repository) bulk CSVs. Two optional imports write
**their own tables** beside it — Quebec's *Répertoire québécois des adresses* (`rqa_import()`,
the `"rqa"` tier) and StatCan's **Road Network File** (`rnf_import()`, the `"rnf"` tier, the one
tier that reaches streets NAR does not carry at all). Keeping each *beside* NAR rather than
merged into it is load-bearing; see [`rqa.md`](rqa.md) and [`rnf.md`](rnf.md). Online:
NRCan's national geolocator, BC's Address Geocoder and Quebec's MRNF geocoder are fallback
tiers, the last two provincial-only and also validation sources. `osm_geocode()` is bound but is
deliberately **not** a tier — its data is ODbL where everything else here is OGL. Reverse
geocoding is NAR-backed and local except for `qc_reverse_geocode()`.

Public API (see `NAMESPACE`): `nar_connection()`, `open_nar()`, `close_nar()`, `nar_provinces()`,
`available_nar_versions()`, `collect_nar()`, `reverse_geocode()`, `normalize_address()`,
`address_pattern()`, `address_key()`, `format_address()`, `geocode()`, `geocode_accept()`,
`geocode_matches()`, `nrcan_geocode()`, `bc_geocode()`, `bc_validate()`, `qc_geocode()`,
`qc_reverse_geocode()`, `qc_validate()`, `osm_geocode()`, `rqa_import()`, `rqa_attribution()`,
`rnf_import()`.

This file records the repo-wide facts: what to run, what the environment needs, how the tests
and vignettes are built, and the conventions. **Why each component is shaped the way it is lives
in a component note beside this file** — see the map under [Architecture](#architecture).

## Commands

Standard devtools workflow (devtools 2.5+, roxygen2 8.x, testthat 3.x are installed locally):

```r
devtools::load_all()      # load package for interactive work
devtools::document()      # regenerate NAMESPACE + man/ from roxygen comments
devtools::check()         # full R CMD check
devtools::install()
devtools::test()                            # run the whole suite
devtools::test(filter = "reverse-geocode")  # one file (matches tests/testthat/test-<filter>.R)
```

Roxygen is the source of truth: never hand-edit `NAMESPACE` or files in `man/`; edit the
roxygen block above the function and re-run `devtools::document()`.

## Required environment

- **`NAR_CACHE_PATH`** (env var, required): directory where `<version>.duckdb` files are
  written. `nar_connection()` errors out immediately if unset. It is set in the developer's
  `~/.Renviron`, so `Sys.getenv("NAR_CACHE_PATH")` may look empty from a plain shell but is
  populated inside R.
- **`options(nar_exdir = ...)`** (optional): points at an already-extracted NAR CSV directory
  so the import path skips the StatCan download. **Use this when testing import changes** — the
  real download is a multi-hundred-MB zip over a connection slow enough that the code bumps
  `options(timeout)` to 20 minutes.

All examples that would hit the network are wrapped in `\dontrun{}`; `devtools::check()` runs
clean (0 errors / 0 warnings / 0 notes) without touching StatCan.

## Tests

The suite never downloads anything and never opens the real ~5 GB database. `helper-nar.R` writes
a **three-address miniature NAR release** to a temp dir and runs the genuine import over it, so
the schema, the geometry mutate, and the metadata are all exercised end to end in about a second.
Two knobs matter:

- `local_nar_fixture(blockface = TRUE/FALSE)` emits the 31-column 2026-06 layout or the older
  29-column one. **Both must keep passing** — that pair is the regression test for the positional
  column shift and for the conditional blockface fallback.
- `nar_province_fixture()` writes a release named the way StatCan names its own
  (`Address_59.csv`, `Address_48.csv`) plus one member no province owns, which is what the
  partial-import and append tests key off. `local_nar_fixture()` deliberately does *not* — its
  `Address_BC.csv` is unplaceable, hence shared, hence always loaded.
- `local_nar_env()` mocks `available_nar_versions()` (the only function that scrapes StatCan) and
  points `NAR_CACHE_PATH` and `nar_exdir` at temp dirs. The mock takes `...` because the
  `refresh` argument is threaded through to it. Anything calling `nar_connection()` needs
  it; `local_nar_connection()` bundles it with the import and cleanup.

`skip_if_no_duckdb_spatial()` guards every test that touches DuckDB, since `LOAD spatial` may
have to fetch the extension. Fixture geometry is fixed: `addr1` has a building point, `addr2`
only a blockface point 50 m away, `addr3` none at all — that 50 m gap is what the radius
boundary tests key off.

## Vignettes

One per objective, plus support, plus **one per data source**: `cangeocode` (getting started,
reverse geocoding), `geocoding`, `address-normalization` (a task in its own right, including
address matching), `querying-nar`, and the source family — `data-sources` (the parent: how the
seven relate, what each tier is worth, which can check another, and why one is not a tier) over
`source-nar`, `source-rqa`, `source-rnf`, `source-bc`, `source-nrcan`, `source-qc`, `source-osm`.

Contract for the source family: each child opens with a one-line backlink to the parent, and the
parent's table is the index — **add a row there when a source is added**, or the new vignette is
reachable only from the navbar. Each child also carries a `## What this adds to the package` and
a `## Licence` section; the licences are what decide which sources may be default tiers (OGL and
CC-BY compose, ODbL does not). `source-osm` is the only one with no live chunks, its accuracy
probe never having been run. The family is where the measurements in `inst/notes/` are surfaced
to a user. `_pkgdown.yml` groups the vignettes under those headings, the source family under
**Data sources**, and orders Address normalization directly below Geocoding in the reference
index — the same first-class claim the objectives make.

All vignettes query the real ~5 GB database, which `R CMD build` cannot do, so they are
**pre-computed**: the sources are `vignettes/<name>.Rmd.orig`, `vignettes/precompute.R` knits them
against a local NAR database, and the resulting `<name>.Rmd` — output already inlined, no live
chunks — is what ships and what is committed.

```r
Rscript vignettes/precompute.R   # needs NAR_CACHE_PATH and an imported database
```

**Never edit `vignettes/*.Rmd` by hand** — edit the `.Rmd.orig` and re-knit, or the next
precompute silently discards the change. The `.orig` files and `precompute.R` are `.Rbuildignore`d;
`map-1.png` is not, since the getting-started vignette references it. Re-run precompute after
importing a new NAR release: the vignettes quote row counts and coverage figures from whatever
database was open at knit time. `devtools::install()` first — precompute runs `library(cangeocode)`
against the installed package, not `load_all()`.

## Architecture

Every component carries its own note in `.claude/`, recording the constraints that are not
visible in the code and that have been re-derived — and re-broken — before. The third column is a
**list of hooks, not the facts themselves**: each clause is a trap the note explains in full.
**Read the note that covers the code you are about to touch.**

| note | covers | hooks |
| --- | --- | --- |
| **[`spatial.md`](spatial.md)** — start here | `R/geo_helpers.R`, `R/reverse_geocode.R`, `collect_nar()`, CRS handling | all spatial SQL is TEMP macros defined once; geometry is stored *untagged* so the RTREE index can exist, and the CRS is re-attached at query time; the zonemap prefilter is the biggest win in the package and is not an index; every lon/lat transform needs `always_xy = TRUE` |
| **[`nar-database.md`](nar-database.md)** | `R/nar.R`, `R/nar_zip.R`, `R/nar_provinces.R`, `R/nar_session.R` — download, schema, partial imports, version discovery, the session connection | `nar_schema_version()` is 6 and older databases must keep working; a single province is fetched by HTTP range over the archive's own central directory; `BG` is Building and `BF` is Blockface, and their distances are not comparable; an unsupplied `con` resolves to a *parked* connection the call that opened it never closes, `"latest"` stops meaning "ask StatCan" once one is parked, and every write path must call `nar_session_release()` |
| **[`normalization.md`](normalization.md)** | `R/normalize_address.R`, `R/normalize_pattern.R`, `R/normalize_variants.R`, `R/normalize_gazetteer.R`, `R/normalize_lexicon.R`, `R/address_format.R` | numbered rural roads carry no street type at all; `STE` is Suite *and* Sainte, and all three unit paths must know it; `name_sim` is not a similarity, and a type word the parser ate is charged twice — landing the right street at 0.828 against a 0.85 bar, which is not answered by moving the bar; the fuzzy branch compares on a *match fold* whose R and SQL halves must stay identical; an alternative reading is generated only where the baseline is *demonstrably* broken, the restored leading compass word being the one unconditional exception; a tie breaks toward the reading that kept the municipality it was given; `nar_strip_lead_prose()` runs before every civic-number rule; a municipality swap is fined 0.88 unless NAR itself attests it, and `mun_evidence` reports which arm decided; a refused match is invisible from outside without `keep_refused` |
| **[`rqa.md`](rqa.md)** | `R/rqa.R` — the RQA import and the `"rqa"` tier | merging RQA into NAR would spend the only instrument Quebec's coverage is measurable with; `nar_schema_version()` is deliberately *not* bumped, since the tables are optional and a bump forces a re-download; NAR keeps the particule inside the street name and RQA in a column of its own; `IN_NAR` is fold equality and over-reports by ~14%; the tier joins on the *match* fold; the normalization pass reuses NAR's `MunAlias` rather than an alias table of its own |
| **[`rnf.md`](rnf.md)** | `R/rnf.R` — the road network file import and the `"rnf"` tier | the file carries no provenance flag on its address ranges, so every threshold rests on measurement; take the shapefile, the only format published for every release; `N/A` is a literal string beside real nulls in TYPE/DIR; an absent type or direction constrains nothing on either side; the municipality needs both `MunAlias` and a direct CSD comparison; ambiguity refuses and a parity mismatch does not, for different reasons |
| **[`geocoding.md`](geocoding.md)** | `R/geocode.R`, `R/known.R`, `R/geocode_accept.R`, `R/geocode_bc.R`, `R/geocode_nrcan.R`, `R/geocode_qc.R`, `R/geocode_osm.R` | `known` is one named list keyed by the *output* column names, and an unrecognized key is an error rather than a dropped constraint; `MUN_NAME` (mailing city, compared straight) and `CSD_NAME` (census subdivision, resolved through the asymmetric `MunAlias`) are two different questions, not one with a mode, and they do not nest; `CSD_NAME` is an input *and* an output and the two are not the same claim, so the constraint travels on the `nar_csd_constraint` attribute and never off the column; `method` names the tiers *in priority order*, and "unplaced" is `is.na(x)`, never `match_method == "none"`; `n_matches` counts points and `n_records` addresses, and only the first may widen `uncertainty_m`; `POSTAL_CODE` is the parse where `match_postal_code` is a lookup that reports nothing unless the candidates agree; a supplied unit narrows the candidates *or does nothing*; `geocode_matches()` is that same candidate set read without the collapse; matching both NAR name families with `OR` instead of a `UNION` is a 99x slowdown; `...` has to reach inward as well as outward; `uncertainty_m` is floored per `mun_evidence`, not per `mun_remapped`; the precision/recall dial is at *report* time in `geocode_accept()` and deliberately not a `strictness` argument; each online binding has a trap of its own — BC, NRCan and Quebec always answer so a response is not a match, Quebec needs the query spelled French-canonical, and OSM is excluded on licence rather than accuracy |

### Status notes

Longer-form notes live in **`inst/notes/`** and ship with the package. They record what has been
*measured*, where the component falls short today, and what to do next — the component notes above
record the design. Each names the `data-raw/` script that reproduces it.

- **[`geocoding-status.md`](../inst/notes/geocoding-status.md)** — what `geocode()` resolves and
  what it does not: tier coverage, the interpolation accuracy tables, what each online tier adds,
  what the acceptance bar costs and buys, and the one pathway still sized but not built (a street
  or municipality centroid tier).
- **[`road-network-file.md`](../inst/notes/road-network-file.md)** — the RNF measured against NAR,
  which is how its missing provenance flag got replaced by numbers: the download contract, the 13
  CircularStrings that fail the read, the `max(95, 0.35 × len_m)` uncertainty model, and why
  refusing on `n_matches > 1` is **necessary and not sufficient**. Read it before moving any RNF
  threshold. `data-raw/probe_rnf.R`.
- **[`nrcan-geolocator.md`](../inst/notes/nrcan-geolocator.md)** — what NRCan's geolocator does on
  the other end of the wire, read from its own source: what `INTERPOLATED_POSITION` certifies,
  which of the floor's checks are vacuous, the one-in-twelve requests it drops that a retry gets
  back, and the Canada-hosted Nominatim sibling. Read before touching `R/geocode_nrcan.R` or
  `R/geocode_osm.R`.
- **[`quebec-addresses.md`](../inst/notes/quebec-addresses.md)** — NAR's Quebec rows measured
  against the register they come from: NAR is carrying RQA's own coordinates, so Quebec's 99.8%
  "building" coverage is not a quality statement; what `rqa_import()` and the `"rqa"` tier
  actually delivered against the projections; and the standing warning against reading a coverage
  share off NAR's residual (the 81.8% → 88.3% figure was a *confirmation-set* effect, not a parser
  gain). Read before trusting any Quebec comparison, `qc_validate()` included.
- **[`nova-scotia-pvsc.md`](../inst/notes/nova-scotia-pvsc.md)** — the first reference here
  *established by measurement* to be independent of NAR, and the strongest accuracy result:
  p50 10.3 m. That independence is earned in stage 0 and cannot be assumed — the more
  authoritative a provincial address file is, the more likely NAR already contains it (NSCAF sits
  1.04 m from NAR and so cannot check it). Also where `n_matches == 1` is shown *not* to be a
  safety guarantee, and where the two parser defects PVSC's split components expose are sized —
  both costing *matches* rather than metres. `data-raw/probe_pvsc.R`, stage 0 being the
  provenance test.
- **[`address-normalization-status.md`](../inst/notes/address-normalization-status.md)** — where
  normalization falls short: the measured failure modes, what was tried and rejected, and the
  ranked next steps. Read it before changing the parser or the gazetteer, and re-run the eval
  harness (instructions are in the file) before and after. Two modes are too rare for a uniform
  sample to see and carry their own at-risk probes — `data-raw/probe_direction.R` and
  `data-raw/probe_type.R`, both comparing against NAR's *own columns* rather than its coordinates,
  the one measurement here with no not-ground-truth caveat.
- **[`deepparse.md`](../inst/notes/deepparse.md)** — the neural tagger measured against this
  parser on four corpora: why it loses on knowledge and wins on *segmentation*, the two guarded
  rules the benchmark produced, and why neither a fine-tune nor a from-scratch model is warranted
  on the evidence. It closes the open question at the end of the normalization status note.
- **[`nar-consistency.md`](../inst/notes/nar-consistency.md)** — finding NAR's misplaced addresses
  using nothing but NAR, which is the one measurement escaping the not-ground-truth caveat: why
  the CSD label is not a third witness, why `d_own`/`d_other` replace distance-from-the-median and
  must be measured *along the road network*, and the extra question that turns 17,224 flags into
  653 rows whose coordinate is the part to disbelieve. Read it before treating any flag as an
  error — the blind spot and the false-positive families are named there, and nothing has been
  repaired. `data-raw/probe_consistency.R`.

## Package-level plumbing (`R/misc.R`)

Package-level `@import dplyr` / `@importFrom` block, a no-op `ignore_unused_imports()` that
references `dbplyr::sql` so R CMD check does not flag the unused import, and the
`globalVariables()` registration for names resolved DuckDB-side. **Any new DuckDB function or
`nar_*` macro called from a `dplyr` pipeline must be added to that vector** or R CMD check
reports an undefined global.

## Conventions

- Native pipe `|>` throughout, 2-space indent (see `cangeocode.Rproj`).
- Inside `dplyr` verbs, column references use `.data$COL` and function-argument references use
  `!!arg` — required to keep R CMD check clean.
- NAR column names are kept in their original SCREAMING_SNAKE_CASE from the StatCan CSVs.
- Coordinates are parsed only through `nar_project()`, and NAR's own lon/lat only as EPSG:4269;
  see the CRS section of [`spatial.md`](spatial.md) before touching either.
