# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cangeocode` is an R package (MIT, R >= 4.1) with **two objectives, both first-class**:
geocoding Canadian addresses in both directions, and **normalizing** free-text addresses into
structured components. Normalization is a step inside `geocode()` and also an end in itself —
matching two address lists to each other needs the parse and never needs a coordinate — so it
carries its own vignette, its own README section, its own pkgdown group and its own status
note. Do not fold it back into the geocoding story.

The current implementation is built entirely on Statistics Canada's **NAR**
(National Address Repository) bulk CSV releases, imported into a local **DuckDB** database with
the `spatial` extension. Three online geocoders are wired up as fallback tiers: NRCan's national
geolocator, the Province of British Columbia's Address Geocoder, and Quebec's MRNF geocoder over
the Répertoire québécois des adresses — the latter two provincial-only and also validation
sources. A fourth, the Government of Canada's OpenStreetMap (Nominatim) instance, is bound as
`osm_geocode()` but is deliberately **not** a tier — its data is ODbL where everything else here
is OGL. Reverse geocoding is NAR-backed and local except for `qc_reverse_geocode()`, the one
online reverse geocoder here. Road network files are named in `DESCRIPTION` as a future source
but are not implemented yet.

Public API (see `NAMESPACE`): `nar_connection()`, `nar_provinces()`,
`available_nar_versions()`, `collect_nar()`, `reverse_geocode()`, `normalize_address()`,
`address_pattern()`, `address_key()`, `format_address()`, `geocode()`, `nrcan_geocode()`,
`bc_geocode()`, `bc_validate()`, `qc_geocode()`, `qc_reverse_geocode()`, `qc_validate()`,
`osm_geocode()`.

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
devtools::test()                          # run the whole suite
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

The vignettes are one per objective plus support: `cangeocode` (getting started, reverse
geocoding), `geocoding` (`geocode()`), `address-normalization` (`normalize_address()` as a task
in its own right, including address matching), and `querying-nar` (the database directly).
`_pkgdown.yml` groups them under those headings in the navbar, and the reference index puts
Address normalization directly below Geocoding for the same reason.

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
visible in the code and that have been re-derived — and re-broken — before. **Read the note that
covers the code you are about to touch.**

| note | covers | what you cannot guess from the code |
| --- | --- | --- |
| **[`spatial.md`](spatial.md)** — start here | `R/geo_helpers.R`, `R/reverse_geocode.R`, `collect_nar()`, CRS handling | all spatial SQL is TEMP macros defined once; geometry is stored *untagged* so the RTREE index can exist and the CRS is re-attached at query time; the zonemap prefilter is the biggest win in the package and is not an index; every lon/lat transform needs `always_xy = TRUE` |
| **[`nar-database.md`](nar-database.md)** | `R/nar.R`, `R/nar_zip.R`, `R/nar_provinces.R` — download, schema, partial imports, version discovery | `nar_schema_version()` is 6 and older databases must keep working; a single province is fetched by HTTP range over the archive's own central directory; `BG` is Building and `BF` is Blockface, and a blockface distance is not comparable to a building one |
| **[`normalization.md`](normalization.md)** | `R/normalize_address.R`, `R/normalize_pattern.R`, `R/normalize_variants.R`, `R/normalize_gazetteer.R`, `R/address_format.R` | numbered rural roads carry no street type at all; `STE` is Suite *and* Sainte, and all three unit paths must know it; `name_sim` is not a similarity; the fuzzy branch compares on a *match fold* that spells `ST` out to `SAINT` and turns the hyphen into a word boundary, and the R and SQL halves of it must stay identical or matching silently degrades; an alternative reading is generated only when the baseline is *demonstrably* broken, because the gazetteer scores a municipality-restricted match higher by construction and so cannot arbitrate a bad candidate away |
| **[`geocoding.md`](geocoding.md)** | `R/geocode.R`, `R/geocode_bc.R`, `R/geocode_nrcan.R`, `R/geocode_qc.R`, `R/geocode_osm.R` | `method` names the tiers *in priority order*; "unplaced" is `is.na(x)`, never `match_method == "none"`; matching both NAR name families with `OR` instead of a `UNION` is a 99x slowdown; BC, the geolocator and Quebec always answer, so a response is not a match, while Nominatim genuinely refuses; a returned title must be re-parsed *without* the gazetteer or the floor launders the error it exists to catch; Quebec's locator needs the query spelled French-canonical (`Rue Notre-Dame Ouest`, not NAR's `NOTRE-DAME RUE O`) or it silently stops matching, and its `Score` is not a precision ranking; `osm_geocode()` is exported but is not a tier, and the reason is the ODbL licence rather than accuracy |

### Status notes

Longer-form notes live in **`inst/notes/`** and ship with the package. They record what has been
*measured*, where the component falls short today, and what to do next — the component notes above
record the design.

- **[`inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md)**
  — what `geocode()` resolves and what it does not: tier coverage, the interpolation
  accuracy tables, how far its points sit from a second source and why that is not a
  benchmark, what the online tiers add, and the pathways sized but not built (road network file, centroid tier).
- **[`inst/notes/nrcan-geolocator.md`](../inst/notes/nrcan-geolocator.md)**
  — what NRCan's geolocator does on the other end of the wire, read from its own source: why a
  fuzzy match over one string answers a different question, what `INTERPOLATED_POSITION`
  certifies, which of the floor's checks are vacuous, the one-in-twelve requests the
  service drops that a retry gets back, and the Canada-hosted Nominatim sibling that
  `osm_geocode()` now binds. Read it before touching `R/geocode_nrcan.R` or `R/geocode_osm.R`.
- **[`inst/notes/quebec-addresses.md`](../inst/notes/quebec-addresses.md)**
  — NAR's Quebec rows measured against the Répertoire québécois des adresses they come from:
  the 2.5-million-address point comparison that shows NAR is carrying RQA's own coordinates,
  why Quebec's 99.8% "building" coverage is not a quality statement, the 225,275 addresses RQA
  has and NAR does not, and the odonyme decomposition — which the gazetteer's match fold has
  since made a smaller prize than it looked. Read it before trusting any Quebec comparison, `qc_validate()` included.
- **[`inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md)**
  — where address normalization currently falls short: the measured failure modes, the things
  tried and rejected, and the ranked next steps. Read it before changing the parser or the
  gazetteer, and re-run the eval harness (instructions are in that file) before and after any
  such change.
- **[`inst/notes/deepparse.md`](../inst/notes/deepparse.md)**
  — the neural tagger measured against this parser on four corpora, two of which the parser
  was never tuned on: why it loses on knowledge and wins on *segmentation*, the two generated
  classes that carry its entire advantage, the leading-prose prefix that takes this parser from
  98% to 0%, the six-line rule that beats the tagger at recovering it, and why a fine-tune and a
  from-scratch model are both unwarranted on the evidence. It closes the open question at the
  end of the normalization status note.

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
