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
sources. Quebec's register, the *Répertoire québécois des adresses*, can also be imported into the same
DuckDB file **as its own tables** by `rqa_import()`, which adds an offline `"rqa"` tier for
Quebec. It is kept beside NAR rather than merged into it, and that is a load-bearing
decision.
A fourth online geocoder, the Government of Canada's OpenStreetMap (Nominatim) instance, is
bound as `osm_geocode()` but is deliberately **not** a tier — its data is ODbL where everything else here
is OGL. Reverse geocoding is NAR-backed and local except for `qc_reverse_geocode()`, the one
online reverse geocoder here. Statistics Canada's **Road Network File** is imported into the
same DuckDB file **as its own tables** by `rnf_import()`, which adds an offline `"rnf"` tier
that interpolates a civic number along the street segment whose address range contains it —
the one tier that reaches streets NAR does not carry at all.

Public API (see `NAMESPACE`): `nar_connection()`, `open_nar()`, `close_nar()`, `nar_provinces()`,
`available_nar_versions()`, `collect_nar()`, `reverse_geocode()`, `normalize_address()`,
`address_pattern()`, `address_key()`, `format_address()`, `geocode()`,
`geocode_matches()`, `nrcan_geocode()`, `bc_geocode()`, `bc_validate()`, `qc_geocode()`, `qc_reverse_geocode()`, `qc_validate()`,
`osm_geocode()`, `rqa_import()`, `rqa_attribution()`, `rnf_import()`.

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

The vignettes are one per objective, plus support, plus **one per data source**:
`cangeocode` (getting started, reverse geocoding), `geocoding` (`geocode()`),
`address-normalization` (`normalize_address()` as a task in its own right, including address
matching), `querying-nar` (the database directly), and the source family -- `data-sources`
(the parent: how the seven relate, what each tier layer is worth, which can check another,
and why one is not a tier) over `source-nar`, `source-rqa`, `source-rnf`, `source-bc`,
`source-nrcan`, `source-qc`, `source-osm`. Each child opens with a one-line backlink to the
parent, and the parent's table is the index -- **add a row there when a source is added**, or
the new vignette is unreachable from anywhere but the navbar. The source family is where the measurements in `inst/notes/` are surfaced to a
user, and **every one of them carries a `## What this adds to the package` section and a
`## Licence` section** -- those two are the contract for the family, not decoration, since the
licences are what decide which sources may be default tiers (OGL and CC-BY compose; ODbL does
not, which is why `osm_geocode()` is not a tier). `source-osm` is the only one with no live
chunks, because its accuracy probe has not been run. `_pkgdown.yml` groups the vignettes under
those headings in the navbar, with the source family under **Data sources**, and the reference
index puts Address normalization directly below Geocoding for the same reason.

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
| **[`nar-database.md`](nar-database.md)** | `R/nar.R`, `R/nar_zip.R`, `R/nar_provinces.R`, `R/nar_session.R` — download, schema, partial imports, version discovery, the session connection | `nar_schema_version()` is 6 and older databases must keep working; a single province is fetched by HTTP range over the archive's own central directory; `BG` is Building and `BF` is Blockface, and a blockface distance is not comparable to a building one; an unsupplied `con` resolves to a *parked* connection that is never closed by the call that opened it, `"latest"` stops meaning "ask StatCan" the moment one is parked, and every write path has to call `nar_session_release()` or the read-only handle blocks its own import |
| **[`normalization.md`](normalization.md)** | `R/normalize_address.R`, `R/normalize_pattern.R`, `R/normalize_variants.R`, `R/normalize_gazetteer.R`, `R/address_format.R` | numbered rural roads carry no street type at all; `STE` is Suite *and* Sainte, and all three unit paths must know it; `name_sim` is not a similarity; the fuzzy branch compares on a *match fold* that spells `ST` out to `SAINT` and turns the hyphen into a word boundary, and the R and SQL halves of it must stay identical or matching silently degrades; an alternative reading is generated only when the baseline is *demonstrably* broken, because the gazetteer scores a municipality-restricted match higher by construction and so cannot arbitrate a bad candidate away — and the third thing that counts as broken, a trailing run longer than the municipality claimed that also names one, is what segments a comma-free string and cannot fire on a delimited one; every civic-number rule anchors on a number at the *front* of the string, so `nar_strip_lead_prose()` cuts a prose prefix off before anything else reads it, and each of its four guards is holding back a real address form; the gazetteer runs a *second* pass over Quebec's own register where NAR left the row unresolved, and the R and SQL folds differ on the en dash unless the SQL half is told about it |
| **[`rqa.md`](rqa.md)** | `R/rqa.R` — the RQA import and the `"rqa"` tier | RQA is kept beside NAR and not merged into it because merging spends the only instrument Quebec's coverage is measurable with; `nar_schema_version()` is deliberately *not* bumped, since the tables are optional and additive and a bump forces a re-download; NAR keeps the particule inside the street name and RQA in a column of its own, so comparing raw reads 1.27M missing where there are 358K; `IN_NAR` is fold equality and knowingly over-reports by ~14%; the tier joins on the *match* fold, not the plain one, because the addresses it exists for are exactly the ones the gazetteer could not resolve; the normalization pass reuses NAR's `MunAlias` rather than an alias table of its own, which is why RQA's `BOROUGH` is needed by the tier and not by the parser |
| **[`rnf.md`](rnf.md)** | `R/rnf.R` — the road network file import and the `"rnf"` tier | the file carries no provenance flag on its address ranges, so every threshold rests on measurement rather than on the file; take the shapefile, which is the only format published for every release *and* the one without the 13 CircularStrings DuckDB refuses; `N/A` is a literal string beside real nulls in TYPE/DIR; an absent type or direction constrains nothing on either side; the municipality needs both `MunAlias` and a direct CSD comparison because 8.3% of RNF's ranged pairs are not in NAR at all; ambiguity refuses and a parity mismatch does not, and the reasons are different |
| **[`geocoding.md`](geocoding.md)** | `R/geocode.R`, `R/geocode_bc.R`, `R/geocode_nrcan.R`, `R/geocode_qc.R`, `R/geocode_osm.R` | `method` names the tiers *in priority order*; "unplaced" is `is.na(x)`, never `match_method == "none"`, and both `ADDR_GUID` and `match_postal_code` survive from the tier that matched the record into whichever tier places the row; `POSTAL_CODE` is the parse and `match_postal_code` is the lookup, and the second is an aggregate over the candidates that reports nothing unless they agree, which is what a candidate set the input gave no unit for leaves it unable to do; `n_matches` counts points and `n_records` counts addresses, 47% of NAR's placed addresses share a point, and the two failures are different -- wrong place versus more than one thing in the right place, so a record count is never widened into `uncertainty_m`; a supplied unit narrows the candidates and 27.5% of the units real filings supply are not in NAR at that civic number, which is why the filter narrows *or does nothing* rather than being enforced, and why the unit vocabulary is translated on the input side only; `geocode_matches()` is that *same* candidate set read without the collapse, sharing the rank and the civic key so `match_rank == 1` is the answered row by construction, and it takes no `method` because no other tier has a set to enumerate; matching both NAR name families with `OR` instead of a `UNION` is a 99x slowdown; BC, the geolocator and Quebec always answer, so a response is not a match, while Nominatim genuinely refuses; a returned title must be re-parsed *without* the gazetteer or the floor launders the error it exists to catch; Quebec's locator needs the query spelled French-canonical (`Rue Notre-Dame Ouest`, not NAR's `NOTRE-DAME RUE O`) or it silently stops matching, and its `Score` is not a precision ranking; `osm_geocode()` is exported but is not a tier, and the reason is the ODbL licence rather than accuracy; BC's `locationDescriptor` is a request and not an instruction, three of its six values return the same point as the default, and the default is already the closest to NAR |

### Status notes

Longer-form notes live in **`inst/notes/`** and ship with the package. They record what has been
*measured*, where the component falls short today, and what to do next — the component notes above
record the design.

- **[`inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md)**
  — what `geocode()` resolves and what it does not: tier coverage, the interpolation
  accuracy tables, how far its points sit from a second source and why that is not a
  benchmark, what the online tiers add, and the pathways sized but not built (road network file, centroid tier).
- **[`inst/notes/road-network-file.md`](../inst/notes/road-network-file.md)**
  — Statistics Canada's Road Network File measured against NAR, which is how the missing
  provenance flag on its address ranges got replaced by a number: 89.7% of NAR civic numbers
  fall inside the range their own side claims, interpolation lands p50 24.3 m from NAR's
  building point, and the shipped tier places 24.5% of the filings `geocode()` fails —
  the largest recovery any tier has offered. It is also where the overlap-vs-residual
  correction bites again, now decomposed against a third baseline into what the tier costs
  (43 → 60 m from the filer's own postal code) and what the residual costs (60 → 149 m). The
  tier is only safe if it refuses when `n_matches > 1` — and that refusal is **necessary and
  not sufficient**: every recovered row past 2 km is unambiguous, and the one real error
  among them is a bad parse the tier placed faithfully, which is the failure mode a tier
  reaching streets NAR lacks cannot check for. Carries the download contract (shapefile only, across releases), the 13
  CircularStrings that fail the read, and the `max(95, 0.35 × len_m)` uncertainty model.
  Read it before building the RNF tier; `data-raw/probe_rnf.R` reproduces all of it.
- **[`inst/notes/nrcan-geolocator.md`](../inst/notes/nrcan-geolocator.md)**
  — what NRCan's geolocator does on the other end of the wire, read from its own source: why a
  fuzzy match over one string answers a different question, what `INTERPOLATED_POSITION`
  certifies, which of the floor's checks are vacuous, the one-in-twelve requests the
  service drops that a retry gets back, and the Canada-hosted Nominatim sibling that
  `osm_geocode()` now binds. Read it before touching `R/geocode_nrcan.R` or `R/geocode_osm.R`.
- **[`inst/notes/quebec-addresses.md`](../inst/notes/quebec-addresses.md)**
  — NAR's Quebec rows measured against the Répertoire québécois des adresses they come from:
  the 2.5-million-address point comparison that shows NAR is carrying RQA's own coordinates,
  why Quebec's 99.8% "building" coverage is not a quality statement, the six-way split of what
  is left of Québec's normalization failures (41.3% of them addresses NAR does not carry — and
  why the previous split understated that by keying on the full postal code), what importing
  those ~308,000 addresses would and would not buy for each of the package's two objectives,
  and the odonyme decomposition — which the gazetteer's match fold has
  since made a smaller prize than it looked, and — since 2026-08-23 — what `rqa_import()` and
  the `"rqa"` tier actually delivered against those projections -- including the correction that
  the 81.8% -> 88.3% normalization figure was a *confirmation-set* effect and not a parser gain,
  which is the standing warning against reading a coverage share off NAR's residual. Read it
  before trusting any Quebec comparison, `qc_validate()` included.
- **[`inst/notes/nova-scotia-pvsc.md`](../inst/notes/nova-scotia-pvsc.md)**
  -- Nova Scotia's PVSC assessment addresses measured against the package, and the first
  reference here established by measurement to be **independent of NAR** rather than NAR
  checked against itself: two separately produced building points for the same house agree to
  p50 10.3 m and are within 50 m 88.2% of the time, the strongest accuracy result here. That
  independence is *earned in stage 0 and cannot be assumed* -- StatCan's Statistical Building
  Register, which NAR is extracted from, names property assessment rolls among its inputs, so
  the suspicion that PVSC is upstream of NAR is correct in mechanism and wrong in target:
  assessment rolls feed NAR's **attributes** (`BU_USE`, `BU_N_CIVIC_ADD`, 100% populated in
  every province) while the universe and the geocoding come from Canada Post Point-of-Call and
  the provincial **911 file**. In NS that is NSCAF, and NAR sits **1.04 m** from it with 95.2%
  of 361K pairs in one 1--2 m bucket and a 3.5 cm residual once a single vector is removed --
  the same coordinate re-datumed, which is the Quebec/RQA result with a different donor. So
  **NSCAF cannot check NAR in NS and PVSC can**, and the general trap is that the more
  authoritative a provincial address file is, the more likely NAR already contains it. It is also
  where `n_matches == 1` is shown *not* to be a safety guarantee -- one exact unambiguous match
  in 180 is over a kilometre wrong, 85% of everything past 5 km is the gazetteer having
  **remapped** the community name, and `uncertainty_m` reports 0 m for every one of them
  because it describes the spread of the candidates found and not whether the search looked in
  the right place. Carries the two parser defects PVSC's split components expose -- a spurious
  `STREET_DIR` stripped off a name that genuinely starts with a compass word (1.14% of rows,
  82% of them unplaced) and a truncated multi-word municipality (2.32%) -- together 13.0% of
  everything the pipeline fails to place in NS, and the correction that both cost *matches* and
  not metres, which the vivid 165 km examples made easy to get backwards. The licence is an OGL
  variant that would permit a tier; the note argues for an `ns_validate()` instead, which must
  be built on PVSC and not on the larger, more authoritative NSCAF for the provenance reason
  above. `data-raw/probe_pvsc.R` reproduces all of it, stage 0 being the provenance test.
- **[`inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md)**
  — where address normalization currently falls short: the measured failure modes, the things
  tried and rejected, and the ranked next steps. Read it before changing the parser or the
  gazetteer, and re-run the eval harness (instructions are in that file) before and after any
  such change.
- **[`inst/notes/deepparse.md`](../inst/notes/deepparse.md)**
  — the neural tagger measured against this parser on four corpora, two of which the parser
  was never tuned on: why it loses on knowledge and wins on *segmentation*, the two generated
  classes that carried its entire advantage, the leading-prose prefix that took this parser from
  98% to 0%, the two guarded rules the benchmark produced — `nar_strip_lead_prose()` and
  segmenting a comma-free string on the municipality inventory — each of which reversed the one
  result the tagger still led, and why a fine-tune and a from-scratch model are both unwarranted
  on the evidence. It closes the open question at the end of the normalization status note.
- **[`inst/notes/nar-consistency.md`](../inst/notes/nar-consistency.md)**
  — finding NAR's misplaced addresses using nothing but NAR: why a row's postal code and
  coordinate disagreeing is an *internal* contradiction and so escapes the not-ground-truth
  caveat every other measurement here carries; that the CSD label is **not** a third witness —
  point-in-polygon against the 2021 digital CSDs agrees with it 98.8% of the time, because it is
  derived from the coordinate, which leaves two sides and no majority to appeal to; that a postal
  code is a *delivery route* and may legitimately be disconnected, so distance-from-the-group's-
  median is the wrong statistic and `d_own`/`d_other` replace it — a ratio, which is what lets
  rural postal codes stay in instead of being excluded wholesale; why `d_other` has to be
  re-measured **along the road network** or Georgian Bay and Whistler supply the flags; and the
  one extra question — is the street at the point or at the postal code — that turns 17,224 flags
  into 653 rows whose *coordinate* is the part to disbelieve, plus the Amos row that shows a
  shared street name fooling that same arbiter. Read it before treating any flag as an error: the
  84,282-address blind spot and the false-positive families are named there, and nothing has been
  repaired. `data-raw/probe_consistency.R` reproduces all of it.

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
