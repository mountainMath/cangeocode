# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cangeocode` is an R package (MIT, R >= 4.1) for geocoding and reverse geocoding Canadian
addresses. The current implementation is built entirely on Statistics Canada's **NAR**
(National Address Repository) bulk CSV releases, imported into a local **DuckDB** database with
the `spatial` extension. One online geocoder is wired up — the Province of British Columbia's
Address Geocoder, as a BC-only fallback and validation source. Road network files are named in
`DESCRIPTION` as a future source but are not implemented yet.

Public API (see `NAMESPACE`): `nar_connection()`, `available_nar_versions()`, `collect_nar()`,
`reverse_geocode()`, `normalize_address()`, `address_pattern()`, `geocode()`, `bc_geocode()`,
`bc_validate()`.

This file records **why the code is shaped the way it is**, and it is the only document in
`.claude/`. Longer-form notes live in **`inst/notes/`** and ship with the package:

- **[`inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md)**
  — what `geocode()` resolves and what it does not: tier coverage, the interpolation
  accuracy tables, how far its points sit from an independent source, and the pathways sized
  but not built (road network file, centroid tier).
- **[`inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md)**
  — where address normalization currently falls short: the measured failure modes, the things
  tried and rejected, and the ranked next steps. Read it before changing the parser or the
  gazetteer, and re-run the eval harness (instructions are in that file) before and after any
  such change.

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
- `local_nar_env()` mocks `available_nar_versions()` (the only function that scrapes StatCan) and
  points `NAR_CACHE_PATH` and `nar_exdir` at temp dirs. The mock takes `...` because the
  `refresh` argument is threaded through to it. Anything calling `nar_connection()` needs
  it; `local_nar_connection()` bundles it with the import and cleanup.

`skip_if_no_duckdb_spatial()` guards every test that touches DuckDB, since `LOAD spatial` may
have to fetch the extension. Fixture geometry is fixed: `addr1` has a building point, `addr2`
only a blockface point 50 m away, `addr3` none at all — that 50 m gap is what the radius
boundary tests key off.

## Vignettes

Both vignettes query the real ~5 GB database, which `R CMD build` cannot do, so they are
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

### `R/geo_helpers.R` — the spatial layer (start here)

All spatial SQL in the package is defined in this one file, as **TEMP macros registered on every
connection** by `nar_register_spatial()`. Because they are temporary rather than persisted into
the database file, there is a single definition of each operation *and* databases built by
earlier package versions keep working without a rebuild. The macro family:

| macro | purpose |
| --- | --- |
| `nar_point(lon, lat)` | lon/lat → point in the storage CRS |
| `nar_xy(x, y)` | a coordinate pair that is already in the storage CRS |
| `nar_lon(geom)` / `nar_lat(geom)` | the inverse, stored geometry → lon/lat |
| `nar_geom(geom)` | tags stored geometry with its CRS, enabling DuckDB's CRS-mismatch check |
| `nar_store(geom)` | drops the tag so the column stays RTREE-indexable |
| `nar_wkb(geom)` | WKB for transfer to `sf`, mapping NULL geometry to an empty point |

**The central design constraint:** DuckDB's spatial extension now supports CRS-aware geometry
(`GEOMETRY('EPSG:3347')` via `ST_SetCRS`, and a CRS-inferring two-argument `ST_Transform`), and
comparing two differently-tagged geometries is a binder error — a genuine safety net. But
**RTREE indexes can only be built over untagged `GEOMETRY` columns**, and that index is the
reason radius queries over ~17M rows are viable. So geometry is *stored* untagged and the CRS is
recorded in the `nar_metadata` table, then re-attached at query time with `nar_geom()`. Untagged
values mix freely with tagged ones, so this buys the CRS check without giving up the index.
`nar_store()` exists because forgetting the cast makes `CREATE INDEX ... USING RTREE` fail.

`nar_within_radius()` takes coordinates **already in the storage CRS** (use `nar_project()` to
get there) and, when the table carries `x`/`y` columns, applies a bounding-box prefilter before
`ST_DWithin`. That prefilter is the single biggest win in the package and it is **not an index**:
DuckDB keeps min/max zonemaps per row group for numeric columns and skips whole row groups whose
range cannot satisfy the comparison. Measured over 17.3M addresses it cuts a radius query from
~0.24s to ~0.04–0.08s (3–7x) and returns byte-identical rows — the box is in the same planar,
metric CRS as the `ST_Distance` that follows, so it cannot drop a row the distance predicate
would keep. `nar_has_xy()` gates it, so databases built before schema version 2 still work, just
without the speedup.

**There is no btree-style spatial index to reach for here, and this was measured, not assumed.**
An ART index on `x` changed nothing (0.042s with it, 0.042s after `DROP INDEX`), and ordering the
table along a `ST_Hilbert` curve was slightly *worse* (0.054s vs 0.042s) because NAR already
arrives grouped by province/CSD. Zonemaps on plain `DOUBLE` columns are the whole mechanism.

`nar_within_radius()` deliberately uses `ST_DWithin` and **does not** route through the RTREE
index. Only `ST_Intersects`-family predicates can drive that index, and while a bounding-box
prefilter does produce an index scan, it is *slower at every radius* once row payloads are
fetched — the index yields row ids that must then be randomly accessed across a ~5 GB file,
whereas DuckDB's parallel columnar scan evaluates the predicate over 17M rows in ~0.25s and
stays flat as the radius grows (measured: 0.24s vs 0.38s at 100 m, 0.25s vs 1.5s at 1000 m).
The index still pays off for `count`/existence queries that never touch the rows, which is why
it is still built at import time. **Do not "optimize" this into an index scan without
re-measuring row retrieval, not `count(*)`.**

`nar_load_spatial()` uses DuckDB's own `LOAD spatial` rather than `duckspatial::ddbs_load()`.
duckspatial is no longer a dependency: its loader creates *persistent* helper macros, which
fails outright on the read-only connections this package hands out.

### `R/nar.R` — data acquisition and the DuckDB schema

`nar_connection(version, refresh)` is both the installer and the accessor. On first call for a
version it downloads + unzips the StatCan release, reads the CSVs with `arrow::open_dataset()`
under an **explicitly declared schema** (`skip_rows = 1`, since the declared schema replaces the
single CSV header row), then builds two permanent tables via a temp-table → geometry-mutate →
permanent-table hop:

- **`Addresses`** — one row per civic address. `geom` is
  `st_point(coalesce(BG_X, BF_REPPOINT_X), coalesce(BG_Y, BF_REPPOINT_Y))` — both pairs are
  already in the storage CRS — and **`geom_source`** records which was used (`"building"` /
  `"blockface"` / `NA`). The chosen pair is **kept as `x`/`y`** to feed the zonemap prefilter;
  they mirror `geom`, not `BG` alone, or the prefilter would disagree with the predicate it
  guards. `BG_X`/`BG_Y` are consumed into `x`/`y`; `BF_REPPOINT_X`/`BF_REPPOINT_Y` stay as their
  own columns. Keyed by `ADDR_GUID`, joins to `Locations` on `LOC_GUID`. Of 17.36M rows, 16.16M
  have a building point, a further 1.14M only a blockface point, and 65k **no geometry** at
  all.
- **`Locations`** — one row per location, carrying CSD/FED/ER codes and names. Built from
  lon/lat through `nar_store(nar_point(...))`, with `x`/`y` derived from the result.
- **`nar_metadata`** — key/value: `version`, `crs`, `lonlat_crs`, `schema_version`,
  `package_version`, `imported_at`. Read via `nar_crs()` / `nar_meta_value()`, which fall back to
  package defaults for databases predating those keys.

`x`/`y` are an internal query aid and are **dropped at the collection boundary** by `collect_nar()`
and `reverse_geocode()` — they duplicate `geom` and would silently go stale if the geometry were
reprojected.

`nar_schema_version()` is 3. Version 2 added the `x`/`y` columns and fixed the lon/lat datum;
version 3 added the blockface fallback and `geom_source`. Older databases still work — version 1
without the prefilter and with `Locations` geometry off by ~1.1 m, version 2 without a
`geom_source` column and with no geometry on the 1.14M blockface-only addresses. Only a
`refresh = TRUE` rebuild picks these up.

Both spatial tables get an RTREE index on `geom` and a btree on `LOC_GUID`. Distances are in
**metres** because the storage CRS is projected, which is why `match_radius` needs no conversion.

DDL here must use `dbExecute()`, not `dbSendQuery()`: an uncleared result set keeps the
connection busy, the final `CHECKPOINT` never lands, and the leftover WAL makes the subsequent
**read-only** reopen fail outright.

### Version discovery and offline use

`available_nar_versions()` **scrapes** the StatCan publication page (`rvest`/`xml2`) for `.zip`
links and caches the result as a CSV in `tempdir()` for the session. The parsing itself lives in
`nar_version_table(page, overview_url)`, which takes an already-parsed document so it can be
tested without the network — **that is where the CSS selector `"section div p a"` lives**, and a
StatCan page layout change breaks version discovery there. It resolves relative hrefs against the
publication page but leaves absolute ones alone, errors if the selector matches nothing, and warns
about (rather than drops silently) labels it cannot date.

`nar_version_date()` parses the heterogeneous version labels ("2022", "May 2024", "Sept. 2025",
full dates) into a `Date`, and `path` is derived from it as `YYYY-MM` — that `path` is the
database filename and the canonical version key. It matches month names against `month.name` /
`month.abb` plus any unambiguous prefix, **not** `strptime`'s `%B`: those constants are English
regardless of `LC_TIME`, so a French or German locale would otherwise fail to parse every label.
A bare year means that year's December release.

`nar_connection()` resolves the version through `nar_resolve_version()`, which **checks the local
cache before the network**: `nar_cached_versions()` lists the `<version>.duckdb` files (keys are
`YYYY-MM`, so a lexical sort is chronological), and an explicitly named version already on disk is
returned without contacting StatCan at all. If lookup fails while offline and `version = "latest"`,
it warns and falls back to the newest cached database rather than erroring. `refresh = TRUE` always
goes to the network.

### `R/reverse_geocode.R` — the query layer

Accepts an `sf`/`sfc` POINT (transformed to 4326 if needed) or a bare `c(lon, lat)` numeric,
then delegates the spatial predicate to `nar_within_radius()`. It opens a connection per call
unless the caller passes one as `con` (in which case `version` is ignored and the caller keeps
ownership — only an internally opened connection is disconnected on exit); reuse it when
geocoding many points in a loop. `output` selects between
`"address"`, `"components"`, and `"multiple"`; `geometry = TRUE` returns an `sf` object. Zero
matches produce a warning and `NULL`.

Results are sorted **in R** after collection, not with `arrange()` in the lazy pipeline: any
subsequent verb wraps the query in a subquery, and DuckDB drops `ORDER BY` in subqueries without
`LIMIT`, so sorting in SQL is silently discarded rather than honoured.

The formatted `address` column is built **column-wise**, via the internal `nar_paste_parts()`
(a vectorised `paste(na.omit(c(...)), collapse = " ")`). It replaced a `rowwise()` pipeline that
did not scale the way the query does: at an 800 m radius, 27k matches spent ~2.4s in R formatting
against ~0.06s in the database. Output is byte-identical — verified over 34k real rows. Do not
reintroduce `rowwise()` here.

### What `BG` and `BF` mean

Straight from the StatCan NAR User Guide, because the abbreviations invite exactly the wrong
guess: **`BG` = Building** and **`BF` = Blockface**. `BG_X`/`BG_Y` is a representative point for
the building (the guide warns it "may not correspond exactly to the physical center of the
building structure itself" — it can be the road access point or driveway). `BF_REPPOINT_*` is the
centroid of *"one side of a street between two consecutive features intersecting that street"*.

The blockface point is therefore **much coarser and is a fallback only**. Measured on the 2026-06
release: 8.67 addresses share each distinct `BF` point versus 1.61 per `BG` point, the median
`BG`→`BF` separation is 50 m (p95 331 m), and among the blockface-only addresses one point is
shared by 578 addresses. Anything that reports or ranks by distance must respect `geom_source` —
a blockface `dist` is not comparable to a building `dist`.

### CRS handling

`nar_project()` is the **single** place user coordinates are parsed. It resolves an `sf`/`sfc`
object or a bare lon/lat pair to storage-CRS coordinates, reprojecting exactly once and in `sf`
rather than in DuckDB, so the caller's PROJ configuration decides whether a WGS84→NAD83 datum
shift is applied. `reverse_geocode()` gained a `crs` argument (default EPSG:4326) for bare
numeric input. Do not add a second parsing path.

NAR's `BG_LATITUDE`/`BG_LONGITUDE` are **NAD83 (EPSG:4269)**, the same datum as `BG_X`/`BG_Y`.
This was established from the data: for locations with exactly one address, both describe the
same point, and over 300k such records re-deriving the projected coordinate from the lon/lat
leaves a median residual of **0.057 m** under EPSG:4269 (just NAR's 6-decimal rounding) versus
**1.08 m** under `OGC:CRS84`, which the package previously used.

Note this contradicts the User Guide, which labels those columns EPSG:4326. The two are
numerically identical wherever PROJ lacks the NAD83↔WGS84 grid-shift files, and diverge by ~1 m
where it has them; EPSG:4269 is exact either way. Tracked in issue #4, pending confirmation from
StatCan.

Every lon/lat transform must pass `always_xy = TRUE`. EPSG:4269 declares its axes lat/lon, so
without the flag DuckDB reads a longitude of -123 as a latitude and returns **`POINT (inf inf)`
instead of an error**.

### `collect_nar()`

Transfers geometry as **WKB** (`nar_wkb()`), which also substitutes an empty point for NULL
geometry, and reads the CRS from the database rather than assuming it. The optional `crs`
argument reprojects, passing `always_xy = TRUE` to `ST_Transform` — authority CRSs such as
EPSG:4326 order their axes lat/lon while `sf` always expects lon/lat, so **omitting that flag
silently returns transposed coordinates**. Always route database→`sf` conversion through this
function.

That `crs` argument goes through `nar_crs_string()` (in `geo_helpers.R`), which normalizes an
EPSG number, an `sf::crs`, or an authority string into something `ST_Transform` accepts — a bare
`4326` reaches DuckDB as `"EPSG:4326"`, since `as.character(4326)` is rejected with "not a
recognized CRS". A CRS with no authority code is passed through as full WKT.

`collect_nar()` needs a lazy table still attached to its NAR connection: it looks the connection up
with `nar_con()` in order to read the storage CRS and register the macros. Handed an
already-collected data frame it raises an explicit error pointing at `sf::st_transform()` instead
of failing deep inside dbplyr.

### Address normalization: numbered rural roads and the pattern recognizer

> Known failure modes, the eval harness, and what to fix next live in
> [`inst/notes/address-normalization-status.md`](../inst/notes/address-normalization-status.md).

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

### Two collisions between the parser's vocabulary and real place names

**`STE` is Suite and it is also Sainte.** Left unguarded, `Sault Ste. Marie` parses as a unit
called `SAULT MARIE` and the municipality is lost outright — 36,711 NAR addresses' worth.
`nar_take_unit_segments()` therefore requires a designator's *value* to look like a unit number
(a digit, or a lone letter) before accepting it. That requirement is confined to
`nar_lex_unit_ambiguous`, which is `STE` and nothing else, and **must not be widened to every
designator**: `APT BSMT` and `APT TRLR` are real units whose value is a word, and applying the
rule to `APT` collapses the whole run into the street name and drops the civic number with it.
Both directions are regression-tested.

**NAR keeps periods in municipality names; `nar_norm_text()` strips them from input.** `ST.
JOHN'S` (54,129 addresses), `SAULT STE. MARIE` (36,711) and `ST. ALBERT` (29,097) can therefore
never match a parsed fold key. `nar_gazetteer_sql()` folds periods out of *both* sides with
`replace(..., '.', '')` — on the `MunAlias` join, the `PostalMun` subquery, `mun_exact`, and the
two fuzzy street comparisons. It deliberately does **not** do so on the exact-branch
`Streets.NAME_FOLD` join, which would cost the `str_name_idx` index — so street-name periods stay
unhandled there by design.

### `R/geocode.R` — the forward query layer

`geocode()` parses with `normalize_address()` and then runs the tiers named in **`method`**,
**in the order given** — that order *is* the priority, since each tier is offered only the
rows its predecessors left without a position. `match_method` reports which one answered and
`uncertainty_m` what that method costs. On the 5,000 Corporations Canada addresses the eval
draws, the exact tier places 84.9% and interpolation lifts that to **89.1%**, in 0.9s for
the whole batch.

The vocabulary is `"nar"` (exact lookup), `"nar_interpolate"`, and `"bc"`, defaulting to
`c("nar", "nar_interpolate")` — the offline pair. **`method` replaced the earlier `source`,
`interpolate` and `fallback` arguments**, which were three ways of saying the same thing and
could not express an ordering. `nar_geocode_methods()` validates it; exact matches beat
prefixes in `pmatch()`, so `"nar"` is unambiguous against `"nar_interpolate"`.

**"Unplaced" is `is.na(out$x)`, not `match_method == "none"`.** That single definition is
what sends a `nar_no_geometry` row on to the next tier — NAR holds the record but no
coordinates, and withholding a position its neighbours can supply would be perverse — while
the `ADDR_GUID` the exact tier found survives whichever tier ends up placing it. The reverse
is a real cost and is documented: a tier that never runs reports nothing, so putting `"nar"`
last leaves interpolated rows with no `ADDR_GUID`.

| `match_method` | meaning | `uncertainty_m` |
| --- | --- | --- |
| `nar_building` | the civic number is in NAR with its own building point | 0 |
| `nar_blockface` | in NAR, but only a blockface centroid | 176 |
| `nar_interpolated` | not in NAR; placed between the flanking civics | `0.5 * span` |
| `nar_no_geometry` | in NAR (`ADDR_GUID` is set) but unplaceable | `NA` |
| `none` | not found | `NA` |

`uncertainty_m` is defined as the **90th-percentile error this package adds relative to
NAR's own building point**, and deliberately says nothing about NAR's own error, which is
neither published nor consistent — the User Guide admits a building point may be the
driveway. So `0` means "this package added nothing", not "this point is exact". The two
non-zero figures are measured: 176 m is the p90 building→blockface separation over the
1.85M addresses carrying both (p50 50, p95 332), and the interpolation figure comes from
the error/span ratio being **scale-invariant** — its p90 is 0.50 in every span bucket from
under 50 m to over 2 km (0.496–0.522), so half the flanking span is the p90 error whatever
the scale.

**Extrapolation is refused rather than flagged.** Past the last known civic on a side there
is no second point, and continuing the run's spacing scores a 15.1 m median but a 237 m p90
— barely better than the nearest neighbour it would displace. 7.3% of NAR civics sit at the
end of a run. Interpolation is same-parity only (4.2 m median against 35.2 m pooling both
sides, and 16.9 m for nearest-known-civic), and takes only `geom_source = 'building'` flanks,
since compounding a 176 m blockface error at each end would be presented as precision.

`prov`, `mun` and `within` are **authoritative** — they override whatever the address string
said, and the override lands on the returned row too, so a result never reports a province
next to a point constrained to a different one. `mun` goes through `MunAlias` rather than
straight at `MAIL_MUN_NAME`, because it is a name a person typed: constraining to `TORONTO`
by mailing city would drop everything NAR files under `SCARBOROUGH`. The parsed municipality
keeps the direct comparison, the gazetteer having already turned it into NAR's own string.
`within` densifies its outline **with the CRS temporarily unset** before reprojecting —
`st_segmentize()` on a geographic geometry needs `lwgeom`, which is not a dependency, and
planar interpolation is what is wanted anyway.

**The one performance trap, and it is a 99x one.** Both name families must be matched, and
writing that as `OFFICIAL = x OR MAIL = x` leaves the join with no equijoin key, so DuckDB
nested-loops the 17.4M-row table: the interpolation tier took **15.87s** that way against
**0.16s** as a `UNION` of two single-column equijoins, for byte-identical results. The exact
tier hid it, `CIVIC_NO = p.civic` having handed the planner a hash key of its own. That is
why `nar_geocode_candidates()` exists and why both tiers go through it. Otherwise no index
is needed: the folded street-key join costs 0.05s for a 5-row probe and 0.08s for a 200-row
one, so **batch into one call rather than looping**.

Street type and direction are compared through `upper()` on the NAR side. NAR stores the
`OFFICIAL_*` family Mixed Case and the `MAIL_*` family UPPERCASE, and the gazetteer hands
back the `OFFICIAL_*` spelling — without the fold, `24 Sussex Dr, Ottawa` matches nothing.

Coordinates are built in `sf` from the returned `x`/`y` rather than through `collect_nar()`,
because these are freshly computed values rather than a stored geometry column; the storage
CRS is still read from the database with `nar_crs()`, and `sf` handles the axis order that
`collect_nar()` needs `always_xy` for.

> Measurements, the tier ceiling and what is not built yet:
> **[`inst/notes/geocoding-status.md`](../inst/notes/geocoding-status.md)**.

### `R/geocode_bc.R` — the one external geocoder

A binding to the Province of British Columbia's [Address Geocoder]. `bc_geocode()` is the
client, `nar_geocode_tier_bc()` is the `"bc"` tier `method` can name, and `bc_validate()`
compares an existing result against BC's answer in metres. BC only: asked about an Ontario
address the service answers with whatever BC place shares the name, so the tier filters on
`PROV_ABVN == "BC"` before sending anything.

**The service always answers, so a response is not a match.**
`1234 Nonexistentzzz Rd, Victoria, BC` comes back as the centre of Victoria with a score of
48 — a point, not an error. Two independent floors decide: `nar_bc_precision()` maps
`matchPrecision` onto a `bc_*` method, and `min_score` (default 60) rejects what the service
itself scored badly. A rejected row keeps its `bc_score` and `bc_faults` and loses only its
`uncertainty_m`, so what was thrown away stays readable.

**The `bc_*` uncertainty figures are the only numbers in this package that were not
measured.** BC publishes `locationPositionalAccuracy` as the categorical
`high`/`medium`/`low`/`coarse` and no distance at all, so `nar_bc_precision()` translates its
precision vocabulary into deliberately pessimistic order-of-magnitude metres. Treat them as a
ranking safe to filter on, not as an error bar comparable to the NAR tiers'. Calibrating them
is named as the next step in the note.

**The tier rebuilds the query string from the components rather than forwarding `input`.** `prov`/`mun` are authoritative and overwrite the parsed columns, so forwarding the
original string would silently discard the caller's constraint the moment a row fell through.
`within` is enforced too, in R — the SQL predicate cannot reach a point that came from another
service, so a fallback point outside the bounds is discarded rather than returned.

**Throttling needs `capacity`, not `rate`.** `httr2::req_throttle(rate = 5)` builds a
`5 * 60 = 300`-token bucket and lets the first 300 requests go at once. `capacity = rate,
fill_time_s = 1` is the actual cap, with the realm named explicitly so a URL-derived realm
cannot give every address its own pool.

`httr2` is in `Suggests` and nothing reaches the network unless one of these functions is
called. The tests run entirely against responses captured from the live service into
`tests/testthat/fixtures/bc-*.json`, which is also the only way the parser stays checkable
once BC changes its scoring; `nar_bc_feature()` takes parsed JSON rather than a response
object precisely so that is possible.

[Address Geocoder]: https://geocoder.api.gov.bc.ca/

### `R/misc.R`

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
  see the CRS section above before touching either.
