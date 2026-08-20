# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cangeocode` is an R package (MIT, R >= 4.1) for geocoding and reverse geocoding Canadian
addresses. The current implementation is built entirely on Statistics Canada's **NAR**
(National Address Repository) bulk CSV releases, imported into a local **DuckDB** database with
the `spatial` extension. Road network files and online geocoders are named in `DESCRIPTION` as
future sources but are not implemented yet.

Public API (see `NAMESPACE`): `nar_connection()`, `available_nar_versions()`, `collect_nar()`,
`reverse_geocode()`.

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
