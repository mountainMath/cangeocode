# NAR: acquisition, schema, and partial imports

> Component note for `cangeocode`. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md);
> the geometry macros and CRS rules this schema is built around are in
> [`spatial.md`](spatial.md).

## `R/nar.R` — data acquisition and the DuckDB schema

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

`nar_schema_version()` is **6**. Version 2 added the `x`/`y` columns and fixed the lon/lat
datum; version 3 added the blockface fallback and `geom_source`; version 4 added the `Streets`
gazetteer; version 5 added `MunAlias` and `PostalMun`; version 6 added the `provinces` metadata
key. Older databases still work — version 1 without the prefilter and with `Locations` geometry
off by ~1.1 m, version 2 without a `geom_source` column and with no geometry on the 1.14M
blockface-only addresses, versions 3 and below with `normalize_address()` falling back to rules
only and `geocode(mun = )` erroring outright, and versions 5 and below reading as national —
which they are, since nothing before 6 could import a subset. Only a `refresh = TRUE` rebuild
picks these up. The gate is `nar_has_streets()`, which tests for the tables rather than reading
the version number.

Both spatial tables get an RTREE index on `geom` and a btree on `LOC_GUID`. Distances are in
**metres** because the storage CRS is projected, which is why `match_radius` needs no conversion.

DDL here must use `dbExecute()`, not `dbSendQuery()`: an uncleared result set keeps the
connection busy, the final `CHECKPOINT` never lands, and the leftover WAL makes the subsequent
**read-only** reopen fail outright.

## What `BG` and `BF` mean

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

## Partial imports: one province instead of the country

> `R/nar_provinces.R` (the vocabulary), `R/nar_zip.R` (the transport),
> `nar_connection(provinces = )` and the `nar_import_*` family in `R/nar.R` (the flow).

The StatCan bulk zip has its members **split by province** — `Addresses/Address_59.csv` is
British Columbia, and Ontario is split further into `_part_N` — and www150 honours HTTP range
requests. Together those two facts mean a caller who wants one province can read the archive's
own central directory for ~7 KB and then fetch only that province's members. Measured on
2026-06: PE 10 MB, NU 1, NT 2, YT 2, NL 23, SK 39, NB 43, MB 51, NS 55, AB 170, BC 192, QC 534,
ON 552, everything 1,666. A working PEI geocoder is 10 MB and ~40 s end to end, and it returns
the **same `ADDR_GUID` and the same coordinates** as the national database — these are the same
NAR rows, not a reduced product.

**Everything in `nar_zip.R` goes through a *reader*** — `function(from, len)` returning raw
bytes, carrying the resource size as an attribute. `nar_range_reader()` is the HTTP one and
`nar_file_reader()` serves a local file through the identical interface, which is what lets
`test-zip.R` exercise the zip parsing and reassembly against archives it builds itself with no
server. Same seam, same reason, as `nar_version_table()` taking an already-parsed document.

Three things in that file are load-bearing and non-obvious:

- **`nar_le()` accumulates into a double.** `readBin()`'s 4-byte integer is *signed*, so offsets
  past 2^31 come back negative — and a 1.7 GB archive is already close to that. ZIP64 is handled
  in both places it can appear (the tail locator and the per-entry extra field id `0x0001`)
  because the release is not far from outgrowing the 32-bit fields.
- **`nar_zip_copy_members()` rebuilds local headers from the central directory rather than
  copying them.** The CD is the authoritative record of the sizes: an archive with the streaming
  flag (bit 3) set writes zeros in the local header and trails the real sizes after the data.
  Rebuilding also lets that flag be cleared, so the output needs no data descriptors. The local
  header's own name/extra lengths still have to be read to find where the data starts — they
  need not match the CD's.
- **Compressed bytes are copied verbatim, never inflated.** The result is an ordinary zip, so
  `utils::unzip()` and everything downstream of it is byte-for-byte the same code path whether
  one province or the country was downloaded.

`nar_release_directory()` **memoizes the parsed index per session, keyed by URL**. Reading it
off the real archive takes ~34 s — a single small range read, but the server is slow to first
byte — and the interactive prompt and the download that follows would otherwise each pay it. A
release at a given URL is immutable, so there is nothing to invalidate.

**Coverage is recorded, not inferred.** `nar_metadata`'s `provinces` key holds either `"ALL"` or
a comma-separated list, and `nar_coverage()` falls back to `"ALL"` for databases without the key
— correctly, since nothing before schema 6 could import a subset. `nar_import_plan()` is the
whole decision and is a pure function of (does the file exist, what does it cover, what was
asked, was `refresh` passed), which is why it is tested directly:

| cached | asked | result |
| --- | --- | --- |
| national | anything | nothing to fetch |
| `BC` | `BC` | nothing to fetch |
| `BC` | `BC`, `AB` | fetch `AB`, **append** |
| `BC` | all | fetch all, **rebuild** |
| `BC` | nothing, `refresh = TRUE` | fetch `BC` — refreshing must not widen the database |
| nothing | nothing | `NULL`, meaning *ask* |

That `NULL` is not the empty vector, and `nar_connection()` tests for it explicitly
(`is.null(plan$fetch) || length(plan$fetch)`) — `length(NULL)` is 0, so the obvious check would
skip the prompt entirely.

**The create path publishes by rename; the append path cannot.** A fresh import builds into
`<path>.duckdb.building` and renames on success, so a failed run leaves nothing later calls
would mistake for a finished database. An append writes into the live file, so instead the
*coverage metadata is written last* — after the data and the derived tables. A crash mid-append
therefore leaves a database that **under**-reports what it holds, costing one redundant
download, rather than one that over-reports and silently answers nothing for a province.

`nar_set_coverage()` computes `nar_coverage_value(provinces)` **before** its own `DELETE`. The
caller passes `union(nar_coverage(con), plan$fetch)`, and R's lazy evaluation would otherwise
force that promise against a table the function had already emptied — which read as `"ALL"` and
recorded coverage of `"AB,ALL"`.

`nar_build_derived()` **drops and recreates** `Streets`/`MunAlias`/`PostalMun` on every import,
append included. They are aggregates over the whole `Addresses` table, so a street that gained
addresses needs its counts and civic range recomputed, not extended.

`nar_import_tables()` holds the geometry decisions **once**, for both paths. An appended
province whose `x`/`y` disagreed with its `geom` would break the zonemap prefilter for those
rows alone, which is close to undebuggable. The append itself is
`INSERT INTO <table> BY NAME (<rendered lazy query>)` — one pass, and matching by name rather
than position so a column-order difference is a no-op instead of a silent transposition.

`geocode()` answers **`not_covered`**, via `nar_geocode_mark_uncovered()`, for a row whose
parsed province is outside the coverage. Only rows where the province was actually parsed *and*
is demonstrably outside are marked: an unparsed province stays `none`, because nothing has been
established about it, and a national database marks nothing at all. Reporting an Ottawa address
as `none` against a PEI database would say the address is wrong; this says the database was
never asked to know.

**`nar_zip_member_province()` returns `NA` for anything that is not a per-province CSV** — the
user guides, the readme, the directory entries — and those members are carried along in every
subset regardless. This is also why `local_nar_fixture()` (which writes `Address_BC.csv`, not
the SGC-coded name) keeps working unchanged: an unplaceable member is shared, so it always
loads. The province tests need `nar_province_fixture()`, which names its files the way StatCan
does.

## Version discovery and offline use

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


## `R/nar_session.R` — the parked connection

`geocode()` and `reverse_geocode()` used to open a connection when `con` was `NULL` and close it
again on the way out, which charged every call ~0.5 s for the file open, the `LOAD spatial` and the
TEMP macro definitions. They now call `nar_session_use()`, which **parks the connection in
`.nar_session` and never closes it**. `open_nar()` / `close_nar()` are the explicit ends of the same
mechanism; neither is required, and `nar_connection()` still hands out a connection the caller owns.

Four things are load-bearing:

- **Validity is asked, not remembered.** `nar_session_state()` calls `DBI::dbIsValid()` on every
  read and clears the slot when it fails. A caller can `dbDisconnect()` the object `open_nar()`
  returned, and the duckdb driver can be finalized, neither of which this package hears about. A
  dead handle must be indistinguishable from no handle, or the next call errors instead of
  reopening.
- **The stored version key is read back out of `nar_metadata`, not taken from the request.** A
  connection opened as `"latest"` is stored as `2026-06`, so a later call naming that release
  explicitly matches it. Without this, `version = "2026-06"` would re-resolve and reopen a database
  that is already open.
- **`"latest"` matches whatever is parked, deliberately.** The point of parking is to stop asking
  StatCan what "latest" means; a release published mid-session must not swap the database out from
  under a running script. Moving releases requires naming one or calling `close_nar()` — this is a
  documented behaviour, not an optimization detail, and `nar_session_matches()` is where it lives.
- **Every write path must call `nar_session_release()` first.** The parked connection is read-only,
  which coexists with other readers and blocks writers — including the package's own imports. It is
  called in `nar_connection()` before `nar_import_release()`, and in `rqa_import()` and
  `rnf_import()` immediately before each opens the file read-write. A new import path that forgets
  it will deadlock against a connection the same session opened implicitly, which is a failure that
  only appears after a `geocode()` call and so does not show up in a fresh session.

Package state is not test state: `local_nar_env()` calls `close_nar()` on entry and defers another,
or a connection parked against a `withr` temp database outlives the directory it points at.

`normalize_address()` and `address_key()` are deliberately **not** session-aware. Their `con` is not
a cost knob — it is what switches the street gazetteer on — so picking up a parked connection would
make the parse depend on whether something else had geocoded earlier in the session.
