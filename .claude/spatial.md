# The spatial layer

> Component note for `cangeocode`. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md);
> the data this layer stores and queries is described in [`nar-database.md`](nar-database.md).

## `R/geo_helpers.R` — where all the spatial SQL lives (start here)

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

## CRS handling

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

## `collect_nar()`

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

## `R/reverse_geocode.R` — the query layer

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

