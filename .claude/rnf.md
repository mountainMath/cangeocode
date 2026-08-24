# The road network file

> Component note for `cangeocode`, covering `R/rnf.R` — the `rnf_import()` path and the
> `"rnf"` tier. Repo-wide guidance is in [`CLAUDE.md`](CLAUDE.md); the tier runs inside the
> machinery in [`geocoding.md`](geocoding.md), joins on folds defined in
> [`normalization.md`](normalization.md), and stores geometry under the rules in
> [`spatial.md`](spatial.md). Every number quoted here was measured by
> `data-raw/probe_rnf.R` and is written up in
> [`../inst/notes/road-network-file.md`](../inst/notes/road-network-file.md) — read that
> before changing the tier's thresholds.

Statistics Canada's Road Network File (product 92-500-X) carries an address range on each
side of each street segment. Interpolating along the segment places an address the rest of
the package cannot. This file is the import and the tier, and — as with
[`rqa.md`](rqa.md) — they are one decision: the tier exists because the tables are optional
and additive, so a database without them still works and `geocode()` says so rather than
guessing.

## The fact the file does not carry

**There is no provenance flag on the ranges.** A range that was observed and a range that
was imputed are the same bytes, and no other field can be pressed into service —
`CLASS` and `RANK` describe the road, not the range. Everything the tier claims about its
own accuracy therefore rests on measurement against NAR rather than on anything the file
says, and the measurement is the note, not the code. If you are tempted to loosen a
threshold here, the number you need is in `road-network-file.md` or does not exist yet.

The headline is **89.7%**: that share of NAR civic numbers falls inside the range the RNF
claims for the side the building actually sits on. 10.3% is the honest floor on the tier's
error rate.

## Take the shapefile

`lrnf000r<YY><t>_e.zip`, and `<t>` must be `a`. Only the shapefile is published for every
release — releases 20, 22, 23, 24 and 25 all serve it; the GeoPackage resolves for 25
alone, so an importer that reaches for the nicer single-file container works this year and
404s on the archive. The GeoPackage is also where the **13 CircularStrings** live that
DuckDB's spatial extension refuses outright, failing the whole read; the shapefile format
cannot express one, so those 13 arrive as ordinary `LINESTRING`s and `ST_Read` returns all
2,251,726 features with no workaround. Two reasons, same answer.

`rnf_latest_release()` HEAD-probes backwards from the current year rather than assuming a
release exists. The zip is ~340 MB, so `options(rnf_shp = ...)` points at an already
extracted shapefile the way `nar_exdir` does for NAR — use it when testing import changes.

## What is stored, and what is not

Only segments that have **a name and a range on at least one side** — 62.4% of the file
(13% of segments are unnamed; of the rest, 71.7% carry a range). What was dropped is recorded as counts in `nar_metadata` rather than being
silently absent. The rest of the file is ramps, service roads and unnamed rural
allowances, none of which can place an address.

The RNF is already EPSG:3347, which is `nar_storage_crs()`, so nothing is reprojected on
the way in unless the storage CRS has been changed. Its `.prj` carries the parameters but
no EPSG code, so `ST_Read` returns untagged geometry — which is what `nar_store()` wants
anyway, since DuckDB refuses an RTREE index over a `GEOMETRY('<crs>')` column.

**`N/A` is a literal string** in `TYPE` and `DIR`, sitting alongside real nulls. Both are
folded to `NULL` on the way in; reading the literal as a value would make every such
segment unmatchable.

## The joins, and why each one is there

The tier matches a parsed probe to a segment on **`MATCH_FOLD`, not `NAME_FOLD`** — the
same fold `normalization.md` describes, spelling `ST` out to `SAINT` and turning the hyphen
into a word boundary. This is not a nicety: the addresses the tier exists for are the ones
NAR could not place, which correlates with the ones the gazetteer could not resolve, which
is exactly where the plain fold fails.

`TYPE` and `DIR` are compared under `upper()` **without `strip_accents`**, because RNF's
vocabularies are the same canonical sets NAR uses and NAR's canonicals keep their accents
(`MONTÉE`, `ALLÉE`), and `nar_geocode_probe()` does not fold the type either. Both are
constrained **only when both sides have a value** — an absent type or direction constrains
nothing, on either side. Reading RNF's `NULL` as a contradiction would refuse the street
rather than accept that the file did not say.

The municipality needs **both routes and neither is redundant**:

* `MunAlias`, because a caller writes a mailing city and only `MunAlias` knows it is a CSD;
* a direct comparison of folded CSD names, because **8.3% of RNF's ranged street/CSD pairs
  are absent from NAR entirely** — and those are precisely the streets the tier is for.

RNF spells its municipality key `PRUID_L:CSDTYPE_L:CSDNAME_L`, which is joinable with NAR's
`MUN_KEY` under `strip_accents(upper(...))`. This is why the test fixture's `PROV_CODE` is
`"59"` and not `"BC"`: the key is built from it, and a fixture spelling the province as
letters makes the two unjoinable exactly where the join has to be tested.

Bounds cannot use `nar_geocode_bounds_sql()` — that constrains `a.x`/`a.y`, which
`RnfSegments` does not have. The tier takes `bounds_geom` and emits its own two-clause
predicate: `st_intersects` against the segment as an index-usable prefilter, then
`st_within` on the placed point.

## Refuse when ambiguous, not when the parity disagrees

Two rules, and they pull in opposite directions on purpose.

**`n_matches > 1` refuses.** A street name that matches more than one segment in the
municipality has no defensible placement: ambiguous rows land p90 **1,678 m** out with
11.7% over a kilometre, against p90 107.8 m and 0.1% for unambiguous ones. Without this rule the tier
ships a 20 km error in a package whose worst honest tier is 176 m, and the cost of the rule
is 9 rows in 5,000. The recovered rows being measurably worse than the shared ones is the
overlap-vs-residual correction biting again — see `quebec-addresses.md` for where that
lesson was learned — and the cause is *ambiguity*, not imputation, which is why this
particular filter fixes it.

**A parity mismatch does not refuse.** Parity chooses *between* the two sides; it never
vetoes one. An even number inside an odd range on the only side that carries a range is
still placed there. A mismatch is not evidence that the range is wrong: the segment may be
a single generalized centreline where the ground has two carriageways, the address point
may be a parcel centroid or a rooftop off the frontage, or the civic number may simply be
misfiled. Refusing would drop a real address to avoid an error the width of a street.
Containment is different and *is* filtered on — a number outside every range on the segment
is not off by a street width, it is a claim the segment never made.

## Placement and uncertainty

`frac = (civic - from)/(to - from)` clamped to [0,1], then a 5% setback at each end
(`0.05 + 0.90*frac`), then a 13 m perpendicular offset to the matched side. The side is
derived from the **local** direction of travel — `ST_LineSubstring` at f±0.02 and the sign
of the 2-D cross product — not from the segment's endpoints, because a curved block puts
houses on the wrong side of a chord drawn end to end. DuckDB's spatial extension has no
`ST_OffsetCurve`, so this is done by hand, and the cross-product term is spelled `byv`
because `by` is a DuckDB reserved word.

The setback is not decoration: it moves p50 from 34.5 m (plain) to 32.1 m, against 49.3 m
for placing every address at the segment midpoint.

**`uncertainty_m = max(95, 0.35 × len_m)`**, following the package convention that
`uncertainty_m` is a 90th-percentile error rather than a mean. The floor exists because a
short segment does not make the placement good — the 21.1 m median offset from centreline
to building is a floor nothing about the range can beat.

The tier sits **below `nar_interpolate`** in `method` order. It is a fallback for streets
NAR does not carry, not a competitor for streets it does.

## What it is worth

On the same 5,000-filing Corporations Canada draw the rest of the package is measured
against, the tier places **25.3%** of the addresses `geocode()` currently fails — the
largest recovery any tier has offered here.
