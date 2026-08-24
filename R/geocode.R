#' The positional error a blockface point carries
#'
#' @description NAR's blockface representative point is the centroid of one side
#' of a street between two intersections, so an address that has no building
#' point is placed at a segment centroid shared with every other address on that
#' segment. Measured over 1.85M addresses that carry *both* points, the
#' building-to-blockface separation has a median of 50 m and a 90th percentile
#' of **176 m**, which is the constant used here.
#'
#' It is a lower bound for the addresses that actually need it. Only the 1.14M
#' blockface-only addresses ever fall back to this point, and they skew rural,
#' where blockfaces are longer than the national mix this was measured over.
#' @return A single number, metres
#' @keywords internal
nar_blockface_uncertainty_m <- function() 176

#' Geocode Canadian addresses to coordinates
#'
#' @description Parses each address with [normalize_address()] and resolves the
#' result, returning one row per input in input order. `method` names the tiers
#' to try and the order to try them in; the column `match_method` records which
#' one answered:
#'
#' * **`nar_building`** -- the civic number is in NAR and carries its own
#'   building representative point. This is the exact match.
#' * **`nar_blockface`** -- the civic number is in NAR but has only a blockface
#'   point, the centroid of one side of a street between two intersections.
#' * **`nar_no_geometry`** -- the civic number is in NAR and `ADDR_GUID` names
#'   the record, but NAR holds no coordinates for it and none could be
#'   interpolated. 65k addresses are in this state. It is a different answer
#'   from `none`, which means the address was not found at all.
#' * **`nar_interpolated`** -- the civic number is *not* in NAR, so the position
#'   is interpolated between the nearest known civic numbers of the same parity
#'   on either side of it. See the section below.
#' * **`rqa_building`**, **`rqa_geocoded`**, **`rqa_uncertain`**, **`rqa_lot`**,
#'   **`rqa_other`** -- answered by the `rqa` tier, which carries Quebec's own
#'   positional-quality class rather than one label. Only `rqa_building` is a
#'   building placement, and it is the only one that reports an
#'   `uncertainty_m`. See [rqa_import()].
#' * **`rnf_interpolated`** -- answered by the `rnf` tier: the civic number
#'   fell inside the address range Statistics Canada's road network file gives
#'   for one side of one street segment, and the address is placed along that
#'   segment. See [rnf_import()].
#' * **`rnf_ambiguous`** -- the `rnf` tier found the civic number in the ranges
#'   of **several** segments and refused to choose between them. No coordinates,
#'   and `n_matches` says how many were in contention. It is reported rather
#'   than silently dropped because the whole of that tier's gross-error tail is
#'   these rows; see the tier description below.
#' * **`bc_site`**, **`bc_civic`**, **`bc_block`**, **`bc_street`**,
#'   **`bc_locality`** -- answered by the `bc` tier. See [bc_geocode()].
#' * **`nrcan`** -- answered by the `nrcan` tier. One value rather than several,
#'   because only one class of geolocator answer survives its floors at all.
#'   See [nrcan_geocode()].
#' * **`qc_address`** -- answered by the `qc` tier. Also one value: the
#'   service's other locator resolves a street rather than an address, and the
#'   tier does not place a row from it. See [qc_geocode()].
#' * **`not_covered`** -- the address parsed to a province this database does
#'   not hold, so no tier could have matched it. Only a partial import (see the
#'   `provinces` argument of [nar_connection()]) ever produces this, and it is
#'   deliberately distinct from `none`: the address may be perfectly good.
#' * **`none`** -- nothing resolved.
#'
#' @section Choosing the tiers: `method` is a vector of tier names in priority
#' order. Each tier is offered only the rows its predecessors left without a
#' position, so the order is what decides which answer wins:
#'
#' * **`"nar"`** -- look the civic number up in NAR directly. Answers
#'   `nar_building`, `nar_blockface` or `nar_no_geometry`.
#' * **`"rqa"`** -- look the civic number up in the **Repertoire quebecois des
#'   adresses**, Quebec's own register, which has to be imported once with
#'   [rqa_import()] and lives beside `Addresses` rather than in it. Quebec only,
#'   offline, and it holds roughly 308,000 civic addresses NAR does not. It
#'   belongs **after `"nar"` and before `"nar_interpolate"`**: a register point
#'   beats an interpolated one, and NAR's own building point beats both. Placed
#'   there it is worth about a third of Quebec's unplaceable tail outright and
#'   replaces an interpolated guess -- median 23 m from RQA's own coordinate,
#'   with 7% of them more than 500 m out -- for most of the rest.
#' * **`"nar_interpolate"`** -- place a civic number NAR does not carry between
#'   its known neighbours. Answers `nar_interpolated`.
#' * **`"rnf"`** -- interpolate along Statistics Canada's **Road Network
#'   File**, which has to be imported once with [rnf_import()] and lives beside
#'   `Addresses` rather than in it. Offline, national, and it answers for
#'   streets NAR does not carry at all -- which is what separates it from
#'   `"nar_interpolate"`, whose flanking civics have to come from NAR itself. It
#'   belongs **after `"nar_interpolate"`**: where both can answer, NAR's own
#'   neighbours are about six times more accurate. On a 5,000-address sample of
#'   business filings it placed a quarter of what the offline pair left
#'   unplaced, the largest recovery any tier here has offered, and **it refuses
#'   whenever more than one segment matches**, which is where its accuracy comes
#'   from rather than a nicety.
#' * **`"bc"`** -- ask the Province of BC's [Address Geocoder][bc_geocode()].
#'   British Columbia only, and **this makes one network request per unplaced BC
#'   row**; nothing contacts it unless the tier is named. The constraints are
#'   honoured: what is sent is rebuilt from the components after any
#'   `prov`/`mun` override, and a point outside `within` is discarded rather
#'   than returned.
#' * **`"qc"`** -- ask the Quebec government's [geocoder][qc_geocode()].
#'   Answers `qc_address`. Quebec only, and the one online tier that does
#'   **not** cost a request per row: it batches 1000 addresses per request, so
#'   naming it is cheap even on a large unplaced tail. It is also the only one
#'   that refuses -- it returns no point rather than a locality centroid when
#'   it cannot match -- so its answers need less rejecting than the others'.
#' * **`"nrcan"`** -- ask NRCan's national [geolocator][nrcan_geocode()].
#'   Answers `nrcan`. One network request per unplaced row, and it covers
#'   the whole country, so unlike `"bc"` and `"qc"` there is no province that
#'   excludes a row from being sent. **It belongs last.** Its surviving answers
#'   are roughly interpolation-grade at the median with a much longer tail, and
#'   it has no score of its own -- everything that separates a hit from a
#'   confident wrong answer is done by re-parsing the returned title, which is
#'   strict but not free of false positives.
#'
#' The default `c("nar", "nar_interpolate")` is offline and prefers a real NAR
#' record over an interpolated one. It does **not** include `"rqa"`, which would
#' otherwise appear and disappear depending on whether [rqa_import()] had been
#' run; in Quebec, `c("nar", "rqa", "nar_interpolate")` is the recommended
#' offline set once it has. `method = "nar"` keeps only the addresses
#' NAR actually carries. `c("nar", "nar_interpolate", "bc")` adds the BC
#' service as a last resort, and `c("bc", "nar")` prefers it over NAR wherever
#' it answers. `"qc"` is the same shape as `"bc"` for the other province that
#' publishes its own geocoder. `"nrcan"` is the national counterpart to both
#' and is the only tier that answers with no local database at all, which is
#' the case it exists for; it should be named after every other tier, never
#' before one.
#'
#' A row NAR holds without coordinates (`nar_no_geometry`) is passed on to the
#' next tier: knowing the address exists is worth reporting, but it is not worth
#' withholding a position a later tier can supply, and the `ADDR_GUID` found
#' survives whichever tier ends up placing the row. Note that the reverse costs
#' something -- a tier that never runs for a row reports nothing about it, so
#' putting `"nar"` last means interpolated rows carry no `ADDR_GUID`.
#'
#' @section Interpolation: Only civics of the **same parity** are used, because
#' odd and even numbers sit on opposite sides of the street and pooling them is markedly worse: measured
#' by leave-one-out over all 10.6M distinct NAR civic points, same-side
#' interpolation has a median error of 4.2 m against 35.2 m for both sides
#' pooled, and beats simply taking the nearest known civic (16.9 m).
#'
#' **Extrapolation is refused.** A civic number past the last known one on its
#' side has no second point to interpolate against, and guessing from the run's
#' spacing is close to worthless -- median error 15.1 m but a 90th percentile of
#' 237 m, barely better than the nearest neighbour it would displace. Those rows
#' fall through to the next tier rather than carrying a number that looks like
#' the others. 7.3% of NAR civics sit at the end of a run.
#'
#' @section Constraining the search: `prov`, `mun` and `within` are assertions
#' about where the address is, not hints. Each overrides whatever the string
#' itself claimed -- a row geocoded with `prov = "BC"` comes back with
#' `PROV_ABVN` reading `BC` no matter what was written -- and they compose, so
#' `prov` plus `mun` is the province-and-postal-city case and either can be
#' combined with a polygon.
#'
#' They earn their keep twice over. They resolve the ambiguity that `n_matches`
#' otherwise only reports, since a bare `100 Main St` means something definite
#' once the municipality is fixed. And `within` is close to free: the bounding
#' box is compared against the stored `x`/`y` columns, which DuckDB prunes with
#' per-row-group zonemaps rather than scanning -- the same mechanism that makes
#' [reverse_geocode()] fast.
#'
#' @section Uncertainty: `uncertainty_m` estimates the **90th-percentile
#' positional error this package's method introduces, relative to NAR's own
#' building point.** It is 0 for `nar_building`, 176 for `nar_blockface`, and
#' half the distance between the two flanking civics for `nar_interpolated`.
#'
#' For `rnf_interpolated` it is `max(95, 0.35 * len_m)` in the segment's length,
#' which is two-part because the error is: a short block is dominated by the
#' setback and the side offset, which do not shrink with it, and a long one by
#' how far along the block the range put the house, which does scale.
#'
#' That last figure is measured, and it holds across scales: the ratio of error
#' to flanking span has a 90th percentile of 0.50 in every span bucket from
#' under 50 m to over 2 km (0.496--0.522). So a 40 m gap between neighbours
#' gives 20 m and a 3 km gap gives 1.5 km, and filtering on `uncertainty_m` is
#' the way to drop the interpolations that are too coarse to use.
#'
#' **NAR's own error is not included and is not estimated.** The User Guide
#' warns that a building point "may not correspond exactly to the physical
#' center of the building structure itself" -- it can be the road access point
#' or the driveway -- and that offset is neither published nor consistent, so
#' `uncertainty_m = 0` means "this package added nothing", not "this point is
#' exact".
#'
#' `n_matches` counts the distinct NAR points that satisfied the query. Anything
#' above 1 means the address was ambiguous -- most often a street name the input
#' did not pin to a municipality -- and `uncertainty_m` is then widened to the
#' distance from the point returned to the furthest rejected candidate.
#'
#' @section How many matched: `n_matches` and `n_records` count two different
#' things and the gap between them is the point of having both.
#'
#' `n_matches` counts distinct **points**. It is the ambiguity measure: it is
#' what widens `uncertainty_m`, and it is what tells you the answer may be in
#' the wrong place. `n_records` counts distinct **NAR addresses**, which is
#' usually the larger number, and it tells you the answer may be in the right
#' place but stand for more than one thing.
#'
#' They come apart because NAR files every unit of a multi-unit building as its
#' own address, all at the building's one coordinate. `49321 Range Road 72` in
#' Brazeau County, Alberta returns `n_matches = 1` and `n_records = 19`: there
#' is exactly one place to put it and nineteen addresses there, units 1 through
#' 29, and `geocode()` parses `APT_NO_LABEL` but does not match on it. This is
#' not a corner case -- **47% of the addresses NAR places share their
#' coordinate with at least one other address.**
#'
#' A record count above 1 is therefore not a warning by itself. It is a warning
#' when the collapsed records disagree about something you care about, and the
#' one such disagreement reported today is the postal code: `match_postal_code`
#' goes `NA` rather than pick one. The Brazeau County address is `NA` for that
#' reason -- its nineteen units carry four postal codes between them.
#'
#' `n_records` is 0 wherever no record was matched: every interpolated row that
#' did not first hit the `nar` or `rqa` tier, and every online tier.
#'
#' @section Two postal codes: the result carries two postal-code columns and
#' they answer different questions. `POSTAL_CODE` comes from
#' [normalize_address()] and is **what the input string said** -- `NA` when it
#' said nothing, which is the usual case for an address typed without one.
#' `match_postal_code` is **what the matched record carries**, and it is filled
#' in from the source rather than from the input.
#'
#' Only the tiers that match a record can fill it: the `nar` tier
#' (`nar_building`, `nar_blockface` and `nar_no_geometry` alike -- an address
#' NAR holds without coordinates still has a postal code) and the `rqa` tier.
#' It then **survives whichever tier ends up placing the row**, exactly as
#' `ADDR_GUID` does, so a `nar_interpolated` row carries a postal code when the
#' exact tier found the record first and NAR simply had no coordinates for it.
#' A row interpolated without such a hit, an `rnf_interpolated` row and every
#' online answer leave it `NA`: none of them resolve to a record with a postal
#' code of its own, an interpolated point sits between two addresses that may
#' not share one, and guessing which flank to copy would produce a value
#' indistinguishable from a looked-up one.
#'
#' It is also `NA` when the candidates disagree. NAR holds one row per address,
#' so a civic number with units contributes many rows, and 1.4% of civic numbers
#' -- 4.2% of addresses, since the buildings this happens to are large -- span
#' more than one postal code. The tier does not match on unit, so where those
#' rows disagree there is nothing in the query that says which of them was
#' meant, and reporting one of them would be a coin flip. `100 Queen St W,
#' Toronto` is one: NAR carries it as `M5H2N1` and `M5H2N2` both. A postal code
#' in the *input* does not break the tie either, since it is what the address
#' claims rather than something the query established.
#'
#' @param x A character vector of address strings, or a data frame of already
#' parsed components as returned by [normalize_address()]. Passing the data
#' frame lets you parse once and geocode repeatedly, or edit a parse before
#' resolving it.
#' @param prov Province code(s) to constrain the search to, length 1 or
#' `length(x)`. **Authoritative**: it overrides whatever the address string
#' said, and is also passed to [normalize_address()], where knowing the province
#' additionally disambiguates the parse.
#' @param mun Municipality name(s) to constrain the search to, length 1 or
#' `length(x)`. **Authoritative**, overriding the string. Resolved through NAR's
#' alias set rather than matched against the mailing city, so `"Toronto"`
#' reaches the addresses NAR files under `SCARBOROUGH`, and a name that denotes
#' several jurisdictions means all of them. Combine with `prov` when a name is
#' used in more than one province.
#' @param within A spatial restriction: an `sf`/`sfc` object, an `st_bbox`, or a
#' length-4 numeric `c(xmin, ymin, xmax, ymax)`, interpreted in `crs` unless it
#' carries its own. **Authoritative**, and applied to every tier.
#' @param method Tiers to try, in priority order: any of `"nar"`, `"rqa"`,
#' `"nar_interpolate"`, `"rnf"`, `"bc"`, `"nrcan"` and `"qc"`. Default
#' `c("nar", "nar_interpolate")`, which is the offline pair. See the section
#' below.
#' @param geometry Whether to return an `sf` object with POINT geometry.
#' Unmatched rows get an empty point. Default `FALSE`, which returns `lon` and
#' `lat` columns instead.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param version NAR version to query, passed to [nar_connection()]. Ignored
#' when `con` is supplied.
#' @param con An open NAR connection to reuse. The caller keeps ownership: a
#' connection passed in here is left open, while one opened internally is closed
#' again before returning.
#' @param ... Passed to the online tiers named in `method`. `rate` is
#' understood by all of them; `api_key` is [bc_geocode()]'s, as is anything
#' else it does not recognize, which it forwards to its own service as a query
#' parameter. [nrcan_geocode()] and [qc_geocode()] are each given only the
#' arguments they declare, so a BC-only argument passed alongside `"nrcan"`
#' reaches the BC tier alone rather than erroring. Note that `min_score` is
#' understood by [bc_geocode()] and [qc_geocode()] both, and means different
#' things to them -- see [qc_geocode()] on why its score is not a ranking.
#' Unused when `method` names no online tier.
#' @return A data frame with one row per input, carrying every column
#' [normalize_address()] returns plus `ADDR_GUID`, `match_method`,
#' `uncertainty_m`, `n_matches`, `n_records`, `match_postal_code`, and either
#' `lon`/`lat` or an `sf` geometry column. `POSTAL_CODE` is the *parsed input* -- what the
#' address string itself said, or `NA` when it said nothing --  while
#' `match_postal_code` is what the matched record carries; see the section
#' below.
#' @export
#' @examples
#' \dontrun{
#' geocode("1055 W Georgia St, Vancouver BC")
#'
#' # Only addresses NAR actually carries -- nothing interpolated.
#' geocode(addresses, method = "nar")
#'
#' # Add the BC service as a last resort. Makes network requests.
#' geocode(addresses, method = c("nar", "nar_interpolate", "bc"))
#'
#' # Quebec's own register, offline, after one rqa_import().
#' geocode(addresses, method = c("nar", "rqa", "nar_interpolate"))
#'
#' # The road network file reaches streets NAR does not carry, after one
#' # rnf_import().
#' geocode(addresses, method = c("nar", "nar_interpolate", "rnf"))
#'
#' # NRCan's geolocator is national, so it can back up the whole country.
#' geocode(addresses, method = c("nar", "nar_interpolate", "nrcan"))
#'
#' # Parse once, resolve many times, and keep only the precise matches.
#' parsed <- normalize_address(addresses)
#' g <- geocode(parsed, geometry = TRUE)
#' g[g$uncertainty_m <= 25, ]
#' }
geocode <- function(x, prov = NULL, mun = NULL, within = NULL,
                    method = c("nar", "nar_interpolate"), geometry = FALSE,
                    crs = 4326, version = "latest", con = NULL, ...) {
  method <- nar_geocode_methods(method)

  # Not closed on the way out: an unsupplied `con` resolves to the session's
  # connection, which the next call reuses. close_nar() is what ends it.
  if (is.null(con)) con <- nar_session_use(version)
  # Checked before any parsing, not when the tier is first reached: whether a
  # tier runs at all depends on what its predecessors left unplaced, so a
  # missing import would otherwise surface on one batch and not the next.
  if ("rqa" %in% method && !nar_has_rqa(con)) {
    stop("The \"rqa\" tier needs the Repertoire quebecois des adresses, which ",
         "this database does not carry. Run rqa_import() first.", call. = FALSE)
  }
  if ("rnf" %in% method && !nar_has_rnf(con)) {
    stop("The \"rnf\" tier needs Statistics Canada's road network file, which ",
         "this database does not carry. Run rnf_import() first.", call. = FALSE)
  }
  if (!is.null(mun) && !nar_has_streets(con)) {
    stop("`mun` resolves through the MunAlias table, which arrived in schema ",
         "version 5. Rebuild with nar_connection(refresh = TRUE), or constrain ",
         "with `within` instead.", call. = FALSE)
  }

  res <- if (is.data.frame(x)) {
    need <- c("CIVIC_NO", "STREET_NAME", "MUN_NAME", "PROV_ABVN")
    missing <- setdiff(need, names(x))
    if (length(missing)) {
      stop("`x` is a data frame but has no ", paste(missing, collapse = ", "),
           " column. Pass address strings, or the output of normalize_address().",
           call. = FALSE)
    }
    x
  } else {
    normalize_address(x, prov = prov, con = con)
  }

  # Authoritative, so the override lands on `res` rather than only on the probe:
  # the caller asserted these, and a result that reported the string's own
  # province next to a point constrained to a different one would be a lie about
  # what was searched. `prov` still reaches normalize_address() as well, where it
  # additionally disambiguates the parse -- ROUTE is New Brunswick's typeless
  # numbered road and Quebec's street type, and only the province separates them.
  if (!is.null(prov)) res$PROV_ABVN <- nar_recycle(prov, nrow(res), "prov")
  if (!is.null(mun))  res$MUN_NAME  <- nar_recycle(mun,  nrow(res), "mun")

  bounds <- nar_geocode_bounds_geom(within, crs, con)
  hits <- nar_geocode_match(res, con, method = method,
                            bounds = nar_geocode_bounds_sql(bounds),
                            bounds_geom = bounds, auth_mun = !is.null(mun), ...)
  out <- cbind(res, hits[, c("ADDR_GUID", "match_method", "uncertainty_m",
                             "n_matches", "n_records",
                             "match_postal_code")])

  nar_geocode_geometry(out, hits$x, hits$y, con, crs = crs, geometry = geometry)
}

#' Attach coordinates to a geocoding result
#'
#' @description Storage-CRS coordinates in, either `lon`/`lat` columns or an
#' `sf` object out. The reprojection is done in `sf` rather than in DuckDB
#' because these are freshly computed coordinates rather than a stored geometry
#' column, and because it leaves the axis-order handling to `sf`, which always
#' means lon/lat -- the `always_xy` trap that [collect_nar()] has to work around
#' does not arise. The storage CRS is still read from the database rather than
#' assumed.
#' @param out The result data frame
#' @param x,y Coordinates in the storage CRS, `NA` where nothing matched
#' @param con A NAR connection
#' @param crs Target CRS
#' @param geometry Whether to return an `sf` object
#' @return `out` with coordinates attached
#' @keywords internal
nar_geocode_geometry <- function(out, x, y, con, crs = 4326, geometry = FALSE) {
  storage <- nar_crs(con)
  ok <- !is.na(x) & !is.na(y)

  pts <- sf::st_sfc(rep(list(sf::st_point()), length(x)), crs = storage)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i) sf::st_point(c(x[i], y[i]))),
                          crs = storage)
  }
  if (!is.null(crs)) pts <- sf::st_transform(pts, crs)

  if (geometry) return(sf::st_sf(out, geometry = pts))

  # Coordinates come off the matched subset, not the whole column: an empty
  # point contributes no row to st_coordinates() rather than a row of NAs, so
  # binding the full matrix on would silently shift every value up.
  # `rep(NA_real_, nrow(out))` and not `NA_real_`: assigning a length-one value
  # into a zero-row data frame is an error rather than a no-op.
  out$lon <- rep(NA_real_, nrow(out))
  out$lat <- rep(NA_real_, nrow(out))
  if (any(ok)) {
    co <- sf::st_coordinates(pts[ok])
    out$lon[ok] <- co[, 1]
    out$lat[ok] <- co[, 2]
  }
  out
}

#' Resolve parsed components by running the requested tiers in priority order
#'
#' @description Each tier is offered only the rows its predecessors left
#' unplaced. Running them in sequence rather than running them all and picking
#' the best answer is worth the extra temp table: a NAR tier is a full scan of
#' the 17.4M-row `Addresses` table, and the all-exact case is the common one, so
#' the later tiers usually see almost nothing.
#'
#' That scan is also why neither NAR query goes through `Streets` or wants an
#' index.
#' Measured on the 2026-06 release, the folded street-key join costs 0.05s for a
#' 5-row probe and **0.08s for a 200-row probe** -- the scan is the whole cost
#' and every probe row shares it, exactly as with the radius query. Batch your
#' addresses into one call rather than looping.
#' @param res Parsed components, as [normalize_address()] returns
#' @param con A NAR connection
#' @param method Tiers to try, in priority order
#' @param bounds A spatial restriction from [nar_geocode_bounds_sql()], or `""`
#' @param bounds_geom The same restriction as an `sfc`, for the tiers that run
#' outside the database
#' @param auth_mun Whether `MUN_NAME` is the caller's authoritative value
#' @param ... Passed to the online tiers; see [geocode()] on how they are split
#' @return A data frame with one row per row of `res`, carrying `ADDR_GUID`,
#' `match_method`, `uncertainty_m`, `n_matches`, `n_records`,
#' `match_postal_code`, `x` and `y`
#' @keywords internal
nar_geocode_match <- function(res, con, method = c("nar", "nar_interpolate"),
                              bounds = "", bounds_geom = NULL,
                              auth_mun = FALSE, ...) {
  n <- nrow(res)
  out <- data.frame(ADDR_GUID     = rep(NA_character_, n),
                    match_method  = rep("none", n),
                    uncertainty_m = rep(NA_real_, n),
                    n_matches     = rep(0L, n),
                    n_records     = rep(0L, n),
                    match_postal_code = rep(NA_character_, n),
                    x             = rep(NA_real_, n),
                    y             = rep(NA_real_, n),
                    stringsAsFactors = FALSE)
  if (!n) return(out)

  probe <- nar_geocode_probe(res, auth_mun = auth_mun)
  dots <- list(...)

  # Priority is expressed as running order: each tier sees only the rows its
  # predecessors left unplaced, and a row is unplaced exactly when it has no
  # coordinates. That definition is what sends `nar_no_geometry` on to the next
  # tier -- knowing the address exists is worth reporting, but not worth
  # withholding a position its neighbours can supply, and the ADDR_GUID found
  # by the exact tier survives whichever tier ends up placing it.
  for (m in method) {
    todo <- which(is.na(out$x))
    if (!length(todo)) break
    out <- switch(m,
      nar             = nar_geocode_tier_nar(out, probe, todo, con, bounds),
      rqa             = nar_geocode_tier_rqa(res, out, probe, todo, con, bounds),
      nar_interpolate = nar_geocode_tier_interp(out, probe, todo, con, bounds),
      rnf             = nar_geocode_tier_rnf(out, probe, todo, con,
                                             bounds = bounds_geom),
      bc              = nar_geocode_tier_bc(res, out, todo, con,
                                            bounds = bounds_geom, ...),
      # The online tiers have different vocabularies, so `...` cannot go to
      # all of them: `min_score` means nothing to the geolocator, and the BC
      # tier forwards anything it does not recognize to its own service as a
      # query parameter, so it is the one that has to keep receiving all of
      # `...`. The other two take closed argument lists and are filtered
      # against them.
      nrcan           = do.call(nar_geocode_tier_nrcan,
                                c(list(res, out, todo, con,
                                       bounds = bounds_geom),
                                  nar_nrcan_dots(dots))),
      qc              = do.call(nar_geocode_tier_qc,
                                c(list(res, out, todo, con,
                                       bounds = bounds_geom),
                                  nar_qc_dots(dots))))
  }
  nar_geocode_mark_uncovered(out, res, con)
}

#' Separate "not in the gazetteer" from "not in this database"
#'
#' @description A partial NAR import holds only the provinces it downloaded, so
#' an address in a province it does not hold cannot match however good the
#' parse is. Reporting that as `none` would say the address is wrong; it says
#' instead that this database was never asked to know.
#'
#' Only rows whose province is both **parsed** and demonstrably outside the
#' coverage are marked. An unparsed province stays `none`, because nothing has
#' been established about it, and a national database marks nothing at all.
#' @param out The result so far
#' @param res Parsed components
#' @param con A NAR connection
#' @return `out`, with `match_method` set to `"not_covered"` where it applies
#' @keywords internal
nar_geocode_mark_uncovered <- function(out, res, con) {
  have <- nar_coverage(con)
  if (identical(have, nar_all_provinces())) return(out)
  prov <- res$PROV_ABVN
  uncovered <- !is.na(prov) & nzchar(prov) & !(toupper(prov) %in% have) &
    out$match_method == "none"
  out$match_method[uncovered] <- "not_covered"
  out
}

#' Write a probe subset to a temporary table and run one tier's query
#'
#' @description The two NAR tiers differ only in their SQL, so the temp-table
#' round trip is shared. The table is dropped on exit rather than left for the
#' connection to clean up, because a caller-supplied connection outlives the
#' call and geocoding in a loop would otherwise accumulate them.
#' @param probe The full probe table
#' @param todo Row indices still needing a position
#' @param con A NAR connection
#' @param sql_fn A function of `(table_name, bounds)` returning SQL
#' @param bounds A spatial restriction, or `""`
#' @return The query result, possibly zero rows
#' @keywords internal
nar_geocode_run_tier <- function(probe, todo, con, sql_fn, bounds) {
  probe <- probe[probe$row_id %in% todo, , drop = FALSE]
  if (!nrow(probe)) return(data.frame())

  tmp <- paste0("nar_geo_", as.integer(stats::runif(1) * 1e9))
  DBI::dbWriteTable(con, tmp, probe, temporary = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, tmp), silent = TRUE), add = TRUE)
  DBI::dbGetQuery(con, sql_fn(tmp, bounds))
}

#' The exact NAR tier
#'
#' @description Looks the civic number up directly. Answers `nar_building` or
#' `nar_blockface` depending on which point NAR carries, or `nar_no_geometry`
#' when it carries the record but no coordinates.
#' @param out The result so far
#' @param probe The probe table
#' @param todo Row indices still needing a position
#' @param con A NAR connection
#' @param bounds A spatial restriction, or `""`
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_nar <- function(out, probe, todo, con, bounds = "") {
  exact <- nar_geocode_run_tier(probe, todo, con, nar_geocode_exact_sql, bounds)
  if (!nrow(exact)) return(out)

  i <- exact$row_id
  located <- !is.na(exact$x)
  out$ADDR_GUID[i]    <- exact$ADDR_GUID
  out$match_method[i] <- ifelse(located, paste0("nar_", exact$geom_source),
                                "nar_no_geometry")
  out$n_matches[i]    <- as.integer(exact$n_points)
  out$n_records[i]    <- as.integer(exact$n_records)
  out$match_postal_code[i] <- exact$match_postal_code
  out$x[i]            <- exact$x
  out$y[i]            <- exact$y
  # The ambiguity widening: pmax, so a blockface match that is also ambiguous
  # keeps whichever of the two errors is larger rather than the later one.
  base <- ifelse(!located, NA_real_,
                 ifelse(exact$geom_source == "blockface",
                        nar_blockface_uncertainty_m(), 0))
  out$uncertainty_m[i] <- pmax(base, exact$spread_m)
  out
}

#' The NAR interpolation tier
#'
#' @description Places a civic number NAR does not carry between the nearest
#' known civics of the same parity on either side of it.
#' @inheritParams nar_geocode_tier_nar
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_interp <- function(out, probe, todo, con, bounds = "") {
  interp <- nar_geocode_run_tier(probe, todo, con, nar_geocode_interp_sql, bounds)
  if (!nrow(interp)) return(out)

  i <- interp$row_id
  out$match_method[i]  <- "nar_interpolated"
  out$uncertainty_m[i] <- 0.5 * interp$span_m
  out$n_matches[i]     <- 2L
  out$x[i]             <- interp$x
  out$y[i]             <- interp$y
  out
}

#' The probe table a geocoding query joins against
#'
#' @description Drops the rows nothing could be done with -- no street name, or
#' no civic number to place along it -- and blanks the `NA`s, because the SQL
#' treats an absent component as "do not constrain on this" and `NULL` would
#' instead make every comparison against it unknown.
#' @param res Parsed components, as [normalize_address()] returns
#' @param auth_mun Whether `MUN_NAME` is the caller's authoritative value, which
#' sends it down the `MunAlias` route instead of the direct one
#' @return A data frame with a `row_id` back-reference into `res`
#' @keywords internal
nar_geocode_probe <- function(res, auth_mun = FALSE) {
  # A hand-built data frame may carry only the columns it needed to; an absent
  # column and an all-NA one mean the same thing here, namely do not constrain.
  blank <- function(name) {
    v <- if (is.null(res[[name]])) NA else res[[name]]
    ifelse(is.na(v), "", as.character(v))[keep]
  }
  keep <- !is.na(res$STREET_NAME) & !is.na(res$CIVIC_NO)
  # `rep("", sum(keep))` and not `""`: when nothing is keepable every other
  # column is length zero, and a length-one literal does not recycle down to
  # zero rows -- data.frame() errors instead. A batch in which no row parsed to
  # both a street and a civic number is a normal input, not a mistake, and has
  # to reach the tiers as an empty probe so they can decline it.
  unconstrained <- rep("", sum(keep))
  data.frame(
    row_id    = which(keep),
    name_fold = nar_fold(res$STREET_NAME[keep]),
    # Only the RQA tier joins on this. The NAR tiers keep the plain fold,
    # which is indexed; see rqa_geocode_sql() on why RQA cannot.
    match_fold = nar_match_fold(res$STREET_NAME[keep]),
    mun_fold  = if (auth_mun) unconstrained else
                  gsub(".", "", nar_fold(blank("MUN_NAME")), fixed = TRUE),
    mun_auth  = if (auth_mun)
                  gsub(".", "", nar_fold(blank("MUN_NAME")), fixed = TRUE)
                else unconstrained,
    prov      = blank("PROV_ABVN"),
    type      = blank("STREET_TYPE"),
    dir       = blank("STREET_DIR"),
    suffix    = nar_fold(blank("CIVIC_NO_SUFFIX")),
    civic     = as.integer(res$CIVIC_NO[keep]),
    stringsAsFactors = FALSE
  )
}

#' The street key both geocoding tiers join on
#'
#' @description Shared so the two tiers cannot drift apart -- an interpolation
#' that selected its flanking civics from a different street than the exact tier
#' searched would be a silent, invisible error.
#'
#' Both NAR name families are matched, as the gazetteer does, because neither is
#' complete on its own. Every other component only ever constrains when the
#' input supplied it: a string that never named a municipality is resolved
#' against the whole province rather than being refused, and the ambiguity that
#' invites is reported through `n_matches` instead.
#'
#' Periods come off both sides of the municipality comparison. NAR files ST.
#' JOHN'S, SAULT STE. MARIE and ST. ALBERT with them while `nar_norm_text()`
#' strips them from input, so without this those cities never match.
#' @return A SQL fragment, with the probe aliased `p` and `Addresses` aliased `a`
#' @keywords internal
nar_geocode_street_key <- function(name_col, bounds = "") {
  sprintf(
   "p.name_fold = strip_accents(upper(a.%1$s))
   AND (p.prov = '' OR a.MAIL_PROV_ABVN = p.prov)
   AND (p.mun_fold = ''
        OR replace(strip_accents(upper(a.MAIL_MUN_NAME)), '.', '') = p.mun_fold)
   AND (p.mun_auth = ''
        OR (a.PROV_CODE || ':' || a.CSD_TYPE_ENG_CODE || ':' || a.CSD_ENG_NAME) IN (
             SELECT m.MUN_KEY FROM MunAlias m
              WHERE replace(m.NAME_FOLD, '.', '') = p.mun_auth
                AND (p.prov = '' OR m.PROV_ABVN = p.prov)))
   AND (p.type = '' OR p.type IN (upper(a.OFFICIAL_STREET_TYPE),
                                  upper(a.MAIL_STREET_TYPE)))
   AND (p.dir  = '' OR p.dir  IN (upper(a.OFFICIAL_STREET_DIR),
                                  upper(a.MAIL_STREET_DIR)))%2$s",
   name_col, bounds)
}

#' Every NAR address on the street a probe row names
#'
#' @description One branch per NAR street-name family, unioned, rather than one
#' join with `OFFICIAL = x OR MAIL = x`. **This is a 99x difference and it is
#' not a micro-optimization.** An `OR` across two columns has no equijoin key,
#' so DuckDB falls back to a nested loop over the whole 17.4M-row table: the
#' interpolation tier, which has no civic-number equality to rescue it, took
#' **15.87s** written that way and **0.16s** as a union, for byte-identical
#' results. The exact tier hid the problem, because `CIVIC_NO = p.civic` gave
#' the planner a hash key of its own.
#'
#' `UNION` and not `UNION ALL`: the two families agree for most addresses, and
#' the select list carries `ADDR_GUID`, so the set union drops exactly the rows
#' both branches matched and nothing else.
#' @param probe Name of the temp table holding the parsed components
#' @param select The select list, with the probe aliased `p` and `Addresses` `a`
#' @param extra Tier-specific predicates, appended to the join condition
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A SQL fragment producing the candidate set
#' @keywords internal
nar_geocode_candidates <- function(probe, select, extra = "", bounds = "") {
  branch <- function(col) {
    sprintf("SELECT %s
        FROM %s p
        JOIN Addresses a
          ON %s%s",
            select, probe, nar_geocode_street_key(col, bounds), extra)
  }
  paste(branch("OFFICIAL_STREET_NAME"), "
      UNION
      ",
        branch("MAIL_STREET_NAME"))
}

#' The exact-match geocoding query
#'
#' @description Kept as its own function, like [nar_gazetteer_sql()], so the SQL
#' can be read and tested without a database.
#'
#' A building point always outranks a blockface one for the same address, and
#' `ADDR_GUID` breaks any remaining tie so the answer is stable across runs
#' rather than depending on scan order. The second aggregation exists only to
#' measure ambiguity: it rejoins the chosen point to every candidate that
#' satisfied the query and reports how many distinct points there were, how
#' many distinct addresses, and how far the furthest of the points sits from
#' the one returned. Points and addresses are counted separately because they
#' routinely differ: every unit of a multi-unit building is its own NAR address
#' at the building's one coordinate, so `n_records` can be 19 where `n_points`
#' is 1 -- see [geocode()].
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A single SQL string
#' @keywords internal
nar_geocode_exact_sql <- function(probe, bounds = "") {
  sprintf("
    WITH cand AS (
      %s
    ),
    best AS (
      SELECT * FROM cand
      QUALIFY row_number() OVER (
        PARTITION BY row_id
        ORDER BY CASE WHEN x IS NULL THEN 2
                      WHEN geom_source = 'building' THEN 0 ELSE 1 END,
                 ADDR_GUID) = 1
    )
    SELECT b.row_id, b.ADDR_GUID, b.geom_source, b.x, b.y,
           count(DISTINCT c.x::VARCHAR || ',' || c.y::VARCHAR) AS n_points,
           count(DISTINCT c.ADDR_GUID) AS n_records,
           max(sqrt((c.x - b.x)^2 + (c.y - b.y)^2)) AS spread_m,
           %2$s
      FROM best b
      JOIN cand c USING (row_id)
     GROUP BY ALL",
    nar_geocode_candidates(
      probe,
      "p.row_id, a.ADDR_GUID, a.geom_source, a.x, a.y, a.MAIL_POSTAL_CODE",
      "\n         AND a.CIVIC_NO = p.civic
         -- A suffix that was written has to be honoured -- 990A and 990 are
         -- different addresses -- but one that was not is left unconstrained,
         -- since the great majority of NAR rows carry no suffix at all.
         AND (p.suffix = '' OR upper(coalesce(a.CIVIC_NO_SUFFIX, '')) = p.suffix)",
      bounds),
    nar_geocode_postal_sql("c.MAIL_POSTAL_CODE"))
}

#' The postal code of the record that was matched
#'
#' @description An aggregate over the *candidate* set rather than a column read
#' off the row that was returned, and that is the whole point of it. NAR carries
#' one row per address, so a civic number with units contributes many rows to
#' `cand`; the tier picks one of them for its coordinates, and picking one of
#' them for a postal code as well would be a coin flip wherever the units of a
#' building do not share one. They usually do -- 98.6% of civic numbers in NAR
#' carry a single postal code -- but the 1.4% that do not are 4.2% of addresses,
#' since a building large enough to split across postal codes is large.
#'
#' So the value is reported only when every candidate agrees, and is `NULL`
#' otherwise. The empty-string fold makes a missing postal code participate in
#' that agreement rather than being skipped by `count(DISTINCT)`: a set that is
#' half `NULL` reports nothing, not the half that had a value.
#' @param col The postal-code column, qualified with the candidate alias
#' @return A SQL fragment, aliased `match_postal_code`
#' @keywords internal
nar_geocode_postal_sql <- function(col) {
  sprintf("CASE WHEN count(DISTINCT coalesce(%1$s, '')) = 1
                THEN nullif(min(coalesce(%1$s, '')), '') END AS match_postal_code",
          col)
}

#' The interpolation query
#'
#' @description Finds the nearest known civic number below and above the one
#' asked for, **on the same side of the street**, and places the address on the
#' straight line between them in proportion to where its number falls in the
#' numbering.
#'
#' Three things in here are load-bearing:
#'
#' * `(a.CIVIC_NO %% 2) = (p.civic %% 2)` is the same-side restriction, and it
#'   is what makes this accurate: 4.2 m median error against 35.2 m when both
#'   sides are pooled.
#' * Candidates are restricted to `geom_source = 'building'`. Interpolating
#'   between two blockface centroids would compound a 176 m error at each end.
#' * The final `WHERE lo_n IS NOT NULL AND hi_n IS NOT NULL` is the refusal to
#'   extrapolate. Both flanks are required; a number past the end of the run
#'   returns nothing at all.
#'
#' Duplicate civic numbers are averaged first. NAR carries one row per address
#' rather than per civic number, so a building with units contributes many rows
#' at one point, and `arg_max` over the raw rows would pick an arbitrary one.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A single SQL string
#' @keywords internal
nar_geocode_interp_sql <- function(probe, bounds = "") {
  sprintf("
    WITH cand AS (
      %s
    ),
    pt AS (
      SELECT row_id, civic, cn, avg(x) AS x, avg(y) AS y
        FROM cand GROUP BY row_id, civic, cn
    ),
    flank AS (
      SELECT row_id, civic,
             max(cn)         FILTER (WHERE cn < civic) AS lo_n,
             arg_max(x, cn)  FILTER (WHERE cn < civic) AS lo_x,
             arg_max(y, cn)  FILTER (WHERE cn < civic) AS lo_y,
             min(cn)         FILTER (WHERE cn > civic) AS hi_n,
             arg_min(x, cn)  FILTER (WHERE cn > civic) AS hi_x,
             arg_min(y, cn)  FILTER (WHERE cn > civic) AS hi_y
        FROM pt
       GROUP BY row_id, civic
    )
    SELECT row_id, lo_n, hi_n,
           lo_x + (hi_x - lo_x) * ((civic - lo_n) / (hi_n - lo_n)::DOUBLE) AS x,
           lo_y + (hi_y - lo_y) * ((civic - lo_n) / (hi_n - lo_n)::DOUBLE) AS y,
           sqrt((hi_x - lo_x)^2 + (hi_y - lo_y)^2) AS span_m
      FROM flank
     WHERE lo_n IS NOT NULL AND hi_n IS NOT NULL",
    nar_geocode_candidates(
      probe,
      "p.row_id, p.civic, a.ADDR_GUID, a.CIVIC_NO AS cn, a.x, a.y",
      "\n         AND a.geom_source = 'building'
         AND a.CIVIC_NO IS NOT NULL
         AND (a.CIVIC_NO % 2) = (p.civic % 2)",
      bounds))
}

#' Pick the `...` arguments the geolocator tier understands
#'
#' @description `geocode(...)` has to serve two online tiers whose arguments do
#' not overlap. The BC tier keeps receiving all of `...`, because
#' [bc_geocode()] forwards what it does not recognize to its own service as a
#' query parameter and a filter would break that. [nrcan_geocode()] has no such
#' passthrough -- the geolocator takes one query parameter -- so its formals are
#' a closed set and unknown names can be dropped rather than raising an error
#' about an argument meant for the other tier.
#'
#' Derived from the formals rather than listed, so an argument added to
#' [nrcan_geocode()] does not have to be remembered here.
#' @param dots `list(...)` as [nar_geocode_match()] captured it
#' @return The subset of `dots` to forward
#' @keywords internal
nar_nrcan_dots <- function(dots) {
  if (!length(dots) || is.null(names(dots))) return(list())
  # The tier supplies these itself, so a caller-supplied one would be silently
  # overridden or would conflict with an argument geocode() already owns.
  supplied <- c("x", "prov", "geometry", "crs", "con")
  dots[intersect(names(dots), setdiff(names(formals(nrcan_geocode)), supplied))]
}

#' Validate and normalize the `method` argument
#'
#' @description Partial matching, deduplication, and an error naming the tier
#' that was not recognized rather than the whole vocabulary. **Order is
#' preserved**, since order is the priority.
#' @param method What the caller passed
#' @return A character vector of tier names
#' @keywords internal
nar_geocode_methods <- function(method) {
  known <- c("nar", "rqa", "nar_interpolate", "rnf", "bc", "nrcan", "qc")
  if (!length(method) || !is.character(method)) {
    stop("`method` must be one or more of ", paste0('"', known, '"', collapse = ", "),
         ", in the order they should be tried.", call. = FALSE)
  }
  # Exact matches win over prefixes in pmatch, so "nar" is unambiguous even
  # though it prefixes "nar_interpolate".
  i <- pmatch(method, known, nomatch = 0L, duplicates.ok = TRUE)
  if (any(i == 0L)) {
    stop("Unknown geocoding method ",
         paste0('"', method[i == 0L], '"', collapse = ", "), ". `method` takes ",
         paste0('"', known, '"', collapse = ", "), ".", call. = FALSE)
  }
  # A tier offered a second time would see only the rows it already declined,
  # so a duplicate is dropped rather than run twice.
  unique(known[i])
}

#' Recycle an authoritative constraint to one value per input
#'
#' @description Length 1 or length `n` and nothing in between: a partial vector
#' would recycle silently and constrain the wrong rows.
#' @param v The supplied value
#' @param n Number of inputs
#' @param what Argument name, for the error message
#' @return A character vector of length `n`
#' @keywords internal
nar_recycle <- function(v, n, what) {
  if (length(v) == 1) return(rep(as.character(v), n))
  if (length(v) == n) return(as.character(v))
  stop("`", what, "` must be length 1 or length ", n, ", not ", length(v), ".",
       call. = FALSE)
}

#' Turn a spatial restriction into a SQL fragment
#'
#' @description Two clauses, and both are wanted. The bounding box is compared
#' against the `x`/`y` columns, which is the cheap half: those are plain
#' `DOUBLE` columns with per-row-group zonemaps, so DuckDB skips whole row
#' groups whose range cannot satisfy it instead of reading them. `ST_Within`
#' then makes the restriction exact for a genuine polygon. For a rectangle the
#' second clause is nearly redundant, but not quite -- a rectangle in the
#' caller\'s CRS is not a rectangle in the storage CRS -- and it keeps one code
#' path rather than two.
#'
#' The outline is densified before reprojection. Transforming only the corners
#' of a longitude/latitude rectangle into a projected CRS and taking the box of
#' the result clips the bulge along each edge, which would silently drop
#' addresses inside the region the caller asked for.
#' @param within An `sf`/`sfc` object, an `st_bbox`, a length-4 numeric
#' `c(xmin, ymin, xmax, ymax)`, or `NULL`
#' @param crs CRS to interpret `within` in when it carries none
#' @param con A NAR connection, for the storage CRS
#' @return A SQL fragment to append to the join condition, or `""`
#' @keywords internal
nar_geocode_bounds <- function(within, crs, con) {
  nar_geocode_bounds_sql(nar_geocode_bounds_geom(within, crs, con))
}

#' Resolve `within` to a geometry in the storage CRS
#'
#' @description Split out from the SQL so the same restriction can be enforced
#' twice: pushed into the NAR query as a predicate, and applied in R to points
#' that came from somewhere else -- the BC fallback, which is a separate service
#' and cannot be given this package's SQL.
#' @param within An `sf`/`sfc`/`sfg`, an `st_bbox`, or a length-4 numeric
#' @param crs CRS to interpret a bare numeric or an untagged geometry in
#' @param con A NAR connection, for the storage CRS
#' @return An `sfc` in the storage CRS, or `NULL`
#' @keywords internal
nar_geocode_bounds_geom <- function(within, crs, con) {
  if (is.null(within)) return(NULL)
  # crs = NULL asks for output in the storage CRS, and a bare bbox given
  # alongside it is naturally in the same coordinates.
  if (is.null(crs)) crs <- nar_crs(con)

  g <- if (inherits(within, "bbox")) {
    sf::st_as_sfc(within)
  } else if (inherits(within, c("sf", "sfc", "sfg"))) {
    sf::st_geometry(sf::st_as_sf(within))
  } else if (is.numeric(within) && length(within) == 4) {
    sf::st_as_sfc(sf::st_bbox(c(xmin = within[1], ymin = within[2],
                                xmax = within[3], ymax = within[4]),
                              crs = sf::st_crs(crs)))
  } else {
    stop("`within` must be an sf/sfc object, an st_bbox, or a length-4 ",
         "numeric c(xmin, ymin, xmax, ymax).", call. = FALSE)
  }
  if (is.na(sf::st_crs(g))) g <- sf::st_set_crs(g, sf::st_crs(crs))

  # Densified with the CRS temporarily off. st_segmentize() on a geographic
  # geometry measures along the great circle and hands the job to lwgeom, which
  # is not a dependency of this package; with no CRS it interpolates in the
  # plane, which is exactly what is wanted here -- extra vertices along the
  # straight edges as drawn, before those edges are bent by the reprojection.
  bb <- sf::st_bbox(g)
  step <- max(bb[["xmax"]] - bb[["xmin"]], bb[["ymax"]] - bb[["ymin"]]) / 100
  crs_in <- sf::st_crs(g)
  g <- sf::st_set_crs(sf::st_segmentize(sf::st_set_crs(g, NA), step), crs_in)
  g <- sf::st_transform(g, nar_crs(con))
  sf::st_union(g)
}

#' The SQL predicate for a resolved `within` geometry
#'
#' @description The bounding box goes first so the zonemap prefilter on `x`/`y`
#' can skip row groups before the polygon test is evaluated -- the same
#' mechanism [nar_within_radius()] relies on.
#' @param g An `sfc` in the storage CRS, or `NULL`
#' @return A SQL fragment, or `""`
#' @keywords internal
nar_geocode_bounds_sql <- function(g) {
  if (is.null(g)) return("")
  b <- sf::st_bbox(g)
  sprintf("
   AND a.x BETWEEN %.3f AND %.3f AND a.y BETWEEN %.3f AND %.3f
   AND st_within(nar_xy(a.x, a.y), st_geomfromtext('%s'))",
          b[["xmin"]], b[["xmax"]], b[["ymin"]], b[["ymax"]],
          sf::st_as_text(g))
}

#' Does an online service's answer agree with the address that was asked for?
#'
#' @description **The online tiers always answer, and their wrong answers are
#' confident.** NRCan's geolocator returns `1 Rue Notre-Dame Ouest, Montreal, QC`
#' as an `INTERPOLATED_POSITION` on a real Rue Notre-Dame Ouest in Lorrainville,
#' 500 km away, and `330 Spadina Rd, Toronto` as `330 Spadina Avenue`, a
#' different street 3 km off. Neither is imprecision -- both are answers to a
#' different question, and no confidence field distinguishes them.
#'
#' What separates them is that the answer is itself an address, so it can be put
#' back into components and compared to the ones that were sent. That is the
#' whole floor, and it is shared by every online tier: **the answer has to come
#' back as the address that was asked for.** Where the components come from
#' differs -- [nar_nrcan_floors()] re-parses a returned title, [nar_osm_floors()]
#' reads fields the service already separated -- but what is done with them once
#' they exist does not, and a second copy of these rules would drift from this
#' one.
#'
#' The comparison is per component rather than a single [address_key()]
#' equality, because the components are not equally strict:
#'
#' * `CIVIC_NO`, `STREET_NAME` -- must be **present on both sides and equal**.
#'   These are what the query was about; a missing one means nothing was
#'   verified.
#' * `STREET_TYPE`, `STREET_DIR`, `PROV_ABVN` -- rejected only when both sides
#'   are present and **contradict**. An absent one cannot contradict anything.
#' * `MUN_NAME` -- whole-word containment either way, not equality. Both services
#'   return incorporated names, so `TORONTO` comes back as `City Of Toronto`,
#'   `NORTH VANCOUVER` as `District Of North Vancouver` and `CHARLOTTETOWN` as
#'   `City of Charlottetown`. Equality would reject all three.
#'
#' Whole-word matters: comparing the municipality against the *whole* returned
#' string with a plain substring test -- the first thing tried -- passes
#' `28 Silver ST, CORNER BROOK` against `28 Brook Street, Corner Brook`, because
#' `Brook` appears in the street name. Field-wise comparison is what catches it.
#' @param q Parsed components of the address that was sent
#' @param t Parsed components of the answer that came back
#' @return A character vector, `NA` where the answer agrees and otherwise a
#' short reason naming the component that disagreed
#' @keywords internal
nar_address_agreement <- function(q, t) {
  col <- function(d, name) {
    v <- d[[name]]
    v <- if (is.null(v)) rep(NA_character_, nrow(d)) else as.character(v)
    v <- nar_key_fold(v)
    ifelse(is.na(v), "", v)
  }
  # `""` is absent, so a comparison against it is never a contradiction. Every
  # rule below is written in terms of "both present" for that reason.
  contradicts <- function(a, b) nzchar(a) & nzchar(b) & a != b
  missing_or_differs <- function(a, b) !nzchar(a) | !nzchar(b) | a != b

  # Only A-Z, 0-9 and spaces survive nar_key_fold(), so the folded value is
  # already safe to paste into a pattern -- there is no metacharacter left.
  contained <- function(a, b) {
    mapply(function(x, y) {
      grepl(paste0("\\b", x, "\\b"), y) || grepl(paste0("\\b", y, "\\b"), x)
    }, a, b, USE.NAMES = FALSE)
  }

  reason <- rep(NA_character_, nrow(q))
  note <- function(bad, what, a, b) {
    bad <- bad & is.na(reason)
    if (!any(bad)) return(reason)
    reason[bad] <<- sprintf("%s %s != %s", what,
                            ifelse(nzchar(a[bad]), a[bad], "?"),
                            ifelse(nzchar(b[bad]), b[bad], "?"))
    reason
  }

  qn <- col(q, "STREET_NAME");     tn <- col(t, "STREET_NAME")
  qc <- col(q, "CIVIC_NO");        tc <- col(t, "CIVIC_NO")
  qt <- col(q, "STREET_TYPE");     tt <- col(t, "STREET_TYPE")
  qd <- col(q, "STREET_DIR");      td <- col(t, "STREET_DIR")
  qm <- col(q, "MUN_NAME");        tm <- col(t, "MUN_NAME")
  qp <- col(q, "PROV_ABVN");       tp <- col(t, "PROV_ABVN")

  reason <- note(missing_or_differs(qn, tn), "street name", qn, tn)
  reason <- note(missing_or_differs(qc, tc), "civic number", qc, tc)
  reason <- note(contradicts(qt, tt), "street type", qt, tt)
  reason <- note(contradicts(qd, td), "street direction", qd, td)
  reason <- note(nzchar(qm) & nzchar(tm) & !contained(qm, tm),
                 "municipality", qm, tm)
  reason <- note(contradicts(qp, tp), "province", qp, tp)
  reason
}
