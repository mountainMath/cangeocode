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
#' result against NAR, returning one row per input in input order. Two tiers are
#' tried in turn and the column `match_method` records which one answered:
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
#' * **`none`** -- nothing resolved.
#'
#' @section Interpolation: Set `interpolate = FALSE` to skip the tier entirely.
#'
#' Only civics of the **same parity** are used, because odd and even numbers sit
#' on opposite sides of the street and pooling them is markedly worse: measured
#' by leave-one-out over all 10.6M distinct NAR civic points, same-side
#' interpolation has a median error of 4.2 m against 35.2 m for both sides
#' pooled, and beats simply taking the nearest known civic (16.9 m).
#'
#' **Extrapolation is refused.** A civic number past the last known one on its
#' side has no second point to interpolate against, and guessing from the run's
#' spacing is close to worthless -- median error 15.1 m but a 90th percentile of
#' 237 m, barely better than the nearest neighbour it would displace. Those rows
#' come back `none` rather than carrying a number that looks like the others.
#' 7.3% of NAR civics sit at the end of a run.
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
#' carries its own. **Authoritative**, and applied to the interpolation tier as
#' well as the exact one.
#' @param source Source dataset. Currently only `"nar"`.
#' @param interpolate Whether to interpolate civic numbers NAR does not carry.
#' Default `TRUE`.
#' @param fallback What to try for rows NAR could not place. `NULL` (default)
#' means nothing; `"bc"` sends the British Columbia rows to the Province of BC's
#' [Address Geocoder][bc_geocode()], which is the only external service wired
#' up and covers no other province. **This makes network requests**, one per
#' unplaced BC address. The constraints are honoured: what is sent is rebuilt
#' from the components after any `prov`/`mun` override, and a point falling
#' outside `within` is discarded rather than returned.
#' @param geometry Whether to return an `sf` object with POINT geometry.
#' Unmatched rows get an empty point. Default `FALSE`, which returns `lon` and
#' `lat` columns instead.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param version NAR version to query, passed to [nar_connection()]. Ignored
#' when `con` is supplied.
#' @param con An open NAR connection to reuse. The caller keeps ownership: a
#' connection passed in here is left open, while one opened internally is closed
#' again before returning.
#' @param ... Passed to [bc_geocode()] when `fallback = "bc"`, which is where
#' `min_score`, `api_key` and `rate` go. Otherwise unused.
#' @return A data frame with one row per input, carrying every column
#' [normalize_address()] returns plus `ADDR_GUID`, `match_method`,
#' `uncertainty_m`, `n_matches`, and either `lon`/`lat` or an `sf` geometry
#' column.
#' @export
#' @examples
#' \dontrun{
#' geocode("1055 W Georgia St, Vancouver BC")
#'
#' # Parse once, resolve many times, and keep only the precise matches.
#' parsed <- normalize_address(addresses)
#' g <- geocode(parsed, geometry = TRUE)
#' g[g$uncertainty_m <= 25, ]
#' }
geocode <- function(x, prov = NULL, mun = NULL, within = NULL, source = "nar",
                    interpolate = TRUE, fallback = NULL, geometry = FALSE,
                    crs = 4326, version = "latest", con = NULL, ...) {
  source <- match.arg(source, choices = c("nar"))
  if (!is.null(fallback)) fallback <- match.arg(fallback, choices = c("bc"))

  if (is.null(con)) {
    con <- nar_connection(version = version)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
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
  hits <- nar_geocode_match(res, con, interpolate = interpolate,
                            bounds = nar_geocode_bounds_sql(bounds),
                            auth_mun = !is.null(mun))
  if (identical(fallback, "bc")) {
    hits <- nar_geocode_bc_fallback(res, hits, con, bounds = bounds, ...)
  }
  out <- cbind(res, hits[, c("ADDR_GUID", "match_method", "uncertainty_m",
                             "n_matches")])

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
  out$lon <- NA_real_
  out$lat <- NA_real_
  if (any(ok)) {
    co <- sf::st_coordinates(pts[ok])
    out$lon[ok] <- co[, 1]
    out$lat[ok] <- co[, 2]
  }
  out
}

#' Resolve parsed components against NAR, exactly and then by interpolation
#'
#' @description Runs the exact tier over every parsed row, then the
#' interpolation tier over only the rows it did not answer. Splitting it that
#' way rather than running both and preferring the exact one is worth the second
#' temp table: each tier is a full scan of the 17.4M-row `Addresses` table, and
#' the all-exact case is the common one.
#'
#' That scan is also why neither query goes through `Streets` or wants an index.
#' Measured on the 2026-06 release, the folded street-key join costs 0.05s for a
#' 5-row probe and **0.08s for a 200-row probe** -- the scan is the whole cost
#' and every probe row shares it, exactly as with the radius query. Batch your
#' addresses into one call rather than looping.
#' @param res Parsed components, as [normalize_address()] returns
#' @param con A NAR connection
#' @param interpolate Whether to run the interpolation tier
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @param auth_mun Whether `MUN_NAME` is the caller's authoritative value
#' @return A data frame with one row per row of `res`, carrying `ADDR_GUID`,
#' `match_method`, `uncertainty_m`, `n_matches`, `x` and `y`
#' @keywords internal
nar_geocode_match <- function(res, con, interpolate = TRUE, bounds = "",
                              auth_mun = FALSE) {
  n <- nrow(res)
  out <- data.frame(ADDR_GUID     = rep(NA_character_, n),
                    match_method  = rep("none", n),
                    uncertainty_m = rep(NA_real_, n),
                    n_matches     = rep(0L, n),
                    x             = rep(NA_real_, n),
                    y             = rep(NA_real_, n),
                    stringsAsFactors = FALSE)
  if (!n) return(out)

  probe <- nar_geocode_probe(res, auth_mun = auth_mun)
  if (!nrow(probe)) return(out)

  tmp <- paste0("nar_geo_", as.integer(stats::runif(1) * 1e9))
  DBI::dbWriteTable(con, tmp, probe, temporary = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, tmp), silent = TRUE), add = TRUE)

  exact <- DBI::dbGetQuery(con, nar_geocode_exact_sql(tmp, bounds))
  if (nrow(exact)) {
    i <- exact$row_id
    located <- !is.na(exact$x)
    out$ADDR_GUID[i]    <- exact$ADDR_GUID
    out$match_method[i] <- ifelse(located, paste0("nar_", exact$geom_source),
                                  "nar_no_geometry")
    out$n_matches[i]    <- as.integer(exact$n_points)
    out$x[i]            <- exact$x
    out$y[i]            <- exact$y
    # The ambiguity widening: pmax, so a blockface match that is also ambiguous
    # keeps whichever of the two errors is larger rather than the later one.
    base <- ifelse(!located, NA_real_,
                   ifelse(exact$geom_source == "blockface",
                          nar_blockface_uncertainty_m(), 0))
    out$uncertainty_m[i] <- pmax(base, exact$spread_m)
  }

  # A record found but unplaced still goes to the interpolation tier. Knowing
  # the address exists is worth reporting, but it is not worth withholding a
  # position that can be derived from its neighbours -- so `nar_no_geometry` is
  # the answer only once interpolation has also declined, and the ADDR_GUID
  # found here survives either way.
  todo <- probe[out$match_method[probe$row_id] %in% c("none", "nar_no_geometry"),
                , drop = FALSE]
  if (!interpolate || !nrow(todo)) return(out)

  tmp2 <- paste0(tmp, "_i")
  DBI::dbWriteTable(con, tmp2, todo, temporary = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, tmp2), silent = TRUE), add = TRUE)

  interp <- DBI::dbGetQuery(con, nar_geocode_interp_sql(tmp2, bounds))
  if (nrow(interp)) {
    i <- interp$row_id
    out$match_method[i]  <- "nar_interpolated"
    out$uncertainty_m[i] <- 0.5 * interp$span_m
    out$n_matches[i]     <- 2L
    out$x[i]             <- interp$x
    out$y[i]             <- interp$y
  }
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
  data.frame(
    row_id    = which(keep),
    name_fold = nar_fold(res$STREET_NAME[keep]),
    mun_fold  = if (auth_mun) "" else
                  gsub(".", "", nar_fold(blank("MUN_NAME")), fixed = TRUE),
    mun_auth  = if (auth_mun)
                  gsub(".", "", nar_fold(blank("MUN_NAME")), fixed = TRUE) else "",
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
#' satisfied the query and reports how many distinct points there were and how
#' far the furthest of them sits from the one returned.
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
           max(sqrt((c.x - b.x)^2 + (c.y - b.y)^2)) AS spread_m
      FROM best b
      JOIN cand c USING (row_id)
     GROUP BY ALL",
    nar_geocode_candidates(
      probe,
      "p.row_id, a.ADDR_GUID, a.geom_source, a.x, a.y",
      "\n         AND a.CIVIC_NO = p.civic
         -- A suffix that was written has to be honoured -- 990A and 990 are
         -- different addresses -- but one that was not is left unconstrained,
         -- since the great majority of NAR rows carry no suffix at all.
         AND (p.suffix = '' OR upper(coalesce(a.CIVIC_NO_SUFFIX, '')) = p.suffix)",
      bounds))
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
