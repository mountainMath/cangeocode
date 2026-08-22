#' The BC Address Geocoder endpoint
#'
#' @description Kept in one place so a test can point it somewhere else and so
#' the URL is not repeated across the request builders.
#' @return A single URL
#' @keywords internal
nar_bc_url <- function() "https://geocoder.api.gov.bc.ca"

#' What each BC match precision is worth
#'
#' @description The BC Address Geocoder **always answers**. Feed it
#' `"1234 Nonexistentzzz Rd, Victoria, BC"` and it returns the centre of
#' Victoria with a score of 48 rather than nothing at all, so a response is not
#' a match and `matchPrecision` is the field that decides what was actually
#' resolved. This table maps its vocabulary onto the same `match_method` and
#' `uncertainty_m` contract [geocode()] uses.
#'
#' **The metres here are the service's own precision semantics translated into
#' order-of-magnitude figures, not a measurement.** Unlike every other number in
#' this package they were not derived from data: BC publishes
#' `locationPositionalAccuracy` as the categorical `high`/`medium`/`low`/`coarse`
#' and no distance at all. They are deliberately pessimistic, and calibrating
#' them against NAR building points over a BC sample is the obvious next step --
#' see `inst/notes/geocoding-status.md`. Treat them as a ranking that is safe to
#' filter on, not as an error bar comparable to the NAR tiers'.
#' @param precision The `matchPrecision` value from a feature
#' @return A one-row data frame of `match_method` and `uncertainty_m`
#' @keywords internal
nar_bc_precision <- function(precision) {
  tab <- list(
    # A specific point on a specific parcel: the service's best answer.
    OCCUPANT      = c("bc_site", 20),
    UNIT          = c("bc_site", 20),
    SITE          = c("bc_site", 20),
    CIVIC_NUMBER  = c("bc_civic", 20),
    INTERSECTION  = c("bc_intersection", 50),
    # Interpolated along a block, which is the BC analogue of this package's
    # own interpolation tier and is priced comparably to a coarse one.
    BLOCK         = c("bc_block", 100),
    # No civic number resolved: a point on the street, or the centre of the
    # locality. Both are answers about a place rather than an address, and the
    # locality one is what garbage input degrades to.
    STREET        = c("bc_street", 500),
    LOCALITY      = c("bc_locality", 5000),
    PROVINCE      = c("none", NA)
  )
  hit <- tab[[toupper(precision %||% "")]]
  if (is.null(hit)) hit <- c("none", NA)
  data.frame(match_method = hit[1], uncertainty_m = as.numeric(hit[2]),
             stringsAsFactors = FALSE)
}

#' Turn one BC Address Geocoder feature into a result row
#'
#' @description Split out from the request so the response shape can be tested
#' against a saved fixture with no network. Takes the parsed JSON of a whole
#' response and reads its first feature, since the package only ever asks for
#' one.
#' @param resp The parsed response, as [jsonlite::fromJSON()] with
#' `simplifyVector = FALSE` returns it
#' @param min_score Scores below this are reported as `none`
#' @return A one-row data frame
#' @keywords internal
nar_bc_feature <- function(resp, min_score = 60) {
  empty <- data.frame(match_method = "none", uncertainty_m = NA_real_,
                      bc_score = NA_integer_, bc_precision = NA_character_,
                      bc_address = NA_character_, bc_faults = NA_character_,
                      lon = NA_real_, lat = NA_real_, stringsAsFactors = FALSE)
  f <- resp$features
  if (!length(f)) return(empty)
  f <- f[[1]]
  p <- f$properties

  out <- nar_bc_precision(p$matchPrecision)
  score <- as.integer(p$score %||% NA)
  # Precision and score are independent, and both have to clear. A misspelt
  # street still resolves to CIVIC_NUMBER while carrying penalties, and a low
  # score there means the service guessed which street was meant.
  if (!is.na(score) && score < min_score) out$match_method <- "none"
  # A rejected row carries no error bar: `uncertainty_m` describes a point that
  # is being offered, and nothing is being offered here. The score and the
  # faults stay, so the caller can still see what was thrown away.
  if (out$match_method == "none") out$uncertainty_m <- NA_real_

  faults <- vapply(p$faults %||% list(), function(x) {
    sprintf("%s %s", x$element %||% "?", x$fault %||% "?")
  }, character(1))

  co <- f$geometry$coordinates
  cbind(out, data.frame(
    bc_score     = score,
    bc_precision = p$matchPrecision %||% NA_character_,
    bc_address   = p$fullAddress %||% NA_character_,
    bc_faults    = if (length(faults)) paste(faults, collapse = "; ") else NA_character_,
    lon = if (length(co) == 2) as.numeric(co[[1]]) else NA_real_,
    lat = if (length(co) == 2) as.numeric(co[[2]]) else NA_real_,
    stringsAsFactors = FALSE))
}

#' Geocode British Columbia addresses with the BC Address Geocoder
#'
#' @description A binding to the Province of British Columbia's public
#' [Address Geocoder](https://geocoder.api.gov.bc.ca/). It covers BC only, and
#' complements the NAR pathway in two ways: as a fallback for BC addresses
#' [geocode()] cannot place, and as a second positional source to check NAR
#' against -- see [bc_validate()].
#'
#' @section What a response means: **The service always answers.** Given
#' `"1234 Nonexistentzzz Rd, Victoria, BC"` it returns the centre of Victoria
#' with a score of 48, not an error, so the presence of a result says nothing.
#' `match_method` is derived from the response's `matchPrecision` and is the
#' field to read: `bc_site` and `bc_civic` are addresses, `bc_block` is
#' interpolated along a block, and `bc_street` and `bc_locality` are answers
#' about a place rather than an address. `min_score` additionally rejects
#' matches the service itself scored poorly, and `bc_faults` says why it did.
#'
#' `uncertainty_m` is on the same scale as [geocode()]'s but is **not measured**
#' -- see [nar_bc_precision()] for exactly what it is and is not.
#'
#' @section Network use and courtesy: One HTTP request per address; there is no
#' public batch endpoint. Requests are throttled to `rate` per second, and the
#' default of 5 is deliberately conservative -- this is a free public service
#' and a large job should register for an API key and pass it as `api_key`.
#' `httr2` is required and lives in `Suggests`, so the package never contacts
#' the network unless this function is called.
#'
#' Results are subject to the Province of British Columbia's terms; the response
#' carries its own copyright notice and licence links.
#'
#' @param x A character vector of address strings.
#' @param min_score Minimum score, 0--100, for a result to count as a match.
#' Default 60. Anything below is reported as `none`, with the score and faults
#' still filled in so you can see what was rejected.
#' @param api_key Optional API key, sent as the `apikey` header.
#' @param rate Requests per second, and also the largest burst allowed before
#' throttling starts. Default 5.
#' @param geometry Whether to return an `sf` object. Default `FALSE`.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param ... Additional query parameters passed to the service, for example
#' `locationDescriptor = "frontDoorPoint"` or `interpolation = "linear"`.
#' @return A data frame with one row per input: `input`, `match_method`,
#' `uncertainty_m`, `bc_score`, `bc_precision`, `bc_address`, `bc_faults`, and
#' either `lon`/`lat` or an `sf` geometry column.
#' @export
#' @examples
#' \dontrun{
#' bc_geocode("525 Superior St, Victoria, BC")
#'
#' # What the service could not make sense of, and what it fell back to.
#' bc_geocode("525 Superor Steet, Victoia, BC")$bc_faults
#' }
bc_geocode <- function(x, min_score = 60, api_key = NULL, rate = 5,
                       geometry = FALSE, crs = 4326, ...) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("bc_geocode() needs the httr2 package. Install it with ",
         'install.packages("httr2").', call. = FALSE)
  }
  x <- as.character(x)

  rows <- lapply(x, function(one) {
    if (is.na(one) || !nzchar(trimws(one))) {
      return(nar_bc_feature(list(features = list()), min_score))
    }
    req <- httr2::request(nar_bc_url())
    req <- httr2::req_url_path_append(req, "addresses.json")
    req <- httr2::req_url_query(req, addressString = one, maxResults = 1,
                                outputSRS = 4326, ...)
    req <- httr2::req_user_agent(req, "cangeocode (R package)")
    # Capacity plus a one-second fill, not the superseded `rate` argument:
    # `rate = 5` builds a 300-token bucket that lets the first 300 requests
    # through at once, which is a burst rather than a throttle. This caps the
    # burst at `rate` and then refills `rate` tokens a second. The realm is
    # named rather than derived, so the query string cannot split one service
    # into a separate pool per address.
    req <- httr2::req_throttle(req, capacity = rate, fill_time_s = 1,
                               realm = "geocoder.api.gov.bc.ca")
    # A failed lookup is data, not an exception: one unreachable address must
    # not abandon the rest of the vector, so the row comes back `none` and the
    # message is carried in bc_faults where it can be read alongside the
    # service's own faults.
    req <- httr2::req_error(req, is_error = function(resp) FALSE)
    if (!is.null(api_key)) req <- httr2::req_headers(req, apikey = api_key)

    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(resp, "error") || httr2::resp_status(resp) >= 400) {
      row <- nar_bc_feature(list(features = list()), min_score)
      row$bc_faults <- if (inherits(resp, "error")) conditionMessage(resp) else
        paste("HTTP", httr2::resp_status(resp))
      return(row)
    }
    nar_bc_feature(httr2::resp_body_json(resp), min_score)
  })

  out <- cbind(data.frame(input = x, stringsAsFactors = FALSE),
               do.call(rbind, rows))
  lon <- out$lon
  lat <- out$lat
  out <- out[, setdiff(names(out), c("lon", "lat")), drop = FALSE]

  # Straight through sf: the service answers in EPSG:4326 and sf means lon/lat
  # by that name, so there is no axis-order question to get wrong here.
  ok <- !is.na(lon) & !is.na(lat)
  pts <- sf::st_sfc(rep(list(sf::st_point()), length(lon)), crs = 4326)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i)
      sf::st_point(c(lon[i], lat[i]))), crs = 4326)
  }
  if (!is.null(crs)) pts <- sf::st_transform(pts, crs)

  if (geometry) return(sf::st_sf(out, geometry = pts))
  out$lon <- NA_real_
  out$lat <- NA_real_
  if (any(ok)) {
    co <- sf::st_coordinates(pts[ok])
    out$lon[ok] <- co[, 1]
    out$lat[ok] <- co[, 2]
  }
  out
}

#' Check NAR geocoding results against the BC Address Geocoder
#'
#' @description Re-geocodes each address with the BC service and reports how far
#' its answer sits from the one already obtained, in metres. It is the way to
#' answer a question [geocode()] cannot answer about itself -- whether a match is
#' right at all -- and where the two differ, **BC's answer is the more reliable**:
#' it is a parcel-level provincial authority, while NAR is a national compilation
#' of what provinces and municipalities supplied.
#'
#' @section What a disagreement does and does not prove: The two sources are
#' **not independent** -- BC's geocoder and NAR's BC records plausibly share
#' upstream data -- so `bc_dist_m` is not a benchmark of NAR's accuracy. Small
#' distances can be two views of one underlying record agreeing with itself, and
#' the distribution is a lower bound on how far apart genuinely independent
#' sources would sit. Use it to find suspect rows, which it does well, rather
#' than to estimate the error `uncertainty_m` excludes.
#'
#' Rows outside British Columbia are skipped rather than sent, since the service
#' does not cover them and would answer about a BC place of the same name.
#'
#' @param g A result from [geocode()], with `lon`/`lat` columns or `sf`
#' geometry.
#' @param x The address strings to send. Defaults to `g$input`.
#' @param ... Passed to [bc_geocode()], including `min_score` and `api_key`.
#' @return `g` with `bc_match_method`, `bc_score`, `bc_precision` and
#' `bc_dist_m` appended. `bc_dist_m` is `NA` where either side has no point.
#' @export
#' @examples
#' \dontrun{
#' g <- geocode(c("525 Superior St, Victoria, BC", "800 Robson St, Vancouver, BC"))
#' bc_validate(g)
#' }
bc_validate <- function(g, x = g$input, ...) {
  n <- nrow(g)
  out <- cbind(g, data.frame(bc_match_method = rep(NA_character_, n),
                             bc_score = rep(NA_integer_, n),
                             bc_precision = rep(NA_character_, n),
                             bc_dist_m = rep(NA_real_, n),
                             stringsAsFactors = FALSE))
  bc_rows <- which(!is.na(g$PROV_ABVN) & toupper(g$PROV_ABVN) == "BC" &
                     !is.na(x) & nzchar(x))
  if (!length(bc_rows)) return(out)

  b <- bc_geocode(x[bc_rows], geometry = TRUE, ...)
  out$bc_match_method[bc_rows] <- b$match_method
  out$bc_score[bc_rows]        <- b$bc_score
  out$bc_precision[bc_rows]    <- b$bc_precision

  here <- nar_geocode_points(g[bc_rows, , drop = FALSE])
  # Distance in a projected CRS, in metres. Doing it in EPSG:4326 would return
  # degrees, or silently invoke great-circle distances on an ellipsoid the two
  # sources do not share.
  both <- !sf::st_is_empty(here) & !sf::st_is_empty(sf::st_geometry(b))
  if (any(both)) {
    a <- sf::st_transform(here[both], 3347)
    z <- sf::st_transform(sf::st_geometry(b)[both], 3347)
    out$bc_dist_m[bc_rows[both]] <- as.numeric(sf::st_distance(a, z,
                                                               by_element = TRUE))
  }
  out
}

#' Recover the point geometry from a geocode() result
#'
#' @description [geocode()] returns either an `sf` object or plain `lon`/`lat`
#' columns, and [bc_validate()] has to work with whichever it is handed.
#' @param g A result from [geocode()]
#' @return An `sfc` of POINTs in EPSG:4326, empty where nothing was placed
#' @keywords internal
nar_geocode_points <- function(g) {
  if (inherits(g, "sf")) return(sf::st_transform(sf::st_geometry(g), 4326))
  if (is.null(g$lon) || is.null(g$lat)) {
    stop("`g` has no coordinates: it needs lon/lat columns or sf geometry, as ",
         "geocode() returns.", call. = FALSE)
  }
  ok <- !is.na(g$lon) & !is.na(g$lat)
  pts <- sf::st_sfc(rep(list(sf::st_point()), nrow(g)), crs = 4326)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i)
      sf::st_point(c(g$lon[i], g$lat[i]))), crs = 4326)
  }
  pts
}

#' Rebuild an address string from parsed components
#'
#' @description Both online services take a string, so the components have to be
#' re-rendered to reach them. Rebuilding rather than forwarding the original input
#' is what carries the authoritative `prov`/`mun` constraints through: those
#' overwrite the parsed columns, and a caller who asserted a municipality would
#' otherwise watch it be ignored the moment a row fell through to the fallback.
#' @param res A [normalize_address()] result
#' @return A character vector of address strings
#' @keywords internal
nar_address_string <- function(res) {
  col <- function(name) {
    v <- res[[name]]
    if (is.null(v)) rep(NA_character_, nrow(res)) else as.character(v)
  }
  # The suffix belongs to the number with no space between them: 990A is one
  # civic, and "990 A" invites the service to read the letter as a unit.
  no <- col("CIVIC_NO")
  sfx <- col("CIVIC_NO_SUFFIX")
  civic <- ifelse(is.na(no), NA_character_,
                  paste0(no, ifelse(is.na(sfx), "", sfx)))
  street <- nar_paste_parts(civic, col("STREET_NAME"), col("STREET_TYPE"),
                            col("STREET_DIR"))
  # Commas, because the service parses locality out of the tail and a
  # space-joined string invites it to read the municipality as more street.
  parts <- cbind(street, col("MUN_NAME"), col("PROV_ABVN"))
  unname(apply(parts, 1, function(r) paste(r[!is.na(r) & nzchar(r)], collapse = ", ")))
}

#' The BC Address Geocoder tier
#'
#' @description The tier behind `geocode(method = c(..., "bc"))`. Only the rows
#' its predecessors left unplaced **and** that are in British Columbia are sent
#' -- the service covers no other province, and asked about an Ontario address
#' it answers with whatever BC place shares the name.
#' @param res The parsed components, after any authoritative override
#' @param out The result so far, as [nar_geocode_match()] builds it
#' @param todo Row indices still needing a position
#' @param con A NAR connection, for the storage CRS
#' @param bounds An `sfc` in the storage CRS, or `NULL`
#' @param ... Passed to [bc_geocode()]
#' @return `out`, with the rows the service placed filled in
#' @keywords internal
nar_geocode_tier_bc <- function(res, out, todo, con, bounds = NULL, ...) {
  prov <- toupper(res$PROV_ABVN %||% rep(NA_character_, nrow(res)))
  todo <- todo[!is.na(prov[todo]) & prov[todo] == "BC"]
  if (!length(todo)) return(out)

  b <- bc_geocode(nar_address_string(res[todo, , drop = FALSE]),
                  geometry = TRUE, crs = nar_crs(con), ...)
  ok <- b$match_method != "none"

  if (any(ok) && !is.null(bounds)) {
    # `within` is authoritative for every tier, and this one runs outside the
    # database where the SQL predicate cannot reach it.
    inside <- lengths(sf::st_within(sf::st_geometry(b)[ok], bounds)) > 0
    ok[ok] <- inside
  }
  if (!any(ok)) return(out)

  co <- sf::st_coordinates(sf::st_geometry(b)[ok])
  rows <- todo[ok]
  out$x[rows] <- co[, 1]
  out$y[rows] <- co[, 2]
  out$match_method[rows] <- b$match_method[ok]
  out$uncertainty_m[rows] <- b$uncertainty_m[ok]
  # The service was asked for one result, so it reports no alternatives to
  # count. Leaving this at 1 says "one answer", not "unambiguous".
  out$n_matches[rows] <- 1L
  out
}
