#' The NRCan geolocator endpoint
#'
#' @description Kept in one place so a test can point it somewhere else. The
#' older `geogratis.gc.ca/services/geolocation` host redirects here.
#' @return A single URL
#' @keywords internal
nar_nrcan_url <- function() "https://geolocator.api.geo.ca/geolocation/en/locate"

#' What a surviving NRCan answer is worth
#'
#' @description Unlike the BC figures, this one is **measured**, on the same
#' 90th-percentile basis as the rest of the package: the distance between a
#' surviving geolocator answer and NAR's own building point, over the
#' `REPEATABLE (42)` sample `data-raw/probe_geolocator.R` draws. Two runs give
#' p90 = 115 m over 204 survivors and 152 m over 88, and the constant is the
#' **more conservative of the two** rather than the better-sampled one.
#'
#' **Do not read it as comparable to `nar_blockface`'s 176 m even though it is
#' smaller.** The two distributions have very different shapes: a blockface
#' error is bounded by the length of a blockface, while this one is a percentile
#' on a long tail -- p95 212 m, p99 648 m, worst survivor 2.7 km. Half the
#' survivors land inside 33 m and a few land in the wrong part of town, so
#' filtering on `uncertainty_m` alone treats the two tiers as interchangeable
#' when they are not. See `inst/notes/geocoding-status.md`.
#' @return A single number, metres
#' @keywords internal
nar_nrcan_uncertainty_m <- function() 150

#' Does the geolocator's answer agree with the address that was asked for?
#'
#' @description **The service always answers, and its wrong answers are
#' confident.** `1 Rue Notre-Dame Ouest, Montreal, QC` comes back as a
#' `INTERPOLATED_POSITION` on a real Rue Notre-Dame Ouest in Lorrainville, 500 km
#' away; `330 Spadina Rd, Toronto` comes back as `330 Spadina Avenue`, a
#' different street 3 km off. Neither is imprecision -- both are answers to a
#' different question, and no confidence field distinguishes them, because the
#' service publishes none.
#'
#' What separates them is that the returned `title` is itself a Canadian address
#' string, so it can be parsed by [normalize_address()] and compared to the
#' components that were sent. That is the whole floor: **the answer has to
#' re-parse to the address that was asked for.**
#'
#' The comparison is per component rather than a single [address_key()]
#' equality, because the components are not equally strict:
#'
#' * `CIVIC_NO`, `STREET_NAME` -- must be **present on both sides and equal**.
#'   These are what the query was about; a missing one means nothing was
#'   verified.
#' * `STREET_TYPE`, `STREET_DIR`, `PROV_ABVN` -- rejected only when both sides
#'   are present and **contradict**. An absent one cannot contradict anything.
#' * `MUN_NAME` -- whole-word containment either way, not equality. The service
#'   returns the incorporated name, so `TORONTO` comes back as
#'   `City Of Toronto` and `NORTH VANCOUVER` as `District Of North Vancouver`.
#'   Equality would reject both.
#'
#' Whole-word matters: comparing the municipality against the *whole* title with
#' a plain substring test -- the first thing tried -- passes
#' `28 Silver ST, CORNER BROOK` against `28 Brook Street, Corner Brook`, because
#' `Brook` appears in the street name. Field-wise comparison is what catches it.
#' @param q Parsed components of the address that was sent
#' @param t Parsed components of the returned `title`
#' @return A character vector, `NA` where the answer agrees and otherwise a
#' short reason naming the component that disagreed
#' @keywords internal
nar_nrcan_agreement <- function(q, t) {
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

#' Read the geolocator's response body
#'
#' @description Split out from the request so the response shapes can be tested
#' against saved fixtures with no network, and because **three different bodies
#' all mean "no answer"**: an empty array, and -- the one that is not obvious --
#' a JSON *object* rather than an array, which is how this service reports
#' `\{"message": "Internal server error"\}` under an HTTP **200**.
#'
#' That 500-inside-a-200 is query-dependent and reproducible rather than
#' transient: `100 Water St, Charlottetown, PE` fails where the spelled-out form
#' succeeds, and `1155 Robson Street, Vancouver, BC` fails where the abbreviated
#' form succeeds. There is no pattern to work around, so it is treated as an
#' absent answer rather than an error.
#'
#' Only the **top** result is read. The list is ranked, and a correct answer
#' further down is not distinguishable from a wrong one -- the ranking is the
#' only signal the service gives.
#' @param resp The parsed response, as [jsonlite::fromJSON()] with
#' `simplifyVector = FALSE` returns it
#' @return A one-row data frame of `nrcan_kind`, `nrcan_qualifier`,
#' `nrcan_title`, `lon` and `lat`, all `NA` when there is no answer
#' @keywords internal
nar_nrcan_top <- function(resp) {
  empty <- data.frame(nrcan_kind = NA_character_, nrcan_qualifier = NA_character_,
                      nrcan_title = NA_character_, lon = NA_real_, lat = NA_real_,
                      stringsAsFactors = FALSE)
  # A named list is a JSON object, which is the error body; an unnamed one is
  # the results array.
  if (!length(resp) || !is.null(names(resp))) return(empty)
  top <- resp[[1]]
  co <- top$geometry$coordinates
  data.frame(
    # `ca.gc.nrcan.geoloc.data.model.Street` -- the package path is constant and
    # only the leaf distinguishes Street from Geoname or Intersection.
    nrcan_kind      = sub("^.*\\.", "", top$type %||% NA_character_),
    nrcan_qualifier = top$qualifier %||% NA_character_,
    nrcan_title     = top$title %||% NA_character_,
    lon = if (length(co) == 2) as.numeric(co[[1]]) else NA_real_,
    lat = if (length(co) == 2) as.numeric(co[[2]]) else NA_real_,
    stringsAsFactors = FALSE)
}

#' Apply the match floors to a batch of geolocator answers
#'
#' @description The floors are cumulative and are applied to every row at once,
#' because the expensive half -- parsing the returned titles -- is vectorized
#' and would otherwise be paid per address.
#'
#' 1. The top result must be a **`Street`** carrying **`INTERPOLATED_POSITION`**.
#'    `INTERPOLATED_CENTROID` on the same type means "found the street, not the
#'    civic number" and is measurably worthless (p50 499 m, p90 11 km);
#'    `Geoname` answers are the service degrading to a populated place and are
#'    catastrophic (p50 133 km).
#' 2. The returned title must agree with the query, component by component --
#'    see [nar_nrcan_agreement()].
#' @section Why the title is parsed without the gazetteer: [normalize_address()]
#' is **not** given a connection here, and that is deliberate rather than an
#' oversight. The gazetteer exists to canonicalize a *caller's* loose input, and
#' turned on the service's answer it negotiates with it instead of checking it:
#' `105 Pouch Cove LINE, BAULINE, NL` comes back as a real address on Pouch Cove
#' Line in **Pouch Cove**, 4.6 km away, and a gazetteer-resolved parse of that
#' title rewrites its municipality to `BAULINE` -- the two adjacent
#' Newfoundland communities resolve to one NAR municipality -- so the floor
#' passes an answer about a different place. It was the worst survivor in the
#' sample, and parsing the title as written is what removes it.
#'
#' Nothing is lost by that: the incorporated-name case (`City Of Toronto` for
#' `TORONTO`) is handled by whole-word containment and the accent case by
#' folding, neither of which needs a database.
#' @param top The parsed top results, as [nar_nrcan_top()] returns them, rbound
#' @param q Parsed components of the addresses that were sent
#' @return `top` with `match_method`, `uncertainty_m` and `nrcan_reject` added
#' @keywords internal
nar_nrcan_floors <- function(top, q) {
  n <- nrow(top)
  reject <- rep(NA_character_, n)

  answered <- !is.na(top$nrcan_title) & !is.na(top$lon) & !is.na(top$lat)
  reject[!answered] <- "no answer"

  kind <- toupper(top$nrcan_kind %||% rep(NA_character_, n))
  qual <- toupper(top$nrcan_qualifier %||% rep(NA_character_, n))
  wrong_class <- answered & !(kind %in% "STREET" & qual %in% "INTERPOLATED_POSITION")
  # Quoted back in the service's own casing, so the reason can be matched
  # against its documentation rather than against this function's normalizing.
  reject[wrong_class] <- sprintf("top result is %s/%s",
                                 top$nrcan_kind[wrong_class],
                                 top$nrcan_qualifier[wrong_class])

  check <- which(is.na(reject))
  if (length(check)) {
    # No `con`: the title is checked as the service wrote it. See the section
    # above -- a gazetteer-resolved parse of the answer can rewrite its
    # municipality into the one that was asked for.
    t <- normalize_address(top$nrcan_title[check])
    reject[check] <- nar_nrcan_agreement(q[check, , drop = FALSE], t)
  }

  ok <- is.na(reject)
  data.frame(
    match_method  = ifelse(ok, "nrcan", "none"),
    # A rejected row is offering no point, so it carries no error bar. The
    # title and the reason survive, so what was thrown away stays visible.
    uncertainty_m = ifelse(ok, nar_nrcan_uncertainty_m(), NA_real_),
    top[, c("nrcan_kind", "nrcan_qualifier", "nrcan_title")],
    nrcan_reject  = reject,
    lon = ifelse(ok, top$lon, NA_real_),
    lat = ifelse(ok, top$lat, NA_real_),
    stringsAsFactors = FALSE)
}

#' Geocode Canadian addresses with NRCan's geolocator
#'
#' @description A binding to Natural Resources Canada's
#' [geolocator](https://geolocator.api.geo.ca/geolocation/en/locate?q=Ottawa),
#' the service behind `geo.ca`. It is **national and needs no API key and no
#' local database**, which makes it the one pathway in this package that works
#' before anything has been downloaded -- and the reason it exists here, since
#' on accuracy it is far behind the NAR tiers.
#'
#' @section What a response means: **The service always answers, and it has no
#' score.** Asked for an address it does not have, it returns a street of a
#' similar name somewhere else in Canada, or a populated place, with no field
#' distinguishing that from a hit. So the filtering is done here, and it is
#' strict: the top result must be an interpolated position on a `Street`, and
#' the address in the returned `title` must re-parse to the address that was
#' sent. `nrcan_reject` says which floor a row failed, and the title it failed
#' on is kept so the rejection can be inspected.
#'
#' Measured against NAR's own building points over a 423-address national
#' sample: **48% of the addresses sent came back placed**, with a median error
#' of 33 m and a 90th percentile of 115 m. The other 52% are not failures of the
#' service so much as answers this refuses -- 27% of everything sent came back
#' as a street centroid or a populated place, and a further 15% as a confident
#' answer about a different address. `geocode()` places 84.9% of comparable
#' input exactly, at 0 m. **This is a fallback, not a substitute**; see
#' `inst/notes/geocoding-status.md`.
#'
#' @section Reverse geocoding: The service does not offer it. There is no
#' coordinate endpoint -- `locate` answers `Missing query parameter 'q'` to a
#' `lat`/`lon` query, and the retired `geogratis` host redirects here. Use
#' [reverse_geocode()], which is NAR-backed and local.
#'
#' @section Network use and courtesy: One HTTP request per address; there is no
#' batch endpoint. Requests are throttled to `rate` per second. `httr2` is
#' required and lives in `Suggests`, so the package never contacts the network
#' unless this function is called.
#'
#' Results are subject to NRCan's terms and the Open Government Licence -- Canada.
#'
#' @param x A character vector of address strings, or a data frame of parsed
#' components as [normalize_address()] returns. Components are needed either
#' way, since the floors compare the answer against them; passing a parsed
#' frame just avoids parsing twice.
#' @param prov Optional province, passed to [normalize_address()] when `x` is a
#' character vector.
#' @param rate Requests per second, and also the largest burst allowed before
#' throttling starts. Default 5.
#' @param geometry Whether to return an `sf` object. Default `FALSE`.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param con An open NAR connection, optional. It is used only to give the
#' parse a gazetteer; the service itself needs nothing local.
#' @return A data frame with one row per input: `input`, `match_method`
#' (`"nrcan"` or `"none"`), `uncertainty_m`, `nrcan_kind`, `nrcan_qualifier`,
#' `nrcan_title`, `nrcan_reject`, and either `lon`/`lat` or an `sf` geometry
#' column.
#' @seealso [geocode()], which can run this as its last tier;
#' [bc_geocode()] for the BC-only service.
#' @export
#' @examples
#' \dontrun{
#' nrcan_geocode("100 Water Street, Charlottetown, PE")
#'
#' # What was rejected, and why: a real street, in the wrong city.
#' nrcan_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")[, c("nrcan_title",
#'                                                           "nrcan_reject")]
#' }
nrcan_geocode <- function(x, prov = NULL, rate = 5, geometry = FALSE,
                          crs = 4326, con = NULL) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("nrcan_geocode() needs the httr2 package. Install it with ",
         'install.packages("httr2").', call. = FALSE)
  }
  res <- if (is.data.frame(x)) x else normalize_address(as.character(x),
                                                        prov = prov, con = con)
  # The abbreviated spelling is deliberate. Spelling street types and provinces
  # out was measured over the same sample and moved recall by one address in
  # 139, so the expansion is not worth maintaining.
  q <- nar_address_string(res)
  input <- if (is.data.frame(x)) q else as.character(x)

  top <- do.call(rbind, lapply(q, function(one) {
    if (is.na(one) || !nzchar(trimws(one))) return(nar_nrcan_top(list()))
    req <- httr2::request(nar_nrcan_url())
    req <- httr2::req_url_query(req, q = one)
    req <- httr2::req_user_agent(req, "cangeocode (R package)")
    req <- httr2::req_timeout(req, 25)
    # `capacity` plus a one-second fill, not the superseded `rate` argument --
    # `rate = 5` builds a 300-token bucket and lets the first 300 requests go at
    # once. The realm is named so the query string cannot split one service into
    # a pool per address. Same trap as R/geocode_bc.R.
    req <- httr2::req_throttle(req, capacity = rate, fill_time_s = 1,
                               realm = "geo.ca")
    # A failed lookup is data, not an exception: one unreachable address must
    # not abandon the rest of the vector.
    req <- httr2::req_error(req, is_error = function(resp) FALSE)

    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(resp, "error") || httr2::resp_status(resp) != 200) {
      return(nar_nrcan_top(list()))
    }
    nar_nrcan_top(tryCatch(httr2::resp_body_json(resp), error = function(e) list()))
  }))

  out <- nar_nrcan_floors(top, res)
  lon <- out$lon
  lat <- out$lat
  out <- cbind(data.frame(input = input, stringsAsFactors = FALSE),
               out[, setdiff(names(out), c("lon", "lat")), drop = FALSE])

  # The service answers in EPSG:4326, and sf means lon/lat by that name, so
  # there is no axis-order question to get wrong here.
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

#' The NRCan geolocator tier
#'
#' @description The tier behind `geocode(method = c(..., "nrcan"))`. Unlike the
#' BC tier there is no province restriction to apply -- the service is national.
#' It is offered only the rows its predecessors left unplaced, and it must sit
#' last: its median answer is an order of magnitude coarser than a NAR building
#' point and its tail is far longer.
#' @param res The parsed components, after any authoritative override
#' @param out The result so far, as [nar_geocode_match()] builds it
#' @param todo Row indices still needing a position
#' @param con A NAR connection, for the storage CRS and the gazetteer
#' @param bounds An `sfc` in the storage CRS, or `NULL`
#' @param ... Passed to [nrcan_geocode()]
#' @return `out`, with the rows the service placed filled in
#' @keywords internal
nar_geocode_tier_nrcan <- function(res, out, todo, con, bounds = NULL, ...) {
  if (!length(todo)) return(out)

  g <- nrcan_geocode(res[todo, , drop = FALSE], geometry = TRUE,
                     crs = nar_crs(con), con = con, ...)
  ok <- g$match_method != "none"

  if (any(ok) && !is.null(bounds)) {
    # `within` is authoritative for every tier, and this one runs outside the
    # database where the SQL predicate cannot reach it.
    inside <- lengths(sf::st_within(sf::st_geometry(g)[ok], bounds)) > 0
    ok[ok] <- inside
  }
  if (!any(ok)) return(out)

  co <- sf::st_coordinates(sf::st_geometry(g)[ok])
  rows <- todo[ok]
  out$x[rows] <- co[, 1]
  out$y[rows] <- co[, 2]
  out$match_method[rows] <- g$match_method[ok]
  out$uncertainty_m[rows] <- g$uncertainty_m[ok]
  # The service ranks its results and this tier reads only the top one, so
  # there are no alternatives to count. 1 says "one answer", not "unambiguous".
  out$n_matches[rows] <- 1L
  out
}
