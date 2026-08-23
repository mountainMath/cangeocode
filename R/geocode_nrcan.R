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


#' Is this response a transient failure worth re-sending?
#'
#' @description **The service loses roughly one request in twelve, and says so
#' with a clean HTTP 500.** Measured over 300 national addresses: 24 came back
#' 500, and every one of the 24 succeeded on a retry -- 23 of them on the very
#' first, with no delay. One query succeeded on the first attempt and then
#' failed three times in a row afterwards, which is what rules out the failure
#' being a property of the query. The 500s also fast-fail, at a 0.23 s median
#' against 0.59 s for a real answer, so they are not timeouts either. Left
#' unretried they were silently costing about 8% of recall, indistinguishable in
#' the output from the service having no answer.
#'
#' Two shapes count as transient:
#'
#' * **any 5xx**, which is the one above, plus `429` for rate limiting on the
#'   chance the service ever starts sending it;
#' * a **`200` whose body is a JSON object rather than an array**, which is the
#'   same server error escaping through a gateway that did not notice. No
#'   current query is known to produce one, but if it comes back it is the same
#'   fault and deserves the same response.
#'
#' A `200` carrying an empty array is **not** transient. That is the service
#' answering "nothing", which is a legitimate answer and not distinguishable
#' from a lost one anyway.
#' @param resp A response object
#' @return `TRUE` if the request is worth re-sending
#' @keywords internal
nar_nrcan_transient <- function(resp) {
  status <- httr2::resp_status(resp)
  if (status >= 500 || status == 429) return(TRUE)
  if (status != 200) return(FALSE)
  # A named list is a JSON object, which for this service is an error body.
  body <- tryCatch(httr2::resp_body_json(resp), error = function(e) NULL)
  length(body) > 0 && !is.null(names(body))
}

#' Read every candidate out of the geolocator's response body
#'
#' @description Split out from the request so the response shapes can be tested
#' against saved fixtures with no network, and because **two different bodies
#' both mean "no answer"**: an empty array, and -- the one that is not obvious
#' -- a JSON *object* rather than an array, which is how this service reports
#' `\{"message": "Internal server error"\}` under an HTTP **200**. That object
#' body belongs to the backend retired in late 2025 and no current query is
#' known to produce one; the guard stays because the handler still has an
#' exception path that returns an object, and whether that surfaces as a real
#' 500 depends on a gateway configuration not visible from outside.
#'
#' **Every result is returned, not only the first.** The list is ranked, but the
#' ranking is not the signal it looks like: [nar_nrcan_floors()] is what
#' separates an answer from a substitution, it is independent of rank, and the
#' candidate that passes it is frequently not the one ranked first. Asked for
#' `1 Rue Notre-Dame Ouest, Montreal, QC` the service ranks Lorrainville first
#' and Montreal seventh; asked for `330 Spadina Rd, Toronto` it ranks Spadina
#' *Avenue* first and Spadina Road seventh. Both correct answers are in the
#' response body already, so scanning the list costs **no additional request**.
#'
#' The service hoists only the *first* `INTERPOLATED_POSITION` to the top
#' (`move_first_interpolated_to_top()` in its own source); every other result
#' keeps its relevance-order position, which is why the rest are worth reading.
#' See `inst/notes/nrcan-geolocator.md`.
#' @param resp The parsed response, as [jsonlite::fromJSON()] with
#' `simplifyVector = FALSE` returns it
#' @return A data frame of `nrcan_kind`, `nrcan_qualifier`, `nrcan_title`, `lon`
#' and `lat`, one row per result **in the order the service ranked them**, and
#' **no rows at all** when there is no answer
#' @keywords internal
nar_nrcan_candidates <- function(resp) {
  empty <- data.frame(nrcan_kind = character(), nrcan_qualifier = character(),
                      nrcan_title = character(), lon = numeric(),
                      lat = numeric(), stringsAsFactors = FALSE)
  # A named list is a JSON object, which is the error body; an unnamed one is
  # the results array.
  if (!length(resp) || !is.null(names(resp))) return(empty)
  do.call(rbind, lapply(resp, function(r) {
    co <- r$geometry$coordinates
    data.frame(
      # `ca.gc.nrcan.geoloc.data.model.Street` -- the package path is constant
      # and only the leaf distinguishes Street from Geoname or Intersection.
      nrcan_kind      = sub("^.*\\.", "", r$type %||% NA_character_),
      nrcan_qualifier = r$qualifier %||% NA_character_,
      nrcan_title     = r$title %||% NA_character_,
      lon = if (length(co) == 2) as.numeric(co[[1]]) else NA_real_,
      lat = if (length(co) == 2) as.numeric(co[[2]]) else NA_real_,
      stringsAsFactors = FALSE)
  }))
}

#' Apply the match floors and choose one answer per address
#'
#' @description The floors are cumulative and are applied to every candidate of
#' every address at once, because the expensive half -- parsing the returned
#' titles -- is vectorized and would otherwise be paid per candidate.
#'
#' 1. A candidate must be a **`Street`** carrying **`INTERPOLATED_POSITION`**.
#'    `INTERPOLATED_CENTROID` on the same type means "found the street, not the
#'    civic number" and is measurably worthless (p50 499 m, p90 11 km);
#'    `Geoname` answers are the service degrading to a populated place and are
#'    catastrophic (p50 133 km).
#' 2. Its title must agree with the query, component by component -- see
#'    [nar_address_agreement()].
#'
#' @section Why the whole list is scanned: The floor is what makes an answer
#' trustworthy, and it does not consult the rank. A candidate at position 7 that
#' passes is exactly as verified as one at position 1 that passes, so stopping
#' at the first result throws away recall for nothing. On the two cases this
#' package documents as its own examples of a confident wrong answer -- Rue
#' Notre-Dame Ouest and Spadina Rd -- the floor accepts exactly one of the 25
#' candidates and rejects every other, and in both cases the one it accepts is
#' not the one ranked first.
#'
#' Because there can now be more than one survivor, the count of them is
#' reported as `n_matches`: two candidates passing means the same street name in
#' two municipalities that both satisfy containment, which is a real ambiguity
#' and not a detail to hide. The highest-ranked survivor is the one returned.
#'
#' A rejected address reports the reason its **best** candidate failed, not its
#' first: the highest-ranked interpolated position if there was one, and
#' otherwise the class of the highest-ranked usable result. Reporting the top
#' result's reason once the rest are being read would describe something that
#' was not what got rejected.
#'
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
#' @param cand The candidates, as [nar_nrcan_candidates()] returns them, rbound
#' across every address. Within one address they must stay in the order the
#' service ranked them, since that is what "best" is read from.
#' @param q Parsed components of the addresses that were sent, one row each
#' @param idx For each row of `cand`, the row of `q` it answers. The default
#' says every candidate belongs to a single address, which is the shape a test
#' or a one-address probe has.
#' @param failed A logical vector over the rows of `q`, `TRUE` where the request
#' never completed. Those rows report `request failed` rather than `no answer`:
#' the service losing a request and the service having nothing to say are both
#' zero candidates, but only one of them is about the address, and a coverage
#' figure that folds them together understates what the tier can do.
#' @return A data frame with one row per row of `q`: `match_method`,
#' `uncertainty_m`, `n_matches`, the `nrcan_*` columns of the chosen candidate,
#' `nrcan_reject`, `lon` and `lat`
#' @keywords internal
nar_nrcan_floors <- function(cand, q, idx = rep(1L, nrow(cand)),
                             failed = rep(FALSE, nrow(q))) {
  n <- nrow(q)
  m <- nrow(cand)

  usable <- !is.na(cand$nrcan_title) & !is.na(cand$lon) & !is.na(cand$lat)
  class_ok <- usable &
    toupper(cand$nrcan_kind) %in% "STREET" &
    toupper(cand$nrcan_qualifier) %in% "INTERPOLATED_POSITION"

  reason <- rep(NA_character_, m)
  chk <- which(class_ok)
  if (length(chk)) {
    # No `con`: the title is checked as the service wrote it. See the section
    # above -- a gazetteer-resolved parse of the answer can rewrite its
    # municipality into the one that was asked for.
    t <- normalize_address(cand$nrcan_title[chk])
    reason[chk] <- nar_address_agreement(q[idx[chk], , drop = FALSE], t)
  }
  pass <- class_ok & is.na(reason)

  # The row of `cand` that is the first to satisfy `ok` for each address, and
  # `NA` where none does. Candidates keep the service's ranking within an
  # address, so the first to satisfy it is the best-ranked to satisfy it.
  first_of <- function(ok) {
    i <- which(ok)
    g <- idx[i]
    keep <- !duplicated(g)
    out <- rep(NA_integer_, n)
    out[g[keep]] <- i[keep]
    out
  }
  hit  <- first_of(pass)      # accepted
  near <- first_of(class_ok)  # got past the class floor and then disagreed
  any_ <- first_of(usable)    # answered at all
  ok <- !is.na(hit)
  sel <- ifelse(ok, hit, ifelse(is.na(near), any_, near))

  # Weakest reason first, so each better-informed one overwrites it.
  reject <- rep("no answer", n)
  # A lost request is not an answer of any kind, and cannot be overwritten by
  # one -- a failed row has no candidates for the rules below to look at.
  reject[failed] <- "request failed"
  seen <- !is.na(any_)
  # Quoted back in the service's own casing, so the reason can be matched
  # against its documentation rather than against this function's normalizing.
  reject[seen] <- sprintf("best result is %s/%s", cand$nrcan_kind[any_[seen]],
                          cand$nrcan_qualifier[any_[seen]])
  got <- !is.na(near)
  reject[got] <- reason[near[got]]
  reject[ok] <- NA_character_

  # `sel` is NA where nothing answered, and indexing by NA yields NA of the
  # right type even when `cand` has no rows at all.
  data.frame(
    match_method  = ifelse(ok, "nrcan", "none"),
    # A rejected row is offering no point, so it carries no error bar. The
    # title and the reason survive, so what was thrown away stays visible.
    uncertainty_m = ifelse(ok, nar_nrcan_uncertainty_m(), NA_real_),
    n_matches     = tabulate(idx[pass], nbins = n),
    nrcan_kind      = cand$nrcan_kind[sel],
    nrcan_qualifier = cand$nrcan_qualifier[sel],
    nrcan_title     = cand$nrcan_title[sel],
    nrcan_reject  = reject,
    lon = ifelse(ok, cand$lon[sel], NA_real_),
    lat = ifelse(ok, cand$lat[sel], NA_real_),
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
#' strict: a result must be an interpolated position on a `Street`, and the
#' address in its `title` must re-parse to the address that was sent.
#' `nrcan_reject` says which floor a row failed, and the title it failed on is
#' kept so the rejection can be inspected.
#'
#' The floors are applied to **every** result the service returned, not just the
#' one it ranked first, because the ranking and the floor answer different
#' questions -- see [nar_nrcan_floors()]. The service returns up to 25 results
#' in one response, so this costs no extra request. `n_matches` counts how many
#' of them passed; the best-ranked one is the row that is returned.
#'
#' Measured against NAR's own building points over a 423-address national
#' sample: **48% of the addresses sent came back placed**, with a median error
#' of 33 m and a 90th percentile of 115 m. The other 52% are not failures of the
#' service so much as answers this refuses -- 27% of everything sent came back
#' as a street centroid or a populated place, and a further 15% as a confident
#' answer about a different address. `geocode()` places 84.9% of comparable
#' input exactly, at 0 m. **This is a fallback, not a substitute**; see
#' `inst/notes/geocoding-status.md`. That sample predates the scan of the whole
#' result list, which is worth about a further point of recall.
#'
#' Addresses with a civic-number **suffix** are a special case worth knowing
#' about: the service cannot see a house number in `990A` at all, so the suffix
#' is dropped from the query. Measured over 20 suffixed NAR points, that moves
#' them from 0 placed to 16.
#'
#' @section Reverse geocoding: The service does not offer it. There is no
#' coordinate endpoint -- `locate` answers `Missing query parameter 'q'` to a
#' `lat`/`lon` query, and the retired `geogratis` host redirects here. Use
#' [reverse_geocode()], which is NAR-backed and local.
#'
#' @section Network use and courtesy: One HTTP request per address; there is no
#' batch endpoint. Requests are throttled to `rate` per second, and the ones the
#' service drops are re-sent up to `retries` times with httr2's exponential
#' backoff, so a run makes slightly more requests than it has addresses.
#' `httr2` is required and lives in `Suggests`, so the package never contacts
#' the network unless this function is called.
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
#' @param retries How many times to send an address before giving up on it.
#' Default 3, and `1` disables retrying. The service loses roughly one request
#' in twelve to a transient HTTP 500 that a single re-send almost always fixes
#' -- see [nar_nrcan_transient()] -- so this is worth about 8 points of
#' coverage, not a rounding error. Rows that exhaust their retries report
#' `request failed` in `nrcan_reject` rather than `no answer`.
#' @param geometry Whether to return an `sf` object. Default `FALSE`.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param con An open NAR connection, optional. It is used only to give the
#' parse a gazetteer; the service itself needs nothing local.
#' @return A data frame with one row per input: `input`, `match_method`
#' (`"nrcan"` or `"none"`), `uncertainty_m`, `n_matches`, `nrcan_kind`,
#' `nrcan_qualifier`, `nrcan_title`, `nrcan_reject`, and either `lon`/`lat` or
#' an `sf` geometry column.
#' @seealso [geocode()], which can run this as its last tier;
#' [bc_geocode()] for the BC-only service.
#' @export
#' @examples
#' \dontrun{
#' nrcan_geocode("100 Water Street, Charlottetown, PE")
#'
#' # What was rejected, and why: the street was found, the civic number was not.
#' nrcan_geocode("1155 Robson Street, Vancouver, BC")[, c("nrcan_title",
#'                                                        "nrcan_reject")]
#'
#' # The service ranks a Rue Notre-Dame Ouest in Lorrainville first, 500 km
#' # away; the one in Montreal is seventh in the same response, and the floor
#' # is what tells them apart.
#' nrcan_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")$nrcan_title
#' }
nrcan_geocode <- function(x, prov = NULL, rate = 5, retries = 3,
                          geometry = FALSE, crs = 4326, con = NULL) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("nrcan_geocode() needs the httr2 package. Install it with ",
         'install.packages("httr2").', call. = FALSE)
  }
  res <- if (is.data.frame(x)) x else normalize_address(as.character(x),
                                                        prov = prov, con = con)
  # The abbreviated spelling is deliberate. Spelling street types and provinces
  # out was measured over the same sample and moved recall by one address in
  # 139, so the expansion is not worth maintaining.
  input <- if (is.data.frame(x)) nar_address_string(res) else as.character(x)
  # The civic suffix is dropped from the query and only from the query. The
  # service finds the house number in the string with `\b(\d{1,5})\b`, and
  # there is no word boundary between the `0` and the `A` of `990A`, so a
  # suffixed civic never reaches its interpolator at all and comes back as a
  # street centroid this then rejects. `990` interpolates. Nothing is laundered:
  # the floor compares `CIVIC_NO`, which never carried the suffix in the first
  # place, and `input` still echoes the address as it was given. See
  # `inst/notes/nrcan-geolocator.md`.
  q <- nar_address_string(res, suffix = FALSE)

  # `TRUE` where the request never came back, which is not the same as coming
  # back empty. Filled in by the loop below.
  failed <- rep(FALSE, length(q))

  per <- lapply(seq_along(q), function(i) {
    one <- q[[i]]
    if (is.na(one) || !nzchar(trimws(one))) return(nar_nrcan_candidates(list()))
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
    # not abandon the rest of the vector. This is set BEFORE req_retry() and
    # does not disable it -- `is_transient` is consulted independently of
    # `is_error`, so the retry still happens and only the final answer comes
    # back as data rather than a condition.
    req <- httr2::req_error(req, is_error = function(resp) FALSE)
    # Re-send the ~8% of requests this service drops. httr2 waits
    # `runif(1, 1, 2^tries)` seconds between attempts, which is more patience
    # than the measurement says is needed -- the failures recover immediately --
    # but backing off is the right thing to do to a service that is telling you
    # it is struggling.
    if (retries > 1) {
      req <- httr2::req_retry(req, max_tries = retries,
                              is_transient = nar_nrcan_transient,
                              retry_on_failure = TRUE)
    }

    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(resp, "error") || httr2::resp_status(resp) != 200) {
      failed[[i]] <<- TRUE
      return(nar_nrcan_candidates(list()))
    }
    body <- tryCatch(httr2::resp_body_json(resp), error = function(e) list())
    # A 200 that is still an object body after every retry is a server error the
    # gateway did not label, not an empty answer.
    if (length(body) && !is.null(names(body))) failed[[i]] <<- TRUE
    nar_nrcan_candidates(body)
  })

  # An address contributes as many rows as it got results, and none if it got
  # none, so `idx` is what puts a candidate back with the address it answers.
  # `rbind` over an empty list is NULL rather than a frame, hence the fallback.
  cand <- if (length(per)) do.call(rbind, per) else NULL
  if (is.null(cand)) cand <- nar_nrcan_candidates(list())
  idx <- rep(seq_along(per), vapply(per, nrow, integer(1)))

  out <- nar_nrcan_floors(cand, res, idx, failed)
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
  # Every result in the response is put through the floors, so more than one can
  # survive -- the same street name in two municipalities that both satisfy
  # containment. That is a real ambiguity and is counted as one.
  out$n_matches[rows] <- g$n_matches[ok]
  out
}
