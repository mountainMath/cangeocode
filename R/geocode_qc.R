#' The Quebec address geocoder endpoint
#'
#' @description Kept in one place so a test can point it somewhere else and so
#' the URL is not repeated across the request builders. Published as a CC-BY
#' resource of the `Adresses Québec` dataset on Données Québec.
#' @return A single URL
#' @keywords internal
nar_qc_url <- function() {
  paste0("https://servicescarto.mrnf.gouv.qc.ca/pes/rest/services/Territoire/",
         "Adresse_Geocodage/GeocodeServer")
}

#' What each Quebec locator is worth
#'
#' @description The service is an Esri `GeocodeServer` with two locators behind
#' it, and **`Loc_name` is the field that says which one answered** --
#' `RQA_Adresse` resolved a civic number, `RQA_Rue` only found the street. This
#' table maps that onto the same `match_method` and `uncertainty_m` contract
#' [geocode()] uses.
#'
#' **`Addr_type` is not the precision field here, and `Score` is not a ranking
#' of it.** `Addr_type` comes back as `Feature` for both locators, so it
#' separates nothing. And the score measures how much of the string that was
#' *sent* was consumed, not how much of the address was resolved. Over the same
#' 400 Quebec addresses [nar_qc_query()] was measured on, the correlation
#' between score and how far the answer landed from NAR's building point is
#' **Spearman 0.018** -- none (`data-raw/probe_qc.R`, `PROBE_PART=agree`). Worse, street-only answers score *higher* than
#' civic ones: civic matches ran 75.9 to 86.2 with a median of 83.0, street
#' centroids 75.8 to 95.2 with a median of 87.0. Ranking by score, or gating on
#' it, removes correct addresses and keeps street centroids, which is why
#' `min_score` defaults to off. Read `Loc_name`, then the floor.
#'
#' The metres are the same order-of-magnitude figures [nar_bc_precision()]
#' uses and are **not measured** -- they are a ranking safe to filter on, not
#' an error bar comparable to the NAR tiers'.
#' @param loc_name The `Loc_name` attribute of a result
#' @return A one-row data frame of `match_method` and `uncertainty_m`
#' @keywords internal
nar_qc_precision <- function(loc_name) {
  tab <- list(
    # A point on a specific address in the Répertoire québécois des adresses.
    RQA_ADRESSE = c("qc_address", 20),
    # The street was found and the civic number was not: an answer about a
    # street rather than about an address, priced like BC's `bc_street`.
    RQA_RUE     = c("qc_street", 500)
  )
  hit <- tab[[toupper(loc_name %||% "")]]
  if (is.null(hit)) hit <- c("none", NA)
  data.frame(match_method = hit[1], uncertainty_m = as.numeric(hit[2]),
             stringsAsFactors = FALSE)
}

#' Render parsed components the way the Quebec locator expects them
#'
#' @description [nar_address_string()] renders the NAR canonical form -- street
#' type *after* the name, direction abbreviated -- and this service will not
#' read it. It is an Esri locator built on the Répertoire québécois des
#' adresses, whose reference strings are French-canonical: `Rue Notre-Dame
#' Ouest`, type first and direction spelled out. Sent the NAR form the locator
#' does not degrade gracefully; it stops matching.
#'
#' Measured by `data-raw/probe_qc.R` (`PROBE_PART=render`) on 400 NAR Quebec
#' addresses with building points, half of them carrying a direction, by share
#' resolved to a civic point:
#'
#' | rendering | civic | street only | unmatched |
#' | --- | --- | --- | --- |
#' | `NOTRE-DAME RUE O` -- [nar_address_string()] | 31.5% | 4.0% | 64.5% |
#' | NAR order, direction spelled out | 58.0% | 3.0% | 39.0% |
#' | FR order, direction spelled out | 58.8% | 14.2% | 27.0% |
#' | NAR order, type and direction spelled out | 95.0% | 3.5% | 1.5% |
#' | **FR order, type and direction** -- this function | **95.5%** | **3.5%** | **1.0%** |
#'
#' So **the abbreviations are what break it, and the word order barely
#' matters**: spelling out the direction is worth 26 points, spelling out the
#' street type another 37, and the order under one. The order is used anyway
#' because it costs nothing and it is the form the service answers in, which
#' keeps the floor comparing like with like.
#'
#' The failure is also silent in the worst way. `1 RUE NOTRE-DAME O, MONTREAL`
#' returns a *street centroid* scoring 92.4 where the correct civic point
#' scores 82.5, so the abbreviated form does not merely lose the address, it
#' replaces it with a confident wrong answer several hundred metres away.
#'
#' Both expansions are French: the direction table below, and the street type
#' from `nar_lex_types` by taking the longest French surface for each
#' canonical (`BOUL` to `BOULEVARD`, `CH` to `CHEMIN`). A canonical with no
#' French surface is sent unchanged.
#' @param res Parsed components, as [normalize_address()] returns
#' @return A character vector of query strings, one per row
#' @keywords internal
nar_qc_query <- function(res) {
  col <- function(name) {
    v <- res[[name]]
    if (is.null(v)) rep(NA_character_, nrow(res)) else as.character(v)
  }
  expand <- function(x, tab) {
    hit <- tab[toupper(x)]
    ifelse(is.na(x), NA_character_, ifelse(is.na(hit), x, unname(hit)))
  }
  # As in nar_address_string(): the suffix is part of the number, not a unit.
  no <- col("CIVIC_NO")
  sfx <- col("CIVIC_NO_SUFFIX")
  civic <- ifelse(is.na(no), NA_character_,
                  paste0(no, ifelse(is.na(sfx), "", sfx)))

  street <- nar_paste_parts(civic, expand(col("STREET_TYPE"), nar_qc_types()),
                            col("STREET_NAME"),
                            expand(col("STREET_DIR"), nar_qc_dirs()))
  parts <- cbind(street, col("MUN_NAME"), col("PROV_ABVN"))
  unname(apply(parts, 1, function(r) paste(r[!is.na(r) & nzchar(r)], collapse = ", ")))
}

#' French long forms for the street directions
#'
#' @description Written out rather than derived from `nar_lex_dirs`, because
#' picking the longest French surface per canonical is ambiguous exactly where
#' it matters: `SO` has `SUD-OUEST` and `SOUTHWEST` at nine characters each.
#' The English canonicals are here too, since a Quebec row parsed from an
#' English surface form carries `W` rather than `O`.
#' @return A named character vector, canonical to French long form
#' @keywords internal
nar_qc_dirs <- function() {
  c(N = "NORD", S = "SUD", E = "EST", O = "OUEST", W = "OUEST",
    NE = "NORD-EST", NO = "NORD-OUEST", NW = "NORD-OUEST",
    SE = "SUD-EST", SO = "SUD-OUEST", SW = "SUD-OUEST")
}

#' French long forms for the street types
#'
#' @description Derived from `nar_lex_types` rather than written out, so a type
#' added to the lexicon is expanded here without a second edit. For each
#' canonical, the longest surface marked French: an abbreviation is always
#' shorter than the word it abbreviates, so the longest surface is the spelled
#' out one. Accents fall out of the lexicon's own unaccented surfaces
#' (`MONTEE`, `COTE`), which the service reads either way.
#' @return A named character vector, canonical to French long form
#' @keywords internal
nar_qc_types <- function() {
  if (!is.null(nar_qc_type_cache$tab)) return(nar_qc_type_cache$tab)
  lex <- nar_lex_types
  lex <- lex[lex$lang %in% c("fr", "both"), c("canonical", "surface")]
  lex <- lex[order(lex$canonical, -nchar(lex$surface)), ]
  lex <- lex[!duplicated(lex$canonical), ]
  tab <- stats::setNames(lex$surface, lex$canonical)
  nar_qc_type_cache$tab <- tab
  tab
}

nar_qc_type_cache <- new.env(parent = emptyenv())

#' Read one batch response into result rows
#'
#' @description Split out from the request so the response shape can be tested
#' against a saved fixture with no network.
#'
#' Two things about this response are easy to get wrong and are handled here.
#' **The service answers out of order** -- send three addresses and the
#' locations come back 3, 1, 2 -- so rows are placed by their `ResultID` rather
#' than by position, and an id the service dropped stays an unmatched row.
#' And **the coordinates are read from `location`, never from the `Latitude`
#' and `Longitude` attributes**: those are rendered in the service's French
#' locale with a comma for the decimal mark (`"45,5061613986714"`), which
#' `as.numeric()` turns into `NA` on a good day and into a different number on
#' a bad one, and they are empty for a street-level match whose `location` is
#' populated.
#' @param resp The parsed response, as [jsonlite::fromJSON()] with
#' `simplifyVector = FALSE` returns it
#' @param n How many addresses were sent, so dropped ids still get a row
#' @return A data frame of `n` rows in the order the addresses were sent
#' @keywords internal
nar_qc_locations <- function(resp, n) {
  out <- data.frame(
    qc_locator = rep(NA_character_, n),
    qc_score   = rep(NA_real_, n),
    qc_status  = rep(NA_character_, n),
    qc_address = rep(NA_character_, n),
    qc_postal  = rep(NA_character_, n),
    lon        = rep(NA_real_, n),
    lat        = rep(NA_real_, n),
    stringsAsFactors = FALSE)

  for (loc in resp$locations %||% list()) {
    a <- loc$attributes %||% list()
    i <- suppressWarnings(as.integer(a$ResultID %||% NA))
    if (is.na(i) || i < 1L || i > n) next
    # `Status` is "M" for matched and "U" for unmatched; an unmatched row
    # carries a score of 0 and empty strings, which would otherwise read as a
    # locator named "".
    st <- toupper(a$Status %||% "")
    out$qc_status[i] <- if (nzchar(st)) st else NA_character_
    if (!identical(st, "M")) next

    nz <- function(v) if (is.null(v) || !nzchar(v)) NA_character_ else v
    out$qc_locator[i] <- nz(a$Loc_name)
    out$qc_score[i]   <- suppressWarnings(as.numeric(a$Score %||% NA))
    out$qc_address[i] <- nz(a$Match_addr)
    out$qc_postal[i]  <- nz(a$ZIP)
    co <- loc$location
    if (!is.null(co$x) && !is.null(co$y)) {
      out$lon[i] <- as.numeric(co$x)
      out$lat[i] <- as.numeric(co$y)
    }
  }
  out
}

#' Apply the floors to the Quebec geocoder's answers
#'
#' @description Two floors, in the order they are cheapest to apply. First the
#' locator: only `RQA_Adresse` resolved a civic number, so only it can be an
#' address match -- see [nar_qc_precision()]. Then the shared agreement test,
#' [nar_address_agreement()], because the service answers with the address it
#' matched and that answer can be put back into components and compared to the
#' ones that were sent.
#'
#' @section Why the answer is parsed without the gazetteer: the same reason
#' [nar_nrcan_floors()] does it, and it is not an oversight. The gazetteer
#' exists to canonicalize a *caller's* loose input; letting it near the
#' *service's* answer lets it rewrite that answer into the address that was
#' asked for, which launders exactly the error the floor exists to catch.
#'
#' @section What this service does that the others do not: it **refuses**.
#' Asked for `Montreal` alone, or for a street that does not exist, it returns
#' `Status = "U"` and no point rather than degrading to a locality centroid the
#' way the BC geocoder and the geolocator both do. So an answer here is already
#' evidence, and the floors reject far less than they do for `nrcan`.
#' @param loc Rows as [nar_qc_locations()] returns them
#' @param q Parsed components of the addresses that were sent
#' @return `loc` with `match_method`, `uncertainty_m` and `qc_reject` added
#' @keywords internal
nar_qc_floors <- function(loc, q) {
  n <- nrow(loc)
  prec <- do.call(rbind, lapply(loc$qc_locator, nar_qc_precision))
  out <- cbind(loc, prec, qc_reject = rep(NA_character_, n),
               stringsAsFactors = FALSE)

  placed <- !is.na(loc$lon) & !is.na(loc$lat)
  # Only a civic-level answer is checked against the address: a street-level
  # one has no civic number to agree with and would fail the floor for a
  # reason that is already stated by its `match_method`.
  chk <- which(out$match_method == "qc_address" & placed)
  if (length(chk)) {
    # No `con`: the answer is checked as the service wrote it.
    t <- normalize_address(loc$qc_address[chk])
    out$qc_reject[chk] <- nar_address_agreement(q[chk, , drop = FALSE], t)
  }

  # Weakest reason first, so each better-informed one overwrites it.
  out$qc_reject[is.na(out$qc_status)] <- "no answer"
  out$qc_reject[!is.na(out$qc_status) & out$qc_status == "U"] <- "unmatched"
  street <- out$match_method == "qc_street"
  out$qc_reject[street] <- "street only, no civic number"
  out$qc_reject[!placed & !is.na(out$qc_status) & out$qc_status == "M"] <-
    "matched with no point"

  failed <- !is.na(out$qc_reject)
  out$match_method[failed] <- "none"
  # A rejected row is offering no point, so it carries no error bar. The
  # matched address and the reason survive, so what was thrown away stays
  # visible.
  out$uncertainty_m[failed] <- NA_real_
  out$lon[failed] <- NA_real_
  out$lat[failed] <- NA_real_
  out
}

#' Send one batch of addresses to the Quebec geocoder
#'
#' @description One HTTP request for up to `MaxBatchSize` addresses. A failed
#' request is data, not an exception: the batch comes back as unanswered rows
#' so one unreachable request does not abandon the rest of the vector.
#' @param q Address strings, at most 1000
#' @param rate Requests per second
#' @param crs Output SRS to ask the service for
#' @return A data frame as [nar_qc_locations()] returns
#' @keywords internal
nar_qc_batch <- function(q, rate = 5, crs = 4326) {
  n <- length(q)
  empty <- nar_qc_locations(list(), n)
  send <- which(!is.na(q) & nzchar(trimws(q)))
  if (!length(send)) return(empty)

  recs <- lapply(send, function(i) {
    list(attributes = list(OBJECTID = i, SingleLine = unname(q[[i]])))
  })
  body <- jsonlite::toJSON(list(records = recs), auto_unbox = TRUE)

  req <- httr2::request(nar_qc_url())
  req <- httr2::req_url_path_append(req, "geocodeAddresses")
  # POST rather than GET: 1000 single-line addresses do not fit in a URL.
  req <- httr2::req_body_form(req, addresses = as.character(body), f = "json",
                              outSR = as.character(crs))
  req <- httr2::req_user_agent(req, "cangeocode (R package)")
  req <- httr2::req_timeout(req, 120)
  # Capacity plus a one-second fill, not the superseded `rate` argument -- see
  # the same trap in R/geocode_bc.R and R/geocode_nrcan.R.
  req <- httr2::req_throttle(req, capacity = rate, fill_time_s = 1,
                             realm = "servicescarto.mrnf.gouv.qc.ca")
  req <- httr2::req_error(req, is_error = function(resp) FALSE)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
  if (inherits(resp, "error") || httr2::resp_status(resp) >= 400) return(empty)
  parsed <- tryCatch(httr2::resp_body_json(resp), error = function(e) NULL)
  if (is.null(parsed)) return(empty)
  nar_qc_locations(parsed, n)
}

#' Geocode Quebec addresses with the Quebec government geocoder
#'
#' @description A binding to the Ministère des Ressources naturelles et des
#' Forêts' [address geocoder](https://www.donneesquebec.ca/recherche/dataset/adresses-quebec),
#' the Esri `GeocodeServer` published as a CC-BY resource of `Adresses Québec`.
#' It covers Quebec only, and complements the NAR pathway the way
#' [bc_geocode()] does for British Columbia: as a fallback for Quebec addresses
#' [geocode()] cannot place, and as a second positional source to check NAR
#' against -- see [qc_validate()].
#'
#' @section What a response means: **unlike the other two online services, this
#' one refuses.** Asked for `Montreal` alone, or for a street that does not
#' exist, it answers `Status = "U"` with no point instead of degrading to a
#' locality centroid. So a response here is already evidence. What it still
#' does is answer about a *street* when it cannot resolve the civic number, and
#' `Loc_name` is the field that separates the two: `qc_address` resolved the
#' address, `qc_street` found only the street. `qc_reject` says which floor a
#' row failed.
#'
#' **Do not rank by `qc_score`.** The score measures the returned string
#' against the string that was sent, not how much of the address was resolved,
#' and the correct civic point routinely scores *below* a street centroid for
#' the same query -- 84.9 against 98.5 for `1 Rue Notre-Dame Ouest, Montreal`.
#' `min_score` is offered because a genuinely poor match does score poorly, but
#' it defaults to `0` and the locator plus the agreement floor are what decide.
#'
#' It also **normalizes**: given unaccented, loosely typed input it returns the
#' canonical accented form with the street type and cardinal point in Quebec's
#' own vocabulary, plus the postal code. That is what makes [qc_validate()] a
#' check on the address and not only on the coordinate.
#'
#' @section Network use and courtesy: **This service batches**, up to 1000
#' addresses per request, which is why it is the only online binding here that
#' does not cost one request per address -- 5,000 addresses is 5 requests, not
#' 5,000. Requests are throttled to `rate` per second. `httr2` and `jsonlite`
#' are required and live in `Suggests`, so the package never contacts the
#' network unless this function is called.
#'
#' The batch endpoint is used even for a single address, because it is the only
#' one that populates `Loc_name`: `findAddressCandidates` returns the field
#' empty, which would leave a civic match and a street centroid
#' indistinguishable.
#'
#' Results are subject to the Government of Quebec's terms and the Creative
#' Commons Attribution 4.0 licence.
#'
#' @param x A character vector of address strings, or a data frame of parsed
#' components as [normalize_address()] returns. Components are needed either
#' way, since the floor compares the answer against them; passing a parsed
#' frame just avoids parsing twice.
#' @param prov Optional province, passed to [normalize_address()] when `x` is a
#' character vector.
#' @param min_score Minimum `qc_score`, 0--100, for a result to count. Default
#' `0`, which is off, and **it should usually stay off**: the score does not
#' rank positional quality, and street centroids outscore civic matches -- see
#' [nar_qc_precision()] for the measurement. It is here to reproduce a
#' published threshold, not because one is recommended.
#' @param batch_size Addresses per request, at most 1000, which is the
#' service's own `MaxBatchSize`.
#' @param rate Requests per second. Default 5.
#' @param geometry Whether to return an `sf` object. Default `FALSE`.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param con An open NAR connection, optional. It is used only to give the
#' parse of the *caller's* input a gazetteer; the service itself needs nothing
#' local, and the parse of the service's own answer never gets one.
#' @return A data frame with one row per input: `input`, `match_method`
#' (`"qc_address"` or `"none"`), `uncertainty_m`, `qc_locator`, `qc_score`,
#' `qc_status`, `qc_address`, `qc_postal`, `qc_reject`, and either `lon`/`lat`
#' or an `sf` geometry column.
#' @seealso [geocode()], which can run this as a tier; [qc_reverse_geocode()];
#' [bc_geocode()] for the BC-only service.
#' @export
#' @examples
#' \dontrun{
#' qc_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")
#'
#' # The canonical form the service answers with, from loose input.
#' qc_geocode("1000 rue de la gauchetiere ouest, montreal")$qc_address
#'
#' # What was rejected, and why.
#' qc_geocode("330 rue Saint-Jean, Quebec")$qc_reject
#' }
qc_geocode <- function(x, prov = NULL, min_score = 0, batch_size = 1000,
                       rate = 5, geometry = FALSE, crs = 4326, con = NULL) {
  if (!requireNamespace("httr2", quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) {
    stop("qc_geocode() needs the httr2 and jsonlite packages. Install them ",
         'with install.packages(c("httr2", "jsonlite")).', call. = FALSE)
  }
  res <- if (is.data.frame(x)) x else normalize_address(as.character(x),
                                                        prov = prov, con = con)
  input <- if (is.data.frame(x)) nar_address_string(res) else as.character(x)
  # The French-canonical rendering, not the NAR one -- see nar_qc_query().
  q <- nar_qc_query(res)

  batch_size <- max(1L, min(1000L, as.integer(batch_size)))
  n <- length(q)
  # `ceiling` on an empty vector would ask for zero batches, and lapply over
  # integer(0) returns a list of length 0 that rbind turns into NULL.
  if (!n) {
    loc <- nar_qc_locations(list(), 0)
  } else {
    groups <- split(seq_len(n), ceiling(seq_len(n) / batch_size))
    parts <- lapply(groups, function(i) nar_qc_batch(q[i], rate = rate,
                                                     crs = 4326))
    loc <- do.call(rbind, parts)
    rownames(loc) <- NULL
  }

  out <- nar_qc_floors(loc, res)
  # The score gate runs after the floors so a row it rejects still carries the
  # locator and the matched address that were thrown away.
  low <- out$match_method != "none" & !is.na(out$qc_score) &
    out$qc_score < min_score
  if (any(low)) {
    out$qc_reject[low] <- sprintf("score %.1f below %.1f", out$qc_score[low],
                                  min_score)
    out$match_method[low] <- "none"
    out$uncertainty_m[low] <- NA_real_
    out$lon[low] <- NA_real_
    out$lat[low] <- NA_real_
  }

  out <- cbind(data.frame(input = input, stringsAsFactors = FALSE),
               out[, c("match_method", "uncertainty_m", "qc_locator",
                       "qc_score", "qc_status", "qc_address", "qc_postal",
                       "qc_reject", "lon", "lat")])
  nar_qc_finish(out, crs = crs, geometry = geometry)
}

#' Turn lon/lat columns into whatever the caller asked for
#'
#' @description Shared by [qc_geocode()] and [qc_reverse_geocode()] so the two
#' cannot drift. The service is asked for EPSG:4326 and `sf` means lon/lat by
#' that name, so there is no axis-order question to get wrong before the
#' transform.
#' @param out A data frame carrying `lon` and `lat`
#' @param crs CRS to return
#' @param geometry Whether to return an `sf` object
#' @return `out`, reprojected, with an `sf` geometry column or `lon`/`lat`
#' @keywords internal
nar_qc_finish <- function(out, crs = 4326, geometry = FALSE) {
  lon <- out$lon
  lat <- out$lat
  out <- out[, setdiff(names(out), c("lon", "lat")), drop = FALSE]

  ok <- !is.na(lon) & !is.na(lat)
  pts <- sf::st_sfc(rep(list(sf::st_point()), length(lon)), crs = 4326)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i)
      sf::st_point(c(lon[i], lat[i]))), crs = 4326)
  }
  if (!is.null(crs)) pts <- sf::st_transform(pts, crs)

  if (geometry) return(sf::st_sf(out, geometry = pts))
  # `nrow(out)` rather than a scalar: assigning a length-1 NA into a zero-row
  # frame is an error, and a zero-row result is reachable.
  out$lon <- rep(NA_real_, nrow(out))
  out$lat <- rep(NA_real_, nrow(out))
  if (any(ok)) {
    co <- sf::st_coordinates(pts[ok])
    out$lon[ok] <- co[, 1]
    out$lat[ok] <- co[, 2]
  }
  out
}

#' Reverse geocode Quebec coordinates with the Quebec government geocoder
#'
#' @description The nearest Quebec address to each point, from the same service
#' [qc_geocode()] uses.
#'
#' @section Why this exists when reverse_geocode() already does: it is the only
#' **online** reverse geocoder bound in this package -- neither NRCan's
#' geolocator nor the BC Address Geocoder offers one, and the Government of
#' Canada's Nominatim instance is not a tier for licence reasons. It is
#' therefore a second, independent-of-your-import answer for Quebec, useful for
#' checking [reverse_geocode()] rather than for replacing it: [reverse_geocode()]
#' is local, national, batched against the whole database and returns
#' `output = "multiple"` neighbours, none of which this does.
#'
#' One HTTP request per point -- the service's batch endpoint is forward-only.
#'
#' @param x Longitudes, or an `sf`/`sfc` object of points, or a two-column
#' matrix or data frame of longitude and latitude.
#' @param y Latitudes, when `x` is a numeric vector.
#' @param crs CRS of `x` and `y` when they are numeric, and the CRS the result
#' is returned in. Default EPSG:4326.
#' @param distance Search radius in metres. Default 100. The service returns
#' nothing beyond it rather than reaching across a municipality.
#' @param rate Requests per second. Default 5.
#' @return A data frame with one row per point: `qc_address`, `qc_postal`,
#' `qc_city`, `qc_dist_m`, `lon` and `lat` of the address that was found, all
#' `NA` where nothing was within `distance`.
#' @seealso [reverse_geocode()], which is NAR-backed, local and national.
#' @export
#' @examples
#' \dontrun{
#' qc_reverse_geocode(-73.5672, 45.5017)
#' }
qc_reverse_geocode <- function(x, y = NULL, crs = 4326, distance = 100,
                               rate = 5) {
  if (!requireNamespace("httr2", quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) {
    stop("qc_reverse_geocode() needs the httr2 and jsonlite packages. ",
         'Install them with install.packages(c("httr2", "jsonlite")).',
         call. = FALSE)
  }
  pts <- nar_qc_points(x, y, crs)
  # Ask in EPSG:4326 whatever was handed in, so the request builder below has
  # one axis convention to know about rather than the caller's.
  pts <- sf::st_transform(pts, 4326)
  co <- suppressWarnings(sf::st_coordinates(pts))
  n <- length(pts)

  rows <- lapply(seq_len(n), function(i) {
    empty <- data.frame(qc_address = NA_character_, qc_postal = NA_character_,
                        qc_city = NA_character_, qc_dist_m = NA_real_,
                        lon = NA_real_, lat = NA_real_,
                        stringsAsFactors = FALSE)
    if (sf::st_is_empty(pts[i])) return(empty)
    loc <- sprintf('{"x":%.8f,"y":%.8f,"spatialReference":{"wkid":4326}}',
                   co[i, 1], co[i, 2])
    req <- httr2::request(nar_qc_url())
    req <- httr2::req_url_path_append(req, "reverseGeocode")
    req <- httr2::req_url_query(req, location = loc, distance = distance,
                                outSR = 4326, f = "json")
    req <- httr2::req_user_agent(req, "cangeocode (R package)")
    req <- httr2::req_timeout(req, 25)
    req <- httr2::req_throttle(req, capacity = rate, fill_time_s = 1,
                               realm = "servicescarto.mrnf.gouv.qc.ca")
    req <- httr2::req_error(req, is_error = function(resp) FALSE)

    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(resp, "error") || httr2::resp_status(resp) >= 400) return(empty)
    nar_qc_reverse_row(tryCatch(httr2::resp_body_json(resp),
                                error = function(e) NULL))
  })

  out <- do.call(rbind, rows)
  # Measured here rather than read from the response: the service reports no
  # distance on this endpoint, and a caller wants to know how far the answer
  # actually is before believing it.
  found <- !is.na(out$lon) & !is.na(out$lat)
  if (any(found)) {
    there <- sf::st_sfc(lapply(which(found), function(i)
      sf::st_point(c(out$lon[i], out$lat[i]))), crs = 4326)
    out$qc_dist_m[found] <- as.numeric(sf::st_distance(
      sf::st_transform(pts[found], 3347), sf::st_transform(there, 3347),
      by_element = TRUE))
  }
  nar_qc_finish(out, crs = crs, geometry = FALSE)
}

#' Coerce the many ways a caller can hand over points
#'
#' @description [qc_reverse_geocode()] takes lon/lat vectors, a matrix, a data
#' frame or an `sf` object, and the rest of it should only ever see an `sfc`.
#' @param x What the caller passed
#' @param y Latitudes, when `x` is numeric
#' @param crs CRS of `x` and `y` when they are numeric
#' @return An `sfc` of POINTs
#' @keywords internal
nar_qc_points <- function(x, y = NULL, crs = 4326) {
  if (inherits(x, "sf")) return(sf::st_geometry(x))
  if (inherits(x, "sfc")) return(x)
  if (is.null(y) && (is.matrix(x) || is.data.frame(x))) {
    if (ncol(x) < 2) {
      stop("`x` needs two columns of longitude and latitude.", call. = FALSE)
    }
    y <- as.numeric(x[[2]] %||% x[, 2])
    x <- as.numeric(x[[1]] %||% x[, 1])
  }
  x <- as.numeric(x)
  y <- as.numeric(y)
  if (length(x) != length(y)) {
    stop("`x` and `y` must be the same length.", call. = FALSE)
  }
  ok <- !is.na(x) & !is.na(y)
  pts <- sf::st_sfc(rep(list(sf::st_point()), length(x)), crs = crs)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i)
      sf::st_point(c(x[i], y[i]))), crs = crs)
  }
  pts
}

#' Read one reverse-geocode response into a result row
#'
#' @description Split out from the request so the response shape is testable
#' against a saved fixture with no network, the way [nar_qc_locations()] is for
#' the forward direction.
#'
#' This endpoint answers with a bare `address` object rather than a `locations`
#' array, and it reports **no distance**, which is why `qc_dist_m` is left
#' empty here for [qc_reverse_geocode()] to measure. Asked about a point with
#' nothing within `distance`, the service returns an object carrying an `error`
#' and no `address`, which is a refusal and not a failure.
#' @param d The parsed response, or `NULL` if the body would not parse
#' @return A one-row data frame, all-`NA` when there was no answer
#' @keywords internal
nar_qc_reverse_row <- function(d) {
  empty <- data.frame(qc_address = NA_character_, qc_postal = NA_character_,
                      qc_city = NA_character_, qc_dist_m = NA_real_,
                      lon = NA_real_, lat = NA_real_,
                      stringsAsFactors = FALSE)
  a <- d$address
  if (is.null(a) || is.null(d$location)) return(empty)
  nz <- function(v) if (is.null(v) || !nzchar(v)) NA_character_ else v
  data.frame(qc_address = nz(a$Match_addr), qc_postal = nz(a$ZIP),
             qc_city = nz(a$City), qc_dist_m = NA_real_,
             lon = as.numeric(d$location$x), lat = as.numeric(d$location$y),
             stringsAsFactors = FALSE)
}

#' Check NAR geocoding results against the Quebec government geocoder
#'
#' @description The Quebec counterpart to [bc_validate()]: re-geocodes each
#' address with the provincial service and reports how far its answer sits from
#' the one already obtained, in metres. It also returns the service's canonical
#' form of the address, which for Quebec is often the more useful half -- an
#' accent, a particle or a cardinal point that the parser read differently
#' shows up in `qc_address` even where `qc_dist_m` is small.
#'
#' @section What a disagreement does and does not prove: the service's locators
#' are named `RQA_Adresse` and `RQA_Rue`, so it is built on the **Répertoire
#' québécois des adresses** -- the same MRNF product that also reaches NAR.
#' The two sources are therefore **not independent**, in the same way and for
#' the same reason [bc_validate()]'s are not, and `qc_dist_m` is a lower bound
#' on how far apart genuinely independent sources would sit rather than a
#' benchmark of NAR. Use it to find suspect rows.
#'
#' Rows outside Quebec are skipped rather than sent, since the service does not
#' cover them.
#'
#' @param g A result from [geocode()], with `lon`/`lat` columns or `sf`
#' geometry.
#' @param x The address strings to send. Defaults to `g$input`.
#' @param ... Passed to [qc_geocode()], including `min_score` and `batch_size`.
#' @return `g` with `qc_match_method`, `qc_locator`, `qc_score`, `qc_address`,
#' `qc_postal` and `qc_dist_m` appended. `qc_dist_m` is `NA` where either side
#' has no point.
#' @export
#' @examples
#' \dontrun{
#' g <- geocode(c("1 Rue Notre-Dame Ouest, Montreal, QC",
#'                "1000 rue de la Gauchetiere Ouest, Montreal, QC"))
#' qc_validate(g)
#' }
qc_validate <- function(g, x = g$input, ...) {
  n <- nrow(g)
  out <- cbind(g, data.frame(qc_match_method = rep(NA_character_, n),
                             qc_locator = rep(NA_character_, n),
                             qc_score = rep(NA_real_, n),
                             qc_address = rep(NA_character_, n),
                             qc_postal = rep(NA_character_, n),
                             qc_dist_m = rep(NA_real_, n),
                             stringsAsFactors = FALSE))
  qc_rows <- which(!is.na(g$PROV_ABVN) & toupper(g$PROV_ABVN) == "QC" &
                     !is.na(x) & nzchar(x))
  if (!length(qc_rows)) return(out)

  b <- qc_geocode(x[qc_rows], geometry = TRUE, ...)
  out$qc_match_method[qc_rows] <- b$match_method
  out$qc_locator[qc_rows]      <- b$qc_locator
  out$qc_score[qc_rows]        <- b$qc_score
  out$qc_address[qc_rows]      <- b$qc_address
  out$qc_postal[qc_rows]       <- b$qc_postal

  here <- nar_geocode_points(g[qc_rows, , drop = FALSE])
  # Distance in a projected CRS, in metres. Doing it in EPSG:4326 would return
  # degrees, or silently invoke great-circle distances on an ellipsoid the two
  # sources do not share.
  both <- !sf::st_is_empty(here) & !sf::st_is_empty(sf::st_geometry(b))
  if (any(both)) {
    a <- sf::st_transform(here[both], 3347)
    z <- sf::st_transform(sf::st_geometry(b)[both], 3347)
    out$qc_dist_m[qc_rows[both]] <- as.numeric(sf::st_distance(a, z,
                                                               by_element = TRUE))
  }
  out
}

#' The Quebec geocoder tier
#'
#' @description The tier behind `geocode(method = c(..., "qc"))`. Only the rows
#' its predecessors left unplaced **and** that are in Quebec are sent -- the
#' service covers no other province.
#'
#' Unlike the other two online tiers this one costs a request per *thousand*
#' rows rather than per row, so naming it is cheap even on a large unplaced
#' tail.
#' @param res The parsed components, after any authoritative override
#' @param out The result so far, as [nar_geocode_match()] builds it
#' @param todo Row indices still needing a position
#' @param con A NAR connection, for the storage CRS
#' @param bounds An `sfc` in the storage CRS, or `NULL`
#' @param ... Passed to [qc_geocode()]
#' @return `out`, with the rows the service placed filled in
#' @keywords internal
nar_geocode_tier_qc <- function(res, out, todo, con, bounds = NULL, ...) {
  prov <- toupper(res$PROV_ABVN %||% rep(NA_character_, nrow(res)))
  todo <- todo[!is.na(prov[todo]) & prov[todo] == "QC"]
  if (!length(todo)) return(out)

  b <- qc_geocode(res[todo, , drop = FALSE], geometry = TRUE,
                  crs = nar_crs(con), ...)
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
  # The service returns one answer per address from the batch endpoint, so
  # there are no alternatives to count. Leaving this at 1 says "one answer",
  # not "unambiguous".
  out$n_matches[rows] <- 1L
  out
}

#' Pick the `...` arguments the Quebec tier understands
#'
#' @description `geocode(...)` serves online tiers whose arguments do not
#' overlap, and an argument meant for one of them must not error the others.
#' Same job as [nar_nrcan_dots()] and the same reason.
#' @param dots The `...` from [nar_geocode_match()]
#' @param supplied Argument names the tier fills in itself
#' @return The subset of `dots` [qc_geocode()] declares
#' @keywords internal
nar_qc_dots <- function(dots, supplied = c("x", "prov", "geometry", "crs",
                                           "con")) {
  if (!length(dots)) return(dots)
  dots[intersect(names(dots), setdiff(names(formals(qc_geocode)), supplied))]
}
