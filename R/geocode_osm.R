#' The Canada-hosted Nominatim endpoint
#'
#' @description Kept in one place so a test can point it somewhere else.
#'
#' **This is not `nominatim.openstreetmap.org`**, and the difference is the
#' reason this binding exists at all. The public OSM instance caps use at one
#' request per second and its usage policy forbids bulk geocoding of an address
#' list outright, which is exactly what this package does. `maps.canada.ca` runs
#' its own Nominatim, keyless, and NRCan's own geolocator aggregator queries it
#' under the `nominatim` service key
#' (`backend/geolocator-bucket-content/services/nominatim-schema.json` in
#' `Canadian-Geospatial-Platform/geoview-api-geolocator`).
#'
#' That removes the policy objection and replaces it with an unanswered one: the
#' instance exists to serve GeoView, nothing published says what bulk use is
#' acceptable, and the aggregator's own timeout against it is 3 seconds, which
#' does not suggest a generous provision. `geo@nrcan-rncan.gc.ca` is the contact
#' its README names. Hence the deliberately slow default `rate`.
#' @return A single URL
#' @keywords internal
nar_osm_url <- function() "https://maps.canada.ca/nominatim/search"

#' What a surviving OSM answer is worth
#'
#' @description **Not measured, and therefore not asserted.** Every other
#' uncertainty constant in this package is a 90th percentile against a stated
#' reference -- `nar_nrcan_uncertainty_m()` is 150 m over the sample
#' `data-raw/probe_geolocator.R` draws -- and there is no equivalent run for
#' this source yet. `data-raw/probe_osm.R` is the harness that would produce
#' one; until it has been run over a national sample, a surviving answer
#' reports `NA` rather than a number invented to look like the others.
#'
#' This is also why [osm_geocode()] is not wired into [geocode()] as a tier.
#' `uncertainty_m` is a column callers filter on, and a tier contributing `NA`
#' to it would quietly make that filter mean something different depending on
#' which tier answered.
#'
#' What can be said without measuring: a surviving answer is a `place_rank` 30
#' object carrying its own house number, which is an address someone entered or
#' imported rather than a position interpolated along a range. That is a
#' different **kind** of answer from the geolocator's, not necessarily a better
#' one -- OSM's Canadian address coverage is uneven and concentrated in
#' municipalities whose open data was imported.
#' @return A single number, metres, or `NA`
#' @keywords internal
nar_osm_uncertainty_m <- function() NA_real_

#' Is this response a transient failure worth re-sending?
#'
#' @description **Precautionary, and not measured against this instance.** The
#' NRCan geolocator loses about one request in twelve to a clean HTTP 500 --
#' see [nar_nrcan_transient()], where the measurement is -- and eight probe
#' requests against this host all came back 200, which establishes nothing
#' either way at that sample size. The predicate is cheap and the failure mode
#' it covers is the one every Lambda-fronted service has, so it is on by
#' default and the note stays here rather than in a claim of reliability.
#'
#' Nominatim reports a bad request as a JSON **object** carrying `error`, where
#' a successful search is an array. An object is therefore retried, which for a
#' genuinely malformed query costs `retries` requests and settles it; a `400`
#' status is not retried, because that is the same thing already labelled.
#'
#' An empty array is **not** transient, and here that matters more than it does
#' for the geolocator: this service genuinely answers "nothing" -- see
#' [nar_osm_candidates()].
#' @param resp A response object
#' @return `TRUE` if the request is worth re-sending
#' @keywords internal
nar_osm_transient <- function(resp) {
  status <- httr2::resp_status(resp)
  if (status >= 500 || status == 429) return(TRUE)
  if (status != 200) return(FALSE)
  body <- tryCatch(httr2::resp_body_json(resp), error = function(e) NULL)
  length(body) > 0 && !is.null(names(body))
}

#' Which of Nominatim's address fields is the municipality
#'
#' @description OSM has no single municipality field. What comes back depends
#' on how the place is tagged: Vancouver arrives as `city`, Corner Brook as
#' `town`, and smaller places as `village` or `municipality`. The first of
#' those that is present is taken.
#'
#' `suburb`, `neighbourhood`, `quarter` and `city_district` are deliberately
#' **not** in the list even though they are often the only other locality field
#' present. They sit below the municipality, not beside it -- `West End` for a
#' Vancouver address, `Vieux-Montreal` for a Montreal one -- and treating one as
#' the municipality would fail the agreement floor against the municipality that
#' was actually asked for.
#' @param addr One result's `address` object, as a named list
#' @return A single string, or `NA`
#' @keywords internal
nar_osm_mun <- function(addr) {
  for (f in c("city", "town", "village", "municipality", "hamlet")) {
    v <- addr[[f]]
    if (!is.null(v) && nzchar(v)) return(as.character(v))
  }
  NA_character_
}

#' Read every candidate out of a Nominatim response body
#'
#' @description Split out from the request so the response shapes can be tested
#' against saved fixtures with no network.
#'
#' @section What this service does that the geolocator does not: **It answers
#' "nothing".** Asked for `99999 Nowhere Rd, Nowhereville, SK` it returns an
#' empty array, where NRCan's geolocator returns a confident position on some
#' other road. Asked for `28 Silver ST, CORNER BROOK` -- the address the
#' geolocator answers with `28 Brook Street, Corner Brook`,
#' a different street -- it returns the road itself at `place_rank` 26 and no
#' house number, which is a refusal rather than a substitution.
#'
#' That does **not** make the agreement floor unnecessary. A road-level result
#' is still a result, and structured search still degrades to a
#' municipality-wide street match; the floor is what turns "I found something"
#' into "I found the address you asked for". It does mean the floor rejects
#' less often here, and rejects different things.
#'
#' @section The fields that are read, and the ones that are not: `lat` and `lon`
#' arrive as **strings**, which is a Nominatim convention and not a mistake in
#' the body. `place_rank` is the resolution: 30 is a house or a building, 26 is
#' a road, lower is coarser. `category` is the OSM key that matched
#' (`building`, `place`, `highway`, `office`), and it is reported but not used
#' as a floor -- a restaurant at the right civic number is a correct answer to
#' that civic number, and its category says only how OSM happens to tag it.
#'
#' `address` is the part that makes this service worth binding: `house_number`
#' and `road` arrive already separated, so the agreement floor never has to
#' recover them from a display string, and `ISO3166-2-lvl4` gives the province
#' as `CA-BC` rather than as prose to be matched. `road` is *not* separated
#' further -- it arrives as `Bute Street`, type included -- so it still has to
#' be parsed; see [nar_osm_floors()].
#'
#' `licence` is carried on every row rather than dropped. It is the same string
#' for every result, and it is the ODbL attribution that comes with using this
#' data at all; a column is harder to lose than an attribute.
#' @param resp The parsed response, as [jsonlite::fromJSON()] with
#' `simplifyVector = FALSE` returns it
#' @return A data frame of `osm_rank`, `osm_category`, `osm_title`,
#' `osm_house_number`, `osm_road`, `osm_mun`, `osm_prov`, `osm_licence`, `lon`
#' and `lat`, one row per result in the order the service ranked them, and **no
#' rows at all** when there is no answer
#' @keywords internal
nar_osm_candidates <- function(resp) {
  empty <- data.frame(osm_rank = integer(), osm_category = character(),
                      osm_title = character(), osm_house_number = character(),
                      osm_road = character(), osm_mun = character(),
                      osm_prov = character(), osm_licence = character(),
                      lon = numeric(), lat = numeric(),
                      stringsAsFactors = FALSE)
  # A named list is a JSON object, which is Nominatim's error body; an unnamed
  # one is the results array. Same test as the geolocator's, different body.
  if (!length(resp) || !is.null(names(resp))) return(empty)
  do.call(rbind, lapply(resp, function(r) {
    addr <- r$address %||% list()
    iso <- addr[["ISO3166-2-lvl4"]] %||% NA_character_
    data.frame(
      osm_rank     = as.integer(r$place_rank %||% NA_integer_),
      osm_category = as.character(r$category %||% NA_character_),
      osm_title    = as.character(r$display_name %||% NA_character_),
      osm_house_number = as.character(addr$house_number %||% NA_character_),
      osm_road     = as.character(addr$road %||% NA_character_),
      osm_mun      = nar_osm_mun(addr),
      # `CA-BC` -- the country half is constant for a countrycodes=ca search.
      osm_prov     = sub("^CA-", "", as.character(iso)),
      osm_licence  = as.character(r$licence %||% NA_character_),
      # Strings in the body, both of them.
      lon = as.numeric(r$lon %||% NA_real_),
      lat = as.numeric(r$lat %||% NA_real_),
      stringsAsFactors = FALSE)
  }))
}

#' Apply the match floors and choose one answer per address
#'
#' @description Two cumulative floors, the same shape as
#' [nar_nrcan_floors()]:
#'
#' 1. A candidate must be **house-level**: `place_rank` at least 30, and an
#'    `house_number` present in its own address. A `place_rank` 26 road is the
#'    service saying it found the street and not the civic number, which is the
#'    same refusal `INTERPOLATED_CENTROID` is on the other service and is worth
#'    the same, namely nothing.
#' 2. Its components must agree with the query's, component by component -- see
#'    [nar_address_agreement()].
#'
#' @section Why the road is parsed but the municipality is not: The components
#' compared on the answer's side are assembled from **two different places**,
#' and deliberately so. `house_number` and `road` are pasted back together and
#' put through [normalize_address()], because `road` arrives as `Bute Street` --
#' one string, name and type together -- and splitting that is what the parser
#' is for. `MUN_NAME` and `PROV_ABVN` are then **overwritten** from the
#' service's own `city`/`town` and `ISO3166-2-lvl4` fields rather than parsed
#' out of anything.
#'
#' Parsing the full `display_name` instead would be the obvious shortcut and is
#' the thing not to do. It is a long chain --
#' `The Berkeley, 990, Bute Street, Davie Village, West End, Vancouver, Metro
#' Vancouver Regional District, British Columbia, V6E, Canada` -- carrying a
#' building name, two sub-municipal localities and a regional district, and
#' recovering the municipality from it reintroduces exactly the failure the
#' field-wise floor exists to remove: a locality migrating into the street name.
#' The fields are already separated; using them is the whole advantage this
#' service has over the geolocator.
#'
#' @section Why the parse gets no gazetteer: [normalize_address()] is not given
#' a connection, for the same reason it is not given one in
#' [nar_nrcan_floors()]: the gazetteer canonicalizes a caller's loose input, and
#' turned on a service's answer it negotiates with the answer instead of
#' checking it, resolving two adjacent communities to one NAR municipality and
#' passing an answer about a different place.
#'
#' @section Why two survivors are usually one address: Nominatim returns each
#' matching OSM object separately, and a single address is frequently several --
#' `1155 Robson St, Vancouver` comes back as the building and as an office
#' inside it, 8 m apart, both carrying the same house number, road and city.
#' Counting those as two matches would report an ambiguity that does not exist,
#' so `n_matches` counts **distinct addresses** among the survivors, folded
#' through [address_key()]. Two after that folding is a real ambiguity: the same
#' civic number on the same street name in two places that both satisfy
#' containment.
#' @param cand The candidates, as [nar_osm_candidates()] returns them, rbound
#' across every address. Within one address they must stay in the order the
#' service ranked them, since that is what "best" is read from.
#' @param q Parsed components of the addresses that were sent, one row each
#' @param idx For each row of `cand`, the row of `q` it answers. The default
#' says every candidate belongs to a single address, which is the shape a test
#' or a one-address probe has.
#' @param failed A logical vector over the rows of `q`, `TRUE` where the request
#' never completed, reported as `request failed` rather than `no answer`
#' @return A data frame with one row per row of `q`: `match_method`,
#' `uncertainty_m`, `n_matches`, the `osm_*` columns of the chosen candidate,
#' `osm_reject`, `lon` and `lat`
#' @keywords internal
nar_osm_floors <- function(cand, q, idx = rep(1L, nrow(cand)),
                           failed = rep(FALSE, nrow(q))) {
  n <- nrow(q)
  m <- nrow(cand)

  usable <- !is.na(cand$osm_title) & !is.na(cand$lon) & !is.na(cand$lat)
  # `place_rank` 30 is house or building level; a road is 26. The house number
  # is required as well rather than trusted from the rank, since rank 30 is also
  # what a named POI carries and a POI need not sit on an addressed object.
  class_ok <- usable & !is.na(cand$osm_rank) & cand$osm_rank >= 30L &
    !is.na(cand$osm_house_number) & nzchar(cand$osm_house_number)

  reason <- rep(NA_character_, m)
  key <- rep(NA_character_, m)
  chk <- which(class_ok)
  if (length(chk)) {
    # No `con`: see the section above.
    t <- normalize_address(paste(cand$osm_house_number[chk],
                                 cand$osm_road[chk]))
    # The service already separated these two, so they are taken rather than
    # recovered from the display string.
    t$MUN_NAME <- cand$osm_mun[chk]
    t$PROV_ABVN <- cand$osm_prov[chk]
    reason[chk] <- nar_address_agreement(q[idx[chk], , drop = FALSE], t)
    key[chk] <- address_key(t)
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
  near <- first_of(class_ok)  # house-level, and then disagreed
  any_ <- first_of(usable)    # answered at all
  ok <- !is.na(hit)
  sel <- ifelse(ok, hit, ifelse(is.na(near), any_, near))

  # Distinct addresses among the survivors, not distinct OSM objects -- see the
  # section above on the building and the office inside it.
  distinct <- pass & !duplicated(data.frame(idx, key))
  n_matches <- tabulate(idx[distinct], nbins = n)

  # Weakest reason first, so each better-informed one overwrites it.
  reject <- rep("no answer", n)
  # A lost request is not an answer of any kind, and cannot be overwritten by
  # one -- a failed row has no candidates for the rules below to look at.
  reject[failed] <- "request failed"
  seen <- !is.na(any_)
  # Quoted back in the service's own vocabulary, so the reason can be matched
  # against Nominatim's documentation rather than against this function.
  reject[seen] <- sprintf("best result is %s at rank %s",
                          cand$osm_category[any_[seen]],
                          cand$osm_rank[any_[seen]])
  got <- !is.na(near)
  reject[got] <- reason[near[got]]
  reject[ok] <- NA_character_

  # `sel` is NA where nothing answered, and indexing by NA yields NA of the
  # right type even when `cand` has no rows at all.
  data.frame(
    match_method  = ifelse(ok, "osm", "none"),
    uncertainty_m = ifelse(ok, nar_osm_uncertainty_m(), NA_real_),
    n_matches     = n_matches,
    osm_rank      = cand$osm_rank[sel],
    osm_category  = cand$osm_category[sel],
    osm_title     = cand$osm_title[sel],
    osm_licence   = cand$osm_licence[sel],
    osm_reject    = reject,
    lon = ifelse(ok, cand$lon[sel], NA_real_),
    lat = ifelse(ok, cand$lat[sel], NA_real_),
    stringsAsFactors = FALSE)
}

#' Render the street line the way Nominatim can match it
#'
#' @description Not [nar_address_string()], which is what the other two
#' services get, and the difference is **measured rather than assumed**. NAR's
#' canonical order puts the type after the name for every language, so
#' `1 Rue Notre-Dame Ouest` becomes `1 NOTRE-DAME RUE O` -- and against this
#' service that finds nothing at all:
#'
#' | sent as | results |
#' | --- | --- |
#' | `1 NOTRE-DAME RUE O` | 0 |
#' | `1 Rue Notre-Dame O` | 0 |
#' | `1 Rue Notre-Dame Ouest` | 1, house-level |
#' | `5150 SHERBROOKE RUE O` | 0 |
#' | `5150 Rue Sherbrooke Ouest` | 2, house-level |
#'
#' Two separate things are going on there, and only one of them is word order.
#'
#' **The type has to sit where the language puts it**, which is what
#' [nar_type_leads()] already knows and what [format_address()] already does:
#' French types lead the name, English types follow it. The type may also be
#' dropped entirely and still match -- `5150 Sherbrooke Ouest` works -- so this
#' is about placement, not presence.
#'
#' **The direction has to be spelled out in French, and only in French.**
#' Nominatim's tokenizer expands English abbreviations, so `100 Queen St W`
#' matches `100 Queen Street West`, but nothing expands `O` to `Ouest` and an
#' unexpanded `O` is a token that matches no street. So `N`, `S`, `E` and `O`
#' are written out **only where the type leads**, which is the same test that
#' decides the word order and the only signal available for which language the
#' address is in. English directions are left abbreviated because they
#' demonstrably work.
#'
#' Accents need no handling: `1 Cote de la Fabrique` matches
#' `1 Côte de la Fabrique`.
#' @param res Parsed components, one row per address
#' @return A character vector of street lines, `""` where there is nothing
#' @keywords internal
nar_osm_street <- function(res) {
  col <- function(name) {
    v <- res[[name]]
    if (is.null(v)) rep(NA_character_, nrow(res)) else as.character(v)
  }
  # The suffix belongs to the number with no space between them; OSM stores
  # `house_number` as free text, so `990A` is a value it can hold and match.
  no <- col("CIVIC_NO")
  sfx <- col("CIVIC_NO_SUFFIX")
  civic <- ifelse(is.na(no), NA_character_,
                  paste0(no, ifelse(is.na(sfx), "", sfx)))

  type <- col("STREET_TYPE")
  name <- col("STREET_NAME")
  dir <- col("STREET_DIR")
  leads <- nar_type_leads(type)

  # Capitals, matching the convention format_address() uses for types and
  # directions. Nominatim folds case, so this is legibility, not matching.
  fr <- c(N = "NORD", S = "SUD", E = "EST", O = "OUEST")
  spelled <- unname(fr[dir])
  dir <- ifelse(leads & !is.na(spelled), spelled, dir)

  street <- ifelse(leads, nar_paste_parts(type, name, dir),
                   nar_paste_parts(name, type, dir))
  nar_paste_parts(civic, nar_blank_na(street))
}

#' Build one Nominatim query from parsed components
#'
#' @description Two shapes, chosen by `structured`, both built on
#' [nar_osm_street()].
#'
#' **Structured** sends `street`, `city` and `state` as separate parameters,
#' which is the shape this binding exists for: the parse never has to be
#' flattened into one string and recovered on the other side. `street` still
#' carries the civic number and the street together, because that is the
#' parameter Nominatim defines.
#'
#' **Free text** sends the same three joined by commas. It is kept because
#' structured search in Nominatim requires every element supplied to match,
#' which can reject an address that free text would find under a municipality
#' the caller spelled differently -- and because a knob the probe harness can
#' flip is how that gets measured instead of guessed. The two agreed on every
#' probe address tried so far, which is not a sample.
#' @param res Parsed components, one row per address
#' @param structured Whether to send separate parameters
#' @return A list of named lists, one per row, ready for `req_url_query()`
#' @keywords internal
nar_osm_query <- function(res, structured = TRUE) {
  col <- function(name) {
    v <- res[[name]]
    if (is.null(v)) rep(NA_character_, nrow(res)) else as.character(v)
  }
  street <- nar_osm_street(res)
  mun <- col("MUN_NAME")
  prov <- col("PROV_ABVN")
  lapply(seq_len(nrow(res)), function(i) {
    one <- list(street = street[[i]], city = mun[[i]], state = prov[[i]])
    # An empty parameter is not the same as an absent one: Nominatim treats a
    # supplied element as a requirement, so sending `city=` would demand a match
    # against nothing.
    one <- one[!vapply(one, function(v) is.na(v) || !nzchar(v), logical(1))]
    if (structured) return(one)
    if (!length(one)) return(one)
    list(q = paste(unlist(one), collapse = ", "))
  })
}

#' Geocode Canadian addresses with OpenStreetMap data
#'
#' @description A binding to the **Nominatim instance NRCan hosts at
#' `maps.canada.ca`**, which searches OpenStreetMap data. It is national,
#' needs no API key and no local database, and it is the only source in this
#' package that is genuinely independent of Statistics Canada.
#'
#' @section Read this before using it: **Results are OpenStreetMap data under
#' the ODbL**, which is a materially different obligation from the Open
#' Government Licence covering NAR, the BC Address Geocoder and NRCan's
#' geolocator. The ODbL requires attribution and carries share-alike terms that
#' attach to *derived databases*, so coordinates from here mixed into a table
#' that is then published are not in the same licensing position as the rest of
#' this package's output. Every row carries the service's own `osm_licence`
#' string so the obligation travels with the data.
#'
#' This is why the function is **exported but not wired into [geocode()]**:
#' nothing fires it unless it is called by name, and no default tier chain can
#' mix ODbL coordinates into a result without the caller having decided to.
#'
#' @section What a response means: Unlike NRCan's geolocator, **this service
#' will say it has nothing.** Asked for an address that does not exist it
#' returns an empty array, and asked for a civic number it does not hold on a
#' street it does hold, it returns the street at `place_rank` 26 and no house
#' number. Neither is a confident wrong answer.
#'
#' The floors are applied anyway, and they are the same two the geolocator tier
#' applies: a result must be house-level, and its components must agree with the
#' ones that were sent. `osm_reject` says which floor a row failed and keeps the
#' `display_name` it failed on, so the rejection can be inspected.
#'
#' **Coverage is the open question, not accuracy.** OSM's Canadian address
#' coverage is uneven -- excellent in municipalities whose open address data was
#' imported, sparse elsewhere -- and it has not been measured here. Neither has
#' the positional error, which is why `uncertainty_m` comes back `NA`; see
#' [nar_osm_uncertainty_m()] and `data-raw/probe_osm.R`.
#'
#' @section Reverse geocoding: The instance does offer `/reverse`, unlike either
#' other online source here, and it is not bound. [reverse_geocode()] is
#' NAR-backed, local, and does not carry the ODbL question.
#'
#' @section Network use and courtesy: One HTTP request per address; there is no
#' batch endpoint. **The default `rate` is 1 request per second**, which is
#' Nominatim's own convention rather than a measured limit -- nothing published
#' says what this instance will tolerate, and it exists to serve GeoView rather
#' than this package. Raising it is the caller's decision to make and
#' `geo@nrcan-rncan.gc.ca` is the address to ask at. `httr2` is required and
#' lives in `Suggests`, so the package never contacts the network unless this
#' function is called.
#'
#' @param x A character vector of address strings, or a data frame of parsed
#' components as [normalize_address()] returns. Components are needed either
#' way, since the floors compare the answer against them; passing a parsed
#' frame just avoids parsing twice.
#' @param prov Optional province, passed to [normalize_address()] when `x` is a
#' character vector.
#' @param rate Requests per second, and also the largest burst allowed before
#' throttling starts. Default 1; see the courtesy section above before raising
#' it.
#' @param retries How many times to send an address before giving up on it.
#' Default 3, and `1` disables retrying. Precautionary rather than measured
#' here -- see [nar_osm_transient()]. Rows that exhaust their retries report
#' `request failed` in `osm_reject` rather than `no answer`.
#' @param limit How many results to ask for per address, default 10. The floors
#' read all of them, and a single address is often several OSM objects.
#' @param structured Whether to send `street`/`city`/`state` as separate
#' parameters rather than one query string. Default `TRUE`; see
#' [nar_osm_query()].
#' @param geometry Whether to return an `sf` object. Default `FALSE`.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param con An open NAR connection, optional. It is used only to give the
#' parse of the *caller's* input a gazetteer; the service itself needs nothing
#' local, and the answer is parsed without one.
#' @return A data frame with one row per input: `input`, `match_method`
#' (`"osm"` or `"none"`), `uncertainty_m`, `n_matches`, `osm_rank`,
#' `osm_category`, `osm_title`, `osm_licence`, `osm_reject`, and either
#' `lon`/`lat` or an `sf` geometry column.
#' @seealso [nrcan_geocode()] and [bc_geocode()], the two services [geocode()]
#' will run as tiers.
#' @export
#' @examples
#' \dontrun{
#' osm_geocode("990 Bute St, Vancouver, BC")
#'
#' # The address the geolocator answers with a Rue Notre-Dame Ouest 500 km away.
#' osm_geocode("1 Rue Notre-Dame Ouest, Montreal, QC")$osm_title
#'
#' # A refusal rather than a substitution: the street, at rank 26.
#' osm_geocode("28 Silver St, Corner Brook, NL")[, c("osm_title", "osm_reject")]
#' }
osm_geocode <- function(x, prov = NULL, rate = 1, retries = 3, limit = 10,
                        structured = TRUE, geometry = FALSE, crs = 4326,
                        con = NULL) {
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("osm_geocode() needs the httr2 package. Install it with ",
         'install.packages("httr2").', call. = FALSE)
  }
  res <- if (is.data.frame(x)) x else normalize_address(as.character(x),
                                                        prov = prov, con = con)
  input <- if (is.data.frame(x)) nar_address_string(res) else as.character(x)
  qs <- nar_osm_query(res, structured = structured)

  # `TRUE` where the request never came back, which is not the same as coming
  # back empty -- and this service does come back empty. Filled in by the loop.
  failed <- rep(FALSE, length(qs))

  per <- lapply(seq_along(qs), function(i) {
    one <- qs[[i]]
    if (!length(one)) return(nar_osm_candidates(list()))
    req <- httr2::request(nar_osm_url())
    req <- httr2::req_url_query(req, !!!one, format = "jsonv2",
                                addressdetails = 1, limit = limit,
                                countrycodes = "ca")
    # Nominatim asks that a bulk user identify itself, and this is the only
    # place in the package where that is a stated condition of use rather than
    # a courtesy.
    req <- httr2::req_user_agent(req, "cangeocode R package (https://github.com/mountainMath/cangeocode)")
    req <- httr2::req_timeout(req, 25)
    # `capacity` plus a one-second fill, not the superseded `rate` argument --
    # `rate = 1` builds a 60-token bucket and lets the first 60 requests go at
    # once. Same trap as R/geocode_bc.R and R/geocode_nrcan.R.
    req <- httr2::req_throttle(req, capacity = rate, fill_time_s = 1,
                               realm = "maps.canada.ca")
    # A failed lookup is data, not an exception: one unreachable address must
    # not abandon the rest of the vector. Set BEFORE req_retry() and it does not
    # disable it -- `is_transient` is consulted independently of `is_error`.
    req <- httr2::req_error(req, is_error = function(resp) FALSE)
    if (retries > 1) {
      req <- httr2::req_retry(req, max_tries = retries,
                              is_transient = nar_osm_transient,
                              retry_on_failure = TRUE)
    }

    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(resp, "error") || httr2::resp_status(resp) != 200) {
      failed[[i]] <<- TRUE
      return(nar_osm_candidates(list()))
    }
    body <- tryCatch(httr2::resp_body_json(resp), error = function(e) list())
    # A 200 that is still an object body after every retry is an error the
    # gateway did not label, not an empty answer.
    if (length(body) && !is.null(names(body))) failed[[i]] <<- TRUE
    nar_osm_candidates(body)
  })

  # An address contributes as many rows as it got results, and none if it got
  # none, so `idx` is what puts a candidate back with the address it answers.
  # `rbind` over an empty list is NULL rather than a frame, hence the fallback.
  cand <- if (length(per)) do.call(rbind, per) else NULL
  if (is.null(cand)) cand <- nar_osm_candidates(list())
  idx <- rep(seq_along(per), vapply(per, nrow, integer(1)))

  out <- nar_osm_floors(cand, res, idx, failed)
  lon <- out$lon
  lat <- out$lat
  out <- cbind(data.frame(input = input, stringsAsFactors = FALSE),
               out[, setdiff(names(out), c("lon", "lat")), drop = FALSE])

  # Nominatim answers in EPSG:4326, and sf means lon/lat by that name, so there
  # is no axis-order question to get wrong here.
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
