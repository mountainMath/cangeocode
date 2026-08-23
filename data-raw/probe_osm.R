# Measures the Canada-hosted Nominatim against NAR's own building points.
#
# `https://maps.canada.ca/nominatim/search` is the OpenStreetMap geocoder the
# Government of Canada runs itself, and the one NRCan's own aggregator queries
# under its `nominatim` service key -- not `nominatim.openstreetmap.org`, whose
# usage policy forbids exactly this. It is keyless and national, so it is the
# second candidate for a `geocode()` tier that works before anything has been
# downloaded, and this harness is what says whether it is good enough.
#
# It is NOT wired into geocode(), and this measurement is not what decides that:
# the data is ODbL, which is a share-alike licence on derived databases, and the
# other tiers are Open Government Licence. See osm_geocode()'s documentation.
# What this harness answers is the narrower question of accuracy -- what a user
# who has read that and chosen to call osm_geocode() is getting.
#
# The difference from data-raw/probe_geolocator.R is the whole point of running
# both. That service always answers, so a response is not a match and the
# accuracy question is really a filtering question. This one refuses: it returns
# an empty array when it has nothing, and a rank-26 road when it has the street
# but not the number. So the two floors measured here reject far less, and the
# number to watch is the answer rate rather than what the floor removed:
#
#   1. a result must be at `place_rank` 30 or better AND carry its own
#      `house_number` -- rank 30 alone is not enough, `24 Sussex Dr, Ottawa`
#      comes back at rank 30 with no number at all;
#   2. its separated fields must agree, component by component, with the address
#      that was sent -- see nar_address_agreement().
#
# nar_osm_candidates() and nar_osm_floors() are called directly, so this
# measures the shipped code rather than a restatement of it.
#
# PROBE_STRUCTURED decides how the query is spelled. Structured (the default)
# sends `street=`, `city=` and `state=` separately; unstructured collapses them
# into one `q=`. Nominatim treats a supplied element as a REQUIREMENT, so the
# structured form is stricter by construction -- a city it cannot match is a
# refusal rather than a wider search. Which of those is better is what the knob
# is for.
#
# The street line is not NAR's canonical order. `1 NOTRE-DAME RUE O` returns
# nothing; `1 RUE NOTRE-DAME OUEST` returns the address. nar_osm_street() puts
# the type where the language puts it and spells out the French directions, and
# is measured here along with everything else.
#
# The reference is NAR's own building point, and a reference is not ground
# truth: NAR is accurate in general but carries its own poor and wrong records,
# so a "miss" here is a DISAGREEMENT and OSM is sometimes the one that is right
# -- more so than for the geolocator, since OSM buildings are often traced from
# imagery independent of any address file. Read the worst-survivors table with
# that in mind. See inst/notes/geocoding-status.md.
#
# Run with:  Rscript data-raw/probe_osm.R   (needs NAR_CACHE_PATH)
#   PROBE_N           addresses to sample                  (default 150)
#   PROBE_STRUCTURED  send street/city/state separately    (default TRUE)
#   PROBE_RETRIES     attempts per address                 (default 3, 1 = none)
#   PROBE_RATE        requests per second                  (default 1)
#   PROBE_LIMIT       results asked for per address        (default 10)
#   PROBE_OUT         where to save the result             (default probe-osm-<form>.rds)
#
# The sample is `REPEATABLE (42)` -- the SAME 150 addresses the geolocator probe
# uses, so the two services are compared over identical input and either run is
# re-analysable without re-querying.
#
# At one request a second, 150 addresses take about three minutes. Do not raise
# PROBE_RATE to hurry a large run: this is a shared public service with no key
# and no quota, and the rate limit is the only thing keeping the ask polite.

if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  suppressMessages(pkgload::load_all(quiet = TRUE))
} else {
  library(cangeocode)
}
suppressMessages(library(dplyr))
library(sf)

n          <- as.integer(Sys.getenv("PROBE_N", "150"))
structured <- !toupper(Sys.getenv("PROBE_STRUCTURED", "TRUE")) %in%
  c("FALSE", "0", "NO")
retries    <- as.integer(Sys.getenv("PROBE_RETRIES", "3"))
rate       <- as.numeric(Sys.getenv("PROBE_RATE", "1"))
limit      <- as.integer(Sys.getenv("PROBE_LIMIT", "10"))
out_path   <- Sys.getenv("PROBE_OUT", paste0(
  "probe-osm-", if (structured) "structured" else "freeform", ".rds"))

con <- nar_connection()
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

samp <- DBI::dbGetQuery(con, sprintf("
  SELECT CIVIC_NO, OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
         MAIL_MUN_NAME, MAIL_PROV_ABVN, x, y
  FROM Addresses
  WHERE geom_source = 'building' AND length(MAIL_MUN_NAME) > 0
        AND length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
  USING SAMPLE reservoir(%d ROWS) REPEATABLE (42)", n))

# NAR's own canonical components, not a re-parse of a string: that is the truth
# this is measured against, and re-parsing would fold the normalizer's errors
# into the service's. The query side is built from the same frame, so what is
# sent is exactly what nar_osm_query() would send for a parsed address.
q_parts <- data.frame(
  CIVIC_NO    = samp$CIVIC_NO,
  STREET_NAME = samp$OFFICIAL_STREET_NAME,
  STREET_TYPE = samp$OFFICIAL_STREET_TYPE,
  STREET_DIR  = samp$OFFICIAL_STREET_DIR,
  MUN_NAME    = samp$MAIL_MUN_NAME,
  PROV_ABVN   = samp$MAIL_PROV_ABVN,
  stringsAsFactors = FALSE)

qs <- nar_osm_query(q_parts, structured = structured)
samp$query <- vapply(qs, function(one) paste(unlist(one), collapse = ", "),
                     character(1))

# `capacity`, not `rate` -- req_throttle(rate = 1) builds a 60-token bucket and
# lets the first 60 requests go at once. Same trap as R/geocode_bc.R.
#
# The retry is precautionary rather than measured: the geolocator drops about
# one request in twelve and this service has not been seen to, but the two sit
# behind different front ends and a lost request would depress the answer rate
# for reasons that have nothing to do with OSM's coverage. PROBE_RETRIES = 1
# turns it off, which is how to find out whether this one drops requests too.
failed <- rep(FALSE, nrow(samp))
hit <- function(one, i) {
  if (!length(one)) return(nar_osm_candidates(list()))
  req <- httr2::request(nar_osm_url()) |>
    httr2::req_url_query(!!!one, format = "jsonv2", addressdetails = 1,
                         limit = limit, countrycodes = "ca") |>
    httr2::req_user_agent("cangeocode R package (https://github.com/mountainMath/cangeocode)") |>
    httr2::req_timeout(25) |>
    httr2::req_throttle(capacity = rate, fill_time_s = 1, realm = "maps.canada.ca") |>
    httr2::req_error(is_error = function(x) FALSE)
  if (retries > 1) {
    req <- httr2::req_retry(req, max_tries = retries,
                            is_transient = nar_osm_transient,
                            retry_on_failure = TRUE)
  }
  r <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(r) || httr2::resp_status(r) != 200) {
    failed[[i]] <<- TRUE
    return(nar_osm_candidates(list()))
  }
  b <- tryCatch(httr2::resp_body_json(r), error = function(e) list())
  if (length(b) && !is.null(names(b))) failed[[i]] <<- TRUE
  nar_osm_candidates(b)
}

message("Probing ", nrow(samp), " addresses, ",
        if (structured) "structured" else "free-form",
        " (about ", round(nrow(samp) / max(rate, 0.1) / 60), " min)")
per <- lapply(seq_len(nrow(samp)), function(i) {
  if (i %% 25 == 0) message("  ", i, "/", nrow(samp))
  hit(qs[[i]], i)
})
cand <- do.call(rbind, per)
if (is.null(cand)) cand <- nar_osm_candidates(list())
idx <- rep(seq_along(per), vapply(per, nrow, integer(1)))
message("addresses answered: ", sum(vapply(per, nrow, integer(1)) > 0), "/",
        nrow(samp), " (", nrow(cand), " candidates); requests lost after ",
        retries, " tries: ", sum(failed))

# The shipped floors, so the recall and the p90 reported here are the ones
# osm_geocode() actually enforces.
floors <- nar_osm_floors(cand, q_parts, idx, failed)

# The point measured is the one the binding would return: the best-ranked
# candidate that passed, or the best-ranked one that did not.
answered <- !is.na(floors$lon) & !is.na(floors$lat)
dist <- rep(NA_real_, nrow(samp))
if (any(answered)) {
  pt <- st_as_sf(data.frame(lon = floors$lon[answered], lat = floors$lat[answered]),
                 coords = c("lon", "lat"), crs = 4326) |> st_transform(nar_crs(con))
  truth <- st_as_sf(samp[answered, c("x", "y")], coords = c("x", "y"),
                    crs = nar_crs(con))
  dist[answered] <- as.numeric(st_distance(pt, truth, by_element = TRUE))
}

out <- data.frame(
  query = samp$query, title = floors$osm_title, rank = floors$osm_rank,
  category = floors$osm_category, dist = dist, reject = floors$osm_reject,
  kept = floors$match_method == "osm", n_matches = floors$n_matches,
  queried = nrow(samp), structured = structured, row.names = NULL)
saveRDS(out, out_path)

# --- what the filters buy -------------------------------------------------

report <- function(out) {
  queried <- out$queried[1]
  ans <- out[!is.na(out$dist), ]
  cat("\n== best result rank x category ==\n")
  print(table(ans$rank, ans$category))
  cat("\n== distance to NAR's building point, by rank (m) ==\n")
  print(ans |> group_by(rank) |>
          summarise(n = n(), p50 = round(median(dist)),
                    p90 = round(quantile(dist, .9)), max = round(max(dist)),
                    .groups = "drop") |> as.data.frame())

  show <- function(lbl, d) cat(sprintf(
    "%-40s n=%3d  p50=%6.0f  p90=%7.0f  max=%8.0f  >1km=%2d\n", lbl, nrow(d),
    median(d$dist), quantile(d$dist, .9), max(d$dist), sum(d$dist > 1000)))
  house <- filter(ans, rank >= 30)
  cat("\n== cumulative filters ==\n")
  show("all usable answers", ans)
  show("rank 30 + own house number", house)
  kept <- filter(ans, kept)
  show("  + component agreement (shipped)", kept)
  cat(sprintf("\nplaced: %d/%d = %.1f%% of queried  (%d with >1 address passing)\n",
              nrow(kept), queried, 100 * nrow(kept) / queried,
              sum(kept$n_matches > 1)))

  # Unlike the geolocator, most of the loss here is the service declining to
  # answer rather than the floor throwing an answer away. `no answer` and
  # `best result is ... at rank 26` are coverage; the rest are disagreements.
  cat("\n== why addresses went unplaced ==\n")
  print(sort(table(sub(" .*", "", sub("^(street|civic|best result|no|request) ",
                                      "\\1_", out$reject[!is.na(out$reject)]))),
             decreasing = TRUE))
  cat("\n== rejected despite a house-level answer ==\n")
  lost <- filter(house, !kept)
  print(head(lost[order(-lost$dist), c("query", "title", "dist", "reject")], 8),
        row.names = FALSE)
  cat("\n== worst 8 survivors ==\n")
  print(head(kept[order(-kept$dist), c("query", "title", "dist")], 8),
        row.names = FALSE)
}
report(out)
