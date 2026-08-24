# Which BC Address Geocoder reference point sits closest to NAR's building point?
#
# The BC Address Geocoder can be asked for several different points on the same
# address -- `locationDescriptor` selects among the parcel centroid, the front
# door, the rooftop, the access point off the road, and the routing point. The
# note `inst/notes/geocoding-status.md` measures NAR against BC's *default*
# point and reports a p50 of ~20 m; that number is only interpretable if we know
# whether the default is the point NAR is trying to be. The NAR User Guide says a
# building representative point "may not correspond exactly to the physical
# center of the building structure itself" and may be the road access point or
# the driveway -- which is a hypothesis about `accessPoint`, not `parcelPoint`.
#
# This probe asks the service for every descriptor over the same sample of NAR
# BC building points and measures the distance to NAR's own coordinate.
#
#   Rscript data-raw/probe_bc.R
#
# Needs NAR_CACHE_PATH, an imported database, and network access to
# geocoder.api.gov.bc.ca. It makes one request per address per descriptor --
# `N_SAMPLE * length(DESCRIPTORS)` in total -- throttled to RATE per second.
# This is a free public service; keep the sample modest or register a key.

suppressPackageStartupMessages({
  library(cangeocode)
  library(dplyr)
})

N_SAMPLE <- as.integer(Sys.getenv("PROBE_BC_N", "400"))
SEED     <- 20260824
RATE     <- 5
OUT      <- "data-raw/probe_bc_results.rds"

# `any` is what bc_geocode() sends today (i.e. the service default).
DESCRIPTORS <- c("any", "parcelPoint", "frontDoorPoint", "rooftopPoint",
                 "accessPoint", "routingPoint")

# ---------------------------------------------------------------- the sample

con <- suppressWarnings(nar_connection())
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Ordering on a hash of the row's own GUID rather than `USING SAMPLE`: DuckDB
# applies a sample after the FROM clause and *before* the WHERE, so sampling
# here would draw from all 17M rows and then filter almost all of them away.
# The hash is deterministic, so the same SEED redraws the same addresses.
sample_sql <- sprintf("
  SELECT ADDR_GUID, CIVIC_NO, CIVIC_NO_SUFFIX, OFFICIAL_STREET_NAME,
         OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR, MAIL_MUN_NAME,
         MAIL_POSTAL_CODE, x, y
  FROM Addresses
  WHERE MAIL_PROV_ABVN = 'BC'
    AND geom_source = 'building'
    AND CIVIC_NO IS NOT NULL
    AND OFFICIAL_STREET_NAME IS NOT NULL
    AND MAIL_MUN_NAME IS NOT NULL
  ORDER BY hash(ADDR_GUID || '%d')
  LIMIT %d", SEED, N_SAMPLE)

addr <- DBI::dbGetQuery(con, sample_sql)

# The string the service is asked. Built from NAR's own official fields rather
# than through format_address(), so a parser difference cannot enter the
# measurement -- this probe is about positions, not about matching.
addr$query <- trimws(gsub("[[:space:]]+", " ", paste0(
  addr$CIVIC_NO, addr$CIVIC_NO_SUFFIX, " ",
  addr$OFFICIAL_STREET_NAME, " ", addr$OFFICIAL_STREET_TYPE, " ",
  addr$OFFICIAL_STREET_DIR, ", ", addr$MAIL_MUN_NAME, ", BC")))

message("Sampled ", nrow(addr), " BC building points.")

# ------------------------------------------------------------- the requests

fetch_one <- function(query, descriptor) {
  req <- httr2::request("https://geocoder.api.gov.bc.ca")
  req <- httr2::req_url_path_append(req, "addresses.json")
  req <- httr2::req_url_query(req, addressString = query, maxResults = 1,
                              outputSRS = 4326, locationDescriptor = descriptor)
  req <- httr2::req_user_agent(req, "cangeocode probe (R package)")
  req <- httr2::req_throttle(req, capacity = RATE, fill_time_s = 1,
                             realm = "geocoder.api.gov.bc.ca")
  req <- httr2::req_error(req, is_error = function(resp) FALSE)
  req <- httr2::req_retry(req, max_tries = 3)

  empty <- data.frame(lon = NA_real_, lat = NA_real_, score = NA_integer_,
                      precision = NA_character_, got = NA_character_,
                      accuracy = NA_character_, full = NA_character_,
                      stringsAsFactors = FALSE)

  resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
  if (inherits(resp, "error") || httr2::resp_status(resp) >= 400) return(empty)
  body <- tryCatch(httr2::resp_body_json(resp), error = function(e) NULL)
  f <- body$features
  if (!length(f)) return(empty)
  p <- f[[1]]$properties
  co <- f[[1]]$geometry$coordinates
  data.frame(
    lon       = if (length(co) == 2) as.numeric(co[[1]]) else NA_real_,
    lat       = if (length(co) == 2) as.numeric(co[[2]]) else NA_real_,
    score     = as.integer(p$score %||% NA),
    precision = p$matchPrecision %||% NA_character_,
    got       = p$locationDescriptor %||% NA_character_,
    accuracy  = p$locationPositionalAccuracy %||% NA_character_,
    full      = p$fullAddress %||% NA_character_,
    stringsAsFactors = FALSE)
}
`%||%` <- function(a, b) if (is.null(a)) b else a

res <- list()
for (d in DESCRIPTORS) {
  message("Requesting ", d, " (", nrow(addr), " addresses) ...")
  rows <- lapply(seq_len(nrow(addr)), function(i) fetch_one(addr$query[i], d))
  res[[d]] <- cbind(
    addr[, c("ADDR_GUID", "CIVIC_NO", "query", "x", "y")],
    asked = d,
    do.call(rbind, rows), stringsAsFactors = FALSE)
}
res <- do.call(rbind, res)
rownames(res) <- NULL

# ------------------------------------------------------------- the distance

# BC answers in EPSG:4326; NAR's x/y are stored in EPSG:3347. Transform BC's
# point rather than NAR's, so NAR's coordinate is used exactly as stored.
ok <- !is.na(res$lon) & !is.na(res$lat)
res$bc_x <- NA_real_
res$bc_y <- NA_real_
pts <- sf::st_transform(
  sf::st_as_sf(res[ok, ], coords = c("lon", "lat"), crs = 4326, remove = FALSE),
  3347)
res$bc_x[ok] <- sf::st_coordinates(pts)[, 1]
res$bc_y[ok] <- sf::st_coordinates(pts)[, 2]
res$dist_m <- sqrt((res$bc_x - res$x)^2 + (res$bc_y - res$y)^2)

saveRDS(res, OUT)
message("Wrote ", OUT)

# -------------------------------------------------------------- the summary

# Only addresses BC resolved to a parcel-level precision, with a high score, and
# where BC's own returned address carries the civic number we asked for -- and
# only those it resolved that way under *every* descriptor, so all six columns
# describe the same rows.
res$civic_ok <- mapply(function(full, no) {
  !is.na(full) && grepl(paste0("(^|[^0-9])", no, "([^0-9]|$)"), full)
}, res$full, res$CIVIC_NO)

usable <- res |>
  mutate(good = precision %in% c("SITE", "OCCUPANT", "UNIT", "CIVIC_NUMBER") &
           !is.na(score) & score >= 95 & civic_ok & !is.na(dist_m))

keep <- usable |>
  group_by(ADDR_GUID) |>
  summarise(all_good = all(good), .groups = "drop") |>
  filter(all_good) |>
  pull(ADDR_GUID)

cmp <- usable |> filter(ADDR_GUID %in% keep)

message("\n", length(keep), " of ", nrow(addr),
        " addresses resolved cleanly under all ", length(DESCRIPTORS),
        " descriptors.\n")

q <- function(x, p) round(as.numeric(stats::quantile(x, p, na.rm = TRUE)), 1)

cat("Distance from NAR's building point, metres:\n\n")
cmp |>
  group_by(asked) |>
  summarise(n = n(), p25 = q(dist_m, .25), p50 = q(dist_m, .50),
            p75 = q(dist_m, .75), p90 = q(dist_m, .90), p95 = q(dist_m, .95),
            mean = round(mean(dist_m), 1),
            within_10m = paste0(round(100 * mean(dist_m <= 10)), "%"),
            within_25m = paste0(round(100 * mean(dist_m <= 25)), "%"),
            .groups = "drop") |>
  arrange(p50) |>
  as.data.frame() |>
  print(row.names = FALSE)

cat("\nWhat the service actually returned for each request (all sampled rows):\n\n")
print(table(asked = res$asked, returned = res$got, useNA = "ifany"))

cat("\nPositional accuracy class BC reports, on the comparison set:\n\n")
cmp |> count(asked, accuracy) |> as.data.frame() |> print(row.names = FALSE)

# Paired: for each address, which descriptor was closest to NAR?
cat("\nPer address, the descriptor closest to NAR's point:\n\n")
cmp |>
  group_by(ADDR_GUID) |>
  slice_min(dist_m, n = 1, with_ties = FALSE) |>
  ungroup() |>
  count(asked, sort = TRUE) |>
  mutate(share = paste0(round(100 * n / sum(n)), "%")) |>
  as.data.frame() |>
  print(row.names = FALSE)

# Paired difference against the default, which is the decision-relevant number.
cat("\nPaired difference vs. the default `any`, metres (negative = closer to NAR):\n\n")
base <- cmp |> filter(asked == "any") |> select(ADDR_GUID, base_dist = dist_m)
cmp |>
  filter(asked != "any") |>
  inner_join(base, by = "ADDR_GUID") |>
  mutate(delta = dist_m - base_dist) |>
  group_by(asked) |>
  summarise(n = n(), median_delta = round(median(delta), 1),
            mean_delta = round(mean(delta), 1),
            closer_than_default = paste0(round(100 * mean(delta < -1)), "%"),
            same_point = paste0(round(100 * mean(abs(delta) <= 1)), "%"),
            .groups = "drop") |>
  as.data.frame() |>
  print(row.names = FALSE)
