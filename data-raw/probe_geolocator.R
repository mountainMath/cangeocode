# Measures NRCan's geolocator against NAR's own building points.
#
# `https://geolocator.api.geo.ca/geolocation/en/locate?q=` is national, keyless
# and needs no database, which makes it the obvious candidate for a `geocode()`
# tier that works before anything has been downloaded (issue #2). This harness
# is what says whether it is good enough, and under which filters.
#
# The service ALWAYS answers, so a response is not a match and the accuracy
# question is really a filtering question. Two floors are measured cumulatively,
# and they are the ones the shipped tier applies -- nar_nrcan_top() and
# nar_nrcan_floors() are called directly, so this measures the code, not a
# restatement of it:
#
#   1. the top result must be type `Street` with qualifier `INTERPOLATED_POSITION`
#      (`INTERPOLATED_CENTROID` means "found the street, not the civic number");
#   2. the returned `title` must re-parse, component by component, to the address
#      that was sent -- see nar_nrcan_agreement().
#
# Floor 2 replaces the substring-on-the-whole-title province and municipality
# checks the first pass used, which are kept in the report for comparison only.
# They let three separate failure modes through: the municipality migrating into
# the street name (`28 Silver ST, CORNER BROOK` -> `28 Brook Street, Corner
# Brook`), a silently substituted street type (`330 Spadina RD` -> `330 Spadina
# Avenue`), and a wholly different street of the right shape (`61 Oakridge BLVD,
# OAK BLUFF, MB` -> `61 Oak Bluff Road, Brandon`, 190 km away). Comparing fields
# rather than the whole string costs no recall and removes all three.
#
# PROBE_EXPAND decides how the query is spelled. This was an open question and
# is now settled: over the same sample the two spellings differ by one address
# in 139, so the tier sends NAR's own abbreviations and the knob is kept only to
# re-check that. It is not cosmetic in one respect -- some queries return HTTP
# 200 with a body of `{"message": "Internal server error"}` instead of an array,
# query-dependent and reproducible rather than transient, and which spelling
# triggers it varies: `100 Water St, Charlottetown, PE` fails where the
# spelled-out form works; `1155 Robson Street, Vancouver, BC` fails where the
# abbreviated form works.
#
# The reference is NAR's own building point, and a reference is not ground
# truth: NAR is accurate in general but carries its own poor and wrong records,
# so a "miss" here is a DISAGREEMENT and the geolocator is sometimes the one
# that is right. Read the worst-survivors table with that in mind -- a long tail
# entry is as likely to be a bad NAR record as a bad answer. See
# inst/notes/geocoding-status.md on why NAR's own accuracy is not a published
# number, and on why the BC geocoder cannot settle it either (more reliable than
# NAR where they differ, but plausibly sharing upstream data with it).
#
# Run with:  Rscript data-raw/probe_geolocator.R   (needs NAR_CACHE_PATH)
#   PROBE_N       addresses to sample                  (default 150)
#   PROBE_EXPAND  spell out street types and provinces (default FALSE)
#   PROBE_OUT     where to save the result             (default probe-<spelling>.rds)
#
# The sample is `REPEATABLE (42)`, so the two spellings are compared over the
# same addresses and the runs are re-analysable without re-querying.

if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  suppressMessages(pkgload::load_all(quiet = TRUE))
} else {
  library(cangeocode)
}
suppressMessages(library(dplyr))
library(sf)

n      <- as.integer(Sys.getenv("PROBE_N", "150"))
expand <- toupper(Sys.getenv("PROBE_EXPAND", "FALSE")) %in% c("TRUE", "1", "YES")
out_path <- Sys.getenv("PROBE_OUT",
                       paste0("probe-", if (expand) "expanded" else "abbreviated", ".rds"))

con <- nar_connection()
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

samp <- DBI::dbGetQuery(con, sprintf("
  SELECT CIVIC_NO, OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
         MAIL_MUN_NAME, MAIL_PROV_ABVN, x, y
  FROM Addresses
  WHERE geom_source = 'building' AND length(MAIL_MUN_NAME) > 0
        AND length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
  USING SAMPLE reservoir(%d ROWS) REPEATABLE (42)", n))

# The expanded spelling takes the longest surface form the lexicon records for
# each canonical type -- which is the spelled-out word, by construction.
types <- read.csv("data-raw/street_types.csv")
expand_type <- types |>
  group_by(canonical) |>
  slice_max(nchar(surface), n = 1, with_ties = FALSE) |>
  ungroup() |>
  (\(d) setNames(d$surface, d$canonical))()
prov_name <- setNames(nar_province_table()$name, nar_province_table()$abvn)

parts <- function(...) { v <- c(...); paste(v[!is.na(v) & nzchar(v)], collapse = " ") }
samp$query <- vapply(seq_len(nrow(samp)), function(i) with(samp[i, ], {
  ty <- if (expand && !is.na(expand_type[OFFICIAL_STREET_TYPE]))
    expand_type[[OFFICIAL_STREET_TYPE]] else OFFICIAL_STREET_TYPE
  pv <- if (expand) prov_name[[MAIL_PROV_ABVN]] else MAIL_PROV_ABVN
  paste0(parts(CIVIC_NO, OFFICIAL_STREET_NAME, ty, OFFICIAL_STREET_DIR),
         ", ", MAIL_MUN_NAME, ", ", pv)
}), character(1))

# `capacity`, not `rate` -- req_throttle(rate = 5) builds a 300-token bucket and
# lets the first 300 requests go at once. Same trap as R/geocode_bc.R.
#
# The body is read by the package's own nar_nrcan_top(), so what is measured
# here is what ships -- including its treatment of the 500-inside-a-200.
hit <- function(q) {
  r <- tryCatch(
    httr2::request(nar_nrcan_url()) |>
      httr2::req_url_query(q = q) |>
      httr2::req_timeout(25) |>
      httr2::req_throttle(capacity = 5, fill_time_s = 1, realm = "geo.ca") |>
      httr2::req_error(is_error = function(x) FALSE) |>
      httr2::req_perform(),
    error = function(e) NULL)
  if (is.null(r) || httr2::resp_status(r) != 200) return(nar_nrcan_top(list()))
  nar_nrcan_top(tryCatch(httr2::resp_body_json(r), error = function(e) list()))
}

message("Probing ", nrow(samp), " addresses, ",
        if (expand) "spelled out" else "abbreviated")
top <- do.call(rbind, lapply(seq_len(nrow(samp)), function(i) {
  if (i %% 25 == 0) message("  ", i, "/", nrow(samp))
  hit(samp$query[i])
}))
message("usable answers: ", sum(!is.na(top$nrcan_title)), "/", nrow(samp))

# The query side is NAR's own canonical components rather than a re-parse of
# the string that was sent: that is the truth this is measured against, and
# re-parsing would fold the normalizer's own errors into the geolocator's.
q_parts <- data.frame(
  CIVIC_NO    = samp$CIVIC_NO,
  STREET_NAME = samp$OFFICIAL_STREET_NAME,
  STREET_TYPE = samp$OFFICIAL_STREET_TYPE,
  STREET_DIR  = samp$OFFICIAL_STREET_DIR,
  MUN_NAME    = samp$MAIL_MUN_NAME,
  PROV_ABVN   = samp$MAIL_PROV_ABVN,
  stringsAsFactors = FALSE)

# The shipped floors, so the recall and the p90 reported here are the ones the
# tier actually enforces.
floors <- nar_nrcan_floors(top, q_parts)

answered <- !is.na(top$lon) & !is.na(top$lat)
dist <- rep(NA_real_, nrow(samp))
if (any(answered)) {
  pt <- st_as_sf(data.frame(lon = top$lon[answered], lat = top$lat[answered]),
                 coords = c("lon", "lat"), crs = 4326) |> st_transform(nar_crs(con))
  truth <- st_as_sf(samp[answered, c("x", "y")], coords = c("x", "y"),
                    crs = nar_crs(con))
  dist[answered] <- as.numeric(st_distance(pt, truth, by_element = TRUE))
}

# The floors the issue #2 comments reported, kept only for comparison: they
# tested the municipality and province as substrings of the WHOLE title, which
# is what let `28 Silver ST, CORNER BROOK` through as `28 Brook Street, Corner
# Brook`. The shipped floor compares field by field instead.
fold <- function(s) toupper(iconv(s, "UTF-8", "ASCII//TRANSLIT"))
prov_name <- setNames(nar_province_table()$name, nar_province_table()$abvn)
title <- top$nrcan_title
old_prov <- !is.na(title) & mapply(function(t, p)
  grepl(fold(prov_name[[p]]), fold(t), fixed = TRUE), title, samp$MAIL_PROV_ABVN)
old_mun <- !is.na(title) & mapply(function(t, m)
  grepl(fold(m), fold(t), fixed = TRUE), title, samp$MAIL_MUN_NAME)

out <- data.frame(
  query = samp$query, title = title, kind = top$nrcan_kind,
  qualifier = top$nrcan_qualifier, dist = dist,
  reject = floors$nrcan_reject, kept = floors$match_method == "nrcan",
  old_prov = old_prov, old_mun = old_mun,
  queried = nrow(samp), row.names = NULL)
saveRDS(out, out_path)

# --- what the filters buy -------------------------------------------------

report <- function(out) {
  queried <- out$queried[1]
  ans <- out[!is.na(out$dist), ]
  cat("\n== top result kind x qualifier ==\n")
  print(table(ans$kind, ans$qualifier))
  cat("\n== distance to NAR's building point, by class (m) ==\n")
  print(ans |> group_by(kind, qualifier) |>
          summarise(n = n(), p50 = round(median(dist)),
                    p90 = round(quantile(dist, .9)), max = round(max(dist)),
                    .groups = "drop") |> as.data.frame())

  show <- function(lbl, d) cat(sprintf(
    "%-40s n=%3d  p50=%6.0f  p90=%7.0f  max=%8.0f  >1km=%2d\n", lbl, nrow(d),
    median(d$dist), quantile(d$dist, .9), max(d$dist), sum(d$dist > 1000)))
  street <- filter(ans, kind == "Street", qualifier == "INTERPOLATED_POSITION")
  cat("\n== cumulative filters ==\n")
  show("all usable answers", ans)
  show("Street + INTERPOLATED_POSITION", street)
  show("  + province/mun in title (old, flawed)", filter(street, old_prov, old_mun))
  kept <- filter(ans, kept)
  show("  + component agreement (shipped)", kept)
  cat(sprintf("\nplaced: %d/%d = %.1f%% of queried\n",
              nrow(kept), queried, 100 * nrow(kept) / queried))

  cat("\n== why answers were rejected ==\n")
  print(sort(table(sub(" .*", "", sub("^(street|civic|top result|no) ", "\\1_",
                                      out$reject[!is.na(out$reject)]))),
             decreasing = TRUE))
  cat("\n== what the shipped floor removed that the old one kept ==\n")
  lost <- filter(street, old_prov, old_mun, !kept)
  print(head(lost[order(-lost$dist), c("query", "title", "dist", "reject")], 8),
        row.names = FALSE)
  cat("\n== worst 8 survivors ==\n")
  print(head(kept[order(-kept$dist), c("query", "title", "dist")], 8),
        row.names = FALSE)
}
report(out)
