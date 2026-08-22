# Measures NRCan's geolocator against NAR's own building points.
#
# `https://geolocator.api.geo.ca/geolocation/en/locate?q=` is national, keyless
# and needs no database, which makes it the obvious candidate for a `geocode()`
# tier that works before anything has been downloaded (issue #2). This harness
# is what says whether it is good enough, and under which filters.
#
# The service ALWAYS answers -- `Zzzzqqq` comes back as 25 Geonames -- so a
# response is not a match and the accuracy question is really a filtering
# question. Three floors are measured cumulatively:
#
#   1. the top result must be type `Street` with qualifier `INTERPOLATED_POSITION`
#      (`INTERPOLATED_CENTROID` means "found the street, not the civic number");
#   2. the province must appear in the returned `title`;
#   3. the municipality must appear in the returned `title`.
#
# Floor 3 is not merely a distance filter. `123 Rue Sainte-Catherine, Montreal,
# QC` returns `123 Rue Guerin, Sainte-Catherine, Quebec` -- wrong street, wrong
# city, INTERPOLATED_POSITION, top-ranked. Only the title check rejects it.
#
# PROBE_EXPAND additionally decides how the query is spelled, which is not a
# cosmetic choice: some queries return HTTP 200 with a body of
# `{"message": "Internal server error"}` instead of an array, and which ones is
# query-dependent and reproducible rather than transient. `100 Water St,
# Charlottetown, PE` fails where the spelled-out form works; `1155 Robson
# Street, Vancouver, BC` fails where the abbreviated form works. So the two
# spellings have different recall and must be compared, not assumed.
#
# Truth is NAR's own building point, so a "miss" here is a disagreement with
# NAR rather than an absolute error -- see inst/notes/geocoding-status.md on
# why NAR's own accuracy is not a published number.
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
hit <- function(q) {
  r <- tryCatch(
    httr2::request("https://geolocator.api.geo.ca/geolocation/en/locate") |>
      httr2::req_url_query(q = q) |>
      httr2::req_timeout(25) |>
      httr2::req_throttle(capacity = 5, fill_time_s = 1, realm = "geo.ca") |>
      httr2::req_error(is_error = function(x) FALSE) |>
      httr2::req_perform(),
    error = function(e) NULL)
  if (is.null(r) || httr2::resp_status(r) != 200) return(NULL)
  j <- tryCatch(httr2::resp_body_json(r), error = function(e) NULL)
  # A named list is a dict, which is the 500-in-a-200. An empty list is an
  # honest "nothing found". Neither is an answer.
  if (!length(j) || !is.null(names(j))) return(NULL)
  top <- j[[1]]
  list(type = sub("^.*\\.", "", top$type), qualifier = top$qualifier,
       title = top$title,
       lon = top$geometry$coordinates[[1]], lat = top$geometry$coordinates[[2]])
}

message("Probing ", nrow(samp), " addresses, ",
        if (expand) "spelled out" else "abbreviated")
res <- lapply(seq_len(nrow(samp)), function(i) {
  if (i %% 25 == 0) message("  ", i, "/", nrow(samp))
  h <- hit(samp$query[i]); if (!is.null(h)) h$i <- i
  h
})
ok <- Filter(Negate(is.null), res)
message("usable answers: ", length(ok), "/", nrow(samp))

idx <- vapply(ok, `[[`, integer(1), "i")
chr <- function(f) vapply(ok, function(h) h[[f]] %||% NA_character_, character(1))
num <- function(f) vapply(ok, `[[`, numeric(1), f)

pt <- st_as_sf(data.frame(lon = num("lon"), lat = num("lat")),
               coords = c("lon", "lat"), crs = 4326) |> st_transform(nar_crs(con))
truth <- st_as_sf(samp[idx, c("x", "y")], coords = c("x", "y"), crs = nar_crs(con))

fold <- function(s) toupper(iconv(s, "UTF-8", "ASCII//TRANSLIT"))
out <- data.frame(
  query = samp$query[idx], type = chr("type"), qualifier = chr("qualifier"),
  title = chr("title"), dist = as.numeric(st_distance(pt, truth, by_element = TRUE)),
  prov_ok = mapply(function(t, p) grepl(fold(prov_name[[p]]), fold(t), fixed = TRUE),
                   chr("title"), samp$MAIL_PROV_ABVN[idx]),
  mun_ok = mapply(function(t, m) grepl(fold(m), fold(t), fixed = TRUE),
                  chr("title"), samp$MAIL_MUN_NAME[idx]),
  queried = nrow(samp), row.names = NULL)
saveRDS(out, out_path)

# --- what the filters buy -------------------------------------------------

report <- function(out) {
  queried <- out$queried[1]
  cat("\n== top result type x qualifier ==\n")
  print(table(out$type, out$qualifier))
  cat("\n== distance to NAR's building point, by class (m) ==\n")
  print(out |> group_by(type, qualifier) |>
          summarise(n = n(), p50 = round(median(dist)),
                    p90 = round(quantile(dist, .9)), max = round(max(dist)),
                    .groups = "drop") |> as.data.frame())

  show <- function(lbl, d) cat(sprintf(
    "%-40s n=%3d  p50=%6.0f  p90=%7.0f  max=%8.0f  >1km=%2d\n", lbl, nrow(d),
    median(d$dist), quantile(d$dist, .9), max(d$dist), sum(d$dist > 1000)))
  street <- filter(out, type == "Street", qualifier == "INTERPOLATED_POSITION")
  cat("\n== cumulative filters ==\n")
  show("all usable answers", out)
  show("Street + INTERPOLATED_POSITION", street)
  show("  + province in returned title", filter(street, prov_ok))
  kept <- filter(street, prov_ok, mun_ok)
  show("  + municipality in returned title", kept)
  cat(sprintf("\nplaced: %d/%d = %.1f%% of queried\n",
              nrow(kept), queried, 100 * nrow(kept) / queried))
  cat("\n== worst 8 survivors ==\n")
  print(head(kept[order(-kept$dist), c("query", "title", "dist")], 8), row.names = FALSE)
}
report(out)
