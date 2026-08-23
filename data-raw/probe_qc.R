# Measures the Quebec government geocoder against NAR's own Quebec points.
#
# `https://servicescarto.mrnf.gouv.qc.ca/.../Adresse_Geocodage/GeocodeServer` is
# an Esri locator over the Répertoire québécois des adresses, published CC-BY,
# keyless, with a 1000-address batch endpoint and a reverse endpoint. This
# harness is what says what a `qc` tier is worth, and how the query has to be
# spelled to get it.
#
# Three things are measured, and PROBE_PART picks which:
#
#   render  -- how the query is spelled, which is NOT cosmetic here. The
#              locator's reference strings are French-canonical (`Rue
#              Notre-Dame Ouest`), and nar_address_string() renders the NAR
#              canonical form (`NOTRE-DAME RUE O`). Five spellings are compared
#              over the same addresses. This is what nar_qc_query() was chosen
#              from; see the table in its roxygen.
#
#   agree   -- how far the service's civic point sits from NAR's building point
#              for the same address, and whether the score predicts that.
#
#   tier    -- what the tier recovers: Quebec corporations addresses run
#              through NAR first, then the ones NAR leaves unplaced sent to the
#              service. This is the number that justifies the tier.
#
# The score deserves its own warning. It is NOT a precision ranking: over the
# `agree` sample the correlation between score and distance-from-NAR is
# Spearman 0.018, and street-only answers score HIGHER than civic ones. Read
# `Loc_name`, not `Score`. See nar_qc_precision().
#
# NAR is the reference here and a reference is not ground truth -- but in this
# case the two are not even independent. The locators are named `RQA_Adresse`
# and `RQA_Rue`, i.e. the service is serving the Répertoire, which is also what
# NAR's Quebec records are built from. A small median disagreement here is
# evidence of shared lineage, not of accuracy. See inst/notes/geocoding-status.md.
#
# Run with:  Rscript data-raw/probe_qc.R    (needs NAR_CACHE_PATH, httr2, sf)
#   PROBE_PART   render | agree | tier | all        (default all)
#   PROBE_N      addresses per part                 (default 400)
#   PROBE_OUT    where to save the raw results      (default probe-qc.rds)
#   EVAL_CACHE   where the corporations CSV lives    (<NAR_CACHE_PATH>/eval)
#
# The NAR samples are `REPEATABLE (42)` and the corporations draw re-seeds with
# set.seed(20260821), the same seed the other two harnesses use, so the runs
# are comparable and re-analysable without re-querying.

if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  suppressMessages(pkgload::load_all(quiet = TRUE))
} else {
  library(cangeocode)
}
suppressMessages(library(dplyr))
library(sf)

part     <- tolower(Sys.getenv("PROBE_PART", "all"))
n        <- as.integer(Sys.getenv("PROBE_N", "400"))
out_path <- Sys.getenv("PROBE_OUT", "probe-qc.rds")
parts_to_run <- if (part == "all") c("render", "agree", "tier") else part

con <- nar_connection()
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
saved <- list()

# One query per PROBE_N/2 rows with a direction and PROBE_N/2 without: the
# direction is where the spellings diverge most, and it is on only a fifth of
# Quebec addresses, so an unstratified draw would barely test it.
nar_qc_sample <- function(n) {
  q <- "
    SELECT * FROM (
      SELECT CIVIC_NO,
             OFFICIAL_STREET_NAME AS STREET_NAME,
             nullif(OFFICIAL_STREET_TYPE, '') AS STREET_TYPE,
             nullif(OFFICIAL_STREET_DIR, '')  AS STREET_DIR,
             coalesce(nullif(CSD_FRE_NAME, ''), CSD_ENG_NAME) AS MUN_NAME,
             nullif(MAIL_POSTAL_CODE, '') AS POSTAL_CODE, x, y
      FROM Addresses
      WHERE PROV_CODE = '24' AND geom_source = 'building'
            AND CIVIC_NO IS NOT NULL AND length(OFFICIAL_STREET_NAME) > 0 AND %s
    ) USING SAMPLE reservoir(%d ROWS) REPEATABLE (42)"
  d <- rbind(
    DBI::dbGetQuery(con, sprintf(q, "nullif(OFFICIAL_STREET_DIR, '') IS NOT NULL",
                                 ceiling(n / 2))),
    DBI::dbGetQuery(con, sprintf(q, "nullif(OFFICIAL_STREET_DIR, '') IS NULL",
                                 floor(n / 2))))
  d$PROV_ABVN <- "QC"
  d$APT_NO_LABEL <- NA_character_
  d$CIVIC_NO_SUFFIX <- NA_character_
  d
}

# The service is asked through the package's own request builder, so what is
# measured is what ships -- including the out-of-order ResultID handling and
# the refusal to read the French-locale Latitude attribute.
ask <- function(q, size = 200) {
  r <- do.call(rbind, lapply(split(seq_along(q), ceiling(seq_along(q) / size)),
                             function(i) nar_qc_batch(q[i])))
  rownames(r) <- NULL
  r
}

# NAR's x/y are stored PROJECTED, in the storage CRS -- not lon/lat. Tagging
# them 4269 and transforming silently produces NA distances.
gap_m <- function(res, nar, ok) {
  if (!any(ok)) return(rep(NA_real_, length(ok)))
  a <- st_transform(st_as_sf(res[ok, c("lon", "lat")], coords = c("lon", "lat"),
                             crs = 4326), 3347)
  b <- st_as_sf(nar[ok, ], coords = c("x", "y"), crs = nar_storage_crs())
  d <- rep(NA_real_, length(ok))
  d[ok] <- as.numeric(st_distance(st_geometry(a), st_transform(st_geometry(b), 3347),
                                  by_element = TRUE))
  d
}

pct <- function(x) sprintf("%5.1f%%", 100 * mean(x))

## ---------------------------------------------------------------- render ----
if ("render" %in% parts_to_run) {
  d <- nar_qc_sample(n)
  message("Rendering ", nrow(d), " Quebec addresses five ways")

  dirs <- nar_qc_dirs()
  types <- nar_qc_types()
  ex <- function(x, tab) {
    hit <- tab[toupper(x)]
    ifelse(is.na(x), NA_character_, ifelse(is.na(hit), x, unname(hit)))
  }
  glue <- function(...) {
    p <- lapply(list(...), function(z) ifelse(is.na(z), "", z))
    s <- trimws(gsub(" +", " ", do.call(paste, p)))
    paste0(s, ", ", d$MUN_NAME, ", ", d$PROV_ABVN)
  }
  spellings <- list(
    "NAR order, abbreviated (nar_address_string)" = nar_address_string(d),
    "NAR order, direction spelled out" =
      glue(d$CIVIC_NO, d$STREET_NAME, d$STREET_TYPE, ex(d$STREET_DIR, dirs)),
    "FR order, direction spelled out" =
      glue(d$CIVIC_NO, d$STREET_TYPE, d$STREET_NAME, ex(d$STREET_DIR, dirs)),
    "NAR order, type and direction spelled out" =
      glue(d$CIVIC_NO, d$STREET_NAME, ex(d$STREET_TYPE, types), ex(d$STREET_DIR, dirs)),
    "FR order, type and direction (nar_qc_query)" = nar_qc_query(d))

  tab <- do.call(rbind, lapply(names(spellings), function(nm) {
    r <- ask(spellings[[nm]])
    civic <- r$qc_locator %in% "RQA_Adresse"
    data.frame(spelling = nm,
               civic = pct(civic),
               street = pct(r$qc_locator %in% "RQA_Rue"),
               unmatched = pct(is.na(r$qc_locator)),
               med_score = round(median(r$qc_score[civic], na.rm = TRUE), 1),
               stringsAsFactors = FALSE)
  }))
  cat("\n== How the query is spelled ==\n")
  print(tab, row.names = FALSE, right = FALSE)
  saved$render <- list(sample = d, spellings = spellings, table = tab)
}

## ----------------------------------------------------------------- agree ----
if ("agree" %in% parts_to_run) {
  d <- nar_qc_sample(n)
  r <- ask(nar_qc_query(d))
  ok <- r$qc_locator %in% "RQA_Adresse" & !is.na(r$lon)
  gap <- gap_m(r, d, ok)
  s <- r$qc_score

  cat("\n== Distance from NAR's building point, civic matches ==\n")
  cat(sprintf("  n = %d of %d\n", sum(ok), nrow(d)))
  cat(sprintf("  p50 %.1f m   p90 %.1f m   p99 %.1f m   over 500 m %s\n",
              median(gap, na.rm = TRUE), quantile(gap, .9, na.rm = TRUE),
              quantile(gap, .99, na.rm = TRUE), pct(gap[ok] > 500)))
  cat("\n== Is the score worth anything? ==\n")
  cat(sprintf("  spearman(score, distance) = %.3f\n",
              suppressWarnings(cor(s[ok], gap[ok], method = "spearman",
                                   use = "complete.obs"))))
  cat(sprintf("  civic  scores %.1f to %.1f, median %.1f\n",
              min(s[ok], na.rm = TRUE), max(s[ok], na.rm = TRUE),
              median(s[ok], na.rm = TRUE)))
  st <- r$qc_locator %in% "RQA_Rue"
  if (any(st)) {
    cat(sprintf("  street scores %.1f to %.1f, median %.1f  <- higher\n",
                min(s[st], na.rm = TRUE), max(s[st], na.rm = TRUE),
                median(s[st], na.rm = TRUE)))
  }
  saved$agree <- list(sample = d, result = r, gap = gap)
}

## ------------------------------------------------------------------ tier ----
if ("tier" %in% parts_to_run) {
  # The same corporations file, filter and seed Part B of eval_normalize.R and
  # the tier-coverage table in geocoding-status.md use, filtered to Quebec.
  # Same location eval_normalize.R caches it in, and the same override.
  cache <- Sys.getenv("EVAL_CACHE",
                      file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
  csv <- file.path(cache, "corporations-active-cbca-en.csv")
  if (!file.exists(csv)) {
    stop("Need ", csv, " -- run data-raw/eval_normalize.R once to fetch it.")
  }
  corp <- as.data.frame(arrow::read_csv_arrow(
    csv, col_select = c("Street", "Street 2", "City/town",
                        "Province/territory", "Postal code")))
  names(corp) <- c("street", "street2", "city", "prov", "postal")
  corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))
  corp <- corp[nzchar(corp$street) & nzchar(corp$city) & corp$prov == "QC" &
                 grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal), ]
  set.seed(20260821)
  corp <- corp[sample.int(nrow(corp), min(n, nrow(corp))), , drop = FALSE]

  addr <- apply(cbind(corp$street, corp$street2, corp$city,
                      trimws(paste(corp$prov, corp$postal))), 1,
                function(v) paste(v[nzchar(v)], collapse = ", "))
  message("Geocoding ", length(addr), " Quebec corporations addresses")

  base <- geocode(addr, method = c("nar", "nar_interpolate"), con = con)
  with_qc <- geocode(addr, method = c("nar", "nar_interpolate", "qc"), con = con)

  # "Unplaced" is is.na() on the coordinate, never match_method == "none".
  unplaced <- is.na(base$lon)
  gained <- unplaced & !is.na(with_qc$lon)
  cat("\n== What the tier recovers ==\n")
  cat(sprintf("  NAR alone placed        %s\n", pct(!unplaced)))
  cat(sprintf("  with the qc tier        %s\n", pct(!is.na(with_qc$lon))))
  cat(sprintf("  recovered by the tier   %s of all, %s of what NAR left\n",
              pct(gained), pct(gained[unplaced])))
  print(table(with_qc$match_method[unplaced], useNA = "ifany"))
  saved$tier <- list(addr = addr, base = base, with_qc = with_qc)
}

saveRDS(saved, out_path)
message("Saved to ", out_path)
