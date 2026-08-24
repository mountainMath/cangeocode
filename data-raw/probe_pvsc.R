# Measures Nova Scotia's PVSC open address data against this package, which is
# the first time any measurement here has had a witness that is genuinely
# INDEPENDENT of NAR -- a claim stage 0 establishes rather than assumes.
#
# Every accuracy number in inst/notes/ so far carries the same caveat: the
# reference is NAR's own building point, so a disagreement is a disagreement and
# not an error. inst/notes/nar-consistency.md got around that once, by finding a
# contradiction INSIDE a NAR row (its postal code against its coordinate), but
# that only reaches rows where the two disagree and it has no third side to
# appeal to.
#
# Property Valuation Services Corporation publishes, through Nova Scotia's
# DataZONE, a point and a split address for every improved property it assesses
# -- 386,186 residential and 42,382 commercial. It is a separate organisation
# reading the same houses for a different purpose (assessment, not addressing),
# so it is a second reading rather than a copy. That is what makes it worth the
# download: for Nova Scotia it turns "NAR and X disagree" into a question with
# an answer.
#
# It measures six things, and they answer different questions:
#
#   0. PROVENANCE -- is PVSC one of NAR's own sources? Joins NAR against both
#      PVSC and NSCAF on keys unique province-wide on both sides and reads the
#      shape of the distance distribution, the way inst/notes/quebec-addresses.md
#      established that NAR carries RQA's coordinates. Downloads NSCAF (~85 MB).
#      Everything below is void if this stage does not clear PVSC.
#   1. INGEST   -- what the two files carry and how much of it is usable: the
#      address components are already SPLIT by PVSC, which stages 4 and 5 need.
#      A file-level fact; no NAR needed.
#   2. LOCATION -- for addresses both sources carry, how far is NAR's building
#      point from PVSC's? This is the headline, and it is the one number in the
#      package that is not measured against NAR itself.
#   3. TIERS    -- what nar_interpolate and rnf recover of what the exact tier
#      misses, and how accurate the recovered points are. Same question
#      road-network-file.md asks, now against an outside reference.
#   4. COVERAGE -- how much of PVSC's address list NAR has no counterpart for at
#      all, which is what a PVSC tier would be worth.
#   5. PARSE    -- PVSC's split components are LABELS, so this is a corpus in
#      the sense data-raw/eval_deepparse.R uses the word. It scores
#      normalize_address() against them and isolates the two failure families
#      that showed up.
#
# Two cautions on reading stage 5. First, the string being parsed is rendered
# from the same fields being scored, so this measures whether the parser can
# invert PVSC's spelling conventions -- a real test, because those conventions
# differ from NAR's (PVSC writes `HIGHWAY 1`, NAR writes `1` + `HWY`), but a
# weaker one than the held-out corpora in inst/notes/deepparse.md. Second, a
# raw street-name mismatch is NOT an error rate: most of them are the gazetteer
# correctly snapping PVSC's spelling to NAR's (`FERGUSONS` -> `FERGUSON`,
# `MCDOUGAL` -> `MCDOUGALL`). Only the two families stage 5 isolates are defects.
#
# The one thing PVSC cannot settle: where the two sources disagree it is still
# not established which is right. What it does settle is that the disagreement
# is real rather than an artefact of comparing NAR to itself, and -- because the
# two failure families it isolates are visible in the PARSE without looking at a
# coordinate at all -- which rows to disbelieve.
#
# Findings are written up in inst/notes/nova-scotia-pvsc.md. Read that first;
# this file is how to reproduce it.
#
# Run with:  Rscript data-raw/probe_pvsc.R    (needs NAR_CACHE_PATH)
#   PVSC_DIR     where the downloads and working files go (default <NAR_CACHE_PATH>/pvsc)
#   PVSC_N       addresses to sample for stages 2-4      (default 40000)
#   PVSC_N_PARSE addresses to sample for stage 5         (default 25000)
#   PVSC_STAGES  any of "012345"                         (default 012345)
#
# The two downloads are ~66 MB together and are cached, so a re-run costs
# nothing. Stages 2-5 take about ten minutes at the default sample sizes.
#
# On the source: DataZONE is a Socrata instance, so the four-by-four dataset
# ids address both a metadata endpoint and a CSV export.
#
#   residential  a859-xvcs   https://www.thedatazone.ca/d/a859-xvcs
#   commercial   9ac6-zg6i   https://www.thedatazone.ca/d/9ac6-zg6i
#
#   metadata  https://www.thedatazone.ca/api/views/<id>.json
#   csv       https://www.thedatazone.ca/api/views/<id>/rows.csv?accessType=DOWNLOAD
#
# LICENCE. The metadata carries no `licenseId`; the licence is in
# `metadata.custom_fields.["License/Attribution"]` and is the "Open Data &
# Information Government Licence - PVSC & Participating Municipalities" v1.0
# (https://www.pvsc.ca/en/home/datazone/datazone-license.aspx). It is an OGL
# variant: worldwide, royalty-free, perpetual, commercial use allowed, the only
# condition being attribution. That composes with the OGL and CC-BY sources this
# package already uses, so -- unlike OSM, which is out on ODbL grounds and not
# on accuracy grounds -- nothing in the licence stops PVSC becoming a tier.

library(cangeocode)
library(duckdb)

DIR <- Sys.getenv("PVSC_DIR", file.path(Sys.getenv("NAR_CACHE_PATH"), "pvsc"))
N       <- as.integer(Sys.getenv("PVSC_N", "40000"))
N_PARSE <- as.integer(Sys.getenv("PVSC_N_PARSE", "25000"))
STAGES  <- Sys.getenv("PVSC_STAGES", "012345")
dir.create(DIR, showWarnings = FALSE, recursive = TRUE)

SETS <- list(
  residential = "a859-xvcs",
  commercial  = "9ac6-zg6i"
)

# PVSC names the same columns differently in the two files.
COLS <- list(
  residential = list(mun = "Municipal Unit", num = "Civic Number",
                     addl = "Civic Additional", dir = "Civic Direction",
                     street = "Civic Street Name", suffix = "Civic Street Suffix",
                     city = "Civic City Name",
                     x = "X Map Coordinate", y = "Y Map Coordinate"),
  commercial  = list(mun = "Municipal Unit", num = "Civic Number",
                     addl = "Civic Additional", dir = "Civic Direction",
                     street = "Civic Street Name", suffix = "Civic Street Suffix",
                     city = "Civic City Name",
                     x = "X Map Coordinate", y = "Y Map Coordinate")
)

stage <- function(n) grepl(n, STAGES, fixed = TRUE)
say   <- function(...) cat(..., "\n", sep = "")
rule  <- function(s) say("\n== ", s, " ", strrep("=", max(0, 62 - nchar(s))))

## Distances are geodesic on the sphere. The storage CRS (EPSG:3347) would do --
## probe_rnf.R uses ST_Distance on it -- but its standard parallels are 49N and
## 77N, and Nova Scotia sits below both, so at the ten-metre scale this reports
## the number without asking how much the projection cost.
hav <- function(lon1, lat1, lon2, lat2) {
  R <- 6371008.8; p <- pi / 180
  a <- sin((lat2 - lat1) * p / 2)^2 +
    cos(lat1 * p) * cos(lat2 * p) * sin((lon2 - lon1) * p / 2)^2
  2 * R * asin(pmin(1, sqrt(a)))
}

quants <- function(x) {
  x <- x[is.finite(x)]
  if (!length(x)) return(NULL)
  sprintf("n=%6d  p50=%7.1f  p75=%8.1f  p90=%9.1f  p95=%10.1f  <=100m %5.1f%%  >1km %5.2f%%",
          length(x), stats::median(x), stats::quantile(x, .75),
          stats::quantile(x, .90), stats::quantile(x, .95),
          100 * mean(x <= 100), 100 * mean(x > 1000))
}

kf <- function(x) cangeocode:::nar_key_fold(ifelse(is.na(x), "", x))

## ---------------------------------------------------------------------------
## 1  INGEST
## ---------------------------------------------------------------------------
## Downloads both files and reduces them to one row per distinct address+point.
##
## The de-duplication matters and is not cosmetic. PVSC emits one row per LIVING
## UNIT, not per address -- its own description warns that a multi-unit parcel
## repeats the assessment account -- so counting rows would weight an apartment
## block by its unit count and quietly make the accuracy figures a statement
## about Halifax.

pvsc_path <- function(id) file.path(DIR, paste0("pvsc_", id, ".csv"))

pvsc_download <- function() {
  for (nm in names(SETS)) {
    f <- pvsc_path(SETS[[nm]])
    if (file.exists(f)) { say("cached  ", nm, ": ", f); next }
    url <- sprintf("https://www.thedatazone.ca/api/views/%s/rows.csv?accessType=DOWNLOAD",
                   SETS[[nm]])
    say("downloading ", nm, " from ", url)
    old <- options(timeout = 1200); on.exit(options(old), add = TRUE)
    utils::download.file(url, f, mode = "wb", quiet = TRUE)
  }
}

pvsc_load <- function() {
  out <- lapply(names(SETS), function(nm) {
    cn <- COLS[[nm]]
    d <- suppressWarnings(readr::read_csv(pvsc_path(SETS[[nm]]),
                                          show_col_types = FALSE, progress = FALSE))
    say(sprintf("%-12s rows %6d   no coordinate %6d (%4.1f%%)   no civic number %5d",
                nm, nrow(d), sum(is.na(d[[cn$x]])), 100 * mean(is.na(d[[cn$x]])),
                sum(is.na(d[[cn$num]]))))
    data.frame(set = nm, mun = d[[cn$mun]], num = d[[cn$num]], addl = d[[cn$addl]],
               dir = d[[cn$dir]], street = d[[cn$street]], suffix = d[[cn$suffix]],
               city = d[[cn$city]], lon = as.numeric(d[[cn$x]]),
               lat = as.numeric(d[[cn$y]]), stringsAsFactors = FALSE)
  })
  p <- do.call(rbind, out)
  say("total rows: ", nrow(p))
  p <- p[!is.na(p$num) & !is.na(p$street) & !is.na(p$lon) & !is.na(p$lat), ]
  say("with a civic number, a street and a coordinate: ", nrow(p))

  ## Rendered the way a user would hand it in: number, optional alpha suffix,
  ## street, optional type, optional direction, community, province.
  p$addr <- paste0(p$num, ifelse(is.na(p$addl), "", p$addl), " ", p$street,
                   ifelse(is.na(p$suffix), "", paste0(" ", p$suffix)),
                   ifelse(is.na(p$dir), "", paste0(" ", p$dir)),
                   ", ", p$city, ", NS")
  p <- p[!duplicated(paste(p$addr, p$lon, p$lat)), ]
  say("distinct address+point: ", nrow(p))
  p
}

CLEAN <- file.path(DIR, "pvsc_clean.rds")
if (stage("1") || !file.exists(CLEAN)) {
  rule("1  INGEST")
  pvsc_download()
  saveRDS(pvsc_load(), CLEAN)
}
p <- readRDS(CLEAN)

con <- nar_connection()
set.seed(1)
s <- p[sample(nrow(p), min(N, nrow(p))), ]

## ---------------------------------------------------------------------------
## 0  PROVENANCE
## ---------------------------------------------------------------------------
## Whether PVSC is independent of NAR at all. Everything after this depends on
## it, and it is not a safe assumption: StatCan's Statistical Building Register
## (https://www23.statcan.gc.ca/imdb/p2SV.pl?Function=getSurvey&SDDS=5380), which
## NAR is extracted from, names "property assessment roles" among its inputs --
## though as a source of ATTRIBUTES (building type and unit usage; NAR's BU_USE
## and BU_N_CIVIC_ADD, 100% populated in every province) rather than of the
## universe, which comes from Canada Post Point-of-Call and the provincial 911
## files. In Nova Scotia the 911 file is NSCAF, so both are measured here.
##
## The test is the one quebec-addresses.md used on RQA: join on keys unique
## PROVINCE-WIDE on both sides -- which sidesteps the municipality, the one field
## NS is known to disagree on -- and look at the SHAPE near zero. A copy has its
## mass in a spike; two readings of the same house do not.

NSCAF_URL <- "https://data.novascotia.ca/api/views/tntn-er5g/rows.csv?accessType=DOWNLOAD"

if (stage("0")) {
  rule("0  PROVENANCE")
  nscaf <- file.path(DIR, "nscaf.csv")
  if (!file.exists(nscaf)) {
    say("downloading NSCAF civic points (~85 MB)")
    old <- options(timeout = 1200); on.exit(options(old), add = TRUE)
    utils::download.file(NSCAF_URL, nscaf, mode = "wb", quiet = TRUE)
  }

  ## Both NAR name families via UNION rather than OR (see .claude/geocoding.md).
  DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE nn AS SELECT * FROM (
      SELECT CAST(CIVIC_NO AS BIGINT) AS civic,
             strip_accents(upper(OFFICIAL_STREET_NAME)) AS nm,
             strip_accents(upper(coalesce(OFFICIAL_STREET_TYPE,''))) AS ty,
             nar_lon(geom) AS lon, nar_lat(geom) AS lat
      FROM Addresses WHERE PROV_CODE='12' AND geom IS NOT NULL
      UNION
      SELECT CAST(CIVIC_NO AS BIGINT), strip_accents(upper(MAIL_STREET_NAME)),
             strip_accents(upper(coalesce(MAIL_STREET_TYPE,''))),
             nar_lon(geom), nar_lat(geom)
      FROM Addresses WHERE PROV_CODE='12' AND geom IS NOT NULL)")

  ## A key survives only if it names ONE point on its own side; otherwise a
  ## disagreement between the sources cannot be told from one within them.
  uniq <- function(src, tbl) DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE TEMP TABLE %s AS
       SELECT civic, nm, ty, any_value(lon) AS lon, any_value(lat) AS lat
       FROM %s GROUP BY 1,2,3
       HAVING count(DISTINCT (lon::VARCHAR||','||lat::VARCHAR))=1", tbl, src))
  uniq("nn", "nnu")

  DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE ns_raw AS
      SELECT CAST(CIVICNUM AS BIGINT) AS civic,
             strip_accents(upper(STRNAME)) AS nm,
             strip_accents(upper(coalesce(STRSUFFIX,''))) AS ty,
             LONG AS lon, LAT AS lat
      FROM read_csv_auto('%s', SAMPLE_SIZE=-1)
      WHERE CIVICNUM IS NOT NULL AND LAT IS NOT NULL", nscaf))
  uniq("ns_raw", "nsu")

  pp <- p
  pp$ty <- ifelse(is.na(pp$suffix), "", pp$suffix)
  duckdb::duckdb_register(con, "pv_raw", pp[, c("num", "street", "ty", "lon", "lat")])
  DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE pv0 AS
      SELECT CAST(num AS BIGINT) AS civic, strip_accents(upper(street)) AS nm,
             strip_accents(upper(ty)) AS ty, lon, lat
      FROM pv_raw WHERE num IS NOT NULL")
  uniq("pv0", "pvu")

  pair <- function(a, b, label) {
    m <- DBI::dbGetQuery(con, sprintf(
      "SELECT a.lon AS alon, a.lat AS alat, b.lon AS blon, b.lat AS blat
         FROM %s a JOIN %s b USING (civic, nm, ty)", a, b))
    d <- hav(m$alon, m$alat, m$blon, m$blat)
    d <- d[is.finite(d)]
    say(sprintf("%-16s n=%7d  p10=%6.3f p50=%7.3f p90=%8.1f  <1m %5.2f%%  ==0 %d",
                label, length(d), stats::quantile(d, .1), stats::median(d),
                stats::quantile(d, .9), 100 * mean(d < 1), sum(d == 0)))
    invisible(list(m = m, d = d))
  }

  say("Quebec's NAR<->RQA, for scale: n=2512836  p50=0.210  <1m 72.3%\n")
  ns <- pair("nnu", "nsu", "NAR <-> NSCAF")
  pv <- pair("nnu", "pvu", "NAR <-> PVSC")
  xx <- pair("nsu", "pvu", "NSCAF <-> PVSC")

  ## If NAR<->NSCAF is a copy, the offset is one VECTOR, not scatter: removing
  ## its mean should leave centimetres. A latitude gradient on top of that is
  ## what distinguishes a datum transform from an arbitrary constant.
  k  <- ns$m[is.finite(ns$d) & ns$d < 5, ]
  dx <- (k$blon - k$alon) * 111320 * cos(k$alat * pi / 180)
  dy <- (k$blat - k$alat) * 110540
  res <- sqrt((dx - mean(dx))^2 + (dy - mean(dy))^2)
  say(sprintf("\nNAR<->NSCAF shift: dx %+.3f m  dy %+.3f m  |d| %.3f m  (n=%d)",
              mean(dx), mean(dy), sqrt(mean(dx)^2 + mean(dy)^2), nrow(k)))
  say(sprintf("residual after removing it: p50 %.3f m  p95 %.3f m",
              stats::median(res), stats::quantile(res, .95)))
  b <- cut(k$alat, stats::quantile(k$alat, seq(0, 1, .25)), include.lowest = TRUE)
  for (lv in levels(b)) say(sprintf("  lat %-16s dy %+.3f m", lv, mean(dy[b == lv])))

  ## Second, independent line: an address DONOR is close to a subset of the
  ## recipient. Neither of these is.
  o <- DBI::dbGetQuery(con, "SELECT
     (SELECT count(*) FROM (SELECT DISTINCT civic,nm,ty FROM pv0)) AS pvsc_keys,
     (SELECT count(*) FROM (SELECT DISTINCT civic,nm,ty FROM nn))  AS nar_keys,
     (SELECT count(*) FROM (SELECT DISTINCT civic,nm,ty FROM pv0)
        ANTI JOIN (SELECT DISTINCT civic,nm,ty FROM nn) USING (civic,nm,ty)) AS pvsc_only,
     (SELECT count(*) FROM (SELECT DISTINCT civic,nm,ty FROM nn)
        ANTI JOIN (SELECT DISTINCT civic,nm,ty FROM pv0) USING (civic,nm,ty)) AS nar_only")
  say(sprintf("\nkeys: PVSC %d (%.1f%% absent from NAR)   NAR %d (%.1f%% absent from PVSC)",
              o$pvsc_keys, 100 * o$pvsc_only / o$pvsc_keys,
              o$nar_keys,  100 * o$nar_only  / o$nar_keys))
  say("\nreading: NAR<->NSCAF is one re-datumed coordinate, so NSCAF cannot check")
  say("NAR in NS. PVSC sits the same distance from both, so it can.")
}

## ---------------------------------------------------------------------------
## 2  LOCATION
## ---------------------------------------------------------------------------
## The headline. Geocodes the sample through the shipped pipeline and measures
## the distance to PVSC's own point.
##
## Split three ways, because the three populations are not comparable:
##
##   * by tier, since a blockface point is not a building point (nar-database.md);
##   * by n_matches, since an ambiguous match resolves to one of several places;
##   * by whether the gazetteer KEPT the community name PVSC gave or REMAPPED it
##     to a different mailing municipality. That third split is the one this
##     probe exists to find -- see the note.

BENCH <- file.path(DIR, "bench.rds")
if (stage("2")) {
  rule("2  LOCATION")
  g <- geocode(s$addr, prov = "NS", con = con)
  s$lonG <- g$lon; s$latG <- g$lat
  s$method <- g$match_method; s$nm <- g$n_matches; s$unc <- g$uncertainty_m
  s$d <- hav(s$lon, s$lat, s$lonG, s$latG)
  saveRDS(s, BENCH)

  say("\nmatch method:")
  print(table(s$method, useNA = "ifany"))

  say("\ndistance to PVSC, by tier:")
  for (m in setdiff(sort(unique(s$method)), "none"))
    say(sprintf("  %-17s %s", m, quants(s$d[s$method == m])))

  b <- s$method == "nar_building" & s$nm == 1
  say("\nexact, unambiguous building matches:")
  say("  ", quants(s$d[b]))
  x <- s$d[b]; x <- x[is.finite(x)]
  for (t in c(10, 25, 50, 100, 250, 500, 1000, 5000))
    say(sprintf("    <=%5dm: %6.2f%%", t, 100 * mean(x <= t)))

  say("\nby whether the community name survived the parse:")
  n <- normalize_address(s$addr, prov = "NS", con = con)
  same <- kf(n$MUN_NAME) == kf(s$city)
  for (g2 in c(TRUE, FALSE)) {
    y <- s$d[b & same == g2]; y <- y[is.finite(y)]
    say(sprintf("  mun %-8s n=%6d  p50=%5.1f  p99=%9.1f  >1km %5.2f%%  >5km %5.2f%%",
                if (g2) "kept" else "remapped", length(y), stats::median(y),
                stats::quantile(y, .99), 100 * mean(y > 1000), 100 * mean(y > 5000)))
  }
  gross <- b & is.finite(s$d) & s$d > 5000
  say(sprintf("  of the %d errors beyond 5 km, %.0f%% had the name remapped",
              sum(gross), 100 * mean(!same[gross])))

  ## uncertainty_m is reported as 0 for every exact NAR match, including the
  ## ones that are kilometres out. It describes the SPREAD of the candidates
  ## found, not the chance that the right candidate was among them.
  say("\nuncertainty_m against observed error, exact matches:")
  u0 <- b & is.finite(s$d) & !is.na(s$unc) & s$unc == 0
  say(sprintf("  uncertainty 0 m: n=%6d  p50=%5.1f  >1km %5.2f%%  >5km %5.2f%%",
              sum(u0), stats::median(s$d[u0]),
              100 * mean(s$d[u0] > 1000), 100 * mean(s$d[u0] > 5000)))
}

## ---------------------------------------------------------------------------
## 3  TIERS
## ---------------------------------------------------------------------------
## What the fallback tiers recover of what the exact tier misses, and what the
## recovered points are worth. Tier by tier rather than in priority order, so
## the attribution is clean.

if (stage("3")) {
  rule("3  TIERS")
  s <- readRDS(BENCH)
  g_nar <- geocode(s$addr, prov = "NS", method = "nar", con = con)
  miss <- is.na(g_nar$lon)
  say(sprintf("nar exact places %.1f%%; %d rows left", 100 * mean(!miss), sum(miss)))

  g_int <- geocode(s$addr[miss], prov = "NS", method = "nar_interpolate", con = con)
  g_rnf <- geocode(s$addr[miss], prov = "NS", method = "rnf", con = con)
  d_int <- hav(s$lon[miss], s$lat[miss], g_int$lon, g_int$lat)
  d_rnf <- hav(s$lon[miss], s$lat[miss], g_rnf$lon, g_rnf$lat)

  say(sprintf("\nnar_interpolate recovers %5.1f%% of the misses", 100 * mean(!is.na(g_int$lon))))
  say("  ", quants(d_int))
  say(sprintf("rnf             recovers %5.1f%% of the misses", 100 * mean(!is.na(g_rnf$lon))))
  say("  ", quants(d_rnf))

  only <- is.na(g_int$lon) & !is.na(g_rnf$lon)
  say(sprintf("\nrows only rnf reaches: %d", sum(only)))
  say("  ", quants(d_rnf[only]))
}

## ---------------------------------------------------------------------------
## 4  COVERAGE
## ---------------------------------------------------------------------------
## Strips the community name and asks whether NAR carries that street and number
## ANYWHERE in the province. A row that fails this is one NAR has no counterpart
## for, as opposed to one the gazetteer merely failed to route -- which is the
## distinction stage 2's remap split turns on, asked the other way round.

if (stage("4")) {
  rule("4  COVERAGE")
  s <- readRDS(BENCH)
  core <- sub(",[^,]*, NS$", ", NS", s$addr)
  g <- geocode(core, prov = "NS", method = "nar", con = con)
  say(sprintf("NAR carries the street and number somewhere in NS: %5.1f%%", 100 * mean(!is.na(g$lon))))
  say(sprintf("  of those, ambiguous (n_matches > 1)            : %5.1f%%",
              100 * mean(g$n_matches > 1, na.rm = TRUE)))
  say(sprintf("no counterpart anywhere in NAR                   : %5.1f%%", 100 * mean(is.na(g$lon))))
  absent <- is.na(g$lon)
  r <- geocode(core[absent], prov = "NS", method = "rnf", con = con)
  say(sprintf("  of the absent, RNF at least knows the street   : %5.1f%%", 100 * mean(!is.na(r$lon))))
  say("\nexamples with no NAR counterpart:")
  print(utils::head(s$addr[absent], 15))
}

## ---------------------------------------------------------------------------
## 5  PARSE
## ---------------------------------------------------------------------------
## PVSC's split components scored as labels.
##
## Two conventions are folded onto the gold side before scoring, because they
## are spelling differences and not parse errors: PVSC writes a numbered
## provincial highway as `HIGHWAY 1` with no type where NAR writes `1` + `HWY`,
## and Cape Breton writes the same road as `NO 3` + `HWY`. Applying that
## knowledge to the labels is fair -- the parser has to do the same work either
## way -- and leaving it out would report a spurious four-point loss.

if (stage("5")) {
  rule("5  PARSE")
  set.seed(7)
  q <- p[sample(nrow(p), min(N_PARSE, nrow(p))), ]

  gname <- toupper(trimws(q$street))
  gtype <- toupper(trimws(ifelse(is.na(q$suffix), "", q$suffix)))
  hw <- grepl("^HIGHWAY [0-9]", gname) & gtype == ""
  gname[hw] <- sub("^HIGHWAY ", "", gname[hw]); gtype[hw] <- "HWY"
  no <- grepl("^NO [0-9]", gname) & gtype == "HWY"
  gname[no] <- sub("^NO ", "", gname[no])

  gold <- data.frame(num = as.character(q$num),
                     sfx = ifelse(is.na(q$addl), "", q$addl),
                     name = gname, type = gtype,
                     dir = ifelse(is.na(q$dir), "", q$dir), stringsAsFactors = FALSE)
  gold[] <- lapply(gold, kf)

  n <- normalize_address(q$addr, prov = "NS", con = con)
  got <- data.frame(num = ifelse(is.na(n$CIVIC_NO), "", as.character(n$CIVIC_NO)),
                    sfx = kf(n$CIVIC_NO_SUFFIX), name = kf(n$STREET_NAME),
                    type = kf(n$STREET_TYPE), dir = kf(n$STREET_DIR),
                    stringsAsFactors = FALSE)

  say("component agreement with PVSC's labels, n = ", nrow(q), ":")
  for (f in names(gold))
    say(sprintf("  %-5s %6.2f%%", f, 100 * mean(gold[[f]] == got[[f]])))
  all5 <- Reduce(`&`, lapply(names(gold), function(f) gold[[f]] == got[[f]]))
  say(sprintf("  all five                %6.2f%%", 100 * mean(all5)))

  ## A street-name mismatch is mostly the gazetteer doing its job, so it is
  ## shown rather than counted.
  say("\nstreet-name disagreements (most are the gazetteer correcting PVSC):")
  bad <- which(gold$name != got$name)
  print(utils::head(data.frame(addr = substr(q$addr[bad], 1, 46),
                               pvsc = gold$name[bad], parsed = got$name[bad]), 15),
        row.names = FALSE)

  ## The two families that ARE defects. Both are visible without a coordinate.
  rule("5b  THE TWO FAILURE FAMILIES")
  s <- readRDS(BENCH)
  n2 <- normalize_address(s$addr, prov = "NS", con = con)
  city <- kf(s$city); mun <- kf(n2$MUN_NAME)
  tok <- strsplit(city, " "); mtok <- strsplit(mun, " ")
  trunc <- mapply(function(a, b) length(b) > 0 && length(b) < length(a) && all(b %in% a),
                  tok, mtok) & nzchar(mun) & mun != city
  spur <- is.na(s$dir) & !is.na(n2$STREET_DIR) & nzchar(n2$STREET_DIR)
  aff <- trunc | spur
  placed <- !is.na(s$lonG)

  say(sprintf("municipality truncated (tokens dropped): %6d  %5.2f%%", sum(trunc), 100 * mean(trunc)))
  say(sprintf("spurious street direction              : %6d  %5.2f%%", sum(spur), 100 * mean(spur)))
  say(sprintf("either                                 : %6d  %5.2f%%", sum(aff), 100 * mean(aff)))

  say("\nwhat they cost -- matches, not metres:")
  say(sprintf("  clean     placed %5.1f%% of n=%6d", 100 * mean(placed[!aff]), sum(!aff)))
  say(sprintf("  truncated placed %5.1f%% of n=%6d", 100 * mean(placed[trunc]), sum(trunc)))
  say(sprintf("  spur dir  placed %5.1f%% of n=%6d", 100 * mean(placed[spur]), sum(spur)))
  say(sprintf("  unplaced rows attributable to them: %d of %d (%.1f%%)",
              sum(aff & !placed), sum(!placed), 100 * mean(aff[!placed])))

  say("\nand what they do NOT cost -- the far tail is the remap, not these:")
  for (g2 in c(FALSE, TRUE)) {
    y <- s$d[is.finite(s$d) & aff == g2]
    say(sprintf("  %-8s n=%6d  p50=%5.1f  >1km %5.2f%%  >5km %5.2f%%",
                if (g2) "affected" else "clean", length(y), stats::median(y),
                100 * mean(y > 1000), 100 * mean(y > 5000)))
  }

  say("\nexamples, truncated municipality:")
  i <- utils::head(which(trunc), 12)
  print(data.frame(addr = substr(s$addr[i], 1, 44), pvsc_city = s$city[i],
                   parsed_mun = n2$MUN_NAME[i], name = n2$STREET_NAME[i],
                   type = n2$STREET_TYPE[i]), row.names = FALSE)
}

DBI::dbDisconnect(con, shutdown = TRUE)
