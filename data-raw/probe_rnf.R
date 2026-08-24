# Measures Statistics Canada's Road Network File against NAR, to decide whether
# an RNF interpolation tier is worth building and what it would have to report.
#
# The RNF (product 92-500-X) carries four address-range fields per segment --
# AFL_VAL/ATL_VAL on the left, AFR_VAL/ATR_VAL on the right -- and NO PROVENANCE
# FLAG. A range that was observed and a range that was imputed are the same
# bytes, so nothing in the file says which is which. That is the whole reason
# this harness exists: NAR's building points are a second, independent reading
# of the same streets, so asking how often a civic number NAR holds falls inside
# the range RNF claims for it turns an unknowable into a measured one.
#
# It measures four things, and they answer different questions:
#
#   1. PRESENCE  -- how much of the file carries a range at all, by province.
#      A file-level fact; no NAR needed.
#   2. VALIDITY  -- for NAR building points, does the range on the side the house
#      actually sits on contain its number? This is the provenance proxy.
#   3. ACCURACY  -- interpolate along the segment and measure the distance to
#      NAR's own building point. This is where uncertainty_m comes from.
#   4. RECOVERY  -- run the SAME 5,000-filing Corporations Canada draw that
#      data-raw/eval_normalize.R and inst/notes/geocoding-status.md use, and ask
#      how many of the addresses geocode() currently FAILS the RNF would place.
#   5. DELIVERED -- the same question again, asked of the SHIPPED tier through
#      geocode() rather than of this file's own SQL. Stages 1-4 are the design
#      target; R/rnf.R does not run the same query, so what it recovers had to
#      be measured separately. Needs only the NAR database with rnf_import()
#      run, so it costs no download.
#
# Stages 2 and 3 are measured where NAR already has the answer, which is the
# complement of the population a tier would serve. Stage 4 exists because that
# gap is exactly the mistake the Quebec register work had to correct: a number
# measured on the overlap is not a number about the residual. Stage 4 checks its
# own answers against the filing's postal code, which nothing in the pathway
# reads, so the confirmation is independent rather than circular.
#
# The reference is NAR's building point, and a reference is not ground truth --
# see inst/notes/nar-not-ground-truth reasoning in geocoding-status.md. A
# disagreement here is a disagreement, and RNF is sometimes the one that is
# right. What survives that caveat is the SHAPE of the error distribution, and
# in particular which rows are safe and which are not.
#
# Findings are written up in inst/notes/road-network-file.md. Read that first;
# this file is how to reproduce it.
#
# Run with:  Rscript data-raw/probe_rnf.R    (needs NAR_CACHE_PATH)
#   RNF_YEAR    two-digit RNF release                (default 25)
#   RNF_DIR     where the download and working db go (default <NAR_CACHE_PATH>/rnf)
#   RNF_N       NAR building points to sample        (default 200000)
#   RNF_STAGES  any of "12345"                       (default 12345)
#   EVAL_CACHE  where the corporations CSV lives     (default <NAR_CACHE_PATH>/eval)
#
# The download is ~340 MB and the working database ~1.6 GB; both are cached, so
# a re-run of stages 2-4 costs neither. Stage 1 builds them.
#
# On the URL: the note this replaces said guessing it does not work. It does,
# but not at the path the 2021 geography pages suggest. The download page at
#   https://www12.statcan.gc.ca/census-recensement/2011/geo/RNF-FRR/index-s-eng.cfm?year=25
# POSTs lang/year/type and 302s to
#   /census-recensement/2011/geo/RNF-FRR/files-fichiers/lrnf000r<YY><t>_e.zip
# with t in a (shapefile), g (GML), f (file geodatabase), p (GeoPackage).
# ONLY THE SHAPEFILE IS PUBLISHED FOR EVERY RELEASE -- 20, 22, 23, 24 and 25 all
# serve `a`, but only 25 serves `p`, so an importer that reaches for the
# GeoPackage works this year and breaks on the archive. Version discovery would
# still want a scraper of https://www150.statcan.gc.ca/n1/en/catalogue/92-500-X,
# which lists the issues as 92-500-X<year>001.

if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  suppressMessages(pkgload::load_all(quiet = TRUE))
} else {
  library(cangeocode)
}
library(duckdb)

YEAR   <- Sys.getenv("RNF_YEAR", "25")
DIR    <- Sys.getenv("RNF_DIR", file.path(Sys.getenv("NAR_CACHE_PATH"), "rnf"))
N      <- as.integer(Sys.getenv("RNF_N", "200000"))
STAGES <- Sys.getenv("RNF_STAGES", "12345")
CACHE  <- Sys.getenv("EVAL_CACHE", file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
NARDB  <- file.path(Sys.getenv("NAR_CACHE_PATH"),
                    paste0(Sys.getenv("RNF_NAR_VERSION", "2026-06"), ".duckdb"))
stopifnot(nzchar(Sys.getenv("NAR_CACHE_PATH")))
dir.create(DIR, showWarnings = FALSE, recursive = TRUE)

DB  <- file.path(DIR, paste0("rnf", YEAR, ".duckdb"))
ZIP <- file.path(DIR, sprintf("lrnf000r%sa_e.zip", YEAR))
URL <- paste0("https://www12.statcan.gc.ca/census-recensement/2011/geo/",
              "RNF-FRR/files-fichiers/", basename(ZIP))

rule <- function(x) cat("\n", strrep("-", 72), "\n", x, "\n", sep = "")
show <- function(con, s) print(DBI::dbGetQuery(con, s), row.names = FALSE)

open_rnf <- function(read_only = TRUE) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = read_only)
  DBI::dbExecute(con, "LOAD spatial;")
  DBI::dbExecute(con, "SET memory_limit='10GB';")
  con
}

## ---------------------------------------------------------------- stage 1 ---
## Download, load, and describe what the file carries.

if (grepl("1", STAGES) && !file.exists(DB)) {
  if (!file.exists(ZIP)) {
    cat("downloading", URL, "(~340 MB)\n")
    to <- options("timeout")
    options(timeout = max(1800, as.numeric(unlist(to)), na.rm = TRUE))
    utils::download.file(URL, ZIP, mode = "wb")
    options(to)
  }
  exdir <- file.path(DIR, paste0("shp", YEAR))
  if (!dir.exists(exdir)) utils::unzip(ZIP, exdir = exdir)
  shp <- list.files(exdir, pattern = "\\.shp$", full.names = TRUE)[1]
  stopifnot(!is.na(shp))

  con <- open_rnf(read_only = FALSE)
  # keep_wkb defers geometry parsing to a second step. It has to: 13 of the
  # 2,251,726 features are CircularStrings, which DuckDB's spatial extension
  # refuses outright ("Unsupported geometry type in WKB"), and without this the
  # whole read fails on them rather than the rows themselves. They are dropped
  # by the WKB type code below -- 0109000000 is a little-endian type 9.
  DBI::dbExecute(con, sprintf("
    CREATE TABLE RnfRaw AS
    SELECT NGD_UID, NAME, TYPE, DIR,
           TRY_CAST(AFL_VAL AS INTEGER) AS AFL, TRY_CAST(ATL_VAL AS INTEGER) AS ATL,
           TRY_CAST(AFR_VAL AS INTEGER) AS AFR, TRY_CAST(ATR_VAL AS INTEGER) AS ATR,
           CSDUID_L, CSDNAME_L, CSDTYPE_L, CSDUID_R, CSDNAME_R, CSDTYPE_R,
           PRUID_L, PRUID_R, CLASS, RANK, geom AS wkb
      FROM st_read('%s', keep_wkb=true)", shp))
  # MUN_KEY_* is spelled to match NAR's own Streets.MUN_KEY, which is
  # PROV_CODE:CSD_TYPE_ENG_CODE:CSD_ENG_NAME -- the same three parts in the same
  # order, so the two files can be joined on a municipality without a crosswalk.
  DBI::dbExecute(con, "
    CREATE TABLE Rnf AS
    SELECT * EXCLUDE (wkb),
           PRUID_L || ':' || CSDTYPE_L || ':' || CSDNAME_L AS MUN_KEY_L,
           PRUID_R || ':' || CSDTYPE_R || ':' || CSDNAME_R AS MUN_KEY_R,
           strip_accents(upper(NAME)) AS NAME_FOLD,
           ST_GeomFromWKB(wkb) AS geom,
           ST_Length(ST_GeomFromWKB(wkb)) AS len_m
      FROM RnfRaw WHERE hex(wkb)[1:10] <> '0109000000'")
  DBI::dbExecute(con, "CREATE INDEX rnf_geom_idx ON Rnf USING RTREE (geom);")
  DBI::dbExecute(con, "CREATE INDEX rnf_name_idx ON Rnf (NAME_FOLD);")
  DBI::dbExecute(con, "DROP TABLE RnfRaw; CHECKPOINT;")
  DBI::dbDisconnect(con, shutdown = TRUE)
}

if (grepl("1", STAGES)) {
  con <- open_rnf(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rule("1. what the file carries")
  show(con, "SELECT count(*) segments, round(sum(len_m)/1000) km,
    round(100.0*count(*) FILTER (WHERE NAME IS NULL OR NAME='')/count(*),1) pct_unnamed,
    round(100.0*count(*) FILTER (WHERE AFL IS NOT NULL OR AFR IS NOT NULL)/count(*),1) pct_any_range,
    round(100.0*count(*) FILTER (WHERE AFL IS NOT NULL AND AFR IS NOT NULL)/count(*),1) pct_both
    FROM Rnf")
  rule("range presence among NAMED segments, by province")
  show(con, "SELECT PRUID_L pr, count(*) n,
    round(100.0*count(*) FILTER (WHERE AFL IS NOT NULL OR AFR IS NOT NULL)/count(*),1) pct_range
    FROM Rnf WHERE NAME IS NOT NULL AND NAME<>'' GROUP BY 1 ORDER BY 1")
  rule("parity of the two sides, and degenerate (from=to) ranges")
  show(con, "SELECT
    count(*) FILTER (WHERE AFL%2=1 AND ATL%2=1) l_odd,
    count(*) FILTER (WHERE AFL%2=0 AND ATL%2=0) l_even,
    count(*) FILTER (WHERE AFR%2=1 AND ATR%2=1) r_odd,
    count(*) FILTER (WHERE AFR%2=0 AND ATR%2=0) r_even,
    count(*) FILTER (WHERE AFL IS NOT NULL AND AFL%2<>ATL%2) l_mixed,
    round(100.0*count(*) FILTER (WHERE AFL=ATL)/nullif(count(*) FILTER (WHERE AFL IS NOT NULL),0),1) l_pct_point
    FROM Rnf")
  rule("street/CSD pairs RNF carries with a range that NAR's Streets does not")
  DBI::dbExecute(con, sprintf("ATTACH '%s' AS nar (READ_ONLY);", NARDB))
  show(con, "
    WITH r AS (SELECT DISTINCT NAME_FOLD, MUN_KEY_L k FROM Rnf
                WHERE NAME IS NOT NULL AND NAME<>'' AND (AFL IS NOT NULL OR AFR IS NOT NULL)),
      n AS (SELECT DISTINCT NAME_FOLD, MUN_KEY k FROM nar.Streets
            UNION SELECT DISTINCT MAIL_NAME_FOLD, MUN_KEY FROM nar.Streets)
    SELECT count(*) rnf_pairs,
      count(*) FILTER (WHERE (r.NAME_FOLD, r.k) NOT IN (SELECT (NAME_FOLD,k) FROM n)) absent_from_nar
    FROM r")
  DBI::dbDisconnect(con, shutdown = TRUE); on.exit()
}

## ------------------------------------------------------------- stages 2-3 ---
## Validity and accuracy, against NAR building points.

if (grepl("2|3", STAGES)) {
  con <- open_rnf()
  DBI::dbExecute(con, sprintf("ATTACH '%s' AS nar (READ_ONLY);", NARDB))
  DBI::dbExecute(con, sprintf("
    CREATE TEMP TABLE Pts AS
    SELECT ADDR_GUID, CAST(CIVIC_NO AS INTEGER) civic, PROV_CODE,
           strip_accents(upper(OFFICIAL_STREET_NAME)) NAME_FOLD, x, y, geom
      FROM (SELECT ADDR_GUID, CIVIC_NO, PROV_CODE, OFFICIAL_STREET_NAME, x, y, geom
              FROM nar.Addresses
             WHERE geom_source='building' AND CIVIC_NO IS NOT NULL
               AND OFFICIAL_STREET_NAME IS NOT NULL AND OFFICIAL_STREET_NAME<>'')
     USING SAMPLE %d ROWS (reservoir, 42)", N))
  # The filter goes in a subquery and the SAMPLE outside it. DuckDB applies a
  # reservoir sample to what it reads, not to what survives a restrictive WHERE,
  # and sampling the filtered form silently returns a fraction of the rows asked
  # for -- which is a wrong denominator, not a visible error.

  # Nearest same-named segment. This is the ceiling, not the tier: it uses the
  # answer's own position to choose the segment. Stage 4 chooses without it.
  DBI::dbExecute(con, "
    CREATE TEMP TABLE Near AS SELECT * FROM (
      SELECT p.ADDR_GUID, p.civic, p.PROV_CODE, p.x, p.y, r.NGD_UID,
             r.AFL, r.ATL, r.AFR, r.ATR, r.len_m, r.geom rgeom,
             ST_Distance(r.geom, p.geom) d,
             row_number() OVER (PARTITION BY p.ADDR_GUID
                                ORDER BY ST_Distance(r.geom, p.geom)) rn
        FROM Pts p JOIN Rnf r ON r.NAME_FOLD = p.NAME_FOLD
       WHERE ST_DWithin(r.geom, p.geom, 150)) WHERE rn = 1")

  # Which side of the centreline the house is on, from the LOCAL direction of
  # travel rather than the segment's endpoints -- a curved block would put a
  # house on the wrong side of a chord drawn end to end. ST_OffsetCurve is not
  # in DuckDB's spatial extension, so this is azimuth-by-hand: the sign of the
  # 2-D cross product of the direction vector with the vector out to the house.
  # Positive is left of the direction of travel, which is RNF's own convention.
  DBI::dbExecute(con, "
    CREATE TEMP TABLE Side AS
    WITH g AS (SELECT *, ST_LineLocatePoint(rgeom, ST_Point(x,y)) f FROM Near),
    h AS (SELECT *, ST_LineSubstring(rgeom, greatest(f-0.02,0), least(f+0.02,1)) sub,
                 ST_LineInterpolatePoint(rgeom, f) np FROM g),
    k AS (SELECT *, ST_X(ST_EndPoint(sub))-ST_X(ST_StartPoint(sub)) ax,
                    ST_Y(ST_EndPoint(sub))-ST_Y(ST_StartPoint(sub)) ay,
                    x-ST_X(np) bx, y-ST_Y(np) byv FROM h)
    SELECT ADDR_GUID, civic, PROV_CODE, x, y, NGD_UID, AFL, ATL, AFR, ATR,
           len_m, rgeom, d, f,
           CASE WHEN ax*byv-ay*bx > 0 THEN 'L' ELSE 'R' END side,
           CASE WHEN ax*byv-ay*bx > 0 THEN 1 ELSE -1 END sgn
      FROM k")
  DBI::dbExecute(con, "
    CREATE TEMP TABLE S2 AS
    SELECT *, CASE side WHEN 'L' THEN AFL ELSE AFR END sf,
              CASE side WHEN 'L' THEN ATL ELSE ATR END st_,
              CASE side WHEN 'L' THEN AFR ELSE AFL END of_,
              CASE side WHEN 'L' THEN ATR ELSE ATL END ot_ FROM Side")

  rule("2. does the range on the house's own side contain its number")
  show(con, "SELECT (SELECT count(*) FROM Pts) sampled, count(*) n_matched,
    round(100.0*count(*)/(SELECT count(*) FROM Pts),1) pct_matched,
    round(median(d),1) med_offset_m, round(quantile_cont(d,0.9),1) p90_offset_m,
    round(100.0*count(*) FILTER (WHERE sf IS NOT NULL)/count(*),1) pct_side_has_range,
    round(100.0*count(*) FILTER (WHERE sf IS NOT NULL AND civic BETWEEN least(sf,st_) AND greatest(sf,st_))
          /nullif(count(*) FILTER (WHERE sf IS NOT NULL),0),1) pct_in_range
    FROM S2")
  rule("the side is real: parity agreement, own side vs the other one")
  show(con, "SELECT count(*) n,
    round(100.0*count(*) FILTER (WHERE sf%2 = civic%2)/count(*),1) pct_own_side,
    round(100.0*count(*) FILTER (WHERE of_%2 = civic%2)/count(*),1) pct_other_side
    FROM S2 WHERE sf IS NOT NULL AND of_ IS NOT NULL")
  rule("by province")
  show(con, "SELECT PROV_CODE, count(*) n,
    round(100.0*count(*) FILTER (WHERE sf IS NOT NULL)/count(*),1) pct_has_range,
    round(100.0*count(*) FILTER (WHERE sf IS NOT NULL AND civic BETWEEN least(sf,st_) AND greatest(sf,st_))
          /nullif(count(*) FILTER (WHERE sf IS NOT NULL),0),1) pct_in_range
    FROM S2 GROUP BY 1 ORDER BY 1")

  # Three placements, so the setback and the offset can be priced separately
  # rather than asserted. 0.05..0.95 keeps a house at the start of a range off
  # the intersection node; the 13 m perpendicular shift moves it off the
  # centreline, which is the systematic half of the error the geolocator's own
  # interpolator does not correct (see inst/notes/nrcan-geolocator.md).
  DBI::dbExecute(con, "
    CREATE TEMP TABLE Err AS
    WITH a AS (SELECT *, CASE WHEN st_=sf THEN 0.5
                 ELSE greatest(0,least(1,(civic-sf)/(st_-sf)::DOUBLE)) END frac
               FROM S2 WHERE sf IS NOT NULL),
    b AS (SELECT *, 0.05+0.90*frac fsb FROM a),
    c AS (SELECT *, ST_LineInterpolatePoint(rgeom,frac) p_raw,
                    ST_LineInterpolatePoint(rgeom,fsb)  p_sb,
                    ST_LineInterpolatePoint(rgeom,0.5)  p_mid,
                    ST_LineSubstring(rgeom, greatest(fsb-0.02,0), least(fsb+0.02,1)) sub2 FROM b),
    e AS (SELECT *, ST_X(ST_EndPoint(sub2))-ST_X(ST_StartPoint(sub2)) ux,
                    ST_Y(ST_EndPoint(sub2))-ST_Y(ST_StartPoint(sub2)) uy FROM c),
    f AS (SELECT *, sqrt(ux*ux+uy*uy) un FROM e)
    SELECT ADDR_GUID, civic, PROV_CODE, d, f AS frac_true, frac, len_m, sf, st_,
      sqrt((ST_X(p_raw)-x)^2+(ST_Y(p_raw)-y)^2) err_raw,
      sqrt((ST_X(p_sb)-x)^2+(ST_Y(p_sb)-y)^2) err_sb,
      sqrt((ST_X(p_mid)-x)^2+(ST_Y(p_mid)-y)^2) err_mid,
      sqrt((ST_X(p_sb)-sgn*uy/nullif(un,0)*13-x)^2 +
           (ST_Y(p_sb)+sgn*ux/nullif(un,0)*13-y)^2) err_off
      FROM f")
  rule("3. how far the interpolated point lands from NAR's own building point")
  show(con, "SELECT count(*) n, round(median(err_mid),1) midpoint,
    round(median(err_raw),1) plain, round(median(err_sb),1) setback,
    round(median(err_off),1) setback_and_offset,
    round(quantile_cont(err_off,0.9),1) p90, round(quantile_cont(err_off,0.95),1) p95
    FROM Err")
  rule("error by segment length, rows whose range contains the number")
  show(con, "SELECT CASE WHEN len_m<100 THEN '1 <100m' WHEN len_m<250 THEN '2 100-250m'
      WHEN len_m<500 THEN '3 250-500m' WHEN len_m<1000 THEN '4 0.5-1km' ELSE '5 >1km' END bucket,
    count(*) n, round(median(err_off),1) p50, round(quantile_cont(err_off,0.9),1) p90
    FROM Err WHERE civic BETWEEN least(sf,st_) AND greatest(sf,st_) GROUP BY 1 ORDER BY 1")
  DBI::dbDisconnect(con, shutdown = TRUE)
}

## ---------------------------------------------------------------- stage 4 ---
## What the tier would actually recover, on addresses geocode() fails today.

if (grepl("4", STAGES)) {
  # The SAME draw as data-raw/eval_normalize.R Part B: same file, same filter,
  # same seed. Anything else and the recovery figure is not comparable to the
  # residual that inst/notes/geocoding-status.md decomposes.
  set.seed(20260821)
  corp <- as.data.frame(arrow::read_csv_arrow(
    file.path(CACHE, "corporations-active-cbca-en.csv"),
    col_select = c("Street","Street 2","City/town","Province/territory","Postal code"),
    as_data_frame = TRUE))
  names(corp) <- c("street","street2","city","prov","postal")
  corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))
  corp <- corp[nzchar(corp$street) & nzchar(corp$city) &
               corp$prov %in% names(cangeocode:::nar_prov_lang) &
               grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal), ]
  corp <- corp[sample.int(nrow(corp), min(5000L, nrow(corp))), ]
  parts <- cbind(corp$street, corp$street2, corp$city, trimws(paste(corp$prov, corp$postal)))
  corp$text <- apply(parts, 1, function(x) paste(x[nzchar(x)], collapse = ", "))

  ncon <- nar_connection()
  g <- geocode(corp$text, method = c("nar", "nar_interpolate"), con = ncon)
  DBI::dbDisconnect(ncon, shutdown = TRUE)
  unplaced <- is.na(g$lon)

  fold <- cangeocode:::nar_fold
  probe <- data.frame(row_id = seq_len(nrow(g)),
    name_fold = fold(ifelse(is.na(g$STREET_NAME), "", g$STREET_NAME)),
    mun_fold  = gsub(".", "", fold(ifelse(is.na(g$MUN_NAME), "", g$MUN_NAME)), fixed = TRUE),
    prov      = ifelse(is.na(g$PROV_ABVN), "", g$PROV_ABVN),
    civic     = ifelse(is.na(g$CIVIC_NO), NA_integer_, as.integer(g$CIVIC_NO)))
  probe <- probe[nzchar(probe$name_fold) & !is.na(probe$civic), ]

  con <- open_rnf()
  DBI::dbExecute(con, sprintf("ATTACH '%s' AS nar (READ_ONLY);", NARDB))
  DBI::dbWriteTable(con, "Probe", probe, temporary = TRUE)
  # The municipality is resolved through NAR's MunAlias, not RNF's own CSD
  # names: a filer writes a mailing city and MunAlias is what already knows it
  # is a CSD. RNF's MUN_KEY_* is spelled to match, so no crosswalk is needed.
  DBI::dbExecute(con, "
    CREATE TEMP TABLE P2 AS
    SELECT p.*, strip_accents(upper(m.MUN_KEY)) key_fold FROM Probe p
    LEFT JOIN (SELECT NAME_FOLD, PROV_ABVN, MUN_KEY,
                 row_number() OVER (PARTITION BY NAME_FOLD, PROV_ABVN
                                    ORDER BY N_ADDRESSES DESC) rn
               FROM nar.MunAlias) m
      ON m.NAME_FOLD = p.mun_fold AND m.PROV_ABVN = p.prov AND m.rn = 1")
  DBI::dbExecute(con, "
    CREATE TEMP TABLE Hit AS
    WITH s AS (SELECT NGD_UID, NAME_FOLD, geom, len_m, AFL, ATL, AFR, ATR,
                 strip_accents(upper(MUN_KEY_L)) kl, strip_accents(upper(MUN_KEY_R)) kr
                 FROM Rnf WHERE NAME IS NOT NULL AND NAME<>''
                   AND (AFL IS NOT NULL OR AFR IS NOT NULL))
    SELECT p.row_id, p.civic, s.NGD_UID, s.geom, s.len_m, s.AFL, s.ATL, s.AFR, s.ATR,
      (p.civic BETWEEN least(s.AFL,s.ATL) AND greatest(s.AFL,s.ATL)) in_l,
      (p.civic BETWEEN least(s.AFR,s.ATR) AND greatest(s.AFR,s.ATR)) in_r,
      (p.civic%2)=(s.AFL%2) par_l, (p.civic%2)=(s.AFR%2) par_r,
      count(*) OVER (PARTITION BY p.row_id) n_matches
      FROM P2 p JOIN s ON s.NAME_FOLD = p.name_fold
     WHERE p.key_fold IS NOT NULL AND (s.kl = p.key_fold OR s.kr = p.key_fold)
       AND (coalesce(p.civic BETWEEN least(s.AFL,s.ATL) AND greatest(s.AFL,s.ATL), false)
         OR coalesce(p.civic BETWEEN least(s.AFR,s.ATR) AND greatest(s.AFR,s.ATR), false))")
  DBI::dbExecute(con, "
    CREATE TEMP TABLE Pos AS
    WITH best AS (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY row_id
        ORDER BY (CASE WHEN (in_l AND par_l) OR (in_r AND par_r) THEN 0 ELSE 1 END), len_m) rn
      FROM Hit) WHERE rn=1),
    a AS (SELECT row_id, civic, NGD_UID, geom, len_m, n_matches,
      CASE WHEN in_l AND par_l THEN AFL WHEN in_r AND par_r THEN AFR
           WHEN in_l THEN AFL ELSE AFR END sf,
      CASE WHEN in_l AND par_l THEN ATL WHEN in_r AND par_r THEN ATR
           WHEN in_l THEN ATL ELSE ATR END st_,
      CASE WHEN (in_l AND par_l) OR (NOT coalesce(in_r,false) AND in_l) THEN 1 ELSE -1 END sgn
      FROM best),
    b AS (SELECT *, CASE WHEN st_=sf THEN 0.5
            ELSE greatest(0,least(1,(civic-sf)/(st_-sf)::DOUBLE)) END frac FROM a),
    c AS (SELECT *, 0.05+0.90*frac fsb FROM b),
    e AS (SELECT *, ST_LineInterpolatePoint(geom,fsb) pt,
            ST_LineSubstring(geom, greatest(fsb-0.02,0), least(fsb+0.02,1)) sub FROM c),
    f AS (SELECT *, ST_X(ST_EndPoint(sub))-ST_X(ST_StartPoint(sub)) ux,
                    ST_Y(ST_EndPoint(sub))-ST_Y(ST_StartPoint(sub)) uy FROM e),
    h AS (SELECT *, sqrt(ux*ux+uy*uy) un FROM f)
    SELECT row_id, civic, NGD_UID, len_m, n_matches,
      ST_X(pt)-sgn*uy/nullif(un,0)*13 x, ST_Y(pt)+sgn*ux/nullif(un,0)*13 y FROM h")
  pos <- DBI::dbGetQuery(con, "SELECT * FROM Pos")
  DBI::dbDisconnect(con, shutdown = TRUE)

  hit <- rep(FALSE, nrow(g)); hit[pos$row_id] <- TRUE
  rule("4. what an RNF tier would recover, on the 5,000-filing draw")
  cat(sprintf("unplaced by c(\"nar\",\"nar_interpolate\"): %d (%.1f%%)\n",
              sum(unplaced), 100*mean(unplaced)))
  cat(sprintf("RNF places, of those:                   %d (%.1f%% of the residual, %.1f%% of all)\n",
              sum(hit & unplaced), 100*sum(hit & unplaced)/sum(unplaced),
              100*mean(hit & unplaced)))
  new <- pos$row_id[unplaced[pos$row_id]]
  amb <- pos$n_matches[match(new, pos$row_id)] > 1
  cat(sprintf("of those, ambiguous (n_matches > 1):    %d\n", sum(amb)))
  cat(sprintf("refusing those still recovers:          %d (%.1f%% of the residual, %.1f%% of all)\n",
              sum(!amb), 100*sum(!amb)/sum(unplaced), 100*sum(!amb)/length(unplaced)))

  # Agreement where geocode() also answered. This is a calibration, not a score:
  # both are estimates of the same house and NAR is the better one.
  ok <- !unplaced & hit
  cmp <- data.frame(row_id = which(ok), lon = g$lon[ok], lat = g$lat[ok],
                    mm = g$match_method[ok])
  cmp <- merge(cmp, pos, by = "row_id")
  xy <- sf::st_coordinates(sf::st_transform(
    sf::st_as_sf(cmp, coords = c("lon","lat"), crs = 4326), nar_storage_crs()))
  cmp$dist <- sqrt((cmp$x-xy[,1])^2 + (cmp$y-xy[,2])^2)
  rule("agreement with geocode(), where geocode() also answered")
  cat(sprintf("  every method : rows %4d  p50 %5.1f  p90 %7.1f\n",
    nrow(cmp), stats::median(cmp$dist), stats::quantile(cmp$dist, .9)))
  b <- cmp[cmp$mm == "nar_building", ]
  for (u in c(NA, FALSE, TRUE)) {
    v <- if (is.na(u)) b$dist else b$dist[(b$n_matches > 1) == u]
    cat(sprintf("  %-13s rows %4d  p50 %5.1f  p90 %7.1f  within 50 m %4.1f%%  over 1 km %4.1f%%\n",
      if (is.na(u)) "nar_building:" else sprintf("n_matches %s 1:", if (u) ">" else "="),
      length(v), stats::median(v),
      stats::quantile(v, .9), 100*mean(v <= 50), 100*mean(v > 1000)))
  }
  rule("uncertainty_m candidates, unambiguous rows only (target ~90% covered)")
  b1 <- b[b$n_matches == 1, ]; long <- b1$len_m > 600
  mods <- list("flat 110" = rep(110, nrow(b1)), "flat 120" = rep(120, nrow(b1)),
               "max(100, 0.30*len_m)" = pmax(100, 0.30*b1$len_m),
               "max(95, 0.35*len_m)"  = pmax(95,  0.35*b1$len_m),
               "max(90, 0.40*len_m)"  = pmax(90,  0.40*b1$len_m))
  print(do.call(rbind, lapply(names(mods), function(k) { u <- mods[[k]]
    data.frame(model = k, covered = sprintf("%.1f%%", 100*mean(b1$dist <= u)),
      long_segments = sprintf("%.1f%%", 100*mean(b1$dist[long] <= u[long])),
      median_m = round(stats::median(u)))})), row.names = FALSE)

  # The postal code is the independent check: nothing above reads it. A rural
  # FSA covers a large area, so distance from its centroid is weak evidence
  # there and the two are reported apart rather than pooled.
  # The baseline matters as much as the check: the same measurement on rows NAR
  # also placed says what "close to your own postal code" looks like when the
  # answer is known to be right, and the recovered rows are then read against
  # that rather than against an absolute standard.
  ncon <- nar_connection()
  chk <- rbind(
    data.frame(grp = "recovered", pc = gsub(" ", "", toupper(g$POSTAL_CODE[new])),
               x = pos$x[match(new, pos$row_id)], y = pos$y[match(new, pos$row_id)]),
    data.frame(grp = "also in NAR", pc = gsub(" ", "", toupper(g$POSTAL_CODE[cmp$row_id])),
               x = xy[, 1], y = xy[, 2]))
  chk <- chk[nchar(chk$pc) == 6 & !is.na(chk$x), ]
  chk$rural <- substr(chk$pc, 2, 2) == "0"
  DBI::dbWriteTable(ncon, "PC", chk, temporary = TRUE)
  r <- DBI::dbGetQuery(ncon, "
    WITH a AS (SELECT replace(upper(MAIL_POSTAL_CODE),' ','') pc, x, y FROM Addresses
                WHERE geom_source='building' AND MAIL_POSTAL_CODE IS NOT NULL),
    f AS (SELECT pc, avg(x) mx, avg(y) my FROM a GROUP BY 1)
    SELECT p.grp, p.rural, sqrt((p.x-f.mx)^2+(p.y-f.my)^2) d FROM PC p JOIN f ON f.pc=p.pc")
  DBI::dbDisconnect(ncon, shutdown = TRUE)
  rule("independent check: distance to the filing's own postal code, NAR-derived")
  for (grp in c("also in NAR", "recovered")) for (u in c(NA, FALSE, TRUE)) {
    v <- r$d[r$grp == grp & (is.na(u) | r$rural %in% u)]
    if (!length(v)) next
    cat(sprintf("  %-11s %-9s: n %4d  p50 %6.0f m  within 500 m %3.0f%%  within 2 km %3.0f%%\n",
      grp, if (is.na(u)) "all" else if (u) "rural FSA" else "urban FSA", length(v),
      stats::median(v), 100*mean(v <= 500), 100*mean(v <= 2000)))
  }
}

## ---------------------------------------------------------------- stage 5 ---
## The SHIPPED tier, end to end. Stage 4 measured a pathway written inside this
## file; this measures what geocode() actually returns, which is not the same
## query. R/rnf.R joins on MATCH_FOLD rather than the plain fold, compares the
## municipality against RNF's own CSD name as well as through MunAlias,
## constrains the street type and direction where both sides carry one, and
## refuses n_matches > 1 instead of picking the shortest candidate. Every one of
## those moves the recovery figure, in both directions, so the design target and
## the delivered number have to be measured separately.
##
## It needs no download and no working database: the tables are the ones
## rnf_import() put in the NAR database, so this stage runs wherever the "rnf"
## tier does.

if (grepl("5", STAGES)) {
  set.seed(20260821)
  corp <- as.data.frame(arrow::read_csv_arrow(
    file.path(CACHE, "corporations-active-cbca-en.csv"),
    col_select = c("Street","Street 2","City/town","Province/territory","Postal code"),
    as_data_frame = TRUE))
  names(corp) <- c("street","street2","city","prov","postal")
  corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))
  corp <- corp[nzchar(corp$street) & nzchar(corp$city) &
               corp$prov %in% names(cangeocode:::nar_prov_lang) &
               grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal), ]
  corp <- corp[sample.int(nrow(corp), min(5000L, nrow(corp))), ]
  parts <- cbind(corp$street, corp$street2, corp$city, trimws(paste(corp$prov, corp$postal)))
  corp$text <- apply(parts, 1, function(x) paste(x[nzchar(x)], collapse = ", "))

  con <- nar_connection()
  stopifnot(cangeocode:::nar_has_rnf(con))
  # crs = NULL returns the storage CRS, which is metres, so every distance below
  # is computed without a reprojection of its own.
  t0 <- system.time(
    base <- geocode(corp$text, method = c("nar", "nar_interpolate"),
                    crs = NULL, con = con))[["elapsed"]]
  t1 <- system.time(
    full <- geocode(corp$text, method = c("nar", "nar_interpolate", "rnf"),
                    crs = NULL, con = con))[["elapsed"]]
  # The tier alone, on every row rather than on the residual, is the only way to
  # see it where geocode() already has an answer to compare against. In a real
  # chain it never sees these rows.
  t2 <- system.time(
    solo <- geocode(corp$text, method = "rnf", crs = NULL, con = con))[["elapsed"]]

  unplaced <- is.na(base$lon)
  rule("5. the shipped tier, on the 5,000-filing draw")
  cat(sprintf("baseline c(\"nar\",\"nar_interpolate\")     placed %4d (%.1f%%)  %.1fs\n",
              sum(!unplaced), 100*mean(!unplaced), t0))
  cat(sprintf("+ \"rnf\"                                placed %4d (%.1f%%)  %.1fs\n",
              sum(!is.na(full$lon)), 100*mean(!is.na(full$lon)), t1))
  cat(sprintf("recovered by the tier                  %4d (%.1f%% of the %d unplaced)\n",
              sum(unplaced & !is.na(full$lon)),
              100*sum(unplaced & !is.na(full$lon))/sum(unplaced), sum(unplaced)))
  cat(sprintf("refused as ambiguous (rnf_ambiguous)   %4d\n",
              sum(full$match_method == "rnf_ambiguous", na.rm = TRUE)))
  cat(sprintf("still unplaced                         %4d (%.1f%%)\n",
              sum(is.na(full$lon)), 100*mean(is.na(full$lon))))
  print(as.data.frame(table(match_method = full$match_method), responseName = "n"),
        row.names = FALSE)

  # Agreement where geocode() also answered. A calibration, not a score: both
  # are estimates of the same house and NAR's building point is the better one.
  ok <- !unplaced & !is.na(solo$lon)
  d <- sqrt((solo$lon[ok] - base$lon[ok])^2 + (solo$lat[ok] - base$lat[ok])^2)
  mm <- base$match_method[ok]
  u  <- solo$uncertainty_m[ok]
  rule("agreement with geocode(), where geocode() also answered (tier run alone)")
  for (k in c("every method", "nar_building", "nar_interpolated")) {
    v <- if (k == "every method") d else d[mm == k]
    w <- if (k == "every method") u else u[mm == k]
    if (!length(v)) next
    cat(sprintf("  %-17s rows %4d  p50 %5.1f  p90 %7.1f  within 50 m %4.1f%%  over 1 km %4.1f%%\n",
                paste0(k, ":"), length(v), stats::median(v), stats::quantile(v, .9),
                100*mean(v <= 50), 100*mean(v > 1000)))
  }
  # The shipped uncertainty model, tested as shipped. len_m is recoverable from
  # it exactly -- rnf_uncertainty_m() is max(95, 0.35 * len_m) -- so the long
  # segments stage 4 reported on separately are u > 210.
  b <- d[mm == "nar_building"]; ub <- u[mm == "nar_building"]
  rule("uncertainty_m as shipped: max(95, 0.35 * len_m) (target ~90% covered)")
  cat(sprintf("  all segments        rows %4d  covered %.1f%%  median u %3.0f m\n",
              length(b), 100*mean(b <= ub), stats::median(ub)))
  cat(sprintf("  segments over 600 m rows %4d  covered %.1f%%\n",
              sum(ub > 210), 100*mean(b[ub > 210] <= ub[ub > 210])))

  # The independent check: nothing in the pathway reads the postal code. Two
  # baselines are what make it readable, and they answer different questions.
  # "nar, same rows" is a known-right answer measured the same way, so it says
  # what "close to your own postal code" looks like at all. "rnf, same rows" is
  # this tier on those same rows, so the gap between it and "rnf, recovered" is
  # the overlap-versus-residual correction with the tier's own error held
  # constant -- which is the thing the Quebec work had to learn to separate.
  new <- which(unplaced & !is.na(full$lon))
  chk <- rbind(
    data.frame(grp = "rnf, recovered", pc = gsub(" ", "", toupper(full$POSTAL_CODE[new])),
               x = full$lon[new], y = full$lat[new]),
    data.frame(grp = "rnf, same rows", pc = gsub(" ", "", toupper(base$POSTAL_CODE[ok])),
               x = solo$lon[ok], y = solo$lat[ok]),
    data.frame(grp = "nar, same rows", pc = gsub(" ", "", toupper(base$POSTAL_CODE[ok])),
               x = base$lon[ok], y = base$lat[ok]))
  chk <- chk[nchar(chk$pc) == 6 & !is.na(chk$x), ]
  chk$rural <- substr(chk$pc, 2, 2) == "0"
  DBI::dbWriteTable(con, "PC", chk, temporary = TRUE, overwrite = TRUE)
  r <- DBI::dbGetQuery(con, "
    WITH a AS (SELECT replace(upper(MAIL_POSTAL_CODE),' ','') pc, x, y FROM Addresses
                WHERE geom_source='building' AND MAIL_POSTAL_CODE IS NOT NULL),
    f AS (SELECT pc, avg(x) mx, avg(y) my FROM a GROUP BY 1)
    SELECT p.grp, p.rural, sqrt((p.x-f.mx)^2+(p.y-f.my)^2) d FROM PC p JOIN f ON f.pc=p.pc")
  rule("independent check: distance to the filing's own postal code, NAR-derived")
  for (grp in c("nar, same rows", "rnf, same rows", "rnf, recovered"))
    for (v in c(NA, FALSE, TRUE)) {
    z <- r$d[r$grp == grp & (is.na(v) | r$rural %in% v)]
    if (!length(z)) next
    cat(sprintf("  %-14s %-9s: n %4d  p50 %6.0f m  within 500 m %3.0f%%  within 2 km %3.0f%%\n",
                grp, if (is.na(v)) "all" else if (v) "rural FSA" else "urban FSA",
                length(z), stats::median(z), 100*mean(z <= 500), 100*mean(z <= 2000)))
  }
  DBI::dbDisconnect(con, shutdown = TRUE)
}
