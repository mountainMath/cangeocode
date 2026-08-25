# Statistics Canada's Road Network File, imported beside NAR rather than into
# it, and the address-range interpolation tier it makes possible.
#
# The RNF (product 92-500-X) is a national street centreline file that carries
# four address-range fields per segment -- AFL/ATL on the left, AFR/ATR on the
# right -- and NO PROVENANCE FLAG. A range that was surveyed and a range that
# was imputed from its neighbours are the same bytes. Everything this tier is
# allowed to claim therefore rests on a measurement rather than on the file's
# own word: 89.7% of NAR civic numbers fall inside the range the segment on
# their own side claims for them, the geometric side agrees with the range's
# parity 94.2% of the time against 7% for the other side, and interpolation
# lands a median 24.3 m from NAR's building point.
#
# `inst/notes/road-network-file.md` records all of it, including the two
# conditions that made the tier worth building -- refuse when more than one
# segment matches, and report `max(95, 0.35 * len_m)` -- and
# `data-raw/probe_rnf.R` reproduces the numbers.

#' The Road Network File download
#'
#' @description One URL, in one place. `release` is the two-digit release year
#' StatCan names the file by; `a` is the shapefile.
#'
#' **Only the shapefile is published for every release.** The download form
#' offers four formats -- `a` shapefile, `g` GML, `f` file geodatabase, `p`
#' GeoPackage -- but the archive is not uniform: 20, 22, 23, 24 and 25 all serve
#' `a`, while only 25 serves `p`. An importer that reached for the GeoPackage
#' would work this year and break on last year's release. The shapefile is also
#' the format that reads cleanly: the GeoPackage carries 13 CircularStrings that
#' DuckDB's spatial extension refuses outright, and the shapefile spells the
#' same 2,251,726 features as plain LINESTRINGs.
#' @param release Two-digit release year, e.g. `"25"`
#' @return A URL string
#' @keywords internal
rnf_url <- function(release) {
  paste0("https://www12.statcan.gc.ca/census-recensement/2011/geo/RNF-FRR/",
         "files-fichiers/lrnf000r", release, "a_e.zip")
}

#' The newest RNF release the server actually has
#'
#' @description Probed rather than hard-coded, so the package does not quietly
#' stay on one release forever, and probed with `HEAD` rather than scraped: the
#' file names are entirely regular, so the only question is which of them exist,
#' and that is one request each to answer. The walk starts at the current year
#' and goes back, because a release appears partway through its year.
#'
#' **A status code is not an answer here.** StatCan serves a missing release as a
#' 302 to a 4 KB HTML error page returned with `200 OK`, so a probe that tests
#' `status_code < 400` accepts a release that does not exist and the failure
#' surfaces much later, as an unzip error. The content type is what distinguishes
#' them: a real release answers `application/x-zip-compressed` and hundreds of
#' megabytes, the error page answers `text/html`. Both are checked, because a
#' server that lies about one may lie about the other.
#'
#' The catalogue page for 92-500-X is the human-readable counterpart and lists
#' the issues as `92-500-X<year>001`; it is not scraped here because it does not
#' say which distribution formats were published, which is the part that varies.
#' @param from Two-digit year to start the walk at
#' @param back How many years back to look before giving up
#' @return A two-digit release string
#' @keywords internal
rnf_latest_release <- function(from = as.integer(format(Sys.Date(), "%y")),
                               back = 8L) {
  for (y in seq(from, from - back)) {
    release <- sprintf("%02d", y)
    ok <- tryCatch({
      handle <- curl::new_handle(nobody = TRUE, followlocation = TRUE)
      res <- curl::curl_fetch_memory(rnf_url(release), handle = handle)
      res$status_code < 400 && rnf_headers_are_a_zip(res$headers)
    }, error = function(e) FALSE)
    if (isTRUE(ok)) return(release)
  }
  stop("No Road Network File release answered between 20", sprintf("%02d", from),
       " and 20", sprintf("%02d", from - back),
       ". Pass one explicitly, e.g. rnf_import(release = \"25\").",
       call. = FALSE)
}

#' Do these response headers describe an actual zip?
#'
#' @description Split out of [rnf_latest_release()] so the soft-404 rule is
#' testable without a network call. A real release is `application/x-zip-*` and
#' hundreds of megabytes; StatCan's missing-file page is `text/html` and 4 KB,
#' served with `200 OK` after a redirect. The size floor is deliberately far
#' below the smallest real release (296 MB for 2024) -- it is there to catch an
#' error page, not to police the file.
#' @param headers Raw or character response headers from [curl::curl_fetch_memory()]
#' @return `TRUE` when the headers describe a zip large enough to be a release
#' @keywords internal
rnf_headers_are_a_zip <- function(headers) {
  h <- curl::parse_headers_list(headers)
  type <- tolower(h[["content-type"]] %||% "")
  size <- suppressWarnings(as.numeric(h[["content-length"]] %||% NA))
  grepl("zip", type, fixed = TRUE) && (is.na(size) || size > 1e6)
}

#' Is the road network file available on this connection?
#'
#' @description Both tables, not either. [rnf_import()] writes `RnfSegments`
#' first and `RnfStreets` second, so a run that died partway through leaves a
#' database that reads as having no RNF rather than as having half of it.
#' @param con A DuckDB connection
#' @return `TRUE` when the RNF tables are present
#' @keywords internal
nar_has_rnf <- function(con) {
  all(c("RnfSegments", "RnfStreets") %in% DBI::dbListTables(con))
}

#' Import Statistics Canada's Road Network File beside NAR
#'
#' @description Loads the **Road Network File** (92-500-X) into the cached NAR
#' database as its own tables, `RnfSegments` and `RnfStreets`. Nothing in
#' `Addresses` is touched. Once imported, `"rnf"` becomes available as a
#' [geocode()] tier, which places a civic number on the street segment whose
#' address range contains it.
#'
#' @section What is imported: **Named segments that carry an address range**,
#' 1.4M of the file's 2.25M. The rest is kept only as counts in `nar_metadata`:
#' an unnamed segment cannot be reached by an address query, and a named one
#' with no range carries nothing to interpolate along. Range presence is very
#' uneven by province -- 89.2% of Nova Scotia's named segments against 36.6% of
#' Saskatchewan's -- so this is where the tier's coverage is decided, and it is
#' decided by the file rather than by anything here.
#'
#' @section Why a separate table: The same reasoning as [rqa_import()]. RNF is a
#' different product with a different geometry -- centrelines, not points -- and
#' folding its ranges into `Addresses` would fabricate address records StatCan
#' never published. Kept apart, the file also stays *measurable* against NAR,
#' which is the only reason anything is known about how good its ranges are.
#'
#' `nar_schema_version()` is deliberately not bumped. The tables are optional
#' and additive, and a bump would force every user to re-download NAR.
#'
#' @section Accuracy: Measured against 200,000 NAR building points, and recorded
#' in full in `system.file("notes", "road-network-file.md", package =
#' "cangeocode")`. The short form: interpolation along the segment, with a 5%
#' setback from each end and a 13 m offset to the correct side, lands a median
#' 24.3 m and a 90th percentile 93.3 m from NAR's own building point. That is
#' about six times worse than `nar_interpolate`, which is why the tier belongs
#' below it, and about as good as the online geolocator at the median with a
#' shorter tail.
#'
#' @param version NAR version whose database receives the tables, passed to
#' [nar_connection()]. Default `"latest"`.
#' @param release Two-digit RNF release year, e.g. `"25"`. Default `"latest"`,
#' which asks the server which releases exist.
#' @param refresh Logical. Re-import even when the tables are already present.
#' @param shp Path to an already-extracted `lrnf000r<YY>a_e.shp`. Defaults to
#' `getOption("rnf_shp")`, and downloading the release when that is unset.
#' @return The path to the database, invisibly.
#' @export
#' @examples
#' \dontrun{
#' rnf_import()
#'
#' con <- nar_connection()
#' geocode("1234 Rue Untel, Saguenay QC",
#'         method = c("nar", "nar_interpolate", "rnf"), con = con)
#' }
rnf_import <- function(version = "latest", release = "latest", refresh = FALSE,
                       shp = NULL) {
  cache_path <- Sys.getenv("NAR_CACHE_PATH")
  if (cache_path == "") {
    stop("Please set the NAR_CACHE_PATH environment variable to a valid directory path.")
  }
  version <- nar_resolve_version(version, cache_path)
  nar_path <- file.path(cache_path, paste0(version, ".duckdb"))
  if (!file.exists(nar_path)) {
    stop("No NAR database at ", nar_path, ". Import one with nar_connection() first.",
         call. = FALSE)
  }

  if (!refresh) {
    con <- DBI::dbConnect(duckdb::duckdb(dbdir = nar_path, read_only = TRUE))
    have <- nar_has_rnf(con)
    DBI::dbDisconnect(con, shutdown = TRUE)
    if (have) {
      message("The road network file is already imported into ", version,
              ". Use refresh = TRUE to rebuild.")
      return(invisible(nar_path))
    }
  }

  # Resolved before the database is opened for writing, exactly as in
  # rqa_import(): the download is the long, failure-prone step, and holding
  # DuckDB's exclusive write lock across it would block every reader.
  src <- rnf_resolve_shp(shp, release)
  on.exit(if (src$temporary) unlink(src$dir, recursive = TRUE), add = TRUE)

  nar_session_release(nar_path)
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = nar_path))
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)
  nar_load_spatial(con)
  nar_register_spatial(con)

  rnf_build_tables(con, src$shp)
  # Last, so an interrupted import reads as absent rather than as present and
  # incomplete.
  rnf_write_metadata(con, src$release, src$shp)

  DBI::dbExecute(con, "CHECKPOINT;")
  DBI::dbDisconnect(con, shutdown = TRUE)
  on.exit(if (src$temporary) unlink(src$dir, recursive = TRUE), add = FALSE)

  message("Road network file 20", src$release, " imported into ", version, ".")
  invisible(nar_path)
}

#' Find or fetch the RNF shapefile
#'
#' @param shp An explicit path, or `NULL`
#' @param release Two-digit release year, or `"latest"`
#' @return A list of `shp` (path), `dir` (extraction directory), `release` and
#' `temporary` (whether this function created the directory and may delete it)
#' @keywords internal
rnf_resolve_shp <- function(shp = NULL, release = "latest") {
  if (is.null(shp)) shp <- getOption("rnf_shp")
  if (!is.null(shp)) {
    if (!file.exists(shp)) stop("No RNF shapefile at ", shp, ".", call. = FALSE)
    found <- regmatches(basename(shp),
                        regexpr("(?<=lrnf000r)[0-9]{2}", basename(shp), perl = TRUE))
    return(list(shp = shp, dir = dirname(shp), temporary = FALSE,
                release = if (length(found)) found else "unknown"))
  }

  if (identical(release, "latest")) release <- rnf_latest_release()
  exdir <- file.path(tempdir(), paste0("rnf_extract_", release))
  unlink(exdir, recursive = TRUE)
  tmp <- tempfile(fileext = ".zip")
  to <- options("timeout")
  # ~340 MB from a server that is not fast; the same 20-minute allowance the
  # NAR download takes.
  options(timeout = max(1800, as.numeric(unlist(to)), na.rm = TRUE))
  message("Downloading the road network file from ", rnf_url(release), ".")
  utils::download.file(rnf_url(release), tmp, mode = "wb")
  options(to)
  utils::unzip(tmp, exdir = exdir)
  unlink(tmp)

  found <- list.files(exdir, pattern = "\\.shp$", full.names = TRUE,
                      recursive = TRUE)
  if (!length(found)) {
    stop("The road network file download contained no shapefile.", call. = FALSE)
  }
  list(shp = found[1], dir = exdir, temporary = TRUE, release = release)
}

#' Build the RnfSegments and RnfStreets tables
#'
#' @description One pass over the shapefile.
#'
#' Three things in here are load-bearing:
#'
#' * The geometry is stored **untagged**, through `nar_store()`, for the same
#'   reason `Addresses.geom` is: DuckDB refuses an RTREE index over a
#'   `GEOMETRY('<crs>')` column. RNF is published in the storage CRS already --
#'   its `.prj` is the Statistics Canada Lambert this package stores in -- so
#'   the transform is a no-op in practice and is written out only so a database
#'   built in some other CRS still gets metres.
#' * `MUN_KEY_L`/`MUN_KEY_R` are spelled to match NAR's own `Streets.MUN_KEY`,
#'   which is `PROV_CODE:CSD_TYPE_ENG_CODE:CSD_ENG_NAME`. The two files use the
#'   same census subdivision vocabulary, so a municipality joins without a
#'   crosswalk -- but only after `strip_accents(upper())`, which is why the
#'   folded forms are stored rather than computed at query time.
#' * `TYPE`/`DIR` carry the literal string `N/A` for absent, alongside real
#'   nulls. Both mean the same thing and both become `NULL` here, or every
#'   comparison against a parsed street type would fail on a third value.
#' * `ENCODING=ISO-8859-1` is **required**, not a preference. The `.dbf` is
#'   Latin-1 and ships with no `.cpg` to say so, so GDAL passes the bytes
#'   through unrecoded and the first accented street name -- `des Carrières`,
#'   in the file's first few thousand rows -- makes DuckDB abort the whole
#'   scan with `invalid code point detected in Utf8Proc::UTF8ToCodepoint`.
#'   The failure is not in the read: `count(*)` succeeds, and so does anything
#'   that never decodes a string, which is why it surfaces at `upper()` and
#'   looks like a problem with the fold instead of with the file.
#' @param con A writable DuckDB connection with the spatial macros registered
#' @param shp Path to the `.shp`
#' @return The connection, invisibly
#' @keywords internal
rnf_build_tables <- function(con, shp) {
  DBI::dbWriteTable(con, "RnfProv", nar_province_table()[, c("code", "abvn")],
                    temporary = TRUE, overwrite = TRUE)
  on.exit(try(DBI::dbExecute(con, "DROP TABLE IF EXISTS RnfProv;"), silent = TRUE),
          add = TRUE)

  crs <- nar_crs(con)
  geom <- if (identical(toupper(crs), "EPSG:3347")) "r.geom" else
    sprintf("st_transform(st_setcrs(r.geom, 'EPSG:3347'), '%s', TRUE)", crs)

  message("Reading the road network file.")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS RnfSegments;")
  DBI::dbExecute(con, sprintf("
    CREATE TABLE RnfSegments AS
    WITH raw AS (
      SELECT NGD_UID, NAME,
             nullif(nullif(TYPE, 'N/A'), '') AS TYPE,
             nullif(nullif(DIR,  'N/A'), '') AS DIR,
             TRY_CAST(AFL_VAL AS INTEGER) AS AFL,
             TRY_CAST(ATL_VAL AS INTEGER) AS ATL,
             TRY_CAST(AFR_VAL AS INTEGER) AS AFR,
             TRY_CAST(ATR_VAL AS INTEGER) AS ATR,
             CSDUID_L, CSDNAME_L, CSDTYPE_L, CSDUID_R, CSDNAME_R, CSDTYPE_R,
             PRUID_L, PRUID_R, CLASS, RANK, geom
        FROM st_read('%1$s', open_options = ['ENCODING=ISO-8859-1'])
       WHERE NAME IS NOT NULL AND NAME <> ''
    ),
    shaped AS (
      SELECT r.NGD_UID, r.NAME, r.TYPE, r.DIR,
             r.AFL, r.ATL, r.AFR, r.ATR,
             r.CSDUID_L, r.CSDNAME_L, r.CSDTYPE_L,
             r.CSDUID_R, r.CSDNAME_R, r.CSDTYPE_R,
             pl.abvn AS PROV_ABVN_L, pr.abvn AS PROV_ABVN_R,
             r.CLASS, r.RANK,
             strip_accents(upper(r.NAME)) AS NAME_FOLD,
             %2$s AS MATCH_FOLD,
             upper(r.TYPE) AS TYPE_FOLD,
             upper(r.DIR)  AS DIR_FOLD,
             strip_accents(upper(r.PRUID_L || ':' || r.CSDTYPE_L || ':' || r.CSDNAME_L)) AS MUN_KEY_L,
             strip_accents(upper(r.PRUID_R || ':' || r.CSDTYPE_R || ':' || r.CSDNAME_R)) AS MUN_KEY_R,
             strip_accents(upper(r.CSDNAME_L)) AS CSD_FOLD_L,
             strip_accents(upper(r.CSDNAME_R)) AS CSD_FOLD_R,
             nar_store(%3$s) AS geom
        FROM raw r
        LEFT JOIN RnfProv pl ON pl.code = r.PRUID_L
        LEFT JOIN RnfProv pr ON pr.code = r.PRUID_R
       WHERE r.AFL IS NOT NULL OR r.AFR IS NOT NULL
    )
    SELECT s.*, st_length(s.geom) AS len_m FROM shaped s;",
    normalizePath(shp, winslash = "/", mustWork = TRUE),
    nar_match_fold_sql("strip_accents(upper(r.NAME))"),
    geom))

  message("Indexing road segments.")
  DBI::dbExecute(con, "CREATE INDEX rnf_geom_idx ON RnfSegments USING RTREE (geom);")
  DBI::dbExecute(con, "CREATE INDEX rnf_match_idx ON RnfSegments (MATCH_FOLD);")
  DBI::dbExecute(con, "CREATE INDEX rnf_name_idx ON RnfSegments (NAME_FOLD);")

  # The street gazetteer, mirroring Streets and RqaStreets: one row per street
  # per census subdivision, with the civic range the segments between them
  # cover. The two sides are unioned rather than kept apart -- a street on a
  # municipal boundary belongs to both, and asking whether RNF knows a street
  # in a municipality is a question about either side.
  message("Building the road gazetteer.")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS RnfStreets;")
  DBI::dbExecute(con, "
    CREATE TABLE RnfStreets AS
    WITH sides AS (
      SELECT NAME, NAME_FOLD, MATCH_FOLD, MUN_KEY_L AS MUN_KEY,
             CSD_FOLD_L AS CSD_FOLD, CSDUID_L AS CSDUID,
             PROV_ABVN_L AS PROV_ABVN,
             least(AFL, ATL) AS lo, greatest(AFL, ATL) AS hi, len_m
        FROM RnfSegments WHERE AFL IS NOT NULL
      UNION ALL
      SELECT NAME, NAME_FOLD, MATCH_FOLD, MUN_KEY_R,
             CSD_FOLD_R, CSDUID_R, PROV_ABVN_R,
             least(AFR, ATR), greatest(AFR, ATR), len_m
        FROM RnfSegments WHERE AFR IS NOT NULL
    )
    SELECT NAME_FOLD, MATCH_FOLD, MUN_KEY, CSD_FOLD, CSDUID, PROV_ABVN,
           any_value(NAME) AS NAME,
           count(*) AS N_SEGMENTS,
           min(lo) AS MIN_CIVIC_NO,
           max(hi) AS MAX_CIVIC_NO,
           round(sum(len_m)) AS LENGTH_M
      FROM sides
     GROUP BY NAME_FOLD, MATCH_FOLD, MUN_KEY, CSD_FOLD, CSDUID, PROV_ABVN;")
  DBI::dbExecute(con, "CREATE INDEX rnf_str_match_idx ON RnfStreets (MATCH_FOLD);")
  DBI::dbExecute(con, "CREATE INDEX rnf_str_mun_idx ON RnfStreets (CSD_FOLD);")

  invisible(con)
}

#' Record what was imported, in the same metadata table NAR uses
#'
#' @description Keyed with an `rnf_` prefix, so [nar_metadata()] reports the
#' whole state of the database in one read and a database with no RNF simply has
#' no such keys. The counts of what was *left out* are recorded here because the
#' tables themselves no longer carry it: only named, ranged segments are stored,
#' and how much of the file that was is the tier's coverage ceiling.
#' @param con A writable DuckDB connection
#' @param release The release that was read
#' @param shp Path the file was read from
#' @return The connection, invisibly
#' @keywords internal
rnf_write_metadata <- function(con, release, shp) {
  n <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n, count(DISTINCT MATCH_FOLD || '|' || MUN_KEY_L) AS pairs,
           round(sum(len_m) / 1000) AS km FROM RnfSegments;")

  DBI::dbExecute(con,
    "CREATE TABLE IF NOT EXISTS nar_metadata (key VARCHAR, value VARCHAR);")
  DBI::dbExecute(con, "DELETE FROM nar_metadata WHERE key LIKE 'rnf_%';")
  DBI::dbAppendTable(con, "nar_metadata", data.frame(
    key = c("rnf_release", "rnf_segments", "rnf_street_pairs", "rnf_length_km",
            "rnf_source", "rnf_imported_at", "rnf_licence"),
    value = c(as.character(release), as.character(n$n), as.character(n$pairs),
              as.character(n$km), basename(shp),
              format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
              "Statistics Canada Open Licence")))
  invisible(con)
}

#' The positional error an RNF interpolation carries
#'
#' @description `max(95, 0.35 * len_m)`, which is a 90th-percentile error like
#' every other `uncertainty_m` in this package, and like them it is measured
#' rather than assumed.
#'
#' It is two-part because the error is. A short segment is dominated by the
#' setback and the side offset, which do not shrink with the block, so a flat
#' floor is the right shape there; a long one is dominated by how far along the
#' block the range put the house, which scales. Measured over the addresses the
#' tier actually recovered, it covers 91.7% of them overall and 93.1% of the
#' segments longer than 600 m, where a flat 110 m covers 90.4% and 67.2% -- the
#' flat model looks equivalent in the aggregate and fails exactly where the
#' answer is least certain.
#' @param len_m Segment length in metres
#' @return A numeric vector, metres
#' @keywords internal
rnf_uncertainty_m <- function(len_m) pmax(95, 0.35 * len_m)

#' The road-network interpolation tier
#'
#' @description Places a civic number on the street segment whose address range
#' contains it, at the position the range implies, offset to the side of the
#' centreline the range belongs to.
#'
#' **It refuses when more than one segment matches.** That is not caution about
#' an unmeasured risk, it is the measurement: on the rows this tier recovered,
#' the unambiguous ones sit a median 34 m from the answer `geocode()` gives when
#' it can answer at all, while the ambiguous ones have a 90th percentile of
#' 1,678 m and one in eight of them lands more than a kilometre away. The whole
#' gross-error tail is ambiguity -- the same street name reached in two
#' municipalities the input did not separate, or two segments of one street
#' whose imputed ranges overlap -- and it costs 9 rows in 96 to remove it. The
#' refusal is reported rather than hidden: `match_method` reads `rnf_ambiguous`
#' and `n_matches` says how many segments were in contention.
#'
#' The municipality is resolved through NAR's `MunAlias` and *also* compared
#' directly against RNF's own census subdivision name. Both are needed: a
#' mailing city is what a caller writes and only `MunAlias` knows it is a CSD,
#' but 8.3% of RNF's named, ranged street/CSD pairs are absent from NAR
#' altogether, and for those `MunAlias` has nothing to say -- they are exactly
#' the streets this tier exists to reach.
#' @inheritParams nar_geocode_tier_nar
#' @param bounds The `within` restriction as an `sfc` in the storage CRS, or
#' `NULL`. Unlike the other database tiers this one cannot take
#' [nar_geocode_bounds_sql()]'s output, which constrains the `x`/`y` columns
#' `Addresses` has and `RnfSegments` does not.
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_rnf <- function(out, probe, todo, con, bounds = NULL) {
  if (!nar_has_rnf(con)) {
    stop("The \"rnf\" tier needs the road network file, which is not in this ",
         "database. Import it once with rnf_import().", call. = FALSE)
  }
  wkt <- if (is.null(bounds)) "" else sf::st_as_text(bounds)
  hit <- nar_geocode_run_tier(probe, todo, con, rnf_geocode_sql, wkt)
  if (!nrow(hit)) return(out)

  placed <- !is.na(hit$x) & hit$n_matches == 1
  if (any(placed)) {
    i <- hit$row_id[placed]
    out$match_method[i]  <- "rnf_interpolated"
    out$n_matches[i]     <- 1L
    out$uncertainty_m[i] <- rnf_uncertainty_m(hit$len_m[placed])
    out$x[i]             <- hit$x[placed]
    out$y[i]             <- hit$y[placed]
  }
  # Reported, not dropped: the address was found on a street the file knows, and
  # what is missing is the one thing that would have made the answer safe. A
  # later tier may still place the row, and if it does it overwrites this.
  amb <- hit$n_matches > 1
  if (any(amb)) {
    i <- hit$row_id[amb]
    out$match_method[i] <- "rnf_ambiguous"
    out$n_matches[i]    <- as.integer(hit$n_matches[amb])
  }
  out
}

#' The road-network interpolation query
#'
#' @description Finds every segment of the named street, in the named
#' municipality, whose address range on one side or the other contains the civic
#' number, and places the address along the one that matched.
#'
#' Four things in here are load-bearing, and all four are measured in
#' `inst/notes/road-network-file.md`:
#'
#' * The join is on `MATCH_FOLD`, not on the plain name fold, for the same
#'   reason [rqa_geocode_sql()] joins that way: the rows that reach this tier
#'   are the ones [normalize_address()]'s gazetteer could not resolve against
#'   NAR, so they still carry the caller's own spelling and `ST-`/`Sainte` and
#'   hyphen-versus-space have to fold together for them to join at all.
#' * The **side is chosen by parity** and then everything follows from it -- the
#'   range that positions the address, and the direction the 13 m offset goes.
#'   RNF's left and right are relative to the direction the segment was
#'   digitized, and that convention is real rather than nominal: the civic
#'   number's parity agrees with the range on the side it geometrically sits on
#'   94.2% of the time and with the other side's 7% of the time. Parity chooses
#'   *between* the two sides; it does not veto one. An even number inside an odd
#'   range on the only side that has one is still placed there, because a parity
#'   mismatch is not evidence that the range is wrong -- the segment may be a
#'   single generalized centreline where the ground has two carriageways, or the
#'   civic number itself may be misfiled -- and refusing would drop a real
#'   address to avoid an error the width of a street.
#' * The **5% setback**. A house at the very start of a range would otherwise be
#'   placed on the intersection node itself. With the setback and the offset the
#'   median error is 24.3 m, against 32.1 m for the setback alone, 34.5 m
#'   placed plainly on the line, and 49.3 m for the segment midpoint.
#' * The offset direction comes from the **local** direction of travel, taken
#'   over a 4% window around the placement, rather than from the segment's
#'   endpoints. A curved block would put the house on the wrong side of a chord
#'   drawn end to end. DuckDB's spatial extension has no `ST_OffsetCurve`, so
#'   this is the 2-D cross product by hand; positive is left of travel.
#'
#' There is no extrapolation. A civic number outside every range on the street
#' is not placed at the nearest end of the nearest segment, it is not placed at
#' all -- the same refusal [nar_geocode_interp_sql()] makes, and for the same
#' reason.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds The `within` restriction as WKT in the storage CRS, or `""`
#' @return A single SQL string
#' @keywords internal
rnf_geocode_sql <- function(probe, bounds = "") {
  # Two clauses from one geometry, and they do different work. The segment test
  # is the cheap prefilter the RTREE index can serve; the point test is the
  # exact one, because a segment that crosses the boundary can carry a house on
  # either side of it.
  seg_clause <- if (!nzchar(bounds)) "" else
    sprintf("\n         AND st_intersects(s.geom, st_geomfromtext('%s'))", bounds)
  pt_clause <- if (!nzchar(bounds)) "" else
    sprintf("\n     AND st_within(nar_xy(x, y), st_geomfromtext('%s'))", bounds)

  # One municipality, not two. RNF has no mailing-city column at all, so the
  # grain the NAR tier distinguishes cannot be expressed here: whichever of the
  # two the caller gave goes through MunAlias, the jurisdiction first because
  # that is what the alias set is keyed on.
  sprintf("
    WITH pr AS (
      SELECT *, coalesce(nullif(mun_auth, ''), mun_fold) AS mun FROM %1$s
    ),
    mk AS (
      SELECT DISTINCT p.row_id, strip_accents(upper(m.MUN_KEY)) AS mun_key
        FROM pr p
        JOIN MunAlias m
          ON replace(m.NAME_FOLD, '.', '') = p.mun
         AND (p.prov = '' OR m.PROV_ABVN = p.prov)
       WHERE p.mun <> ''
    ),
    cand AS (
      SELECT p.row_id, p.civic, s.NGD_UID, s.geom, s.len_m,
             s.AFL, s.ATL, s.AFR, s.ATR,
             coalesce(p.civic BETWEEN least(s.AFL, s.ATL)
                              AND greatest(s.AFL, s.ATL), false) AS in_l,
             coalesce(p.civic BETWEEN least(s.AFR, s.ATR)
                              AND greatest(s.AFR, s.ATR), false) AS in_r,
             coalesce((p.civic %% 2) = (s.AFL %% 2), false) AS par_l,
             coalesce((p.civic %% 2) = (s.AFR %% 2), false) AS par_r
        FROM pr p
        JOIN RnfSegments s
          ON s.MATCH_FOLD = p.match_fold
         AND (p.prov = '' OR s.PROV_ABVN_L = p.prov OR s.PROV_ABVN_R = p.prov)
         -- An absent street type or direction constrains nothing, on either
         -- side: RNF leaves both null on a great many segments, and reading a
         -- null as a contradiction would refuse the street rather than accept
         -- that the file did not say.
         AND (p.type = '' OR s.TYPE_FOLD IS NULL OR s.TYPE_FOLD = p.type)
         AND (p.dir  = '' OR s.DIR_FOLD  IS NULL OR s.DIR_FOLD  = p.dir)
         AND (p.mun = ''
              OR s.CSD_FOLD_L = p.mun OR s.CSD_FOLD_R = p.mun
              OR EXISTS (SELECT 1 FROM mk
                          WHERE mk.row_id = p.row_id
                            AND mk.mun_key IN (s.MUN_KEY_L, s.MUN_KEY_R)))
         AND (coalesce(p.civic BETWEEN least(s.AFL, s.ATL)
                                AND greatest(s.AFL, s.ATL), false)
           OR coalesce(p.civic BETWEEN least(s.AFR, s.ATR)
                                AND greatest(s.AFR, s.ATR), false))%2$s
    ),
    n AS (
      SELECT *, count(*) OVER (PARTITION BY row_id) AS n_matches FROM cand
    ),
    side AS (
      SELECT row_id, civic, NGD_UID, geom, len_m,
             AFL, ATL, AFR, ATR,
             CASE WHEN in_l AND par_l THEN 'L' WHEN in_r AND par_r THEN 'R'
                  WHEN in_l THEN 'L' ELSE 'R' END AS sd
        FROM n WHERE n_matches = 1
    ),
    rng AS (
      SELECT *, CASE sd WHEN 'L' THEN AFL ELSE AFR END AS f_no,
                CASE sd WHEN 'L' THEN ATL ELSE ATR END AS t_no,
                CASE sd WHEN 'L' THEN 1 ELSE -1 END AS sgn
        FROM side
    ),
    frc AS (
      SELECT *, CASE WHEN t_no = f_no THEN 0.5
                     ELSE greatest(0, least(1, (civic - f_no)
                                               / (t_no - f_no)::DOUBLE)) END AS frac
        FROM rng
    ),
    sb AS (SELECT *, 0.05 + 0.90 * frac AS fsb FROM frc),
    ln AS (
      SELECT *, st_lineinterpolatepoint(geom, fsb) AS pt,
                st_linesubstring(geom, greatest(fsb - 0.02, 0),
                                       least(fsb + 0.02, 1)) AS sub
        FROM sb
    ),
    dv AS (
      SELECT *, st_x(st_endpoint(sub)) - st_x(st_startpoint(sub)) AS ux,
                st_y(st_endpoint(sub)) - st_y(st_startpoint(sub)) AS uy
        FROM ln
    ),
    nv AS (SELECT *, sqrt(ux * ux + uy * uy) AS un FROM dv),
    pos AS (
      SELECT row_id, NGD_UID, len_m,
             st_x(pt) - coalesce(sgn * uy / nullif(un, 0) * 13, 0) AS x,
             st_y(pt) + coalesce(sgn * ux / nullif(un, 0) * 13, 0) AS y
        FROM nv
    ),
    kept AS (SELECT * FROM pos WHERE x IS NOT NULL%3$s)
    SELECT a.row_id, a.n_matches, k.NGD_UID, k.len_m, k.x, k.y
      FROM (SELECT row_id, max(n_matches) AS n_matches FROM n GROUP BY row_id) a
      LEFT JOIN kept k ON k.row_id = a.row_id",
    probe, seg_clause, pt_clause)
}
