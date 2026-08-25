# Quebec's own address register, imported beside NAR rather than into it.
#
# The Repertoire quebecois des adresses (RQA) is the register Statistics
# Canada's Quebec rows are ultimately derived from, and it carries about
# 308,000 civic addresses NAR does not. Why it is a separate table and not a
# merge is recorded in `inst/notes/quebec-addresses.md`; the short form is that
# every measurement in that note exists because the two registers are
# separately readable, and merging would spend the instrument to buy the rows.

#' The RQA bulk download
#'
#' @description One URL, in one place, so the note and the code cannot drift
#' apart. 778 MB, extracting to a ~3 GB `RQA.csv` plus a small
#' `Odonymes_renvois.csv` this package does not currently read.
#' @return A URL string
#' @keywords internal
rqa_url <- function() "https://diffusion.mern.gouv.qc.ca/Diffusion/RQA/RQA_CSV.zip"

#' Attribution required by the RQA licence
#'
#' @description RQA is CC-BY 4.0 where NAR is the Statistics Canada Open Licence.
#' Both are attribution licences and they compose, which is what makes this
#' import possible at all -- unlike the ODbL data behind [osm_geocode()], which
#' is why that function exists but is not a geocoding tier. Attribution is the
#' user's obligation, so it is returned rather than merely documented.
#' @return A character scalar
#' @export
#' @examples
#' rqa_attribution()
rqa_attribution <- function() {
  paste("Contains information licensed under CC-BY 4.0 from the Repertoire",
        "quebecois des adresses, Ministere des Ressources naturelles et des",
        "Forets, Gouvernement du Quebec.")
}

#' Is RQA available on this connection?
#'
#' @description Both tables, not either: [rqa_import()] writes `RqaAddresses`
#' first and `RqaStreets` second, so a run that died partway through leaves a
#' database that reads as having no RQA rather than as having half of it.
#' @param con A DuckDB connection
#' @return `TRUE` when the RQA tables are present
#' @keywords internal
nar_has_rqa <- function(con) {
  all(c("RqaAddresses", "RqaStreets") %in% DBI::dbListTables(con))
}

#' Import Quebec's address register beside NAR
#'
#' @description Loads the **Repertoire quebecois des adresses** into the cached
#' NAR database as its own tables, `RqaAddresses` and `RqaStreets`. Nothing in
#' `Addresses` is touched. Once imported, `"rqa"` becomes available as a
#' [geocode()] tier.
#'
#' @section Why a separate table: RQA holds roughly 308,000 civic addresses NAR
#' does not, about 9% on top of NAR's Quebec, and for
#' [normalize_address()] -- which has no online fallback -- that coverage is the
#' single largest block of what still fails in Quebec. It is nonetheless kept
#' separate rather than merged, for three reasons recorded in
#' `system.file("notes", "quebec-addresses.md", package = "cangeocode")`:
#'
#' * Merging destroys the only instrument Quebec has. Everything known about
#'   what NAR is missing in Quebec is known because the two registers can be
#'   read against each other; a merged table can no longer be asked the
#'   question.
#' * The added rows are positionally *weaker* than what NAR carries -- 20.3%
#'   building-placed against 26.9% register-wide, and 30.0% flagged
#'   `Incertaine` by RQA itself -- so merging would quietly degrade what
#'   `geom_source = 'building'` means for Quebec, which is already misleading
#'   there.
#' * A merged table stops being NAR. [nar_provinces()], the row counts in the
#'   vignettes and `nar_schema_version()` all describe a Statistics Canada
#'   release.
#'
#' @section What is imported: Certified rows only (`etat = 'Certifiee'`), which
#' drops about 7,500 retired addresses. The whole register is loaded, not just
#' the rows NAR lacks: the gap is a property of the pair and has to be
#' recomputable, and a table subset against one NAR release would be silently
#' wrong against the next. `IN_NAR` records which side of the gap each row fell
#' on **for the release it was imported into**, since the tables live inside
#' that release's database file.
#'
#' Street names are reshaped to NAR's convention on the way in, because that is
#' what makes the two joinable: NAR keeps the leading particule inside the
#' street name (`de la Cote-de-Liesse`) and RQA keeps it in a column of its own,
#' so `STREET_NAME` here is particule plus specifique, and the generique
#' becomes `STREET_TYPE`, canonicalized through the same French lexicon
#' [normalize_address()] uses. Comparing the raw columns instead reads
#' 1,265,940 missing addresses where there are 357,723.
#'
#' @param version NAR version whose database receives the tables, passed to
#' [nar_connection()]. Default `"latest"`.
#' @param refresh Logical. Re-import even when the tables are already present.
#' @param csv Path to an already-extracted `RQA.csv`. Defaults to
#' `getOption("rqa_csv")`, and downloading the release when that is unset.
#' @return The path to the database, invisibly.
#' @export
#' @examples
#' \dontrun{
#' rqa_import()
#'
#' con <- nar_connection()
#' geocode("431 Courtemanche, Montreal-Est QC",
#'         method = c("nar", "rqa", "nar_interpolate"), con = con)
#' }
rqa_import <- function(version = "latest", refresh = FALSE, csv = NULL) {
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
    have <- nar_has_rqa(con)
    DBI::dbDisconnect(con, shutdown = TRUE)
    if (have) {
      message("RQA is already imported into ", version,
              ". Use refresh = TRUE to rebuild.")
      return(invisible(nar_path))
    }
  }

  # Resolved before the database is opened for writing: the download is the
  # long, failure-prone step, and holding the write lock across it would block
  # every reader for the duration.
  src <- rqa_resolve_csv(csv)
  on.exit(if (src$temporary) unlink(src$dir, recursive = TRUE), add = TRUE)

  nar_session_release(nar_path)
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = nar_path))
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)
  nar_load_spatial(con)
  nar_register_spatial(con)

  rqa_build_tables(con, src$csv)

  # Last, like the coverage marker on an appended NAR province: an interrupted
  # import then reads as absent rather than as present and incomplete.
  rqa_write_metadata(con, src$csv)

  DBI::dbExecute(con, "CHECKPOINT;")
  DBI::dbDisconnect(con, shutdown = TRUE)
  on.exit(if (src$temporary) unlink(src$dir, recursive = TRUE), add = FALSE)

  message("RQA imported into ", version, ". ", rqa_attribution())
  invisible(nar_path)
}

#' Find or fetch the RQA CSV
#'
#' @param csv An explicit path, or `NULL`
#' @return A list of `csv` (path), `dir` (extraction directory) and `temporary`
#' (whether this function created the directory and may delete it)
#' @keywords internal
rqa_resolve_csv <- function(csv = NULL) {
  if (is.null(csv)) csv <- getOption("rqa_csv")
  if (!is.null(csv)) {
    if (!file.exists(csv)) stop("No RQA CSV at ", csv, ".", call. = FALSE)
    return(list(csv = csv, dir = dirname(csv), temporary = FALSE))
  }

  exdir <- file.path(tempdir(), "rqa_extract")
  unlink(exdir, recursive = TRUE)
  tmp <- tempfile(fileext = ".zip")
  to <- options("timeout")
  options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
  message("Downloading RQA from ", rqa_url(), ".")
  utils::download.file(rqa_url(), tmp, mode = "wb")
  options(timeout = to)
  utils::unzip(tmp, exdir = exdir)
  unlink(tmp)

  found <- list.files(exdir, pattern = "^RQA\\.csv$", full.names = TRUE,
                      recursive = TRUE)
  if (!length(found)) {
    stop("The RQA download contained no RQA.csv.", call. = FALSE)
  }
  list(csv = found[1], dir = exdir, temporary = TRUE)
}

#' A surface-to-canonical lookup, with the French reading resolved in R
#'
#' @description The lexicons are language-conditioned and the preference logic
#' lives in [nar_lex_lookup()], so it is resolved here -- once, over a few
#' hundred surfaces -- rather than reimplemented in SQL where it would be free
#' to drift. Quebec takes the French reading: `AVENUE` is `AV`, not `AVE`.
#' @param lex A lexicon data frame
#' @return A two-column data frame of `surface_fold` and `canonical`
#' @keywords internal
rqa_lex_map <- function(lex) {
  surface <- unique(lex$surface_fold)
  data.frame(surface_fold = surface,
             canonical = nar_lex_lookup(surface, lex, "fr"),
             stringsAsFactors = FALSE)
}

#' Build the RqaAddresses and RqaStreets tables
#'
#' @description One pass over the CSV. `all_varchar` is not optional: the
#' register writes empty strings rather than nulls and `numero_municipal`
#' infers as a number in some partitions and a string in others, so letting
#' DuckDB guess produces a schema that depends on which rows it sampled.
#'
#' `IN_NAR` is a left semi-join against this database's own `Addresses`, keyed
#' on forward sortation area, civic number and the folded street name -- the
#' same fold the gazetteer matches on, so `ST-`/`Sainte` and hyphen-versus-space
#' are already handled. Both NAR name families are unioned into the key,
#' because neither is complete on its own. It is deliberately an *equality*
#' test and not a containment one, so it over-reports the gap by roughly 14%:
#' NAR sometimes carries the same address under a longer name that contains
#' RQA's. Containment has no equijoin key and would turn a scan into a product.
#' @param con A writable DuckDB connection with the spatial macros registered
#' @param csv Path to `RQA.csv`
#' @return The connection, invisibly
#' @keywords internal
rqa_build_tables <- function(con, csv) {
  DBI::dbWriteTable(con, "RqaTypeMap", rqa_lex_map(nar_lex_types),
                    temporary = TRUE, overwrite = TRUE)
  DBI::dbWriteTable(con, "RqaDirMap", rqa_lex_map(nar_lex_dirs),
                    temporary = TRUE, overwrite = TRUE)
  on.exit({
    try(DBI::dbExecute(con, "DROP TABLE IF EXISTS RqaTypeMap;"), silent = TRUE)
    try(DBI::dbExecute(con, "DROP TABLE IF EXISTS RqaDirMap;"), silent = TRUE)
    try(DBI::dbExecute(con, "DROP TABLE IF EXISTS RqaNarKeys;"), silent = TRUE)
  }, add = TRUE)

  message("Keying NAR's Quebec addresses.")
  DBI::dbExecute(con, paste0("
    CREATE TEMP TABLE RqaNarKeys AS
    SELECT DISTINCT substr(MAIL_POSTAL_CODE, 1, 3) AS FSA,
           CIVIC_NO, ", nar_match_fold_sql("strip_accents(upper(NAME))"), " AS MATCH_FOLD
      FROM (SELECT MAIL_POSTAL_CODE, CIVIC_NO, OFFICIAL_STREET_NAME AS NAME
              FROM Addresses WHERE PROV_CODE = '24'
            UNION ALL
            SELECT MAIL_POSTAL_CODE, CIVIC_NO, MAIL_STREET_NAME
              FROM Addresses WHERE PROV_CODE = '24')
     WHERE CIVIC_NO IS NOT NULL AND length(MAIL_POSTAL_CODE) = 6
       AND length(NAME) > 0;"))

  message("Importing RQA addresses.")
  # STREET_NAME is particule + specifique, which is NAR's convention; the
  # generique becomes STREET_TYPE. An unknown generique keeps its own folded
  # spelling rather than being promoted to a canonical type: six of RQA's --
  # Domaine, Traverse, Descente, Chaussee, Trait-carre, Carrefour -- have no
  # counterpart anywhere in NAR, and a canonical for them would parse cleanly
  # and then join nothing.
  DBI::dbExecute(con, "DROP TABLE IF EXISTS RqaAddresses;")
  DBI::dbExecute(con, sprintf("
    CREATE TABLE RqaAddresses AS
    WITH src AS (
      SELECT * FROM read_csv('%1$s', header = true, sample_size = 200000,
                             all_varchar = true)
       WHERE etat = 'Certifi\u00e9e'
    ),
    shaped AS (
      SELECT
        r.identifiant_unique_adresse AS RQA_ID,
        TRY_CAST(r.numero_municipal AS INTEGER) AS CIVIC_NO,
        nullif(upper(coalesce(r.numero_municipal_suffixe, '')), '') AS CIVIC_NO_SUFFIX,
        nullif(coalesce(r.numero_unite, ''), '') AS UNIT_NO,
        r.seqodo AS SEQODO,
        trim(coalesce(r.particule_odonyme, '') || ' ' ||
             coalesce(r.specifique_odonyme, '')) AS STREET_NAME,
        coalesce(t.canonical, strip_accents(upper(r.generique_odonyme))) AS STREET_TYPE,
        d.canonical AS STREET_DIR,
        nullif(coalesce(r.generique_odonyme, ''), '') AS STREET_GENERIC,
        nullif(coalesce(r.particule_odonyme, ''), '') AS STREET_PARTICULE,
        nullif(coalesce(r.specifique_odonyme, ''), '') AS STREET_SPECIFIC,
        r.odonyme_recompose_normal AS STREET_FULL,
        r.nom_municipalite AS MUN_NAME,
        r.code_municipalite AS MUN_CODE,
        nullif(coalesce(r.nom_arrondissement, ''), '') AS BOROUGH,
        'QC' AS PROV_ABVN,
        '24' AS PROV_CODE,
        nullif(upper(replace(coalesce(r.code_postal, ''), ' ', '')), '') AS POSTAL_CODE,
        r.qualite_positionnement_geometrique AS POS_QUALITY,
        r.nom_region_administrative AS REGION,
        r.adresse_formatee AS FORMATTED,
        nar_store(nar_point(TRY_CAST(r.longitude AS DOUBLE),
                            TRY_CAST(r.latitude AS DOUBLE))) AS geom
      FROM src r
      LEFT JOIN RqaTypeMap t ON t.surface_fold = strip_accents(upper(r.generique_odonyme))
      LEFT JOIN RqaDirMap  d ON d.surface_fold = strip_accents(upper(r.point_cardinal_odonyme))
    ),
    folded AS (
      SELECT s.*,
             st_x(s.geom) AS x, st_y(s.geom) AS y,
             strip_accents(upper(s.STREET_NAME)) AS NAME_FOLD,
             %2$s AS MATCH_FOLD,
             strip_accents(upper(s.MUN_NAME)) AS MUN_FOLD,
             substr(s.POSTAL_CODE, 1, 3) AS FSA
        FROM shaped s
    )
    SELECT f.*,
           EXISTS (SELECT 1 FROM RqaNarKeys k
                    WHERE k.FSA = f.FSA AND k.CIVIC_NO = f.CIVIC_NO
                      AND k.MATCH_FOLD = f.MATCH_FOLD) AS IN_NAR
      FROM folded f;",
    normalizePath(csv, winslash = "/", mustWork = TRUE),
    nar_match_fold_sql("strip_accents(upper(s.STREET_NAME))")))

  message("Indexing RQA addresses.")
  DBI::dbExecute(con, "CREATE INDEX rqa_geom_idx ON RqaAddresses USING RTREE (geom);")
  DBI::dbExecute(con, "CREATE INDEX rqa_name_idx ON RqaAddresses (NAME_FOLD);")
  DBI::dbExecute(con, "CREATE INDEX rqa_fsa_idx ON RqaAddresses (FSA);")

  # The street gazetteer, mirroring Streets: one row per odonyme per
  # municipality, with the civic range that makes interpolation possible.
  message("Building RQA street gazetteer.")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS RqaStreets;")
  DBI::dbExecute(con, "
    CREATE TABLE RqaStreets AS
    SELECT SEQODO, STREET_NAME, STREET_TYPE, STREET_DIR, STREET_GENERIC,
           MUN_NAME, MUN_CODE, PROV_ABVN, NAME_FOLD, MATCH_FOLD, MUN_FOLD,
           count(*) AS N_ADDRESSES,
           count(*) FILTER (WHERE NOT IN_NAR) AS N_NOT_IN_NAR,
           min(CIVIC_NO) AS MIN_CIVIC_NO,
           max(CIVIC_NO) AS MAX_CIVIC_NO
      FROM RqaAddresses
     GROUP BY ALL;")
  DBI::dbExecute(con, "CREATE INDEX rqa_str_name_idx ON RqaStreets (NAME_FOLD);")
  DBI::dbExecute(con, "CREATE INDEX rqa_str_mun_idx ON RqaStreets (MUN_FOLD);")

  invisible(con)
}

#' Record what was imported, in the same metadata table NAR uses
#'
#' @description Keyed with an `rqa_` prefix rather than in a table of its own,
#' so [nar_metadata()] reports the whole state of the database in one read and
#' a database with no RQA simply has no such keys.
#' @param con A writable DuckDB connection
#' @param csv Path the register was read from
#' @return The connection, invisibly
#' @keywords internal
rqa_write_metadata <- function(con, csv) {
  n <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n, count(*) FILTER (WHERE NOT IN_NAR) AS gap,
           max(nullif(POSTAL_CODE, '')) IS NOT NULL AS ok FROM RqaAddresses;")
  version <- DBI::dbGetQuery(con, sprintf(
    "SELECT max(date_diffusion_version) AS v FROM read_csv('%s', header = true,
       sample_size = 200000, all_varchar = true);",
    normalizePath(csv, winslash = "/", mustWork = TRUE)))$v

  # A database old enough to predate the metadata table can still receive
  # RQA; it simply gets the table created here.
  DBI::dbExecute(con,
    "CREATE TABLE IF NOT EXISTS nar_metadata (key VARCHAR, value VARCHAR);")
  DBI::dbExecute(con, "DELETE FROM nar_metadata WHERE key LIKE 'rqa_%';")
  DBI::dbAppendTable(con, "nar_metadata", data.frame(
    key = c("rqa_version", "rqa_rows", "rqa_not_in_nar", "rqa_imported_at",
            "rqa_licence"),
    value = c(as.character(version), as.character(n$n), as.character(n$gap),
              format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"), "CC-BY 4.0")))
  invisible(con)
}

#' The RQA geocoding tier
#'
#' @description Looks the civic number up in Quebec's own register. Quebec only,
#' and gated on the parsed province exactly as the online [qc_geocode()] tier
#' is: `RqaAddresses` holds nothing outside Quebec, so an address that never
#' named a province would otherwise be placed there on the strength of a street
#' name that exists in every province.
#'
#' `match_method` carries RQA's own positional class rather than one label,
#' because the register states how each point was placed and the classes are
#' not interchangeable -- only a quarter of them are building placements.
#' `uncertainty_m` is `0` for `rqa_building` and `NA` otherwise: nothing in this
#' package has measured what `Geocodee` or `Incertaine` are worth on the ground,
#' and a number invented here would be indistinguishable from a measured one.
#' Where several RQA points satisfy the query the spread between them is
#' reported, which is a genuine lower bound whatever the class.
#' @inheritParams nar_geocode_tier_nar
#' @param res Parsed components, for the province gate
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_rqa <- function(res, out, probe, todo, con, bounds = "") {
  if (!nar_has_rqa(con)) {
    stop("The \"rqa\" tier needs Quebec's address register, which is not in ",
         "this database. Import it once with rqa_import().", call. = FALSE)
  }
  prov <- toupper(res$PROV_ABVN %||% rep(NA_character_, nrow(res)))
  todo <- todo[!is.na(prov[todo]) & prov[todo] == "QC"]
  if (!length(todo)) return(out)

  hit <- nar_geocode_run_tier(probe, todo, con, rqa_geocode_sql, bounds)
  if (!nrow(hit)) return(out)

  i <- hit$row_id
  out$match_method[i] <- rqa_method_label(hit$POS_QUALITY)
  out$n_matches[i]    <- as.integer(hit$n_points)
  out$n_records[i]    <- as.integer(hit$n_records)
  out$match_postal_code[i] <- hit$match_postal_code
  out$x[i]            <- hit$x
  out$y[i]            <- hit$y
  base <- ifelse(out$match_method[i] == "rqa_building", 0, NA_real_)
  out$uncertainty_m[i] <- ifelse(is.na(base) & hit$spread_m > 0, hit$spread_m,
                                 pmax(base, hit$spread_m))
  out
}

#' RQA's positional-quality classes as `match_method` labels
#'
#' @description The register's own vocabulary, transliterated. An unrecognized
#' class becomes `rqa_other` rather than being dropped, so a future RQA release
#' that adds one still places its addresses.
#' @param quality RQA's `qualite_positionnement_geometrique`
#' @return A character vector of `match_method` values
#' @keywords internal
rqa_method_label <- function(quality) {
  key <- nar_fold(quality)
  out <- c("BATIMENT"    = "rqa_building",
           "GEOCODEE"    = "rqa_geocoded",
           "INCERTAINE"  = "rqa_uncertain",
           "CENTRE LOT"  = "rqa_lot",
           "FRONT LOT"   = "rqa_lot")[key]
  unname(ifelse(is.na(out), "rqa_other", out))
}

#' The RQA exact-match query
#'
#' @description The same shape as [nar_geocode_exact_sql()] and for the same
#' reasons, with two differences that are not cosmetic.
#'
#' It joins on `MATCH_FOLD`, not on the plain name fold. The NAR tiers can
#' afford the stricter key because [normalize_address()] has already resolved
#' the name against NAR's own gazetteer and handed back NAR's spelling; RQA
#' spells the same street its own way, and the addresses this tier exists for
#' are precisely the ones the gazetteer could not resolve, because NAR does not
#' carry them. Folding `ST-` to `SAINT`, the hyphen to a space and the particule
#' into the name is what lets those still join.
#'
#' There is no `MunAlias` route. RQA has no alias set, so both municipality
#' grains -- `known$MUN_NAME` and `known$CSD_NAME` alike -- are compared
#' directly against the municipality and the borough both: `Montreal` has to
#' reach `Ville-Marie`, and in RQA those are different columns rather than
#' different names for one place. The grain distinction the NAR tier draws is
#' therefore not available here, and the two probe columns simply both apply.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds_sql()], or `""`
#' @return A single SQL string
#' @keywords internal
rqa_geocode_sql <- function(probe, bounds = "") {
  mun <- "replace(a.MUN_FOLD, '.', '')"
  borough <- "replace(strip_accents(upper(coalesce(a.BOROUGH, ''))), '.', '')"
  cand <- sprintf(
    "SELECT p.row_id, a.RQA_ID, a.POS_QUALITY, a.POSTAL_CODE, a.x, a.y, %5$s
        FROM %1$s p
        JOIN RqaAddresses a
          ON a.MATCH_FOLD = p.match_fold
         AND a.CIVIC_NO = p.civic
         AND a.x IS NOT NULL
         AND (p.suffix = '' OR upper(coalesce(a.CIVIC_NO_SUFFIX, '')) = p.suffix)
         AND (p.mun_fold = '' OR %2$s = p.mun_fold OR %3$s = p.mun_fold)
         AND (p.mun_auth = '' OR %2$s = p.mun_auth OR %3$s = p.mun_auth)
         AND (p.type = '' OR p.type = strip_accents(upper(a.STREET_TYPE)))
         AND (p.dir  = '' OR p.dir  = coalesce(a.STREET_DIR, ''))%4$s",
    probe, mun, borough, bounds, nar_geocode_unit_hit("a.UNIT_NO"))
  # Quebec's register carries a unit on 1,665,467 of its 5,315,435 rows, so the
  # same narrowing applies, through the same filter.
  cand <- nar_geocode_unit_filter(cand)
  # The surrounding shape -- pick one, then measure the set it came from -- is
  # shared with the NAR tier; only the candidates, the quality order and the
  # column names are Quebec's.
  nar_geocode_best_sql(
    cand,
    "CASE WHEN POS_QUALITY = 'B\u00e2timent' THEN 0
                      WHEN POS_QUALITY = 'G\u00e9ocod\u00e9e' THEN 1
                      WHEN POS_QUALITY = 'Incertaine' THEN 3
                      ELSE 2 END,
                 RQA_ID",
    "b.row_id, b.POS_QUALITY, b.x, b.y",
    "RQA_ID", "POSTAL_CODE")
}
