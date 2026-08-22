# Get NAR data
#
#
#
#' Get NAR data
#' @description This function downloads the NAR data if necessary and returns a connection the NAR database
#' @param version Version of the NAR database to connect to. Default is "latest".
#' @param refresh Logical indicating whether to refresh the local cache of the NAR database.
#' @return A connection to the NAR database containing Addresses and Locations tables
#' @export
#' @examples
#' \dontrun{
#' con <- nar_connection()
#' }
nar_connection <- function(version="latest", refresh=FALSE) {
  cache_path <- Sys.getenv("NAR_CACHE_PATH")
  if (cache_path == "") {
    stop("Please set the NAR_CACHE_PATH environment variable to a valid directory path.")
  }
  if (!dir.exists(cache_path)) {
    dir.create(cache_path, recursive=TRUE)
  }
  version <- nar_resolve_version(version, cache_path, refresh = refresh)
  nar_path <- file.path(cache_path, paste0(version,".duckdb"))
  if (!file.exists(nar_path) || refresh) {
    url <- available_nar_versions() |>
      filter(.data$path == !!version | .data$version == !!version) |>
      pull(url)
    exdir <- getOption("nar_exdir")
    # Only a directory this function created may be deleted afterwards; a
    # caller-supplied `nar_exdir` belongs to the caller.
    downloaded <- is.null(exdir)
    if (downloaded) {
      message("Downloading NAR data version ",version," from StatCan.")
      tmp <- tempfile(fileext = ".zip")
      to <- options("timeout")
      # set timeout to 20 minutes if it's less than that, StatCan connection can be very slow
      options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
      utils::download.file(url, tmp, mode="wb")
      options(timeout = to)

      exdir <- file.path(tempdir(),"nar_extract")
      utils::unzip(tmp, exdir=exdir)
      unlink(tmp)
    } else {
      message("Using cached NAR data version ",version," from ",exdir,".")
    }

    address_data_paths <- list.files(exdir, pattern="Address_.*\\.csv$", full.names=TRUE, recursive=TRUE)
    location_data_paths <- list.files(exdir, pattern="Location_.*\\.csv$", full.names=TRUE, recursive=TRUE)
    if (!length(address_data_paths) || !length(location_data_paths)) {
      stop("No NAR Address/Location CSVs found under ", exdir, ".")
    }

    address_schema <- nar_csv_schema(address_data_paths[1], nar_address_types(),
                                     required = c("LOC_GUID", "ADDR_GUID", "BG_X", "BG_Y"))
    location_schema <- nar_csv_schema(location_data_paths[1], nar_location_types(),
                                      required = c("LOC_GUID", "BG_LATITUDE", "BG_LONGITUDE"))

    # Releases before 2026-06 carry no blockface columns, so the fallback has to
    # be built from what the header actually offers.
    has_blockface <- all(c("BF_REPPOINT_X", "BF_REPPOINT_Y") %in%
                           names(address_schema))

    address_arrow <- arrow::open_dataset(address_data_paths,
                                         format = "csv",
                                         skip_rows = 1,
                                         schema = address_schema)
    location_arrow <- arrow::open_dataset(location_data_paths,
                                          format = "csv",
                                          skip_rows = 1,
                                          schema = location_schema)



    # Build into a side path and publish by renaming once the import has
    # completed. A database that fails partway through would otherwise be left
    # at `nar_path`, where every later call -- seeing the file exist -- would
    # treat it as a finished import and hand out a connection with no
    # Addresses table.
    build_path <- paste0(nar_path, ".building")
    if (file.exists(build_path)) unlink(build_path, recursive = TRUE)
    import_complete <- FALSE
    on.exit(if (!import_complete && file.exists(build_path))
              unlink(build_path, recursive = TRUE), add = TRUE)

    con <- DBI::dbConnect(duckdb::duckdb(dbdir=build_path))
    nar_load_spatial(con)
    nar_write_metadata(con, version)
    nar_register_spatial(con)

    # The building point (BG) is the primary geometry; where it is absent the
    # blockface centroid (BF) stands in -- a much coarser point shared by every
    # address on one side of a street -- and geom_source records which was used.
    # x/y mirror whichever point geom ended up holding rather than BG alone:
    # DuckDB maintains min/max zonemaps for plain numeric columns, and the
    # bounding-box prefilter in nar_within_radius() uses them to skip most row
    # groups, so they have to agree with the geometry they are filtering.
    message("Importing address data.")

    dplyr::copy_to(con,
                   address_arrow |>
                     arrow::to_duckdb(),
                   name = "AddressesTemp", temporary = TRUE, overwrite = TRUE)

    addresses <- con |> tbl("AddressesTemp")

    if (has_blockface) {
      addresses <- addresses |>
        mutate(x=dplyr::coalesce(.data$BG_X, .data$BF_REPPOINT_X),
               y=dplyr::coalesce(.data$BG_Y, .data$BF_REPPOINT_Y),
               geom_source=dplyr::case_when(!is.na(.data$BG_X) ~ "building",
                                            !is.na(.data$BF_REPPOINT_X) ~ "blockface",
                                            TRUE ~ NA_character_))
    } else {
      addresses <- addresses |>
        mutate(x=.data$BG_X, y=.data$BG_Y,
               geom_source=dplyr::if_else(is.na(.data$BG_X),
                                          NA_character_, "building"))
    }

    dplyr::copy_to(con,
                   addresses |>
                     mutate(geom=st_point(.data$x, .data$y)) |>
                     select(-"BG_X", -"BG_Y"),
                   name = "Addresses", temporary = FALSE, overwrite = TRUE)

    message("Indexing address data.")

    DBI::dbExecute(con, "DROP TABLE AddressesTemp;")
    DBI::dbExecute(con, "CREATE INDEX add_geom_idx ON Addresses USING RTREE (geom);")
    DBI::dbExecute(con, "CREATE INDEX add_loc_guid_idx ON Addresses (LOC_GUID);")

    message("Importing location data.")
    dplyr::copy_to(con,
                   location_arrow |>
                     arrow::to_duckdb(),
                   name = "LocationsTemp", temporary = TRUE, overwrite = TRUE)

    dplyr::copy_to(con,
                   con |>
                     tbl("LocationsTemp") |>
                     mutate(geom=nar_store(nar_point(.data$BG_LONGITUDE,.data$BG_LATITUDE))) |>
                     mutate(x=st_x(.data$geom), y=st_y(.data$geom)) |>
                     select(-"BG_LATITUDE", -"BG_LONGITUDE"),
                   name = "Locations", temporary = FALSE, overwrite = TRUE)

    message("Indexing location data.")

    DBI::dbExecute(con, "DROP TABLE LocationsTemp;")
    DBI::dbExecute(con, "CREATE INDEX loc_geom_idx ON Locations USING RTREE (geom);")
    DBI::dbExecute(con, "CREATE INDEX loc_loc_guid_idx ON Locations (LOC_GUID);")

    # The street gazetteer that normalize_address() resolves against: one row
    # per distinct street, which is 374k rows against the address table's 17.4M.
    # Both name families are carried because neither is complete on its own --
    # MAIL_STREET_NAME is empty for 957k addresses while OFFICIAL_STREET_NAME is
    # empty for 95, and where both are present they still differ beyond case for
    # 530k. A parser that only knew one of them would fail to match the other.
    #
    # NAME_FOLD is the join key: accent- and case-insensitive, so a user typing
    # an accent-free query string reaches the accented stored name. It is
    # materialized rather than computed per query so the join can use the index.
    message("Building street gazetteer.")
    # MUN_KEY is the jurisdictional bucket a street sits in -- the CSD, which is
    # a statistical boundary drawn from jurisdictional divisions. It is what
    # candidate streets are restricted to, rather than the mailing city, because
    # the two do not nest: see MunAlias below.
    DBI::dbExecute(con, "
      CREATE TABLE Streets AS
      SELECT OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
             MAIL_STREET_NAME, MAIL_STREET_TYPE, MAIL_STREET_DIR,
             MAIL_MUN_NAME, MAIL_PROV_ABVN, PROV_CODE, CSD_ENG_NAME,
             PROV_CODE || ':' || CSD_TYPE_ENG_CODE || ':' || CSD_ENG_NAME AS MUN_KEY,
             strip_accents(upper(OFFICIAL_STREET_NAME)) AS NAME_FOLD,
             strip_accents(upper(MAIL_STREET_NAME)) AS MAIL_NAME_FOLD,
             count(*) AS N_ADDRESSES,
             min(CIVIC_NO) AS MIN_CIVIC_NO,
             max(CIVIC_NO) AS MAX_CIVIC_NO
      FROM Addresses
      GROUP BY ALL;")
    DBI::dbExecute(con, "CREATE INDEX str_name_idx ON Streets (NAME_FOLD);")
    DBI::dbExecute(con, "CREATE INDEX str_mail_name_idx ON Streets (MAIL_NAME_FOLD);")
    DBI::dbExecute(con, "CREATE INDEX str_mun_key_idx ON Streets (MUN_KEY);")

    # Every name a locality answers to, mapped to the buckets it can mean.
    #
    # A mailing city and a CSD are different kinds of object and neither
    # contains the other. One mailing city can span several jurisdictions, one
    # jurisdiction carries many mailing cities, and amalgamation left legacy
    # names alive on both sides -- people still write Scarborough, and NAR still
    # files it that way, while the CSD has been Toronto for decades. Treating
    # the municipality as a single canonical string therefore loses matches in
    # both directions, so it is stored as an alias set instead: mailing city,
    # English CSD name and French CSD name all become lookup keys onto the same
    # MUN_KEY, and a name that means several buckets simply returns all of them.
    DBI::dbExecute(con, "
      CREATE TABLE MunAlias AS
      SELECT NAME_FOLD, PROV_ABVN, MUN_KEY, sum(n) AS N_ADDRESSES
      FROM (
        SELECT strip_accents(upper(MAIL_MUN_NAME)) AS NAME_FOLD,
               MAIL_PROV_ABVN AS PROV_ABVN,
               PROV_CODE || ':' || CSD_TYPE_ENG_CODE || ':' || CSD_ENG_NAME AS MUN_KEY,
               count(*) AS n
          FROM Addresses
         WHERE length(MAIL_MUN_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
         GROUP BY ALL
        UNION ALL
        SELECT strip_accents(upper(CSD_ENG_NAME)), MAIL_PROV_ABVN,
               PROV_CODE || ':' || CSD_TYPE_ENG_CODE || ':' || CSD_ENG_NAME, count(*)
          FROM Addresses
         WHERE length(CSD_ENG_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
         GROUP BY ALL
        UNION ALL
        SELECT strip_accents(upper(CSD_FRE_NAME)), MAIL_PROV_ABVN,
               PROV_CODE || ':' || CSD_TYPE_ENG_CODE || ':' || CSD_ENG_NAME, count(*)
          FROM Addresses
         WHERE length(CSD_FRE_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
         GROUP BY ALL
      )
      GROUP BY ALL;")
    DBI::dbExecute(con, "CREATE INDEX mun_alias_idx ON MunAlias (NAME_FOLD);")

    # Forward-sortation-area to municipality. 10k rows for 1,672 FSAs, and the
    # median FSA maps to exactly one municipality, so a postal code in the input
    # pins the municipality even when the string never names it -- which is what
    # makes a comma-less address resolvable.
    DBI::dbExecute(con, "
      CREATE TABLE PostalMun AS
      SELECT substr(MAIL_POSTAL_CODE, 1, 3) AS FSA,
             MAIL_MUN_NAME, MAIL_PROV_ABVN,
             count(*) AS N_ADDRESSES
      FROM Addresses
      WHERE length(MAIL_POSTAL_CODE) = 6 AND length(MAIL_MUN_NAME) > 0
      GROUP BY ALL;")
    DBI::dbExecute(con, "CREATE INDEX pm_fsa_idx ON PostalMun (FSA);")



    # Checkpoint and fully shut down before reopening read-only: any WAL left
    # behind cannot be replayed by a read-only connection, which then fails to
    # open the database at all.
    DBI::dbExecute(con, "CHECKPOINT;")
    DBI::dbDisconnect(con, shutdown = TRUE)

    if (file.exists(nar_path)) unlink(nar_path, recursive = TRUE)
    if (!file.rename(build_path, nar_path)) {
      stop("Could not move the imported database into place at ", nar_path, ".")
    }
    import_complete <- TRUE

    message("NAR data version ",version," successfully imported.")


    # cleanup
    address_arrow <- NULL
    location_arrow <- NULL
    if (downloaded) unlink(exdir, recursive=TRUE)
  }

  con <- DBI::dbConnect(duckdb::duckdb(dbdir=nar_path, read_only = TRUE))
  # Streets is deliberately not required: it arrived in schema version 4, and a
  # version 3 database stays perfectly usable, just without gazetteer
  # resolution in normalize_address().
  missing_tables <- setdiff(c("Addresses", "Locations"), DBI::dbListTables(con))
  if (length(missing_tables)) {
    DBI::dbDisconnect(con, shutdown = TRUE)
    stop("The cached NAR database at ", nar_path, " is incomplete (missing ",
         paste(missing_tables, collapse = ", "),
         "). Rebuild it with nar_connection(version = \"", version,
         "\", refresh = TRUE).")
  }
  nar_load_spatial(con)
  nar_register_spatial(con)

  return(con)
}


#' Scrape availabe NAR versions from the StatCan website
#' @param refresh Logical indicating whether to refresh the cached version list
#' @return A tibble with available NAR versions and their URLs
#' @export
#' @examples
#' \dontrun{
#' versions <- available_nar_versions()
#' }
available_nar_versions <- function(refresh=FALSE){
  version_cache_path <- file.path(tempdir(), "nar_versions.csv")
  if (refresh || !file.exists(version_cache_path)) {
    overview_url <- "https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm"
    versions <- nar_version_table(xml2::read_html(overview_url), overview_url)
    readr::write_csv(versions, version_cache_path)
  } else {
    versions <- readr::read_csv(version_cache_path,
                                col_types = readr::cols(
                                  version = readr::col_character(),
                                  url = readr::col_character(),
                                  Date = readr::col_date(format = "%Y-%m-%d"),
                                  path = readr::col_character()
                                ))
  }
  versions
}

#' Parse a StatCan version label into a date
#'
#' @description StatCan labels releases inconsistently -- a bare year, a month
#' and year, or a full date -- so each form is matched explicitly.
#'
#' This deliberately avoids `strptime`'s `%B`, which reads month names through
#' `LC_TIME` and returns `NA` for an English name under, say, a French locale.
#' `month.name`/`month.abb` are English constants in base R whatever the locale,
#' so matching against them keeps version discovery working everywhere. A silent
#' `NA` here is expensive: it propagates into `path`, which is both the database
#' filename and the canonical version key.
#' @param version Character vector of version labels
#' @return A `Date` vector, `NA` where the label could not be parsed
#' @keywords internal
nar_version_date <- function(version) {
  months <- stats::setNames(rep(1:12, 2), tolower(c(month.name, month.abb)))

  # Exact name or abbreviation first, then any unambiguous prefix, so labels
  # like "Sept. 2025" resolve without hard-coding every abbreviation StatCan
  # has used.
  month_number <- function(token) {
    token <- tolower(token)
    exact <- months[token]
    if (!is.na(exact)) return(unname(exact))
    hit <- which(startsWith(tolower(month.name), token))
    if (length(hit) == 1) hit else NA_integer_
  }

  one <- function(label) {
    label <- trimws(label)
    # A bare year labels that year's December release.
    if (grepl("^[0-9]{4}$", label)) return(paste0(label, "-12-01"))

    my <- regmatches(label, regexec("^([A-Za-z]+)\\.?\\s+([0-9]{4})$", label))[[1]]
    if (length(my) == 3) {
      month <- month_number(my[2])
      if (!is.na(month)) return(sprintf("%04d-%02d-01", as.integer(my[3]), month))
    }

    dmy <- regmatches(label, regexec("^([0-9]{1,2})\\s+([A-Za-z]+)\\.?\\s+([0-9]{4})$", label))[[1]]
    if (length(dmy) == 4) {
      month <- month_number(dmy[3])
      if (!is.na(month)) {
        return(sprintf("%04d-%02d-%02d", as.integer(dmy[4]), month, as.integer(dmy[2])))
      }
    }

    # ISO dates are locale-independent, so pass those through; anything else is
    # unparseable and must become NA rather than throwing from as.Date().
    if (grepl("^[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}$", label)) return(label)
    NA_character_
  }

  as.Date(vapply(version, one, character(1), USE.NAMES = FALSE))
}

#' Extract the version table from the StatCan publication page
#'
#' @description Split out from [available_nar_versions()] so the parsing can be
#' exercised without a network round trip. A layout change on the StatCan side
#' is the most likely way version discovery breaks, so it fails loudly rather
#' than returning an empty table.
#' @param page Parsed HTML, from `xml2::read_html()`
#' @param overview_url URL the page came from, used to resolve relative links
#' @return A tibble of `version`, `url`, `Date` and `path`, newest first
#' @keywords internal
nar_version_table <- function(page, overview_url) {
  links <- rvest::html_nodes(page, "section div p a")
  versions <- tibble(version=trimws(rvest::html_text(links)),
                     url=rvest::html_attr(links, "href")) |>
    filter(grepl("\\.zip$", .data$url)) |>
    # Resolve relative hrefs only: file.path() would mangle an absolute URL
    # into the publication page's directory.
    mutate(url=ifelse(grepl("^[A-Za-z][A-Za-z0-9+.-]*://", .data$url), .data$url,
                      file.path(dirname(overview_url), .data$url))) |>
    mutate(Date=nar_version_date(.data$version)) |>
    mutate(path=strftime(.data$Date,"%Y-%m")) |>
    arrange(desc(.data$Date))

  if (nrow(versions) == 0) {
    stop("Found no NAR download links on ", overview_url,
         ". The page layout has probably changed; see the CSS selector in ",
         "nar_version_table().")
  }

  unparsed <- versions$version[is.na(versions$Date)]
  if (length(unparsed)) {
    warning("Ignoring NAR version(s) with an unrecognized date label: ",
            paste(unparsed, collapse = ", "), ".")
    versions <- versions |> filter(!is.na(.data$Date))
  }
  versions
}

#' Versions already present in the local cache
#'
#' @param cache_path Directory holding the `<version>.duckdb` files
#' @return Character vector of version keys, newest first
#' @keywords internal
nar_cached_versions <- function(cache_path) {
  if (!dir.exists(cache_path)) return(character(0))
  files <- list.files(cache_path, pattern = "\\.duckdb$")
  # Keys are YYYY-MM, so a lexical sort is chronological.
  sort(sub("\\.duckdb$", "", files), decreasing = TRUE)
}

#' Resolve a requested version, preferring the cache over the network
#'
#' @description `nar_connection()` used to resolve every request against the
#' StatCan publication page before looking at the cache, which made an already
#' downloaded multi-gigabyte database unusable offline. A version key that names
#' a cached database is now answered locally, and `"latest"` falls back to the
#' newest cached database when StatCan cannot be reached.
#'
#' Resolving still needs the network when there is a genuine question to answer:
#' which release is currently latest, or which key a label like `"May 2024"`
#' corresponds to.
#' @param version Requested version, or `"latest"`
#' @param cache_path Directory holding the cached databases
#' @param refresh Whether the database is being rebuilt, which always needs the
#' download URL and so always needs the network
#' @return A version key
#' @keywords internal
nar_resolve_version <- function(version, cache_path, refresh = FALSE) {
  cached <- nar_cached_versions(cache_path)

  if (!refresh && version %in% cached) {
    return(version)
  }

  resolved <- try(normalized_nar_version(version, refresh = refresh), silent = TRUE)

  if (inherits(resolved, "try-error")) {
    if (!refresh && version == "latest" && length(cached)) {
      warning("Could not reach StatCan to look up the latest NAR version (",
              sub("\n.*", "", conditionMessage(attr(resolved, "condition"))),
              "). Using the newest cached version, ", cached[1],
              ". Pass refresh = TRUE once a connection is available to check ",
              "for a newer release.")
      return(cached[1])
    }
    stop(conditionMessage(attr(resolved, "condition")))
  }

  if (length(resolved) == 0 || is.null(resolved) || !nzchar(resolved)) {
    stop("Invalid version specified. Valid versions are: ",
         paste(c("latest", available_nar_versions()$version), collapse = ", "))
  }
  resolved
}

#' Normalize NAR version string
#' @param version Version of the NAR database to connect to. Default is "latest
#' @param refresh Logical indicating whether to refresh the cached version list
#' @return Normalized version string
#' @keywords internal
normalized_nar_version <- function(version, refresh=FALSE) {
  available_versions <- available_nar_versions(refresh = refresh)

  if (version == "latest") {
    normalized_version <- available_versions$path[1]
  } else {
    normalized_version <- available_versions |>
      filter(toupper(.data$version) == toupper(!!version) | .data$path == !!version) |>
      pull(.data$path)
  }
  if (length(normalized_version) > 1) {
    stop(paste0("Multiple versions matched the specified version: ",normalized_version))
  }
  normalized_version
}

#' Collect a NAR table as an sf object
#'
#' @description Collects a lazy NAR table into an \code{sf} object. Geometry is
#' transferred as WKB rather than WKT, and the CRS is read from the database
#' rather than assumed, so the result is correct for any NAR version.
#' @param tbl nar table to collect
#' @param crs Optional CRS to return the geometry in. Defaults to the CRS the
#' geometry is stored in (EPSG:3347). Pass e.g. \code{"EPSG:4326"} for
#' longitude/latitude; coordinates are always returned in lon/lat order.
#' @return An sf object. The internal `x`/`y` storage-coordinate columns are
#' dropped: they duplicate the geometry and would not survive a reprojection.
#' @export
#' @examples
#' \dontrun{
#' con <- nar_connection()
#' nar_sf <- con |>
#'   dplyr::tbl("Addresses") |>
#'   head(20) |>
#'   collect_nar()
#' }
collect_nar <- function(tbl, crs = NULL) {
  con <- nar_con(tbl)
  if (is.null(con)) {
    stop("collect_nar() needs a lazy table backed by a NAR database connection; ",
         "got an object that has already been collected. Call it directly on ",
         "the result of dplyr::tbl(con, ...), and use sf::st_transform() to ",
         "reproject afterwards.")
  }
  storage_crs <- nar_crs(con)
  nar_register_spatial(con, crs = storage_crs)

  if (!is.null(crs)) {
    # always_xy = TRUE: authority-defined CRSs such as EPSG:4326 order their
    # axes lat/lon, but sf always expects lon/lat, so the flag is required to
    # avoid silently returning transposed coordinates.
    tbl <- tbl |> mutate(geom = st_transform(nar_geom(.data$geom),
                                             !!nar_crs_string(crs), TRUE))
    out_crs <- crs
  } else {
    out_crs <- storage_crs
  }

  result <- tbl |>
    mutate(geom = nar_wkb(.data$geom)) |>
    select(-dplyr::any_of(c("x", "y"))) |>
    collect()

  result$geom <- sf::st_as_sfc(structure(unclass(result$geom), class = "WKB"),
                               EWKB = FALSE, crs = sf::st_crs(out_crs))

  sf::st_as_sf(result)
}


#' Non-string column types in the NAR address file
#'
#' @description Every other column is read as a string, matching the original
#' StatCan text. Only the columns listed here are given a numeric type.
#' @return A named list of `arrow` types
#' @keywords internal
nar_address_types <- function() {
  list(
    CIVIC_NO = arrow::int64(),
    BG_X = arrow::float64(),
    BG_Y = arrow::float64(),
    BF_REPPOINT_X = arrow::float64(),
    BF_REPPOINT_Y = arrow::float64(),
    BU_USE = arrow::int64()
  )
}

#' Non-string column types in the NAR location file
#'
#' @return A named list of `arrow` types
#' @keywords internal
nar_location_types <- function() {
  list(
    BG_LATITUDE = arrow::float64(),
    BG_LONGITUDE = arrow::float64(),
    BF_REPPOINT_LATITUDE = arrow::float64(),
    BF_REPPOINT_LONGITUDE = arrow::float64()
  )
}

#' Build an Arrow schema from a NAR CSV header
#'
#' @description The schema is derived from the file's own header, with types
#' attached **by name**, rather than declared as a fixed positional list.
#'
#' This matters because NAR's layout changes between releases and `arrow` maps a
#' declared schema onto CSV columns by position. The June 2026 release inserted
#' `BF_REPPOINT_X`/`BF_REPPOINT_Y` in the *middle* of the address record, after
#' `BG_X`/`BG_Y`, shifting `BU_N_CIVIC_ADD` and `BU_USE` along by two. A fixed
#' list that was merely extended at the end would have read blockface centroid
#' coordinates into `BU_N_CIVIC_ADD` without complaint; only the column count
#' differing made the mismatch an error rather than silent corruption.
#'
#' Reading the header keeps the import working across releases and surfaces a
#' genuinely breaking change -- a column that disappears -- through `required`.
#' @param path Path to a NAR CSV
#' @param types Named list of `arrow` types for the columns that are not strings
#' @param required Column names that must be present
#' @return An `arrow` schema
#' @keywords internal
nar_csv_schema <- function(path, types, required = character(0)) {
  header <- readLines(path, n = 1, warn = FALSE)
  if (!length(header) || !nzchar(header)) {
    stop("Could not read a column header from ", path, ".")
  }
  cols <- trimws(strsplit(header, ",", fixed = TRUE)[[1]])
  cols <- gsub('"', "", cols, fixed = TRUE)
  # StatCan writes a UTF-8 BOM; strip whatever precedes the first column name
  # rather than matching the byte sequence, which depends on the locale.
  cols[1] <- sub("^[^A-Za-z_]+", "", cols[1])
  cols <- cols[nzchar(cols)]

  missing <- setdiff(required, cols)
  if (length(missing)) {
    stop("NAR file ", basename(path), " is missing expected column(s): ",
         paste(missing, collapse = ", "), ".")
  }

  fields <- lapply(cols, function(nm) if (is.null(types[[nm]])) arrow::string() else types[[nm]])
  do.call(arrow::schema, stats::setNames(fields, cols))
}
