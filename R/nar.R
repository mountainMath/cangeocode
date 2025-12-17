# Get NAR data
#
#
#
#' Get NAR data
#' @description This function downloads the NAR data if necessary and returns a connection the NAR database
#' @param version Version of the NAR database to connect to. Default is "latest".
#' @param format Format of the database. Currently only "duckdb" and "parquet" are supported.
#' @param refresh Logical indicating whether to refresh the local cache of the NAR database.
#' @return A connection to the NAR database
#' @export
#' @examples
#' con <- nar_connection()
nar_connection <- function(version="latest", format=c("duckdb","parquet"), refresh=FALSE) {
  cache_path <- Sys.getenv("NAR_CACHE_PATH")
  if (cache_path == "") {
    stop("Please set the NAR_CACHE_PATH environment variable to a valid directory path.")
  }
  if (!dir.exists(cache_path)) {
    dir.create(cache_path, recursive=TRUE)
  }
  version <- normalized_nar_version(version)
  if (is.null(version)||nchar(version)==0) {
    stop(paste0("Invalid version specified. Valid versions are: ",
                paste(c("latest",available_nar_versions()$version), collapse=", ")))
  }
  format <- match.arg(format,c("duckdb","parquet"))
  nar_path <- file.path(cache_path, paste0(version,".",format))
  if (!file.exists(nar_path) && !dir.exists(nar_path) || refresh) {
    url <- available_nar_versions() |>
      filter(.data$path == !!version | .data$version == !!version) |>
      pull(url)
    tmp <- tempfile(fileext = ".zip")
    to <- options("timeout")
    # set timeout to 20 minutes if it's less than that, StatCan connection can be very slow
    options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
    utils::download.file(url, tmp, mode="wb")
    options(timeout = to)

    exdir <- file.path(tempdir(),"nar_extract")
    utils::unzip(tmp, exdir=exdir)

    address_schema <- arrow::schema(
      LOC_GUID = arrow::string(),
      ADDR_GUID = arrow::string(),
      APT_NO_LABEL = arrow::string(),
      CIVIC_NO = arrow::int64(),
      CIVIC_NO_SUFFIX = arrow::string(),
      OFFICIAL_STREET_NAME = arrow::string(),
      OFFICIAL_STREET_TYPE = arrow::string(),
      OFFICIAL_STREET_DIR = arrow::string(),
      PROV_CODE = arrow::string(),
      CSD_ENG_NAME = arrow::string(),
      CSD_FRE_NAME = arrow::string(),
      CSD_TYPE_ENG_CODE = arrow::string(),
      CSD_TYPE_FRE_CODE = arrow::string(),
      MAIL_STREET_NAME = arrow::string(),
      MAIL_STREET_TYPE = arrow::string(),
      MAIL_STREET_DIR = arrow::string(),
      MAIL_MUN_NAME = arrow::string(),
      MAIL_PROV_ABVN = arrow::string(),
      MAIL_POSTAL_CODE = arrow::string(),
      BG_DLS_LSD = arrow::string(),
      BG_DLS_QTR = arrow::string(),
      BG_DLS_SCTN = arrow::string(),
      BG_DLS_TWNSHP = arrow::string(),
      BG_DLS_RNG = arrow::string(),
      BG_DLS_MRD = arrow::string(),
      BG_X = arrow::float64(),
      BG_Y = arrow::float64(),
      BU_N_CIVIC_ADD = arrow::string(),
      BU_USE = arrow::int64()
    )

    address_data_paths <- list.files(exdir, pattern="Address_.*\\.csv$", full.names=TRUE, recursive=TRUE)
    location_data_paths <- list.files(exdir, pattern="Location_.*\\.csv$", full.names=TRUE, recursive=TRUE)


    address_arrow <- arrow::open_dataset(address_data_paths,
                                        format = "csv",
                                        skip_rows = 1,
                                        schema = address_schema)
    if (format == "parquet") {
      arrow::write_dataset(address_arrow,
                           path = nar_path,
                           format = "parquet",
                           existing_data_behavior = "overwrite")
    } else if (format == "duckdb") {
      con <- DBI::dbConnect(duckdb::duckdb(dbdir=nar_path))
      duckspatial::ddbs_install(con)
      duckspatial::ddbs_load(con)
      dplyr::copy_to(con,
                     address_arrow |>
                       arrow::to_duckdb(),
                     name = "AddressesTemp", temporary = TRUE, overwrite = TRUE)

      dplyr::copy_to(con,
                     con |>
                       tbl("AddressesTemp") |>
                       mutate(geom=st_point(.data$BG_X,.data$BG_Y)) |>
                       select(-"BG_X", -"BG_Y"),
                     name = "Addresses", temporary = FALSE, overwrite = TRUE)

      DBI::dbSendQuery(con, "CREATE INDEX geom_idx ON Addresses USING RTREE (geom);")
      DBI::dbSendQuery(con, "DROP TABLE AddressesTemp;")
      DBI::dbSendQuery(con, "CREATE OR REPLACE FUNCTION lat_lon(x,y) as (
        st_transform(st_point(x,y), 'OGC:CRS84', 'EPSG:3347')
);")
      con |> DBI::dbDisconnect()
    }


    # cleanup
    address_arrow <- NULL
    unlink(tmp)
    unlink(exdir, recursive=TRUE)
  }
  if (format == "parquet") {
    con <- arrow::open_dataset(nar_path, format="parquet")
  } else if (format == "duckdb") {
    con <- DBI::dbConnect(duckdb::duckdb(dbdir=nar_path, read_only = TRUE))
    duckspatial::ddbs_load(con)
    con <- con |>
      tbl("Addresses")
  }

  return(con)
}


#' Scrape availabe NAR versions from the StatCan website
#' @param refresh Logical indicating whether to refresh the cached version list
#' @return A tibble with available NAR versions and their URLs
#' @export
#' @examples
#' versions <- available_nar_versions()
available_nar_versions <- function(refresh=FALSE){
  version_cache_path <- file.path(tempdir(), "nar_versions.csv")
  if (refresh || !file.exists(version_cache_path)) {
    overview_url <- "https://www150.statcan.gc.ca/n1/pub/46-26-0002/462600022022001-eng.htm"
    page <- xml2::read_html(overview_url)
    links <- rvest::html_nodes(page, "section div p a")
    versions <- tibble(version=rvest::html_text(links),
           url=rvest::html_attr(links, "href")) |>
      filter(grepl("\\.zip$", url)) |>
      mutate(url=file.path(dirname(overview_url), .data$url)) |>
      mutate(Date=as.Date(case_when(grepl("^\\d{4}$",.data$version) ~ paste0("01 December ",.data$version),
                             grepl("^[A-Z][a-z]+ \\d{4}$",.data$version) ~ paste0("01 ",.data$version),
                             TRUE ~ .data$version),
                          format="%d %B %Y")) |>
      mutate(path=strftime(.data$Date,"%Y-%m")) |>
      arrange(desc(.data$Date))
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

#' Normalize NAR version string
#' @param version Version of the NAR database to connect to. Default is "latest
#' @param refresh Logical indicating whether to refresh the cached version list
#' @return Normalized version string
#' @keywords internal
normalized_nar_version <- function(version, refresh=FALSE) {
  available_versions <- available_nar_versions()

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

#' Collect sf object from a car connection
#' @param tbl nar table to collect
#' @return An sf object
#' @export
#' @examples
#' con <- nar_connection()
#' nar_sf <- con |>
#'   head(20) |>
#'   collect_nar()
collect_nar <- function(tbl) {
  uses2 <- sf::sf_use_s2()
  suppressMessages(sf::sf_use_s2(FALSE))
  result <- tbl |>
    mutate(geom=st_astext(.data$geom)) |>
    collect() |>
    mutate(geom=ifelse(is.na(.data$geom),"POINT EMPTY",.data$geom)) |>
    sf::st_as_sf(wkt="geom",crs="EPSG:3347")

  if (uses2) {
    suppressMessages(sf::sf_use_s2(uses2))
  }

  result
}
