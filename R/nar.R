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
#' con <- nar_connection()
nar_connection <- function(version="latest", refresh=FALSE) {
  cache_path <- Sys.getenv("NAR_CACHE_PATH")
  if (cache_path == "") {
    stop("Please set the NAR_CACHE_PATH environment variable to a valid directory path.")
  }
  if (!dir.exists(cache_path)) {
    dir.create(cache_path, recursive=TRUE)
  }
  version <- normalized_nar_version(version)
  if (length(version)==0||is.null(version)||nchar(version)==0) {
    stop(paste0("Invalid version specified. Valid versions are: ",
                paste(c("latest",available_nar_versions()$version), collapse=", ")))
  }
  nar_path <- file.path(cache_path, paste0(version,".duckdb"))
  if (!file.exists(nar_path) && !dir.exists(nar_path) || refresh) {
    url <- available_nar_versions() |>
      filter(.data$path == !!version | .data$version == !!version) |>
      pull(url)
    exdir <- getOption("nar_exdir")
    if (is.null(exdir)) {
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

    location_schema <- arrow::schema(
      LOC_GUID = arrow::string(),
      CSD_CODE = arrow::string(),
      FED_CODE = arrow::string(),
      FED_ENG_NAME = arrow::string(),
      FED_FRE_NAME = arrow::string(),
      ER_CODE = arrow::string(),
      ER_ENG_NAME = arrow::string(),
      ER_FRE_NAME = arrow::string(),
      BG_LATITUDE = arrow::float64(),
      BG_LONGITUDE = arrow::float64()
    )

    address_data_paths <- list.files(exdir, pattern="Address_.*\\.csv$", full.names=TRUE, recursive=TRUE)
    location_data_paths <- list.files(exdir, pattern="Location_.*\\.csv$", full.names=TRUE, recursive=TRUE)


    address_arrow <- arrow::open_dataset(address_data_paths,
                                         format = "csv",
                                         skip_rows = 1,
                                         schema = address_schema)
    location_arrow <- arrow::open_dataset(location_data_paths,
                                          format = "csv",
                                          skip_rows = 1,
                                          schema = location_schema)



    if (file.exists(nar_path)) {
      unlink(nar_path,recursive = TRUE)
    }
    con <- DBI::dbConnect(duckdb::duckdb(dbdir=nar_path))
    suppressMessages(duckspatial::ddbs_install(con))
    suppressMessages(duckspatial::ddbs_load(con))
    DBI::dbSendQuery(con, "CREATE OR REPLACE FUNCTION lon_lat(x,y) as (
        st_transform(st_point(x,y), 'OGC:CRS84', 'EPSG:3347')
      );")

    message("Importing address data.")

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

    message("Indexing address data.")

    DBI::dbSendQuery(con, "DROP TABLE AddressesTemp;")
    DBI::dbSendQuery(con, "CREATE INDEX add_geom_idx ON Addresses USING RTREE (geom);")
    DBI::dbSendQuery(con, "CREATE INDEX add_loc_guid_idx ON Addresses (LOC_GUID);")

    message("Importing location data.")
    dplyr::copy_to(con,
                   location_arrow |>
                     arrow::to_duckdb(),
                   name = "LocationsTemp", temporary = TRUE, overwrite = TRUE)

    dplyr::copy_to(con,
                   con |>
                     tbl("LocationsTemp") |>
                     mutate(geom=lon_lat(.data$BG_LONGITUDE,.data$BG_LATITUDE)) |>
                     select(-"BG_LATITUDE", -"BG_LONGITUDE"),
                   name = "Locations", temporary = FALSE, overwrite = TRUE)

    message("Indexing location data.")

    DBI::dbSendQuery(con, "DROP TABLE LocationsTemp;")
    DBI::dbSendQuery(con, "CREATE INDEX loc_geom_idx ON Locations USING RTREE (geom);")
    DBI::dbSendQuery(con, "CREATE INDEX loc_loc_guid_idx ON Locations (LOC_GUID);")



    con |> DBI::dbDisconnect()

    message("NAR data version ",version," successfully imported.")


    # cleanup
    address_arrow <- NULL
    location_arrow <- NULL
    unlink(exdir, recursive=TRUE)
  }

  con <- DBI::dbConnect(duckdb::duckdb(dbdir=nar_path, read_only = TRUE))
  suppressMessages(duckspatial::ddbs_load(con))


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
