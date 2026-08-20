

#' Reverse Geocode Coordinates to Address
#'
#' @description Determines closest address(s) to given coordinates using the NAR dataset based on maximym match radius
#'
#' @param x An `sf` POINT object with coordinates to reverse geocode, or a
#' length-2 numeric vector of longitude and latitude
#' @param crs CRS of `x`, used when `x` is a bare numeric vector or an `sf`
#' object without a CRS. Defaults to EPSG:4326, the longitude/latitude that GPS
#' receivers and web maps report. An `sf` object that carries its own CRS is
#' reprojected from that CRS and this argument is ignored.
#' @param match_radius Maximum distance (in meters) to search for matching addresses (default
#' is 100 meters)
#' @param output Type of output to return. Options are "address" (returns a single formatted address string),
#' "components" (returns a data frame with address components for the closest match), or "multiple" (returns a data frame with all matches within the match radius).
#' Default is "multiple".
#' @param source Source dataset to use for reverse geocoding. Currently only "nar" (National Address Repository) is supported.
#' @param version NAR version to query, passed to [nar_connection()]. Ignored
#' when \code{con} is supplied.
#' @param con An open NAR connection to reuse. Supplying one avoids reopening
#' the database on every call, which matters when reverse geocoding many points
#' in a loop. The caller keeps ownership: a connection passed in here is left
#' open, while one opened internally is closed again before returning.
#' @param geometry Logical, whether to return the result as an \code{sf} object with the
#' matched address point geometry. Default is \code{FALSE}.
#' @param ... Additional arguments (currently unused)
#' @return Depending on the `output` parameter, either a single address string, a data frame with address components for the closest match, or a data frame with all matches within the match radius.
#'
#' @section Match precision: Results carry a `geom_source` column naming the
#' point each match was measured from. `"building"` is NAR's building
#' representative point, specific to the address. `"blockface"` is the centroid
#' of one side of a street between two intersections, used for the ~7% of
#' addresses that have no building point; it is shared by every address on that
#' blockface -- a median of 2, a mean of 3.9, and as many as 578 -- so those
#' rows are a street-segment approximation and their `dist` is not comparable
#' to a building match. Filter on `geom_source` when only precise matches will do.
#' Databases built before schema version 3 have no blockface fallback and no
#' `geom_source` column.
#' @export
#' @examples
#' \dontrun{
#' library(sf)
#' # Create an sf POINT object with coordinates to reverse geocode
#' point <- st_sfc(st_point(c(-75.6972, 45.4215)), crs = 4326)
#' # Reverse geocode the point to get the closest address
#' address <- reverse_geocode(point, match_radius = 200, output = "address")
#' print(address)
#' }

reverse_geocode <- function(x, match_radius = 100, output = "multiple", source = "nar", geometry = FALSE, crs = 4326, version = "latest", con = NULL, ...) {
  source <- match.arg(
    source,
    choices = c("nar")
  )
  if (length(source)!=1) {
    stop("Please specify exactly one valid source.")
  }
  output <- match.arg(
    output,
    choices = c("address","components","multiple")
  )
  if (length(output)!=1) {
    stop("Please specify exactly one valid output type.")
  }
  if (source == "nar") {
    if (is.null(con)) {
      con <- nar_connection(version = version)
      on.exit(DBI::dbDisconnect(con), add = TRUE)
    }

    # Reproject straight to the CRS the addresses are stored in. Going via
    # lon/lat would transform twice and pin the intermediate step to a datum the
    # caller never asked for.
    xy <- nar_project(x, crs = crs, storage_crs = nar_crs(con))

    matches <- con |>
      tbl("Addresses") |>
      nar_within_radius(xy[1], xy[2], match_radius)

    if (geometry) {
      results <- matches |> collect_nar()
      results <- results[order(results$dist), , drop = FALSE]
      geom_col <- sf::st_geometry(results)
      results <- sf::st_drop_geometry(results)
    } else {
      results <- matches |>
        select(-"geom", -dplyr::any_of(c("x", "y"))) |>
        collect() |>
        arrange(.data$dist)
      geom_col <- NULL
    }

  } else {
    stop("Unsupported source. Please use 'nar'.")
  }

  if (nrow(results) == 0) {
    match_radius_string <- ifelse(match_radius>=1000,
                                  paste0(match_radius/1000," km"),
                                  paste0(match_radius," m"))
    warning(paste0("No address found within ",match_radius_string," radius."))
    return(NULL)
  }

  # Built column-wise rather than with rowwise(): the query itself stays flat as
  # the radius grows, but row-by-row formatting did not -- 27k matches spent
  # ~2.4s here against ~0.06s in the database.
  results <- results |>
    mutate(across(where(is.character),\(x)ifelse(x=="",NA_character_,x))) |>
    mutate(address=paste0(ifelse(is.na(.data$APT_NO_LABEL), "",
                                 paste0(.data$APT_NO_LABEL, "-")),
                          nar_paste_parts(.data$CIVIC_NO, .data$CIVIC_NO_SUFFIX,
                                          .data$MAIL_STREET_DIR, .data$MAIL_STREET_NAME,
                                          .data$MAIL_STREET_TYPE),
                          ", ",.data$MAIL_MUN_NAME," ",.data$MAIL_POSTAL_CODE),
           .after="ADDR_GUID")

  if (!is.null(geom_col)) {
    results <- sf::st_sf(results, geometry = geom_col)
  }

  if (output == "address") {
    return(results$address[1])
  } else if (output == "components") {
    return(results[1,])
  } else if (output == "multiple") {
    return(results)
  } else {
    stop("Unsupported output type. Please use 'address' or 'components'.")
  }
}


#' Join the address parts that are present with single spaces
#'
#' @description The vectorised equivalent of
#' `paste(na.omit(c(...)), collapse = " ")` applied per row, returning `""` when
#' every part is missing. Interior spacing inside a part is preserved, so a
#' street name is never reflowed.
#' @param ... Equal-length vectors of address components
#' @return A character vector with no missing values
#' @keywords internal
nar_paste_parts <- function(...) {
  parts <- lapply(list(...), as.character)
  joined <- Reduce(function(acc, part) {
    dplyr::case_when(is.na(acc) & is.na(part) ~ NA_character_,
                     is.na(acc) ~ part,
                     is.na(part) ~ acc,
                     .default = paste(acc, part))
  }, parts)
  ifelse(is.na(joined), "", joined)
}
