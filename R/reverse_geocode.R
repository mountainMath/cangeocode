

#' Reverse Geocode Coordinates to Address
#'
#' @description Determines closest address(s) to given coordinates using the NAR dataset based on maximym match radius
#'
#' @param x An `sf` POINT object with coordinates to reverse geocode
#' @param match_radius Maximum distance (in meters) to search for matching addresses (default
#' is 100 meters)
#' @param output Type of output to return. Options are "address" (returns a single formatted address string),
#' "components" (returns a data frame with address components for the closest match), or "multiple" (returns a data frame with all matches within the match radius).
#' Default is "multiple".
#' @param source Source dataset to use for reverse geocoding. Currently only "nar" (National Address Repository) is supported.
#' @param ... Additional arguments (currently unused)
#' @return Depending on the `output` parameter, either a single address string, a data frame with address components for the closest match, or a data frame with all matches within the match radius.
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

reverse_geocode <- function(x, match_radius = 100, output = "multiple", source = "nar", ...) {
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
    if (inherits(x, "sf") || inherits(x, "sfc")) {
      crs <- sf::st_crs(x)
      if (!is.na(crs) && crs$epsg != 4326) {
        x <- sf::st_transform(x, 4326)
      }
      xx <- sf::st_coordinates(x)
    } else if (inherits(x, "numeric") && length(x) == 2) {
      xx <- x
    } else {
      stop("Input x must be an sf POINT object or a vector with lon/lat.")
    }
    x_coord <- xx[1]
    y_coord <- xx[2]
    con <- nar_connection()

    results <- con |>
      filter(st_distance(.data$geom,lat_lon(x_coord,y_coord))<=match_radius) |>
      mutate(dist=st_distance(.data$geom,lat_lon(x_coord,y_coord))) |>
      arrange(.data$dist) |>
      collect()

    con[["src"]][["con"]] |> DBI::dbDisconnect()

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

  results <- results |>
    mutate(across(everything(),\(x)ifelse(x=="",NA_character_,x))) |>
    rowwise() |>
    mutate(address1=ifelse(is.na(.data$APT_NO_LABEL),
                          "",paste0(.data$APT_NO_LABEL,"-"))) |>
    mutate(address2=paste0(na.omit(c(.data$CIVIC_NO,.data$CIVIC_NO_SUFFIX,
                                     .data$MAIL_STREET_DIR, .data$MAIL_STREET_NAME, .data$MAIL_STREET_TYPE)),
                           collapse=" ")) |>
    mutate(address=paste0(.data$address1,.data$address2,", ",.data$MAIL_MUN_NAME," ",.data$MAIL_POSTAL_CODE),
           .after="ADDR_GUID") |>
    select(-matches("address\\d+"))

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
