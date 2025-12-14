#' @import dplyr
#' @importFrom rlang .data
#' @importFrom stats na.omit

# silence warning that dbplyer is not used (explicitly)
ignore_unused_imports <- function(){
  dbplyr::sql(NULL)
}

NULL

## quiets concerns of R CMD check re: the .'s that appear in pipelines
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))

utils::globalVariables(c("lat_lon", "st_distance", "st_point","st_astext"))
