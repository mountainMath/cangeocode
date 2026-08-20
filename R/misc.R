#' @import dplyr
#' @importFrom rlang .data
#' @importFrom stats setNames

# silence warning that dbplyer is not used (explicitly)
ignore_unused_imports <- function(){
  dbplyr::sql(NULL)
}

NULL

## quiets concerns of R CMD check re: the .'s that appear in pipelines
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))

# Names resolved by DuckDB inside dplyr pipelines: the spatial extension's own
# functions, and the nar_* macros registered by nar_register_spatial().
utils::globalVariables(c("st_distance", "st_dwithin", "st_point", "st_transform",
                         "st_x", "st_y",
                         "nar_point", "nar_xy", "nar_geom", "nar_store", "nar_wkb",
                         "nar_lon", "nar_lat"))
