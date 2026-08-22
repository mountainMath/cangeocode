#' @import dplyr
#' @importFrom rlang .data %||%
#' @importFrom stats setNames runif

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

# The lexicon tables in R/sysdata.rda, built by data-raw/build_lexicons.R and
# reached by name from the normalizer.
utils::globalVariables(c("nar_lex_types", "nar_lex_dirs", "nar_lex_prov",
                         "nar_prov_lang", "nar_lex_unit_words",
                         "nar_lex_unit_bare"))
