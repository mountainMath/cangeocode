#' @import dplyr
#' @importFrom rlang .data

# silence warning that dbplyer is not used (explicitly)
ignore_unused_imports <- function(){
  dbplyr::sql(NULL)
}

NULL

## quiets concerns of R CMD check re: the .'s that appear in pipelines
if(getRversion() >= "2.15.1")  utils::globalVariables(c("."))
