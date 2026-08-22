# Provinces: the vocabulary a partial NAR import is described in.

#' Province and territory crosswalk
#'
#' @description The three ways a province is named in this package, in one
#' place. `code` is the Standard Geographical Classification two-digit
#' identifier, which is what NAR's `PROV_CODE` column holds **and what the
#' member files inside the StatCan bulk zip are named by** -- `Address_59.csv`
#' is British Columbia. `abvn` is the two-letter abbreviation NAR carries in
#' `MAIL_PROV_ABVN`, which is also what [normalize_address()] and [geocode()]
#' speak. `name` is for messages and prompts.
#'
#' The pairing was verified against the 2026-06 release rather than assumed:
#' every `PROV_CODE` in `Addresses` maps to exactly one `MAIL_PROV_ABVN`.
#' @return A data frame with `code`, `abvn` and `name`
#' @keywords internal
nar_province_table <- function() {
  data.frame(
    code = c("10", "11", "12", "13", "24", "35", "46", "47", "48", "59",
             "60", "61", "62"),
    abvn = c("NL", "PE", "NS", "NB", "QC", "ON", "MB", "SK", "AB", "BC",
             "YT", "NT", "NU"),
    name = c("Newfoundland and Labrador", "Prince Edward Island", "Nova Scotia",
             "New Brunswick", "Quebec", "Ontario", "Manitoba", "Saskatchewan",
             "Alberta", "British Columbia", "Yukon", "Northwest Territories",
             "Nunavut"),
    stringsAsFactors = FALSE
  )
}

#' The value that marks a database as holding the whole country
#'
#' @description Kept as a constant rather than spelled out at each comparison,
#' because it is written into `nar_metadata` and read back by
#' [nar_coverage()]; the two have to agree exactly.
#' @return A character scalar
#' @keywords internal
nar_all_provinces <- function() "ALL"

#' Resolve however a caller named a province to the canonical abbreviation
#'
#' @description Accepts the two-letter abbreviation, the two-digit SGC code
#' (as a string or a number), or the full name, in any case, and returns the
#' abbreviation. `"ALL"`, `"all"`, `"national"` and `"canada"` all resolve to
#' the whole-country marker.
#'
#' Unrecognized input is an error listing what is available rather than a
#' silent drop: a typo'd province would otherwise produce a database that is
#' quietly missing the data the caller asked for, and the mistake would only
#' surface later as unmatched addresses.
#' @param x Character or numeric vector of province identifiers, or `NULL`
#' @return A character vector of two-letter abbreviations, or the single value
#'   `"ALL"`; `NULL` passes through unchanged
#' @keywords internal
nar_normalize_provinces <- function(x) {
  if (is.null(x)) return(NULL)
  x <- trimws(as.character(x))
  if (!length(x)) return(NULL)

  if (any(toupper(x) %in% c("ALL", "NATIONAL", "CANADA", "CA"))) {
    if (length(x) > 1) {
      stop("`provinces` names the whole country and individual provinces at ",
           "the same time. Ask for one or the other.")
    }
    return(nar_all_provinces())
  }

  tbl <- nar_province_table()
  up <- toupper(x)
  hit <- match(up, tbl$abvn)
  hit[is.na(hit)] <- match(sub("^0", "", up[is.na(hit)]),
                           sub("^0", "", tbl$code))
  hit[is.na(hit)] <- match(up[is.na(hit)], toupper(tbl$name))

  if (anyNA(hit)) {
    stop("Unrecognized province: ", paste(x[is.na(hit)], collapse = ", "),
         ".\nUse a two-letter abbreviation (",
         paste(tbl$abvn, collapse = ", "), "), an SGC code, or \"all\".")
  }
  unique(tbl$abvn[hit])
}

#' Provinces a NAR database holds
#'
#' @description Reads the `provinces` metadata key. Databases built before
#' schema version 6 have no such key and were necessarily national, so the
#' fallback is the whole country -- the same pattern [nar_crs()] uses for
#' databases predating the `crs` key.
#' @param con A DuckDB connection
#' @return `"ALL"`, or a character vector of two-letter abbreviations
#' @keywords internal
nar_coverage <- function(con) {
  value <- nar_meta_value(con, "provinces", nar_all_provinces())
  if (identical(value, nar_all_provinces())) return(nar_all_provinces())
  trimws(strsplit(value, ",", fixed = TRUE)[[1]])
}

#' Render a coverage set for a message
#'
#' @param provinces `"ALL"` or a character vector of abbreviations
#' @return A character scalar
#' @keywords internal
nar_coverage_label <- function(provinces) {
  if (identical(provinces, nar_all_provinces())) return("all provinces")
  paste(sort(provinces), collapse = ", ")
}

#' Provinces a NAR database holds, as a user-facing value
#'
#' @description The public read of what [nar_connection()] actually downloaded.
#' A national database reports every province rather than the internal `"ALL"`
#' marker, so the return value can be compared against a `PROV_ABVN` column
#' without special-casing.
#' @param con A NAR connection, as returned by [nar_connection()]
#' @return A character vector of two-letter province abbreviations
#' @export
#' @examples
#' \dontrun{
#' con <- nar_connection(provinces = "BC")
#' nar_provinces(con)
#' #> [1] "BC"
#' }
nar_provinces <- function(con) {
  have <- nar_coverage(con)
  if (identical(have, nar_all_provinces())) return(nar_province_table()$abvn)
  sort(have)
}
