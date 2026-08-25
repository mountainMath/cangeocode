#' Sort Canadian address strings into structural buckets
#'
#' @description Reports the shape each address parsed as, without returning the
#' parse itself. The point is triage: run it over a column of addresses and the
#' buckets separate the ordinary cases from the ones that need attention --
#' `po_box` and `rural_route` will never match NAR whatever you do to them,
#' `unparsed` and `street_only` are the rows to look at by hand, and the
#' regional forms tell you which conventions your data actually contains.
#'
#' This is the same value [normalize_address()] returns in its `pattern`
#' column; use that instead when you want the components too.
#'
#' @inheritParams normalize_address
#' @return A factor, one element per element of `x`, with these levels in this
#' order -- each address gets the first one that applies:
#' \describe{
#'   \item{`po_box`}{A post office box, `case postale`, or general delivery.}
#'   \item{`rural_route`}{An `RR`/`SS` rural route, with or without site and
#'     compartment. Like `po_box`, a delivery instruction rather than a place,
#'     and absent from NAR entirely.}
#'   \item{`intersection`}{Two streets joined by `&` rather than a civic number.}
#'   \item{`numbered_road`}{A typeless numbered rural road: the prairie
#'     `Range Road 272` and `Township Road 514`, New Brunswick's `Route 105`,
#'     Ontario concessions and county roads.}
#'   \item{`grid`}{A numbered street with a quadrant, the Calgary and Edmonton
#'     convention -- `96A Street NW`.}
#'   \item{`numeric_street`}{A numbered street with no quadrant: `25th Ave`,
#'     `Line 5`.}
#'   \item{`french_street`}{The French order, with the type leading the name:
#'     `rue de Vernon`, `boulevard du President-Kennedy`.}
#'   \item{`unit_civic`}{An ordinary street address carrying a unit.}
#'   \item{`civic_street`}{An ordinary street address.}
#'   \item{`street_only`}{A street with no civic number.}
#'   \item{`postal_only`}{Nothing but a postal code.}
#'   \item{`unparsed`}{Empty, or nothing the rules recognised.}
#' }
#'
#' @seealso [normalize_address()], which produces this alongside the parsed
#' components.
#' @export
#' @examples
#' address_pattern(c("53222 Range Road 272, Spruce Grove, AB",
#'                   "9819 96A Street NW, Edmonton, AB",
#'                   "845, rue de Vernon, Gatineau, QC",
#'                   "PO Box 40, Iqaluit, NU"))
address_pattern <- function(x, known = NULL, ...) {
  if (!is.character(x)) {
    if (is.factor(x)) x <- as.character(x) else
      stop("`x` must be a character vector of address strings.")
  }
  # Only the province reaches the rules parse. The rest of `known` names
  # components rather than changing how the string is read, and a pattern is a
  # statement about the string.
  nar_parse_rules(x, prov = nar_known(known, length(x))$PROV_ABVN)$pattern
}

#' The address patterns the recognizer sorts input into
#'
#' @description The bucket names, in the priority order [nar_address_pattern()]
#' applies them. Exposed as a function rather than a constant so the factor
#' levels and the documentation cannot drift apart.
#' @return A character vector of pattern names
#' @keywords internal
nar_address_patterns <- function() {
  c("po_box", "rural_route", "intersection", "numbered_road", "grid",
    "numeric_street", "french_street", "unit_civic", "civic_street",
    "street_only", "postal_only", "unparsed")
}

#' Flag the address forms NAR has no civic address for
#'
#' @description Post office boxes and rural routes are delivery instructions,
#' not locations, and NAR contains neither. Recognizing them is worth more than
#' parsing them: it separates "this address is wrong" from "this address was
#' never going to be in the gazetteer", which are very different problems for
#' whoever is looking at the output.
#' @param txt A character vector of normalized address strings
#' @return A character vector of `"po_box"`, `"rural_route"` or `NA`
#' @keywords internal
nar_delivery_marks <- function(txt) {
  # BOX has to be anchored to the start of the string or of a comma segment and
  # followed by a number: "Box 40" is a PO box, "Box Grove Bypass" is a street
  # in Markham. CP and BP are the French abbreviations.
  box <- "(^|, )(P ?O |POST OFFICE )?BOX [0-9]|CASE POSTALE|GENERAL DELIVERY|POSTE RESTANTE|(^|, )(CP|BP) [0-9]"
  # RR 3, Rural Route 3, SS 1 (suburban service), and the Site/Comp pair that
  # goes with them.
  rr <- "(^|, )R ?R ?[0-9]|RURAL ROUTE|(^|, )S ?S ?[0-9]|(^|, )SITE [0-9A-Z]+ ?, ?(COMP|BOX)"
  out <- rep(NA_character_, length(txt))
  out[!is.na(txt) & grepl(rr, txt)]  <- "rural_route"
  out[!is.na(txt) & grepl(box, txt)] <- "po_box"
  out
}

#' Sort parsed addresses into buckets by the shape they parsed as
#'
#' @description Assigns each row exactly one pattern, testing in the order
#' [nar_address_patterns()] lists. The order is what makes the buckets useful:
#' the regional and structural quirks are checked before the ordinary forms, so
#' `grid` and `french_street` describe the addresses that are actually unusual
#' rather than being swamped by the `civic_street` majority they overlap with.
#' A row therefore reports the most specific thing true of it, and only that.
#' @param res A tibble from [nar_parse_rules()]
#' @param traits A character vector of comma-joined parse traits, one per row
#' @param marks A character vector from [nar_delivery_marks()]
#' @return A factor with the levels [nar_address_patterns()] gives
#' @keywords internal
nar_address_pattern <- function(res, traits, marks) {
  has <- function(t) grepl(t, traits, fixed = TRUE)
  nm  <- res$STREET_NAME
  # A street name that is only digits, optionally with an ordinal or a letter:
  # 96A, 25TH, 1RE. These are numbered grid streets, not names.
  numeric_name <- !is.na(nm) & grepl("^[0-9]+[A-Z]{0,2}$", nm)
  quadrant <- !is.na(res$STREET_DIR) &
    res$STREET_DIR %in% c("NE", "NW", "SE", "SW", "NO", "SO")

  out <- rep("unparsed", nrow(res))
  # Assigned last-to-first so the earlier, more specific tests overwrite the
  # later ones -- the priority in nar_address_patterns() read backwards.
  out[!is.na(res$POSTAL_CODE)]                      <- "postal_only"
  out[!is.na(nm)]                                   <- "street_only"
  out[!is.na(nm) & !is.na(res$CIVIC_NO)]            <- "civic_street"
  out[!is.na(nm) & !is.na(res$APT_NO_LABEL)]        <- "unit_civic"
  out[has("type_leads")]                            <- "french_street"
  out[numeric_name & !quadrant]                     <- "numeric_street"
  out[numeric_name & quadrant]                      <- "grid"
  out[has("numbered_road")]                         <- "numbered_road"
  out[has("intersection")]                          <- "intersection"
  out[!is.na(marks)]                                <- marks[!is.na(marks)]

  factor(out, levels = nar_address_patterns())
}
