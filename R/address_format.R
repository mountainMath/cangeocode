# Rendering parsed components back into a string.
#
# normalize_address() takes an address apart; these two put it back together --
# once for a machine to join on (address_key), once for a person to read
# (format_address). Both accept either a normalize_address() result or the raw
# strings, so a caller who only wants the string never has to name the columns.

#' Build a match key from parsed address components
#'
#' @description Collapses a parsed address into a single string that two
#' spellings of the same address share, which is what joining or deduplicating
#' two address lists needs. Components are folded to an accent- and
#' case-insensitive form and punctuation is dropped, so `St. John's` and
#' `SAINT JOHNS` key alike, and the fields are laid out broad to narrow --
#' province, municipality, street, civic number -- so sorting the keys clusters
#' a street together.
#'
#' Normalizing is what does the real work here; the key only makes the result
#' joinable. Pass `con` (or normalize with it first) whenever the input is
#' messy: the gazetteer is what turns a misspelled street into NAR's own
#' spelling, and two lists cannot key alike on a name only one of them got
#' right.
#'
#' @param x Either a data frame of parsed components, as returned by
#' [normalize_address()], or a character vector of address strings to normalize
#' first.
#' @param unit Include the unit number in the key? The default `FALSE` keys a
#' *building*, so every suite in a tower collapses to one key. Set it to `TRUE`
#' to key a tenant instead -- at the cost that the unit is the least reliably
#' parsed component, so rows that agree on everything else will split whenever
#' one of them wrote its suite somewhere the parser did not find it.
#' @param sep The separator between fields. It only has to be a character the
#' components cannot contain; the default is fine unless a downstream tool
#' treats `|` specially.
#' @param prov,con Passed to [normalize_address()], and only allowed when `x`
#' is a character vector -- a data frame has already been parsed.
#'
#' @return A character vector, one element per row of `x`, and `NA` for any row
#' with no street name. Those are rows nothing could be keyed from, and they are
#' `NA` rather than an empty key so they cannot all collapse onto each other.
#' Note that `dplyr`'s joins match `NA` to `NA` by default, so filter them out
#' or pass `na_matches = "never"`.
#'
#' @seealso [format_address()] for the human-readable form,
#' [normalize_address()] for the components themselves.
#' @export
#' @examples
#' # Five spellings of one building, one key.
#' address_key(c("1055 W Georgia St, Vancouver, BC",
#'               "1055 West Georgia Street, Vancouver, British Columbia",
#'               "Suite 1500 - 1055 W Georgia St, Vancouver BC",
#'               "#1500-1055 west georgia st., vancouver, b.c.",
#'               "1055 WEST GEORGIA ST, VANCOUVER, BC V6E 3P3"))
#'
#' # Keying the tenant instead of the building keeps the suites apart.
#' address_key(c("Suite 1500 - 1055 W Georgia St, Vancouver BC",
#'               "Suite 800 - 1055 W Georgia St, Vancouver BC"), unit = TRUE)
address_key <- function(x, unit = FALSE, sep = "|", prov = NULL, con = NULL) {
  parts <- nar_as_components(x, prov = prov, con = con)

  fields <- c("PROV_ABVN", "MUN_NAME", "STREET_NAME", "STREET_TYPE",
              "STREET_DIR", "CIVIC_NO", "CIVIC_NO_SUFFIX")
  if (unit) fields <- c(fields, "APT_NO_LABEL")

  folded <- lapply(parts[fields], function(f) {
    f <- nar_key_fold(f)
    ifelse(is.na(f), "", f)
  })
  key <- do.call(paste, c(folded, list(sep = sep)))

  # Nothing to key on is not the same as an empty key: without this every
  # unparseable row would join to every other unparseable row.
  key[is.na(parts$STREET_NAME) | !nzchar(nar_key_fold(parts$STREET_NAME))] <- NA_character_
  key
}

#' Render parsed address components back into one line
#'
#' @description The readable counterpart to [address_key()]: the canonical
#' components written out the way an address is written, with the unit
#' hyphenated onto the civic number and the postal code spaced. Use it to show
#' what a parse actually resolved to, or to write a cleaned address column back
#' out.
#'
#' The street type is placed by language, not by province: French types lead the
#' name (`123 Rue Notre-Dame E`) and English types follow it (`123 Main St W`),
#' so a `Rue` in Ottawa still reads correctly.
#'
#' Component *case* is left exactly as parsed, which for a gazetteer-resolved
#' row means NAR's own convention: street names in mixed case (`Burrard`,
#' `McTavish`), types and directions in capitals (`ST`, `NW`), municipalities in
#' capitals. That mix is deliberate rather than an oversight -- re-casing a name
#' would fight capitals NAR is careful about, and NAR is the authority the rest
#' of this package defers to. A rules-only row has nothing to defer to and comes
#' back upper case throughout.
#'
#' @inheritParams address_key
#' @return A character vector, one element per row of `x`, and `NA` for a row
#' with no components at all.
#'
#' @seealso [address_key()] for the joinable form.
#' @export
#' @examples
#' format_address(c("302-1055 w georgia st, vancouver bc v6e3p3",
#'                  "12 1/2 rue notre-dame e, montreal, quebec",
#'                  "100 queen street west, toronto, ontario"))
format_address <- function(x, prov = NULL, con = NULL) {
  parts <- nar_as_components(x, prov = prov, con = con)
  blank <- function(v) ifelse(is.na(v), "", v)

  # `990A` is glued, `12 1/2` is not: a suffix carrying punctuation is a
  # fraction, and running it onto the number reads as `121/2`.
  suffix <- blank(parts$CIVIC_NO_SUFFIX)
  glue <- ifelse(grepl("[^A-Za-z0-9]", suffix), " ", "")
  civic <- trimws(paste0(blank(parts$CIVIC_NO), glue, suffix))
  civic <- ifelse(nzchar(civic) & !is.na(parts$APT_NO_LABEL),
                  paste0(parts$APT_NO_LABEL, "-", civic),
                  ifelse(nzchar(civic), civic, blank(parts$APT_NO_LABEL)))

  street <- ifelse(nar_type_leads(parts$STREET_TYPE),
                   nar_paste_parts(parts$STREET_TYPE, parts$STREET_NAME,
                                   parts$STREET_DIR),
                   nar_paste_parts(parts$STREET_NAME, parts$STREET_TYPE,
                                   parts$STREET_DIR))

  pc <- parts$POSTAL_CODE
  pc <- ifelse(!is.na(pc) & nchar(pc) == 6,
               paste0(substr(pc, 1, 3), " ", substr(pc, 4, 6)), blank(pc))

  segments <- cbind(nar_paste_parts(nar_blank_na(civic), nar_blank_na(street)),
                    blank(parts$MUN_NAME),
                    nar_paste_parts(parts$PROV_ABVN, nar_blank_na(pc)))
  out <- apply(segments, 1, function(s) paste(s[nzchar(s)], collapse = ", "))
  ifelse(nzchar(out), out, NA_character_)
}

#' Resolve either input form to a frame of address components
#'
#' @description [address_key()] and [format_address()] both take a
#' [normalize_address()] result *or* the strings it would be given, so that a
#' caller who only wants the output string never has to know the column names.
#' A data frame has already been parsed, which is why `prov` and `con` are
#' refused for one -- silently ignoring them would drop a constraint the caller
#' asked for.
#' @param x A data frame of components or a character vector of addresses
#' @inheritParams normalize_address
#' @return A data frame carrying the component columns
#' @keywords internal
nar_as_components <- function(x, prov = NULL, con = NULL) {
  needed <- c("APT_NO_LABEL", "CIVIC_NO", "CIVIC_NO_SUFFIX", "STREET_NAME",
              "STREET_TYPE", "STREET_DIR", "MUN_NAME", "PROV_ABVN",
              "POSTAL_CODE")

  if (is.data.frame(x)) {
    if (!is.null(prov) || !is.null(con)) {
      stop("`prov` and `con` only apply when `x` is a character vector of ",
           "addresses; `x` is already parsed. Pass them to ",
           "normalize_address() instead.")
    }
    missing <- setdiff(needed, names(x))
    if (length(missing)) {
      stop("`x` is missing the component column(s) ",
           paste0("`", missing, "`", collapse = ", "),
           ". A data frame here should come from normalize_address().")
    }
    out <- as.data.frame(x[needed])
    out[] <- lapply(out, as.character)
    return(out)
  }

  if (is.factor(x)) x <- as.character(x)
  if (!is.character(x)) {
    stop("`x` must be a character vector of address strings, or a data frame ",
         "of components from normalize_address().")
  }
  as.data.frame(normalize_address(x, prov = prov, con = con)[needed])
}

#' Fold a component to a match key
#'
#' @description [nar_fold()] handles case and accents; this drops what is left.
#' Periods and apostrophes vanish outright, because NAR keeps them in
#' municipality names and the parser does not (`ST. JOHN'S` against
#' `ST JOHNS`); every other separator becomes a space, so a hyphenated
#' `NOTRE-DAME` keys the same as the spaced spelling rather than as
#' `NOTREDAME`.
#' @param x A character vector
#' @return A character vector
#' @keywords internal
nar_key_fold <- function(x) {
  x <- nar_fold(x)
  x <- gsub("[.']", "", x)
  x <- gsub("[^A-Z0-9]+", " ", x)
  trimws(x)
}

#' Does this street type go in front of the name?
#'
#' @description French writes `Rue Notre-Dame`, English writes `Main Street`.
#' The test is the *type*, not the province: `RUE` is unambiguously French
#' wherever it appears, and NAR has plenty of them outside Quebec. Only the
#' canonical types that exist in French alone lead, so the three that both
#' vocabularies share stay in English order.
#' @param type A character vector of canonical street types
#' @return A logical vector
#' @keywords internal
nar_type_leads <- function(type) {
  english <- nar_lex_types$canonical[nar_lex_types$lang %in% c("en", "both")]
  french <- setdiff(nar_lex_types$canonical[nar_lex_types$lang == "fr"], english)
  !is.na(type) & type %in% french
}

#' Map the empty string back to NA
#'
#' @description [nar_paste_parts()] treats `NA` as absent but an empty string as
#' a part, so anything assembled before it is handed over has to say which one
#' it means.
#' @param x A character vector
#' @return A character vector with `""` replaced by `NA`
#' @keywords internal
nar_blank_na <- function(x) ifelse(is.na(x) | !nzchar(x), NA_character_, x)
