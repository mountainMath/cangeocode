# Lexicon lookup for the deterministic address parser.
#
# The tables themselves live in R/sysdata.rda, built by data-raw/build_lexicons.R
# from the crosswalks in data-raw/. Everything here is matching machinery.

#' Fold text to an accent-insensitive, case-insensitive match key
#'
#' @description NAR stores accented street types verbatim, so folding to ASCII
#' is only ever applied to the *key* a lookup matches on, never to a value that
#' ends up in the output.
#' The value handed back is the lexicon's `canonical` column, accents intact.
#' @param x A character vector
#' @return A character vector, uppercased and transliterated to ASCII
#' @keywords internal
nar_fold <- function(x) {
  stringi::stri_trans_general(toupper(stringi::stri_trans_nfc(x)), "Latin-ASCII")
}

#' The canonicalization language a province takes
#'
#' @description Quebec's NAR street types are French (`RUE`/`AV`/`BOUL` run
#' 94-100% Quebec); every other province takes the English tokens. An unknown or
#' missing province falls back to English, which is the majority form for all
#' but one of the thirteen.
#' @param prov A character vector of two-letter province codes
#' @return A character vector of `"en"` / `"fr"`
#' @keywords internal
nar_prov_language <- function(prov) {
  out <- unname(nar_prov_lang[prov])
  out[is.na(out)] <- "en"
  out
}

#' Look a surface form up in a lexicon, preferring the province's language
#'
#' @description Resolution is language-conditioned: `AVENUE` canonicalizes to
#' `AVE` in Ontario but `AV` in Quebec, and `WEST` to `W` against `O`. A row
#' tagged `"both"` matches either language. When the preferred language has no
#' entry the lookup falls back to any language, so a French street type in an
#' English province (`RUE` in Ottawa) still resolves rather than being dropped.
#' @param surface A character vector of already-folded surface forms
#' @param lex A lexicon data frame with `surface_fold`, `canonical` and `lang`
#' @param lang A character vector of `"en"` / `"fr"`, recycled against `surface`
#' @return A character vector of canonical tokens, `NA` where nothing matched
#' @keywords internal
nar_lex_lookup <- function(surface, lex, lang = "en") {
  if (!length(surface)) return(character(0))
  lang <- rep_len(lang, length(surface))

  exact <- match(paste(surface, lang), paste(lex$surface_fold, lex$lang))
  both  <- match(paste(surface, "both"), paste(lex$surface_fold, lex$lang))
  any_l <- match(surface, lex$surface_fold)

  idx <- ifelse(!is.na(exact), exact, ifelse(!is.na(both), both, any_l))
  lex$canonical[idx]
}

#' Is this token a street type in the given language context?
#' @inheritParams nar_lex_lookup
#' @return A logical vector
#' @keywords internal
nar_is_street_type <- function(surface, lang = "en") {
  !is.na(nar_lex_lookup(surface, nar_lex_types, lang))
}

#' Is this token a street direction in the given language context?
#' @inheritParams nar_lex_lookup
#' @return A logical vector
#' @keywords internal
nar_is_street_dir <- function(surface, lang = "en") {
  !is.na(nar_lex_lookup(surface, nar_lex_dirs, lang))
}
