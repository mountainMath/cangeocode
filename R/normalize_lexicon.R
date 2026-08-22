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

# --- the municipality inventory --------------------------------------------
# `nar_lex_muns` is 9,748 place names with the province and the number of
# addresses NAR files under each, built by data-raw/observe_municipalities.R
# out of `MunAlias`. It exists to arbitrate, not to parse: nothing about the
# shape of "1234 Main St 100 Mile House BC" says the municipality is three
# tokens long while "1234 Main St TH25 Vancouver" has one, and only a list of
# real places can say so.

# Populated on first use rather than at build time, because the index is a
# hashed environment and those do not survive being saved to sysdata.rda.
.nar_mun_cache <- new.env(parent = emptyenv())

#' A hashed index from municipality key to address count
#'
#' @description `match()` rebuilds its hash on every call, and the parser asks
#' this question up to six times per candidate parse, so the index is built once
#' and kept. Keys are stored twice: bare, and qualified by province, so a
#' province the string named can tighten the lookup without a second table.
#' Bare keys are assigned in ascending address count, leaving the commonest
#' place of that name as the value -- which is the only reading a tie-break
#' could sensibly prefer.
#' @return An environment mapping key to address count
#' @keywords internal
nar_mun_index <- function() {
  if (is.null(.nar_mun_cache$idx)) {
    lex <- nar_lex_muns[order(nar_lex_muns$n), , drop = FALSE]
    e <- new.env(hash = TRUE, parent = emptyenv(), size = 2L * nrow(lex) + 1L)
    for (i in seq_len(nrow(lex))) {
      n <- lex$n[i]
      assign(lex$surface_fold[i], n, envir = e)
      assign(paste0(lex$prov[i], "|", lex$surface_fold[i]), n, envir = e)
    }
    .nar_mun_cache$idx <- e
  }
  .nar_mun_cache$idx
}

#' How many NAR addresses sit in a municipality of this name
#'
#' @description The province is a preference rather than a filter. NAR's own
#' `MunAlias` province can disagree with the one the string named -- a mailing
#' city near a boundary, a name shared across two provinces -- and refusing the
#' place on that basis would throw away the evidence over a detail the
#' arbitration does not turn on.
#' @param key A folded municipality key, as [nar_mun_key()] builds it
#' @param prov A two-letter province code, or `NA`
#' @return The address count, or `NA` when no municipality of that name exists
#' @keywords internal
nar_municipality_n <- function(key, prov = NA_character_) {
  if (is.na(key) || !nzchar(key)) return(NA_real_)
  idx <- nar_mun_index()
  if (!is.na(prov) && nzchar(prov)) {
    hit <- idx[[paste0(prov, "|", key)]]
    if (!is.null(hit)) return(as.numeric(hit))
  }
  hit <- idx[[key]]
  if (is.null(hit)) NA_real_ else as.numeric(hit)
}

#' Fold a municipality name to the lexicon's match key
#'
#' @description The lexicon is keyed the way [nar_norm_text()] leaves input --
#' periods stripped, commas spaced out -- because NAR keeps the periods that
#' input does not: `ST. JOHN'S` (54,129 addresses), `SAULT STE. MARIE` (36,711)
#' and `ST. ALBERT` (29,097) can otherwise never meet a key built from tokens.
#' @param x A character vector
#' @return A character vector of match keys
#' @keywords internal
nar_mun_key <- function(x) {
  x <- gsub("[.]", "", nar_fold(x))
  x <- gsub(",", " ", x, fixed = TRUE)
  trimws(gsub("[[:space:]]+", " ", x))
}

#' Is this token run a municipality NAR files addresses under?
#' @inheritParams nar_municipality_n
#' @return `TRUE` when the key names a real place
#' @keywords internal
nar_is_municipality <- function(key, prov = NA_character_) {
  !is.na(nar_municipality_n(key, prov))
}
