# Candidate parses and the arbitration between them.
#
# A single left-to-right walk of the tokens has to commit to a reading of the
# string before it has any evidence about whether that reading exists. Some of
# those commitments are unrecoverable: once "TH25 VANCOUVER" has been taken as
# the municipality there is nothing downstream that can put the unit back.
#
# So the parser produces *readings* rather than a parse, and something with
# evidence chooses between them -- the municipality inventory when parsing is
# rules-only, and the NAR street gazetteer when a connection is available. The
# baseline reading is always candidate 1 and always wins a tie, so a candidate
# can only ever displace it on evidence.

#' Every reading of one address string worth arbitrating between
#'
#' @description Candidate 1 is the ordinary left-to-right parse, unchanged.
#' The rest come from [nar_mun_anchor_variants()], which finds the municipality
#' first and hands the parser the remainder.
#'
#' Identical readings are collapsed, which is the common case: a
#' comma-delimited string already puts the municipality in a segment of its own,
#' so anchoring rediscovers the same split the comma made and there is nothing
#' to arbitrate.
#' @inheritParams nar_parse_one
#' @return A data frame of one row per candidate, in priority order, carrying
#' the columns [nar_parse_one()] returns plus `strategy` and `.cand`
#' @keywords internal
nar_parse_variants <- function(s, lang = "en", prov = NA_character_) {
  base <- nar_parse_one(s, lang, prov)
  base$strategy <- "baseline"

  # The overwhelmingly common case is one reading, and the collapse machinery
  # below costs more than the parse itself -- leave before paying for it.
  if (!nar_baseline_is_defective(base, prov)) {
    base$.cand <- 1L
    return(base)
  }
  anchored <- nar_mun_anchor_variants(s, lang, prov)
  if (!length(anchored)) {
    base$.cand <- 1L
    return(base)
  }

  cands <- do.call(rbind, c(list(base), anchored, list(stringsAsFactors = FALSE)))

  keys <- do.call(paste, c(cands[, setdiff(names(cands), "strategy")], sep = "\r"))
  cands <- cands[!duplicated(keys), , drop = FALSE]

  cands$.cand <- seq_len(nrow(cands))
  row.names(cands) <- NULL
  cands
}

#' Has the ordinary parse produced a reading worth offering an alternative to?
#'
#' @description The gate on generating anchored readings at all, and it is the
#' load-bearing part of this file. Measured on the Part A sample, offering an
#' alternative unconditionally *cost* rows: `80 rue Albanel, QC` names no
#' municipality, Albanel is a real Quebec one, and anchoring it leaves a street
#' called `RUE`. The same happened to `22 avenue de la Durantaye`,
#' `135 de Nantes` and `1037 boulevard de l'Assomption` -- every one a place
#' name doing duty as a street name in a string that never named a place.
#'
#' The gazetteer does not rescue those, because it cannot: a match restricted to
#' a real municipality outscores an unrestricted one by construction, so the
#' worse reading wins on a score that was never meant to compare two different
#' parses of the same string. Arbitration cannot fix a candidate that should
#' not have been offered.
#'
#' So a second reading is offered only where the first one is demonstrably
#' broken, which is two things and no others:
#'
#' * the municipality it proposes **is not a place**. `TH25 VANCOUVER` is not,
#'   `100 MILE HOUSE` is, and no rule about token shapes tells them apart.
#' * the street name it proposes **contains a `#`**, which
#'   [nar_norm_text()] guarantees introduces a unit and which no street name
#'   can therefore contain. That is the signature of a string nothing split.
#'
#' A baseline that proposes *no* municipality is not defective. The string
#' simply did not carry one, `NA` is the right answer, and the status note
#' treats recovering it as a gazetteer question rather than a parsing one.
#' @param base A one-row parse from [nar_parse_one()]
#' @param prov A two-letter province code, or `NA`
#' @return `TRUE` when an alternative reading is worth generating
#' @keywords internal
nar_baseline_is_defective <- function(base, prov = NA_character_) {
  if (!is.na(base$mun) &&
      !nar_is_municipality(nar_mun_key(base$mun), prov)) return(TRUE)
  !is.na(base$name) && grepl("(^|[[:space:]])#([[:space:]]|$)", base$name)
}

#' Readings that take the municipality off the end before parsing the street
#'
#' @description The trailing token run is tested against the municipality
#' inventory, longest first, and every run that names a real place becomes a
#' candidate whose remainder is parsed with the municipality already decided.
#' Both lengths can be real -- `NORTH BAY` and `BAY` are each municipalities --
#' so both are offered rather than the longer one being assumed.
#'
#' This is what makes a trailing comma inconsequential. `"... TH25, Vancouver"`
#' parses today only because the comma bounds the municipality, and
#' `"... TH25 Vancouver"` fails for want of it; anchoring reaches the same
#' remainder from both, and the comma stops carrying the parse.
#'
#' Two guards keep this from inventing splits. A run is only considered if it
#' lies inside the last comma segment -- a municipality never spans a comma the
#' writer put in -- and a candidate is dropped unless a street name survives in
#' the remainder, which is what stops `"123 Kingston"` from resolving to the
#' city of Kingston with no street at all.
#' @inheritParams nar_parse_one
#' @return A list of one-row data frames, possibly empty
#' @keywords internal
nar_mun_anchor_variants <- function(s, lang = "en", prov = NA_character_) {
  toks <- nar_tokens(s)
  m <- length(toks)
  if (m < 2) return(list())

  # A municipality never spans a comma, so the run may reach back only as far
  # as the start of the last segment.
  last_comma <- if (any(toks == ",")) max(which(toks == ",")) else 0L
  # Six tokens covers the inventory: the longest names NAR carries are
  # "Stanley Bridge, Hope River, Bayview, Cavendish and North Rustico" and the
  # parenthesised BC regional districts, and both are past any plausible input.
  max_k <- min(6L, m - last_comma - 1L)
  if (max_k < 1L) return(list())

  out <- list()
  for (k in max_k:1L) {
    run <- toks[(m - k + 1L):m]
    key <- nar_mun_key(paste(run, collapse = " "))
    if (!nar_is_municipality(key, prov)) next

    rest <- utils::head(toks, m - k)
    # The comma that separated the municipality goes with it -- this is the
    # whole point of anchoring, and leaving it behind would put an empty
    # trailing segment where the parser looks for one.
    rest <- rest[!(seq_along(rest) == length(rest) & rest == ",")]
    if (!length(rest)) next

    cand <- nar_parse_one(paste(rest, collapse = " "), lang, prov,
                          mun_fixed = paste(run, collapse = " "))
    # No street left means the run was the address rather than a place in it.
    if (is.na(cand$name)) next

    cand$strategy <- paste0("mun_anchor_", k)
    out[[length(out) + 1L]] <- cand
  }
  out
}

#' Choose between candidate parses on the evidence rules alone can muster
#'
#' @description Without a database the only evidence available is whether the
#' municipality each reading proposes is a place that exists, which is exactly
#' the question the readings disagree about. `TH25 VANCOUVER` is not a
#' municipality and `VANCOUVER` is; `100 MILE HOUSE` is one and `MILE HOUSE` is
#' not, which is why the same rule cannot be written as a heuristic about
#' token shapes.
#'
#' The order is: a municipality that exists beats one that does not, then the
#' completeness score, then candidate order -- so the baseline reading is
#' displaced only by a candidate that is strictly better evidenced, never by a
#' tie. When a connection is supplied [nar_resolve_gazetteer()] arbitrates
#' again over the same candidates, with better evidence.
#' @param cand A candidate tibble carrying `.row`, `.cand`, `MUN_NAME`,
#' `PROV_ABVN` and `confidence`
#' @return An integer vector of the winning row of `cand` per `.row`
#' @keywords internal
nar_arbitrate_rules <- function(cand) {
  # Almost every string yields one reading, and for those there is nothing to
  # arbitrate -- skip the inventory lookup rather than pay it per row.
  if (!anyDuplicated(cand$.row)) return(seq_len(nrow(cand)))
  known <- !is.na(nar_municipality_vec(cand$MUN_NAME, cand$PROV_ABVN))
  o <- order(cand$.row, -known, -cand$confidence, cand$.cand)
  o[!duplicated(cand$.row[o])]
}

#' Address counts for a vector of municipality names
#' @param mun A character vector of municipality names as parsed
#' @param prov A character vector of province codes, recycled against `mun`
#' @return A numeric vector of address counts, `NA` where the name is not a place
#' @keywords internal
nar_municipality_vec <- function(mun, prov) {
  key <- nar_mun_key(mun)
  prov <- rep_len(prov, length(key))
  vapply(seq_along(key), function(i) nar_municipality_n(key[i], prov[i]),
         numeric(1))
}
