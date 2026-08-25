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
#' The rest come from two places. [nar_mun_anchor_variants()] finds the
#' municipality first and hands the parser the remainder, and it fires only
#' when [nar_baseline_is_defective()] says the baseline is visibly broken.
#' [nar_dir_lead_variant()] puts a stripped leading compass word back into the
#' street name, and it fires whenever there was one -- the whole point of that
#' defect is that the baseline looks perfectly well-formed.
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
  lead <- attr(base, "dir_lead")
  attr(base, "dir_lead") <- NULL
  base$strategy <- "baseline"
  toks <- nar_tokens(s)

  restored <- nar_dir_lead_variant(base, lead)

  # The overwhelmingly common case is one reading, and the collapse machinery
  # below costs more than the parse itself -- leave before paying for it.
  anchored <- if (nar_baseline_is_defective(base, toks, prov)) {
    nar_mun_anchor_variants(toks, lang, prov)
  } else {
    list()
  }
  if (!length(restored) && !length(anchored)) {
    base$.cand <- 1L
    return(base)
  }

  cands <- do.call(rbind, c(list(base), restored, anchored,
                            list(stringsAsFactors = FALSE)))

  keys <- do.call(paste, c(cands[, setdiff(names(cands), "strategy")], sep = "\r"))
  cands <- cands[!duplicated(keys), , drop = FALSE]

  cands$.cand <- seq_len(nrow(cands))
  row.names(cands) <- NULL
  cands
}

#' The reading in which a leading compass word is part of the street name
#'
#' @description `East Beaver Creek Rd` and `West Beaver Creek Rd` are two
#' streets in Richmond Hill; `North Edgely Ave` and `South Edgely Ave` are two
#' streets in Scarborough. The left-to-right parse reads the opening word as a
#' direction and hands the gazetteer `BEAVER CREEK`, which whole-word
#' containment scores 0.90 against *both* halves of the pair. Direction
#' agreement is worth 0.06 and the stripped reading has no direction left in the
#' name to agree with, so the mirror image wins about as often as the street
#' does -- and it wins *confidently*, with nothing in the output to say so. Some
#' 92,000 NAR addresses are on a street NAR itself spells with the word in the
#' name and no direction on either name family.
#'
#' So the unstripped reading is offered as a parallel candidate rather than as a
#' fallback fired when the stripped one finds nothing: a fallback repairs only
#' the addresses that end up unplaced, which measured as 68 of 453 losses, and
#' leaves the other 385 confidently on the wrong street. As a candidate the
#' restored probe `EAST BEAVER CREEK` matches one of the pair exactly at 1.0 and
#' the other not at all, so it beats 0.868 outright.
#'
#' Two things keep it from displacing a correct reading. Both candidates carry
#' the same municipality, so the comparison is like-for-like and the
#' restricted-beats-unrestricted asymmetry that governs
#' [nar_baseline_is_defective()] never arises -- which is why this one needs no
#' gate. And a street genuinely called `Park` still wins, because the baseline
#' probe `PARK` matches it exactly at 1.0 while the restored probe `NORTH PARK`
#' matches nothing; when neither exists the restored probe falls under the name
#' threshold and is refused, leaving today's answer untouched.
#'
#' The word is restored verbatim, abbreviations included. `W GEORGIA` is not a
#' street name NAR carries, so that candidate simply loses; the ~2,000
#' addresses whose NAR name really does open with an abbreviated compass word
#' are the ones it is there for.
#' @param base The baseline reading, one row, with `strategy` already set
#' @param word The leading direction token as it arrived, or `NA`/`NULL`
#' @return A list holding one candidate row, or an empty list
#' @keywords internal
nar_dir_lead_variant <- function(base, word) {
  if (is.null(word) || is.na(word) || is.na(base$name)) return(list())
  v <- base
  v$name <- paste(word, base$name)
  v$dir <- NA_character_
  v$strategy <- "dir_in_name"
  list(v)
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
#' * **a longer trailing run than the one it claimed is also a place.** This
#'   is the comma-free case and the only one of the three that can fire on a
#'   baseline with nothing visibly wrong with it. `3908 loraine ave north
#'   vancouver` reads `NORTH` as the street's direction and leaves `VANCOUVER`,
#'   which is a real municipality, so neither of the tests above sees anything;
#'   `NORTH VANCOUVER` is a real municipality too, and that is the entire
#'   evidence for offering the other reading. It also covers the baselines that
#'   proposed *no* municipality because a street type inside the place name ate
#'   the boundary -- `maple ridge`, `bowen island`, `brentwood bay`, `qualicum
#'   beach` all end in a NAR street type, and `4830 scott ave terrace` ends in
#'   one that is the whole name.
#'
#' A baseline that proposes no municipality where the string holds no run that
#' names one is still not defective. The string simply did not carry a place,
#' `NA` is the right answer, and the status note treats recovering it as a
#' gazetteer question rather than a parsing one.
#'
#' The run scan is bounded by the same reach anchoring uses, which is one token
#' short of the last comma segment, so a comma-delimited municipality can never
#' trigger it: what the comma already gave the baseline is longer than anything
#' the scan is allowed to propose. That is what confines this third test to
#' strings the writer never delimited.
#' @param base A one-row parse from [nar_parse_one()]
#' @param toks The token vector the parse was built from
#' @param prov A two-letter province code, or `NA`
#' @return `TRUE` when an alternative reading is worth generating
#' @keywords internal
nar_baseline_is_defective <- function(base, toks = character(0),
                                      prov = NA_character_) {
  if (!is.na(base$mun) &&
      !nar_is_municipality(nar_mun_key(base$mun), prov)) return(TRUE)
  if (!is.na(base$name) &&
      grepl("(^|[[:space:]])#([[:space:]]|$)", base$name)) return(TRUE)
  claimed <- if (is.na(base$mun)) 0L else length(nar_tokens(base$mun))
  length(nar_mun_anchor_runs(toks, prov, min_k = claimed + 1L)) > 0L
}

#' Trailing token runs that name a municipality
#'
#' @description Shared by the gate and by [nar_mun_anchor_variants()] so the two
#' agree on what "the municipality could reach back this far" means, and so the
#' gate pays for the inventory lookups only down to the length it cares about.
#'
#' The reach stops one token short of the last comma segment. A municipality
#' never spans a comma the writer put in, and taking the whole of the last
#' segment would only rediscover the split the comma already made.
#' @param toks A token vector, comma tokens included
#' @param prov A two-letter province code, or `NA`
#' @param min_k The shortest run worth testing
#' @return An integer vector of run lengths, longest first
#' @keywords internal
nar_mun_anchor_runs <- function(toks, prov = NA_character_, min_k = 1L) {
  m <- length(toks)
  if (m < 2 || min_k < 1L) return(integer(0))
  last_comma <- if (any(toks == ",")) max(which(toks == ",")) else 0L
  # Six tokens covers the inventory: the longest names NAR carries are
  # "Stanley Bridge, Hope River, Bayview, Cavendish and North Rustico" and the
  # parenthesised BC regional districts, and both are past any plausible input.
  max_k <- min(6L, m - last_comma - 1L)
  if (max_k < min_k) return(integer(0))
  k <- max_k:min_k
  k[vapply(k, function(i) nar_is_municipality(
    nar_mun_key(paste(toks[(m - i + 1L):m], collapse = " ")), prov), logical(1))]
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
#' Four guards keep this from inventing splits, and the last two are what let
#' the gate open as wide as it now does.
#'
#' * A run is only considered if it lies inside the last comma segment -- see
#'   [nar_mun_anchor_runs()].
#' * A candidate is dropped unless a street name survives in the remainder,
#'   which is what stops `"123 Kingston"` from resolving to the city of Kingston
#'   with no street at all.
#' * **A residue that is not a street name counts as no street name.** Every
#'   place name that also does duty as a street name fails here rather than at
#'   the gate: `135 de Nantes` anchors Nantes and leaves `DE`, `22 avenue de la
#'   Durantaye` leaves `DE LA`, `80 rue Albanel` leaves `RUE`. Particules are
#'   not a name, and neither is a street type standing alone.
#' * **A run that is a street type has to be one the street can spare.** `TRAIL`
#'   is a municipality in Ontario and a street type everywhere, and `82
#'   Fesroches Trail` is the second; `4830 scott ave terrace` is the first, and
#'   the only thing that separates them is that the street in it still names a
#'   type of its own once `TERRACE` is taken away. Same for `maple ridge`,
#'   `bowen island`, `brentwood bay` and `qualicum beach`, all of which end in a
#'   NAR street type.
#' @param toks A token vector, as [nar_tokens()] produces
#' @inheritParams nar_parse_one
#' @return A list of one-row data frames, possibly empty
#' @keywords internal
nar_mun_anchor_variants <- function(toks, lang = "en", prov = NA_character_) {
  m <- length(toks)
  out <- list()
  for (k in nar_mun_anchor_runs(toks, prov)) {
    run <- toks[(m - k + 1L):m]

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
    if (!nar_is_street_name(cand$name, lang)) next
    # A street type can only become the municipality if the street keeps one.
    if (is.na(cand$type) &&
        nar_is_street_type(nar_fold(run[length(run)]), lang)) next

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

#' Could this token run be a street name at all?
#'
#' @description The residue test for [nar_mun_anchor_variants()], and the reason
#' a place name that is also a street name does not have to be listed anywhere.
#' Anchoring a municipality off the end of `135 de Nantes` leaves `DE`, off `22
#' avenue de la Durantaye` leaves `DE LA`, off `80 rue Albanel` leaves `RUE` --
#' three different failures that are all the same failure, and all of them
#' visible in what is left rather than in what was taken.
#'
#' Particules do not name a street on their own, and a street type standing
#' alone does not either. Both tests are on the residue after the particules
#' come off, so `RUE DE LA` fails as surely as `RUE` does.
#' @param name A street name as parsed, or `NA`
#' @param lang `"en"` or `"fr"`
#' @return `FALSE` when the name is nothing a street could be called
#' @keywords internal
nar_is_street_name <- function(name, lang = "en") {
  if (is.na(name)) return(FALSE)
  f <- nar_fold(nar_tokens(name))
  f <- f[!nar_is_particule(f)]
  length(f) > 1L || (length(f) == 1L && !nar_is_street_type(f, lang))
}
