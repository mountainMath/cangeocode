#' What the caller already knows about an address
#'
#' @description Address data does not always arrive as one string. An
#' assessment roll carries the community in a column of its own, a filing
#' carries the province, a cleaned list may carry everything but the street.
#' `known` is how that structure is handed to the parser and to the search
#' instead of being thrown away and re-derived from a string it was concatenated
#' into.
#'
#' Every key is **authoritative**: it overrides whatever the string parsed to,
#' it lands on the returned row, and it constrains the search. `NA` for a row
#' means nothing is known about that row, so the parse stands.
#'
#' What lands is the caller's own value, normalized the way a parse is -- upper
#' case, accents kept, punctuation that only decorates abbreviations dropped.
#' It is *not* replaced by NAR's spelling of whatever matched, because nothing
#' was matched: the component was asserted rather than resolved. So an asserted
#' `CSD_NAME = "Toronto"` comes back `TORONTO` where a resolved one comes back
#' NAR's `Toronto`.
#'
#' @section The two kinds of municipality: `MUN_NAME` and `CSD_NAME` are
#' different questions and this is the argument that separates them.
#'
#' * `MUN_NAME` is the **mailing city** -- NAR's `MAIL_MUN_NAME`, the name on
#'   the envelope. It restricts to streets NAR files under that exact name.
#' * `CSD_NAME` is the **census subdivision**, the administrative unit. It is
#'   resolved through NAR's alias set, so `Toronto` reaches everything filed
#'   under `SCARBOROUGH`, `NORTH YORK` and `ETOBICOKE`, and a name denoting
#'   several jurisdictions means all of them.
#'
#' The two do not nest -- one mailing city can span several jurisdictions and
#' one jurisdiction carries many mailing cities -- so asking for the wrong one
#' is not a near miss. `MUN_NAME = "Toronto"` will *not* find an address NAR
#' files under `SCARBOROUGH`; `CSD_NAME = "Toronto"` will. Supply both and both
#' constrain, which is how a caller narrows to one community inside a large
#' amalgamated city.
#'
#' A municipality that resolves to nothing leaves the row unresolved rather
#' than being ignored, which is what a parsed municipality already does.
#'
#' @section Keys: The NAR-shaped column names, so the list form and the
#' data-frame form of `x` share one vocabulary:
#' `APT_NO_LABEL`, `CIVIC_NO`, `CIVIC_NO_SUFFIX`, `STREET_NAME`, `STREET_TYPE`,
#' `STREET_DIR`, `MUN_NAME`, `CSD_NAME`, `PROV_ABVN`, `POSTAL_CODE`.
#' Anything else is an error rather than a silently dropped constraint: a
#' constraint that does not bind produces a confident wrong answer, which is the
#' failure this argument exists to prevent.
#'
#' @param known A named list, or `NULL`
#' @param n The number of addresses being resolved
#' @return A data frame with `n` rows and one column per supplied key, or `NULL`
#' @keywords internal
nar_known <- function(known, n) {
  if (is.null(known)) return(NULL)
  if (is.data.frame(known)) known <- as.list(known)
  if (!is.list(known) || is.null(names(known)) || !all(nzchar(names(known)))) {
    stop("`known` must be a named list, e.g. list(PROV_ABVN = \"BC\").",
         call. = FALSE)
  }
  if (anyDuplicated(names(known))) {
    stop("`known` names each component once; ",
         paste(unique(names(known)[duplicated(names(known))]), collapse = ", "),
         " is given more than once.", call. = FALSE)
  }
  bad <- setdiff(names(known), nar_known_keys())
  if (length(bad)) {
    stop("`known` does not take ", paste(bad, collapse = ", "), ". Valid ",
         "components are ", paste(nar_known_keys(), collapse = ", "), ".",
         call. = FALSE)
  }
  known <- known[lengths(known) > 0L]
  if (!length(known)) return(NULL)

  out <- lapply(names(known), function(nm) {
    v <- known[[nm]]
    if (length(v) != 1L && length(v) != n) {
      stop("`known$", nm, "` must be length 1 or length ", n, ", not ",
           length(v), ".", call. = FALSE)
    }
    v <- rep_len(v, n)
    switch(nm,
      CIVIC_NO    = suppressWarnings(as.numeric(v)),
      PROV_ABVN   = nar_known_prov(v),
      POSTAL_CODE = nar_known_postal(v),
      nar_known_text(v))
  })
  names(out) <- names(known)
  # `n` can be zero -- an empty input is a normal call, not a mistake -- and
  # data.frame() needs to be told the row count when every column is empty.
  as.data.frame(out, stringsAsFactors = FALSE, row.names = NULL)[seq_len(n), ,
                                                                 drop = FALSE]
}

#' The components `known` accepts
#' @return A character vector of column names
#' @keywords internal
nar_known_keys <- function() {
  c("APT_NO_LABEL", "CIVIC_NO", "CIVIC_NO_SUFFIX", "STREET_NAME",
    "STREET_TYPE", "STREET_DIR", "MUN_NAME", "CSD_NAME", "PROV_ABVN",
    "POSTAL_CODE")
}

#' Put a supplied component into the shape the parser produces
#'
#' @description Through [nar_norm_text()], the same normalization the address
#' string gets, so an asserted `"Howie Centre"` and a parsed `HOWIE CENTRE` are
#' one value. Without it the override would be authoritative and unmatchable at
#' the same time.
#' @param v A character vector
#' @return A character vector
#' @keywords internal
nar_known_text <- function(v) {
  v <- nar_norm_text(as.character(v))
  v <- gsub("[[:space:]]+", " ", v)
  v <- trimws(v)
  ifelse(is.na(v) | !nzchar(v), NA_character_, v)
}

#' Canonicalize a supplied province
#'
#' @description `"British Columbia"`, `"B.C."` and `"bc"` are the same
#' constraint, and NAR stores only the last of them. Anything the province
#' lexicon does not recognize is passed through folded rather than refused --
#' it will simply match nothing, which is the honest outcome for a province
#' code that is not one.
#' @param v A character vector
#' @return A character vector of two-letter codes
#' @keywords internal
nar_known_prov <- function(v) {
  key <- nar_fold(nar_known_text(v))
  i <- match(key, nar_lex_prov$surface_fold)
  ifelse(is.na(key), NA_character_,
         ifelse(is.na(i), key, nar_lex_prov$canonical[i]))
}

#' Canonicalize a supplied postal code
#'
#' @description Stored six characters with no space, so the space a person
#' types comes out.
#' @param v A character vector
#' @return A character vector
#' @keywords internal
nar_known_postal <- function(v) {
  p <- toupper(gsub("[^A-Za-z0-9]", "", as.character(v)))
  ifelse(is.na(p) | !nzchar(p), NA_character_, p)
}

#' Overwrite parsed components with the ones the caller asserted
#'
#' @description Applied twice in [normalize_address()]: before the gazetteer, so
#' it restricts on what was asserted rather than on what the string happened to
#' yield, and after it, so a substitution the gazetteer would otherwise make
#' cannot overwrite the caller.
#' @param res A parse, one row per address
#' @param k The recycled `known` frame, or `NULL`
#' @param rows Which row of `k` each row of `res` belongs to
#' @return `res`, with the asserted components written in
#' @keywords internal
nar_known_apply <- function(res, k, rows = seq_len(nrow(res))) {
  if (is.null(k) || !nrow(res)) return(res)
  for (nm in names(k)) {
    # Created rather than skipped: a hand-built data frame carries only the
    # columns it needed to, and a constraint that silently failed to land is
    # exactly the wrong answer this argument exists to prevent.
    if (is.null(res[[nm]])) {
      res[[nm]] <- if (nm == "CIVIC_NO") NA_real_ else NA_character_
    }
    v <- k[[nm]][rows]
    ok <- !is.na(v)
    if (any(ok)) res[[nm]][ok] <- v[ok]
  }
  res
}

#' Which rows had their municipality asserted rather than resolved
#'
#' @description A caller who names the municipality has settled the question the
#' swap penalty exists to arbitrate, so those rows report `mun_evidence`
#' `"kept"` and no remap -- there was nothing for the gazetteer to substitute.
#' @param k The recycled `known` frame, or `NULL`
#' @param n The number of rows
#' @return A logical vector
#' @keywords internal
nar_known_has_mun <- function(k, n) {
  if (is.null(k)) return(rep(FALSE, n))
  out <- rep(FALSE, n)
  for (nm in intersect(c("MUN_NAME", "CSD_NAME"), names(k))) {
    out <- out | !is.na(k[[nm]])
  }
  out
}

#' Drop a parsed mailing city that contradicts an asserted jurisdiction
#'
#' @description `CSD_NAME` and `MUN_NAME` both constrain, which is what lets a
#' caller narrow to one community inside an amalgamated city. That is only the
#' right reading when the caller supplied both. A caller who asserted
#' `CSD_NAME = "Vancouver"` over a string that says `Toronto` has *contradicted*
#' the parse, and leaving the parsed mailing city in place would let it veto the
#' assertion -- the search would run in the intersection of two jurisdictions
#' that do not overlap and return nothing, which is the confident wrong answer
#' `known` exists to prevent. The mailing city is cleared instead, and the
#' gazetteer or the tier fills it back in from whatever it actually matched.
#' @param res A parse, one row per address
#' @param k The recycled `known` frame, or `NULL`
#' @param rows Which row of `k` each row of `res` belongs to
#' @return `res`, with the contradicted mailing city removed
#' @keywords internal
nar_known_clear_mun <- function(res, k, rows = seq_len(nrow(res))) {
  if (is.null(k) || is.null(k$CSD_NAME) || !nrow(res)) return(res)
  if (is.null(res$MUN_NAME)) return(res)
  drop <- !is.na(k$CSD_NAME[rows])
  if (!is.null(k$MUN_NAME)) drop <- drop & is.na(k$MUN_NAME[rows])
  res$MUN_NAME[drop] <- NA_character_
  res
}

#' Which municipality-as-jurisdiction actually restricts the search
#'
#' @description `CSD_NAME` is both an input and an output, and the two must not
#' be confused. As an input it is a **constraint** -- the caller naming the
#' jurisdiction. As an output it is a **report** -- the census subdivision the
#' street or record that matched happens to belong to, which is not the same
#' claim: the search was never restricted to it.
#'
#' Feeding a reported one back in as a constraint narrows the next search to
#' something no one asked for. `5491 Route 11, Brantville, NB` is the case that
#' found this: the gazetteer resolves the street inside `13:MRM:Tracadie`, while
#' NAR files Brantville's Route 11 addresses across that key, `13:RCR:Alnwick`
#' and a blank one. Restricting to Tracadie drops the flanking civic numbers the
#' interpolation tier needs, and a row that placed becomes a row that does not.
#' So `geocode(normalize_address(x, con = con))` would answer differently from
#' `geocode(x, con = con)`, which is the one thing the two forms may not do.
#'
#' A frame the caller built themselves is input, not a report, so its own
#' `CSD_NAME` does constrain. `parse_source` is what tells the two apart --
#' every [normalize_address()] result carries it and nothing else does.
#' @param res The components being searched on
#' @param k The recycled `known` frame, or `NULL`
#' @param from_frame Whether `res` is a frame the caller built rather than a parse
#' @return A character vector, or `NULL` if nothing constrains
#' @keywords internal
nar_known_csd <- function(res, k, from_frame = FALSE) {
  out <- if (is.null(k) || is.null(k$CSD_NAME)) rep(NA_character_, nrow(res))
         else k$CSD_NAME
  if (from_frame && !is.null(res$CSD_NAME)) {
    out <- ifelse(is.na(out), as.character(res$CSD_NAME), out)
  }
  if (all(is.na(out))) NULL else out
}
