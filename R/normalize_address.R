#' Normalize Canadian address strings into NAR components
#'
#' @description Parses free-text Canadian addresses into the structured
#' components NAR is keyed on, which is what a forward geocode has to join
#' against. Parsing is deterministic: a tokenizer plus the closed street-type,
#' direction and province vocabularies that NAR itself uses. Supplying `con`
#' additionally resolves the result against the NAR street gazetteer, which
#' corrects misspellings and fills in components the string left ambiguous.
#'
#' @param x A character vector of address strings
#' @param known Components the caller already has, as a named list of vectors
#' each length 1 or `length(x)` -- `list(PROV_ABVN = "NS", MUN_NAME = "Howie
#' Centre")`. Authoritative: each one overrides what the string parsed to,
#' lands on the returned row, and restricts the gazetteer. `MUN_NAME` is the
#' mailing city and `CSD_NAME` the administrative one, and they are different
#' searches; see [nar_known()] for the full key list and for why the two are
#' separate. `PROV_ABVN` additionally reaches the parser, where it materially
#' changes the result: canonicalization is language-conditioned, so `"avenue"`
#' normalizes to `AVE` in Ontario and `AV` in Quebec.
#' @param con An open NAR connection. Supplying one enables gazetteer
#' resolution; without it parsing is lexicon-only. The caller keeps ownership --
#' a connection passed here is left open, matching [reverse_geocode()].
#' @param ... Passed to the gazetteer layer when `con` is supplied, and ignored
#' otherwise -- `threshold`, `name_threshold`, `mun_swap_penalty` and
#' `keep_refused`; see [nar_resolve_gazetteer()]. `keep_refused = TRUE` adds a
#' `refused_for` column and resolves the rows the threshold would have left
#' unresolved, flagged with the gate they failed.
#'
#' @return A tibble with one row per element of `x`, carrying the NAR-shaped
#' columns `APT_NO_LABEL`, `CIVIC_NO`, `CIVIC_NO_SUFFIX`, `STREET_NAME`,
#' `STREET_TYPE`, `STREET_DIR`, `MUN_NAME`, `CSD_NAME`, `PROV_ABVN` and
#' `POSTAL_CODE`,
#' alongside the original `input`, the structural `pattern` it parsed as (see
#' [address_pattern()] for the buckets), a `confidence` in `[0, 1]`, a
#' `mun_remapped` flag with its `mun_evidence` companion, and a
#' `parse_source` naming which layer settled the row: `"rules"` for the
#' lexicon-only parse, `"gazetteer"` for a match against NAR's streets, and
#' `"rqa"` for one against Quebec's own register -- available only once
#' [rqa_import()] has been run, and meaning the street was canonicalized
#' against a register NAR does not carry it in, so a join against `Addresses`
#' will still not find it.
#'
#' `MUN_NAME` is the **mailing city** and `CSD_NAME` the **census subdivision**
#' the gazetteer actually searched. They answer different questions and neither
#' contains the other: `CSD_NAME` is `TORONTO` for a street whose `MUN_NAME` is
#' `SCARBOROUGH`. `CSD_NAME` is `NA` on a row the gazetteer did not resolve, and
#' on one it resolved without a locality to restrict to.
#'
#' `mun_remapped` and `mun_evidence` are the pair to read before trusting
#' `MUN_NAME`. See the section below.
#'
#' @section When the municipality is not the one you wrote: The gazetteer
#' restricts its candidate streets through `MunAlias`, which keys on the
#' **census subdivision** rather than on the community. So writing `MILFORD, NS`
#' admits every street in all three CSDs that name resolves to -- Halifax
#' Regional Municipality among them, which is 166 communities and 225,837
#' addresses spanning 127 km. `CSD_NAME` reports which of them answered, and
#' `known = list(MUN_NAME = "Milford")` is how a caller who means the community
#' and not the jurisdiction says so. Whichever street wins is then reported with *its*
#' own `MAIL_MUN_NAME`, which need not be the one that was written.
#'
#' That substitution is usually the feature working: it is how a rural community
#' reaches the mailing municipality NAR files it under, and `Toronto` resolving
#' to `SCARBOROUGH` is the value a join against NAR actually needs. But the same
#' step is what puts an address in the wrong community when the name it wrote
#' could not be matched exactly, and neither `confidence` nor -- downstream --
#' [geocode()]'s `n_matches` can see that it happened.
#'
#' `mun_remapped` reports it: `TRUE` when the municipality being handed back is
#' not the one the string named, `FALSE` when it is, and `NA` when the row
#' carries no municipality at all. It is `TRUE` for a municipality the string
#' never named as well, since that is also a place chosen by the gazetteer
#' rather than asserted by the input.
#'
#' `mun_evidence` says *why* the substitution was allowed to stand, which is the
#' part that decides how much to worry. Three of its six values are
#' attestations, and all three are read out of NAR rather than out of a curated
#' alias list:
#'
#' \describe{
#'   \item{`kept`}{nothing was substituted -- NAR files the address under the
#'     name that was written.}
#'   \item{`copostal`}{the two names appear on the same *full* six-character
#'     postal code somewhere in NAR, so they are two labels for one delivery
#'     geography. `HOWIE CENTER` and `SYDNEY` share three.}
#'   \item{`csd`}{the name written is the census subdivision the street sits in.
#'     This is what carries amalgamations and legacy names -- `Toronto` for a
#'     street NAR still mails to `NORTH YORK` -- which no postal code will ever
#'     attest, because the merger did not merge the delivery names.}
#'   \item{`unattested`}{checked against both, and corroborated by neither. This
#'     is the class the swap penalty fines; see [nar_resolve_gazetteer()].}
#'   \item{`untestable`}{NAR files no postal-coded mail under the name that was
#'     written, so there was nothing to test the substitution against. An
#'     absence of evidence about an unknown name is not evidence of a bad swap,
#'     and this class is exempt from the penalty for that reason.}
#'   \item{`inferred`}{the string named no municipality and one was determined
#'     for it.}
#' }
#'
#' `NA` in both columns means the row carries no municipality at all.
#' [geocode()] carries the pair into `uncertainty_m`, and prices only the three
#' unattested ones -- measured against an independent reading of the same
#' houses, an attested substitution lands no further out than a municipality the
#' input got right.
#'
#' @section Comma-less input: Addresses that separate their parts with commas
#' parse most reliably, because the commas bound the street from the
#' municipality. A comma-less string such as `"100 queen st w toronto"` has to
#' guess where the street ends, and the guess is only as good as the street-type
#' vocabulary -- a municipality whose name contains a street-type word (Port
#' Hope, Grand Falls) can be mis-split. Passing `con` resolves these against the
#' gazetteer and is strongly recommended for messy input.
#'
#' @export
#' @examples
#' normalize_address("302-1055 W Georgia St, Vancouver, BC V6E 3P3")
#' normalize_address("1234A-990 boul. du President-Kennedy Ouest, Montreal, QC")
#' # Structure the caller already has, kept instead of re-derived from a string
#' normalize_address("4 Oceanview Dr",
#'                   known = list(MUN_NAME = "Port Lorne", PROV_ABVN = "NS"))
#'
#' \dontrun{
#' con <- nar_connection()
#' normalize_address("100 queen st w toronto on", con = con)
#' DBI::dbDisconnect(con)
#' }
normalize_address <- function(x, known = NULL, con = NULL, ...) {
  if (!is.character(x)) {
    if (is.factor(x)) x <- as.character(x) else
      stop("`x` must be a character vector of address strings.")
  }
  k <- nar_known(known, length(x))

  out <- nar_parse_rules(x, prov = k$PROV_ABVN)

  # Onto the losing readings as well as the winner. The gazetteer probes every
  # candidate, and an assertion that reached only the arbitrated one would be
  # silently absent from the readings it is most needed to correct.
  cand <- attr(out, "nar_candidates")
  out <- nar_known_clear_mun(nar_known_apply(out, k), k)
  if (!is.null(cand)) {
    attr(out, "nar_candidates") <-
      nar_known_clear_mun(nar_known_apply(cand, k, cand$.row), k, cand$.row)
  }

  if (!is.null(con)) {
    res <- nar_resolve_gazetteer(out, con, known = k, ...)
    # Again, because the gazetteer writes NAR's own spelling back over every
    # component it matched -- including the municipality it may have
    # substituted. The caller asserted these, so they are what comes out.
    res <- nar_known_apply(res, k)
    asserted <- nar_known_has_mun(k, nrow(res))
    if (any(asserted)) {
      res$mun_remapped[asserted] <- FALSE
      res$mun_evidence[asserted] <- "kept"
    }
    return(res)
  }

  # The losing readings are only useful to the gazetteer; without one they are
  # internal detail rather than part of the return value.
  attr(out, "nar_candidates") <- NULL
  out
}

# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

#' Normalize an address string ahead of tokenizing
#'
#' @description Uppercases, drops the punctuation that only ever decorates
#' abbreviations (`St.`, `boul.`, `B.C.`), folds the several dash and fraction
#' characters real address data arrives with onto one form, and pads commas so
#' they survive as their own tokens. Accents are *kept* -- NAR stores them, and
#' the normalizer's output should match -- so folding to ASCII happens only in
#' [nar_fold()], at match time, and never on the way out.
#' @param x A character vector
#' @return A character vector
#' @keywords internal
nar_norm_text <- function(x) {
  x <- stringi::stri_trans_nfc(x)
  x <- toupper(x)
  # En/em dashes and non-breaking hyphens all mean "-" in an address.
  x <- gsub("[\u2010-\u2015\u2212]", "-", x)  # every dash Unicode offers
  x <- gsub("\u00bd", " 1/2", x, fixed = TRUE)  # the vulgar fraction one-half
  # Periods only ever abbreviate here; semicolons and newlines act as commas.
  x <- gsub("[.]", "", x)
  # "#" always introduces a unit, so give it its own token rather than letting
  # it fuse onto the number.
  x <- gsub("#", " # ", x, fixed = TRUE)
  # People type the unit-civic hyphen spaced ("302 - 1055"). Collapse it only
  # between a short label and digits, so a spaced hyphen used as a general
  # separator ("100 MAIN ST - APT 5") is left alone.
  x <- gsub("([0-9A-Z]{1,6}) *- *([0-9]+)(?= |$)", "\\1-\\2", x, perl = TRUE)
  x <- gsub("[;\n\r\t]", ",", x)
  # Commas become standalone tokens so the tokenizer can see the boundaries.
  x <- gsub(",", " , ", x, fixed = TRUE)
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

# ---------------------------------------------------------------------------
# Layer 1: the deterministic parser
# ---------------------------------------------------------------------------

#' The NAR-shaped columns normalize_address() produces
#' @return A character vector of column names
#' @keywords internal
nar_normalized_columns <- function() {
  c("APT_NO_LABEL", "CIVIC_NO", "CIVIC_NO_SUFFIX", "STREET_NAME",
    "STREET_TYPE", "STREET_DIR", "MUN_NAME", "PROV_ABVN", "POSTAL_CODE")
}

#' Parse address strings with the lexicon rules alone
#' @param x A character vector of address strings
#' @param prov Optional character vector of province codes, already recycled
#' @return A tibble, one row per input
#' @keywords internal
nar_parse_rules <- function(x, prov = NULL) {
  n <- length(x)
  txt <- nar_norm_text(x)

  # Read off the string as it arrived: the postal code, province and country
  # are about to be cut out of it, and a PO box marker can sit anywhere.
  marks <- nar_delivery_marks(txt)

  # --- leading prose ------------------------------------------------------
  # "located at 41 Cultus Rd", "attn: J Smith, 119 Markham St". Everything
  # downstream anchors the civic number at the front of the string, so this has
  # to come off before anything else reads it. Delivery lines are exempt: the
  # number in a PO box is not a civic number.
  keep <- is.na(marks)
  if (any(keep)) {
    txt[keep] <- vapply(txt[keep], nar_strip_lead_prose, character(1),
                        USE.NAMES = FALSE)
  }

  # --- postal code -------------------------------------------------------
  # Stored six characters with no space, so the space is optional on input and
  # absent on output. D, F, I, O, Q and U never open a Canadian FSA, and W and
  # Z never appear there either.
  pc_re <- "\\b([ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ]) ?([0-9][ABCEGHJKLMNPRSTVWXYZ][0-9])\\b"
  m <- regexpr(pc_re, txt, perl = TRUE)
  postal <- rep(NA_character_, n)
  # regexpr() reports NA for an NA input, which would make `hit` unusable as an
  # index; treat those as "no postal code" like any other unparseable string.
  hit <- !is.na(m) & m > 0
  if (any(hit)) {
    # regmatches() returns one element per *match*, not per input, so it is
    # already the length of sum(hit); indexing it again with `hit` would shift
    # every code onto the wrong row. The any() guard matters too: with no
    # matches at all it returns character(0), and a zero-length replacement is
    # an error rather than a no-op.
    postal[hit] <- gsub(" ", "", regmatches(txt, m), fixed = TRUE)
    txt[hit] <- trimws(sub(pc_re, " ", txt[hit], perl = TRUE))
  }

  # --- country -----------------------------------------------------------
  # A trailing "Canada" is pure noise here, but noise that displaces the
  # municipality: it takes the last comma segment, so "Ottawa, ON, Canada"
  # leaves the parser reading CANADA as the city and OTTAWA as part of the
  # street. It comes off before the province for the same reason.
  txt <- vapply(txt, nar_strip_country, character(1), USE.NAMES = FALSE)

  # --- province ----------------------------------------------------------
  # Matched against the trailing tokens only. "ONTARIO STREET" in Kingston is a
  # street, not a province, so a province word mid-string is left alone.
  province <- rep(NA_character_, n)
  for (i in seq_len(n)) {
    r <- nar_match_trailing_prov(txt[i])
    province[i] <- r$prov
    txt[i] <- r$rest
  }
  if (!is.null(prov)) {
    supplied <- nar_lex_lookup(nar_fold(prov), nar_lex_prov)
    province <- ifelse(is.na(province), supplied, province)
  }
  lang <- nar_prov_language(province)

  # --- street / municipality --------------------------------------------
  # Each string yields one or more readings rather than a parse; see
  # R/normalize_variants.R for why, and what chooses between them.
  parts <- vector("list", n)
  for (i in seq_len(n)) {
    parts[[i]] <- nar_parse_variants(txt[i], lang[i], province[i])
    parts[[i]]$.row <- i
  }
  # rbind() of nothing is a zero-column matrix, not a zero-row data frame, so an
  # empty input would reach the column references below as an atomic. One
  # throwaway parse supplies the shape instead -- geocoding a vector that a
  # filter emptied is a normal thing to do, and has to come back with the same
  # columns as any other call.
  parts <- if (n) do.call(rbind, c(parts, list(stringsAsFactors = FALSE))) else {
    empty <- nar_parse_variants("", "en", NA)[0, , drop = FALSE]
    empty$.row <- integer(0)
    empty
  }

  cand <- dplyr::tibble(
    input            = x[parts$.row],
    APT_NO_LABEL     = parts$unit,
    CIVIC_NO         = suppressWarnings(as.numeric(parts$civic)),
    CIVIC_NO_SUFFIX  = parts$suffix,
    STREET_NAME      = parts$name,
    STREET_TYPE      = parts$type,
    STREET_DIR       = parts$dir,
    MUN_NAME         = parts$mun,
    # The administrative half of the municipality, which the rules cannot know:
    # a census subdivision is not a name people write. Only the gazetteer can
    # fill it in, and it stays NA where nothing resolved.
    CSD_NAME         = NA_character_,
    PROV_ABVN        = province[parts$.row],
    POSTAL_CODE      = postal[parts$.row]
  )
  cand$pattern      <- nar_address_pattern(cand, parts$traits, marks[parts$.row])
  cand$confidence   <- nar_rules_confidence(cand)
  cand$parse_source <- "rules"
  # The rules parse only ever reports the municipality the string itself named,
  # so nothing here is remapped by construction. The gazetteer is the only layer
  # that can substitute a different one, and it overwrites this.
  cand$mun_remapped <- ifelse(is.na(cand$MUN_NAME), NA, FALSE)
  cand$mun_evidence <- ifelse(is.na(cand$MUN_NAME), NA_character_, "kept")
  cand$.row         <- parts$.row
  cand$.cand        <- parts$.cand
  cand$.strategy    <- parts$strategy

  res <- cand[nar_arbitrate_rules(cand), , drop = FALSE]
  res <- res[, setdiff(names(res), c(".row", ".cand", ".strategy")), drop = FALSE]
  # The losing readings ride along for nar_resolve_gazetteer(), which arbitrates
  # the same set again against evidence the rules do not have. An attribute
  # rather than an argument, so address_pattern() and every existing caller of
  # this function keep working unchanged.
  attr(res, "nar_candidates") <- cand
  res
}

#' Strip a trailing country name from a normalized string
#' @param s A single normalized address string
#' @return The string with any trailing country token removed
#' @keywords internal
nar_strip_country <- function(s) {
  if (is.na(s)) return(s)
  toks <- nar_tokens(s)
  if (!length(toks)) return(s)
  if (!nar_fold(toks[length(toks)]) %in% c("CANADA", "CAN")) return(s)
  rest <- utils::head(toks, -1)
  # The comma that separated it goes too, or the municipality is left as the
  # last-but-one segment of a trailing empty one.
  rest <- rest[!(seq_along(rest) == length(rest) & rest == ",")]
  paste(rest, collapse = " ")
}

#' Strip a leading prose prefix from a normalized address string
#'
#' @description Free-text address fields open with a great deal that is not the
#' address -- `"located at 41 Cultus Rd"`, `"attn: J Smith, 119 Markham St"`,
#' `"Toronto General Hospital, 200 Elizabeth St"`. Every civic-number rule in
#' this parser anchors on a number at the *front* of the string, so a prefix
#' does not degrade the parse, it collapses it: the prefix and the civic number
#' together are read as one street name and the pattern falls to `street_only`.
#' On the generated dirty corpus the affected classes go from 0--19% correct to
#' over 90% once the prefix comes off; see `inst/notes/deepparse.md`.
#'
#' Cutting to the first digit-initial token is easy to get wrong, because a lot
#' of legitimate address openings put words in front of the first number. Four
#' guards, and each one is holding back a real address form:
#'
#' * At most **one comma** may be crossed. The rule can reach past a care-of
#'   line or a building name, never past a municipality.
#' * A number that **closes its comma segment** is the tail of a street name,
#'   not the head of an address: `Highway 7`, `Line 5`, `Rang 9`, and the
#'   leading `Suite 200,` of `Suite 200, 119 Markham St`.
#' * A **unit designator** anywhere in the dropped run means the number is a
#'   unit: `Apt 4B-1234 Bloor St W`, `Unit 5 100 Main St`, `# 5 100 Main St`.
#'   So does a **digit inside a dropped token**, which is how an undesignated
#'   unit shows up: `PH12, 2160 Terry-Fox Av`, `E10, 20 Palace St`. Prose does
#'   not carry digits.
#' * A **street type or numbered-road word** directly in front of the number --
#'   after peeling the French particules that sit between them -- means the
#'   number belongs to the name: `Range Road 272`, `County Road 21 North`,
#'   `Chemin du 4e Rang`, `Avenue du 8 Mai`. Only the run *after* the last
#'   comma is examined, because a type separated from the number by a comma
#'   cannot be governing it (`Sunnybrook Health Sciences Centre, 2075 Bayview`).
#'
#' Strings carrying a delivery mark are exempt, and the caller enforces that: a
#' PO box or rural route line is an instruction rather than an address, and the
#' number in it is not a civic number.
#'
#' @param s A single normalized address string (post-[nar_norm_text()], so
#' uppercase with commas standing as their own tokens)
#' @return `s`, or `s` with the prefix removed
#' @keywords internal
nar_strip_lead_prose <- function(s) {
  if (is.na(s) || !nzchar(s)) return(s)
  toks <- nar_tokens(s)
  if (length(toks) < 3L) return(s)

  at <- which(grepl("^[0-9]", toks))
  if (!length(at)) return(s)
  at <- at[1]
  if (at == 1L) return(s)               # nothing in front of it

  drop <- toks[seq_len(at - 1L)]
  if (sum(drop == ",") > 1L) return(s)
  if (at == length(toks) || toks[at + 1L] == ",") return(s)

  f <- nar_fold(drop[drop != ","])
  if (!length(f)) return(s)
  if (any(f %in% c(nar_lex_unit_words, nar_lex_unit_bare, "#"))) return(s)
  # Prose does not carry digits; a token that does and is not the number we are
  # cutting to is a unit sitting in front of it, often in a comma segment of its
  # own -- "PH12, 2160 Terry-Fox Av", "Suite-1606, 80 Alton Towers Cir".
  if (any(grepl("[0-9]", f))) return(s)

  # The type test looks only at what shares a comma segment with the number.
  cut <- utils::tail(c(0L, which(drop == ",")), 1L)
  seg <- if (cut < length(drop)) nar_fold(drop[seq(cut + 1L, length(drop))]) else
    character(0)
  k <- length(seg)
  while (k >= 1L && nar_is_particule(seg[k])) k <- k - 1L
  if (k >= 1L && (nar_is_street_type(seg[k]) || seg[k] %in% nar_road_tail_words()))
    return(s)

  paste(toks[seq(at, length(toks))], collapse = " ")
}

#' Is this folded token a French particule or article?
#'
#' @description The words that join a street type to its specific -- `RUE DE LA
#' PAIX`. They are load-bearing in two places that both have to decide where a
#' name ends: the leading-prose strip peels them off before testing what governs
#' a number, and municipality anchoring rejects a reading whose street name is
#' nothing but these, which is what `135 de Nantes` leaves behind once Nantes is
#' taken for the municipality it also is.
#' @param x A folded token
#' @return `TRUE` for a particule
#' @keywords internal
nar_is_particule <- function(x) {
  x %in% c("DU", "DE", "DES", "D", "LA", "LE", "LES", "L", "AU", "AUX",
           "A", "EN") | grepl("^[DL]'", x)
}

#' The last word of every numbered-road surface form
#'
#' @description `nar_is_street_type()` claims most of them (`ROAD`, `RD`,
#' `CONCESSION`, `ROUTE`) but not all -- `MUN` is a numbered-road word and not a
#' street type -- so the leading-prose guard needs the lexicon's own tails too.
#' @return A character vector
#' @keywords internal
nar_road_tail_words <- function() {
  s <- nar_lex_numbered_roads$surface_fold
  s <- s[!is.na(s) & nzchar(s)]
  unique(vapply(strsplit(s, " ", fixed = TRUE),
                function(z) z[length(z)], character(1)))
}

#' Strip a trailing province name from a normalized string
#' @param s A single normalized address string
#' @return A list with `prov` and the remaining `rest`
#' @keywords internal
nar_match_trailing_prov <- function(s) {
  toks <- nar_tokens(s)
  if (!length(toks)) return(list(prov = NA_character_, rest = s))
  # Province names run to four tokens ("TERRITOIRES DU NORD-OUEST"); try the
  # longest tail first so "NEWFOUNDLAND AND LABRADOR" wins over "LABRADOR".
  for (k in min(4L, length(toks)):1L) {
    tail_toks <- utils::tail(toks, k)
    if (any(tail_toks == ",")) next
    cand <- nar_lex_lookup(nar_fold(paste(tail_toks, collapse = " ")), nar_lex_prov)
    if (!is.na(cand)) {
      rest <- utils::head(toks, length(toks) - k)
      rest <- rest[!(seq_along(rest) == length(rest) & rest == ",")]
      return(list(prov = cand, rest = paste(rest, collapse = " ")))
    }
  }
  list(prov = NA_character_, rest = s)
}

#' Split a normalized string into tokens, commas included
#' @param s A single normalized address string
#' @return A character vector of tokens
#' @keywords internal
nar_tokens <- function(s) {
  if (is.na(s) || !nzchar(s)) return(character(0))
  toks <- strsplit(s, " ", fixed = TRUE)[[1]]
  toks <- toks[nzchar(toks)]
  # A hyphen still standing on its own at this point is a separator that the
  # unit-civic collapse in nar_norm_text() declined to join, because what
  # followed it was not a bare number: "1688 - 152nd Street". It carries no
  # information, and left in place it becomes the first word of the street
  # name. Hyphens *inside* a token ("ST-JEAN", "302-1055") are untouched.
  toks[toks != "-"]
}

#' Parse one normalized address string into its components
#'
#' @description Walks the tokens left to right: unit, then civic number and
#' suffix, then direction, street type and name. The order matters -- the unit
#' has to come off before the civic number, or `302-1055` reads as a civic
#' number of 302 and the real one is lost.
#' @param s A single normalized string, province and postal code already removed
#' @param lang `"en"` or `"fr"`, deciding the canonical forms
#' @param prov A two-letter province code, or `NA`. Only the numbered-road
#' step consults it, and only for the entries that are province-specific.
#' @param mun_fixed A municipality already taken off the string by
#' [nar_mun_anchor_variants()], or `NA` to locate one here. When it is supplied
#' every remaining token is the street: the comma split no longer nominates a
#' municipality, and whatever trails the street type is dropped rather than
#' becoming one. That is what makes a trailing comma inconsequential --
#' `"6093 Iona Dr TH25"` and `"6093 Iona Dr TH25 ,"` are the same token stream
#' once the comma is gone.
#' @return A one-row data frame of components
#' @keywords internal
nar_parse_one <- function(s, lang = "en", prov = NA_character_,
                          mun_fixed = NA_character_) {
  empty <- data.frame(unit = NA_character_, civic = NA_character_,
                      suffix = NA_character_, name = NA_character_,
                      type = NA_character_, dir = NA_character_,
                      mun = NA_character_, traits = "",
                      stringsAsFactors = FALSE)
  toks <- nar_tokens(s)
  if (!length(toks)) return(empty)

  # Traits record *how* the string parsed rather than what it parsed to, so
  # nar_address_pattern() can tell apart forms that end in identical columns.
  traits <- if ("&" %in% toks) "intersection" else character(0)

  # Commas bound the street from the municipality. With two or more segments
  # the last one is the municipality and the rest are the street; with one, the
  # split has to be inferred after the street type is located.
  segs <- nar_split_commas(toks)
  seg_unit <- nar_take_unit_segments(segs, lang)
  segs <- seg_unit$segs
  mun <- mun_fixed
  if (!is.na(mun_fixed)) {
    # The municipality is already decided, so no segment is competing to be it
    # and the remaining commas carry no information.
    toks <- unlist(segs, use.names = FALSE)
  } else if (length(segs) >= 2) {
    mun <- paste(segs[[length(segs)]], collapse = " ")
    toks <- unlist(segs[-length(segs)], use.names = FALSE)
  } else {
    toks <- segs[[1]]
  }
  if (!length(toks)) return(transform(empty, mun = mun))

  unit <- seg_unit$unit; civic <- NA_character_; suffix <- NA_character_

  # --- leading unit ------------------------------------------------------
  lead <- nar_take_leading_unit(toks, lang)
  if (is.na(unit)) unit <- lead$unit
  civic <- lead$civic; toks <- lead$rest

  # --- civic number and suffix -------------------------------------------
  if (is.na(civic) && length(toks)) {
    cv <- nar_take_civic(toks)
    civic <- cv$civic; suffix <- cv$suffix; toks <- cv$rest
  }

  # --- trailing unit -----------------------------------------------------
  if (is.na(unit) && length(toks)) {
    tr <- nar_take_trailing_unit(toks)
    unit <- tr$unit; toks <- tr$rest
  }

  # --- numbered rural road, e.g. "RANGE ROAD 272" ------------------------
  # These carry no street type and, in practice, no direction either (99,556
  # blank against 113 with one), so a hit takes the whole street and the type
  # and direction steps below are skipped entirely. Left to them, "RANGE ROAD
  # 272" would read as name RANGE, type RD, and a stray 272 nobody claims.
  nr <- nar_take_numbered_road(toks, prov)
  if (!is.na(nr$name)) {
    if (is.na(mun) && length(nr$after)) mun <- paste(nr$after, collapse = " ")
    return(data.frame(unit = unit, civic = civic, suffix = suffix,
                      name = nr$name, type = NA_character_, dir = NA_character_,
                      mun = mun, traits = paste(c(traits, "numbered_road"),
                                                collapse = ","),
                      stringsAsFactors = FALSE))
  }

  # --- leading direction, e.g. "1055 W GEORGIA ST" -----------------------
  # The word is also recorded as it arrived, because stripping it is a guess:
  # some 92,000 NAR addresses sit on a street whose *name* opens with a compass
  # word -- "East Uniacke Rd", "West Beaver Creek Rd" -- and taking it off
  # leaves a probe that matches the mirror image of the street as readily as the
  # street. nar_parse_variants() offers the unstripped reading alongside this
  # one; the canonical abbreviation in `dir` is no use to it, since NAR spells
  # the word out inside the name and the match fold does not expand `E` to
  # `EAST`, so the original token has to travel.
  dir <- NA_character_
  dir_lead <- NA_character_
  if (length(toks) >= 3) {
    cand <- nar_lex_lookup(nar_fold(toks[1]), nar_lex_dirs, lang)
    if (!is.na(cand)) { dir <- cand; dir_lead <- toks[1]; toks <- toks[-1] }
  }

  # --- trailing direction, e.g. "QUEEN ST WEST" --------------------------
  if (is.na(dir) && length(toks) >= 2) {
    cand <- nar_lex_lookup(nar_fold(utils::tail(toks, 1)), nar_lex_dirs, lang)
    if (!is.na(cand)) { dir <- cand; toks <- utils::head(toks, -1) }
  }

  # --- street type -------------------------------------------------------
  ty <- nar_take_type(toks, lang)
  type <- ty$type; toks <- ty$rest
  if (isTRUE(ty$leads)) traits <- c(traits, "type_leads")

  # A direction can also sit between the type and the municipality
  # ("100 QUEEN ST WEST TORONTO"), which only becomes visible once the type is
  # consumed and the tail is no longer the end of the string.
  if (is.na(dir) && length(ty$after)) {
    cand <- nar_lex_lookup(nar_fold(ty$after[1]), nar_lex_dirs, lang)
    if (!is.na(cand)) { dir <- cand; ty$after <- ty$after[-1] }
  }

  # Whatever trails the street in a comma-less string is the municipality.
  if (is.na(mun) && length(ty$after)) mun <- paste(ty$after, collapse = " ")

  # With the municipality already settled -- by a comma, or by anchoring -- a
  # lone unit-shaped token after the street type has nowhere else to belong:
  # "100 Main St TH25, Vancouver" is a townhouse. It used to be dropped
  # silently, which read as a clean parse of an address missing its unit.
  else if (is.na(unit) && length(ty$after) == 1 &&
           nar_is_undesignated_unit(ty$after)) {
    unit <- ty$after
  }

  name <- if (length(toks)) paste(toks, collapse = " ") else NA_character_

  out <- data.frame(unit = unit, civic = civic, suffix = suffix, name = name,
                    type = type, dir = dir, mun = mun,
                    traits = paste(traits, collapse = ","),
                    stringsAsFactors = FALSE)
  # An attribute rather than a column: every other return path above would have
  # to carry it, and rbind() over the readings drops it in any case.
  attr(out, "dir_lead") <- dir_lead
  out
}

#' Split a token vector on comma tokens
#' @param toks A character vector of tokens
#' @return A list of non-empty token vectors
#' @keywords internal
nar_split_commas <- function(toks) {
  idx <- cumsum(toks == ",")
  segs <- split(toks[toks != ","], idx[toks != ","])
  segs <- segs[vapply(segs, length, integer(1)) > 0]
  if (!length(segs)) list(character(0)) else unname(segs)
}

#' Lift comma-delimited unit segments out of a split address
#'
#' @description A segment that is nothing but a unit -- `", 320,"`, `", # 500,"`,
#' `", Suite 600,"`, `", 5th Floor,"` -- is a form real filings use constantly,
#' and one the segment split otherwise mangles: it is neither the street nor the
#' municipality, so it gets absorbed into whichever neighbour it is handed to.
#' `"9320 Boulevard Saint-Laurent, 320, Montreal"` read its street name as
#' `SAINT-LAURENT 320` before this existed.
#'
#' The last remaining segment is the municipality, so a unit segment has to come
#' out before that choice is made rather than after it.
#'
#' @param segs A list of token vectors from [nar_split_commas()]
#' @param lang `"en"` or `"fr"`
#' @return A list of `unit` (or `NA`) and the remaining `segs`
#' @keywords internal
nar_take_unit_segments <- function(segs, lang = "en") {
  none <- list(unit = NA_character_, segs = segs)
  # With one segment there is nothing to lift: it is the whole address.
  if (length(segs) < 2) return(none)

  is_num <- function(t) grepl("^[0-9]+[A-Z]?$", t)

  # STE is the one unit designator that is also an ordinary word: it
  # abbreviates Suite, but it is equally Sainte, so left unguarded "Sault Ste.
  # Marie" reads as a unit called "Sault Marie" and the municipality is lost.
  # Only for those words is the value required to look like a unit number, which
  # is what nar_is_unit_value() tests. Every other designator is unambiguous and
  # keeps taking whatever follows it, because "Apt Bsmt" and "Apt Trlr" are real
  # units whose value is a word rather than a number.
  needs_number <- function(f) any(f %in% nar_lex_unit_ambiguous)

  classify <- function(seg) {
    f <- nar_fold(seg)
    n <- length(seg)
    # "# 500" -- nar_norm_text() makes the hash its own token.
    if (n == 2 && seg[1] == "#" && is_num(seg[2])) return(seg[2])
    # A bare number standing alone between commas is never a municipality.
    if (n == 1 && is_num(seg[1])) return(seg[1])
    # A bare label: ", BSMT,".
    if (n == 1 && f %in% nar_lex_unit_bare) return(seg[1])
    # A unit word plus its value, in either order: "Suite 600", "5th Floor".
    # Capped at three tokens so a street segment that merely happens to contain
    # one of these words is not swallowed whole.
    if (n <= 3 && any(f %in% nar_lex_unit_words)) {
      value <- seg[!(f %in% nar_lex_unit_words) & seg != "#"]
      if (!length(value)) return(seg[1])
      if (!needs_number(f) || all(nar_is_unit_value(value)))
        return(paste(value, collapse = " "))
    }
    NA_character_
  }

  cand <- vapply(segs, classify, character(1))

  # A leading bare number is the civic number, not a unit, unless the segment
  # after it carries one of its own. "845, rue de Vernon" is the ordinary
  # French civic-comma-street form and by far the commoner reading; "302, 1055
  # W Georgia St" is a unit only because 1055 is already there to be the civic
  # number. Nothing but the following segment distinguishes them.
  if (!is.na(cand[1]) && length(segs[[1]]) == 1 && is_num(segs[[1]][1]) &&
      !grepl("^[0-9]", segs[[2]][1])) {
    cand[1] <- NA_character_
  }

  hit <- which(!is.na(cand))
  # Something has to survive to be the address.
  if (!length(hit) || length(hit) == length(segs)) return(none)

  list(unit = cand[hit[1]], segs = segs[-hit])
}

#' Take a leading unit designator off the front of a street
#'
#' @description Handles the four leading forms Canadian addresses use: the
#' hyphenated `302-1055`, an explicit designator (`APT 302`, `BUREAU 12`), a
#' `#302`, and a bare label such as `BSMT`. This is the step that has to be
#' right -- a unit left attached is read as the civic number, and the real
#' civic number is then lost entirely.
#' @param toks A character vector of tokens
#' @param lang `"en"` or `"fr"`
#' @return A list with `unit`, `civic` (set only by the hyphenated form) and `rest`
#' @keywords internal
nar_take_leading_unit <- function(toks, lang = "en") {
  none <- list(unit = NA_character_, civic = NA_character_, rest = toks)
  if (!length(toks)) return(none)
  first <- toks[1]

  # "#302 1055 ...". Normalization split the "#" off, and the value it
  # introduces may itself be hyphenated ("#5-123"), so it goes through the same
  # split as the bare hyphenated form below.
  if (first == "#" && length(toks) >= 2) {
    sp <- nar_split_unit_civic(toks[2])
    return(list(unit = sp$unit, civic = sp$civic, rest = toks[-(1:2)]))
  }

  # "302-1055 W GEORGIA ST". The trailing half must be all digits and the
  # leading half a short alphanumeric label; anything longer is a street name
  # that happens to be hyphenated.
  sp <- nar_split_unit_civic(first)
  if (!is.na(sp$civic)) {
    return(list(unit = sp$unit, civic = sp$civic, rest = toks[-1]))
  }

  # "APT 302 1055 ...", "UNIT 4B 100 ...". Requires something to follow the
  # unit value, or the whole string is just a unit and no street.
  #
  # The value itself may be hyphenated -- "SUITE 800-666 BURRARD ST" is the
  # standard Canadian office form -- so it goes through the same split as the
  # "#" branch above. Taking toks[2] whole made the designator *worse* than no
  # designator at all: bare "800-666 BURRARD ST" split correctly while the
  # spelled-out version yielded a unit of "800-666" and no civic number, which
  # is the one thing that cannot be recovered downstream.
  if (nar_fold(first) %in% nar_lex_unit_words && length(toks) >= 3 &&
      (!nar_fold(first) %in% nar_lex_unit_ambiguous ||
       nar_is_unit_value(toks[2]))) {
    sp <- nar_split_unit_civic(toks[2])
    return(list(unit = sp$unit, civic = sp$civic, rest = toks[-(1:2)]))
  }

  # "BSMT 1055 ..." -- a bare label standing in for a unit number.
  if (nar_fold(first) %in% nar_lex_unit_bare && length(toks) >= 2 &&
      grepl("^[0-9]", toks[2])) {
    return(list(unit = first, civic = NA_character_, rest = toks[-1]))
  }

  none
}

#' Take a numbered rural road off the front of a street
#'
#' @description The prairie grid and its cousins name a road with a phrase and
#' a number and no street type at all: NAR files `Range Road 272` as
#' `OFFICIAL_STREET_NAME` with `OFFICIAL_STREET_TYPE` empty, and the same holds
#' for Alberta township roads, New Brunswick routes, Ontario concessions and
#' county roads, and Manitoba's `Mun` roads. Left to the ordinary path these
#' parse as name `RANGE` type `RD` plus a stray `272`, which joins to nothing.
#'
#' Two collisions make this narrower than it looks. `Highway 7` is *not* one of
#' these -- NAR stores it as name `7` type `HWY`, 115,175 rows -- so highways
#' are deliberately absent from the crosswalk. And `Route` splits by province:
#' New Brunswick writes typeless `Route 105` (50,942 rows) while Quebec files
#' `Route 132` as name `132` type `ROUTE` (56,673 rows). Entries carrying a
#' `prov` therefore fire only in that province, and never when the province is
#' unknown, which leaves the commoner reading in place.
#'
#' @param toks A character vector of tokens, civic number already removed
#' @param prov A two-letter province code, or `NA`
#' @return A list with `name` (`NA` if no match) and the `after` tokens the
#' phrase did not consume
#' @keywords internal
nar_take_numbered_road <- function(toks, prov = NA_character_) {
  none <- list(name = NA_character_, after = toks)
  if (length(toks) < 2) return(none)
  f <- nar_fold(toks)

  # The number that closes the phrase. A trailing letter is real -- NAR carries
  # Range Road 212A -- but nothing longer, or a street name gets swallowed.
  is_num <- function(i) i <= length(f) && grepl("^[0-9]+[A-Z]?$", f[i])

  lex <- nar_lex_numbered_roads
  lex <- lex[!nzchar(lex$prov) | (!is.na(prov) & lex$prov == prov), , drop = FALSE]

  # A leading number can belong to the *name*: 53222 Range Road 272 is one
  # street, and its addresses carry their own small civic numbers. The civic
  # number has already been taken by the time we get here, so a number still
  # sitting in front of the phrase is part of the name by elimination.
  starts <- if (is_num(1)) c(1L, 2L) else 1L

  for (start in starts) {
    for (k in seq(min(3L, length(f) - start), 1L)) {
      phrase <- paste(f[seq(start, start + k - 1L)], collapse = " ")
      j <- match(phrase, lex$surface_fold)
      if (is.na(j) || !is_num(start + k)) next
      canonical <- lex$canonical[j]
      lead <- if (start == 2L) paste0(toks[1], " ") else ""
      return(list(name = paste0(lead, canonical, " ", toks[start + k]),
                  after = toks[-seq_len(start + k)]))
    }
  }

  # The open family: a proper name in front of a road word, still closed by a
  # number -- Bruce Road 1, Southgate Sideroad 21, Ramsay Concession 4. Only
  # the second token may be the road word, so this cannot reach past a name.
  road <- c(ROAD = "ROAD", RD = "ROAD", CONCESSION = "CONCESSION",
            SIDEROAD = "SIDEROAD", SDRD = "SIDEROAD")
  if (length(f) >= 3 && f[2] %in% names(road) && is_num(3) &&
      !grepl("^[0-9]", f[1]) && !nar_is_street_type(f[1]) &&
      is.na(nar_lex_lookup(f[1], nar_lex_dirs)) &&
      !f[1] %in% c(nar_lex_unit_words, nar_lex_unit_bare)) {
    return(list(name = paste(toks[1], road[[f[2]]], toks[3]),
                after = toks[-(1:3)]))
  }

  none
}

#' Split a hyphenated unit-civic token
#'
#' @description `302-1055` is a unit and a civic number, the near-universal
#' Canadian convention. The trailing half must be all digits and the leading
#' half a short alphanumeric label; a longer leading half is a hyphenated street
#' name, not a unit.
#' @param tok A single token
#' @return A list with `unit` and `civic`, the latter `NA` if the token did not split
#' @keywords internal
nar_split_unit_civic <- function(tok) {
  m <- regmatches(tok, regexec("^([0-9A-Z]{1,6})-([0-9]+)$", tok))[[1]]
  if (length(m) == 3) list(unit = m[2], civic = m[3])
  else list(unit = tok, civic = NA_character_)
}

#' Take the civic number and its suffix off the front of a street
#'
#' @description `CIVIC_NO_SUFFIX` holds a single letter or `1/2` and nothing
#' else, so only those two forms are recognised. The letter is taken only when
#' it is attached to the digits (`990A`): a spaced `990 W` is a direction far
#' more often than a suffix, by roughly three orders of magnitude.
#' @param toks A character vector of tokens
#' @return A list with `civic`, `suffix` and `rest`
#' @keywords internal
nar_take_civic <- function(toks) {
  none <- list(civic = NA_character_, suffix = NA_character_, rest = toks)
  if (!length(toks)) return(none)
  first <- toks[1]

  if (grepl("^[0-9]+$", first)) {
    if (length(toks) >= 2 && toks[2] == "1/2") {
      return(list(civic = first, suffix = "1/2", rest = toks[-(1:2)]))
    }
    return(list(civic = first, suffix = NA_character_, rest = toks[-1]))
  }

  m <- regmatches(first, regexec("^([0-9]+)([A-Z])$", first))[[1]]
  if (length(m) == 3) {
    return(list(civic = m[2], suffix = m[3], rest = toks[-1]))
  }

  # A civic range ("1000-1010 MAIN ST") that nar_take_leading_unit() declined
  # because both halves are long; the low end is the address.
  m <- regmatches(first, regexec("^([0-9]+)-[0-9]+$", first))[[1]]
  if (length(m) == 2) {
    return(list(civic = m[2], suffix = NA_character_, rest = toks[-1]))
  }

  none
}

#' Does this token look like a unit number rather than a word?
#'
#' @description The test that keeps the ambiguous designators in
#' `nar_lex_unit_ambiguous` -- which is `STE`, Suite and equally Sainte -- from
#' taking an ordinary word as their value. A unit number carries a digit
#' (`600`, `4B`, `5TH`) or is a lone letter (`A`). It is applied only to those
#' designators: `APT BSMT` and `APT TRLR` are real units whose value is a word,
#' and requiring a number everywhere collapses them into the street name.
#' @param x A character vector of tokens, unfolded
#' @return A logical vector
#' @keywords internal
nar_is_unit_value <- function(x) {
  f <- nar_fold(x)
  grepl("[0-9]", f) | grepl("^[A-Z]$", f)
}

#' Does this token announce itself as a unit with no designator in front of it?
#'
#' @description Narrower than [nar_is_unit_value()], and deliberately so: that
#' test asks whether a value *offered* as a unit looks like one, with a
#' designator already vouching for it. This one has no such warrant, so it has
#' to carry the claim itself.
#'
#' A bare number does not. `Cascumpec - Rte 12` and `Chicoltin-Bella Coola
#' Highway 20` are street names that end in one, and reading the number as a
#' unit takes it off a name that needs it -- both were measured, in the Part A
#' sample. A letter-and-digit token (`TH25`, `4B`, `PH2`) is not a street name's
#' last word in any of the 374k NAR carries.
#' @param x A single token, unfolded
#' @return `TRUE` when the token can stand as a unit unaided
#' @keywords internal
nar_is_undesignated_unit <- function(x) {
  f <- nar_fold(x)
  grepl("^[0-9]+[A-Z]$", f) | grepl("^[A-Z]{1,3}[0-9]+[A-Z]?$", f)
}

#' Take a trailing unit designator off the end of a street
#' @param toks A character vector of tokens
#' @return A list with `unit` and `rest`
#' @keywords internal
nar_take_trailing_unit <- function(toks) {
  none <- list(unit = NA_character_, rest = toks)
  n <- length(toks)
  if (!n) return(none)
  last <- toks[n]

  # "100 MAIN ST # 25". nar_norm_text() gives "#" a token of its own, so a
  # trailing "#25" arrives here as the *pair* ("#", "25") and never as one
  # token -- which is why testing `last` for a leading "#" matched nothing.
  # Three tokens are required so a street survives the removal.
  if (n >= 3 && toks[n - 1] == "#" && nar_is_unit_value(last)) {
    return(list(unit = last, rest = utils::head(toks, -2)))
  }
  # The same STE guard nar_take_unit_segments() applies, for the same reason:
  # in a comma-less string the municipality is not a segment of its own, so
  # "123 Main St Sault Ste Marie ON" reaches here with STE second-from-last and
  # without this reads as a unit called "Marie" on a street in "Sault".
  if (n >= 3 && nar_fold(toks[n - 1]) %in% nar_lex_unit_words &&
      (!nar_fold(toks[n - 1]) %in% nar_lex_unit_ambiguous ||
       nar_is_unit_value(last))) {
    return(list(unit = last, rest = utils::head(toks, -2)))
  }
  if (n >= 2 && nar_fold(last) %in% nar_lex_unit_bare) {
    # Only if a street survives the removal. "1 Boulevard Main" ends in a bare
    # label, but taking it leaves nothing but the type -- there the trailing
    # word is the street name, not a unit.
    rest <- utils::head(toks, -1)
    if (any(!nar_is_street_type(nar_fold(rest)))) {
      return(list(unit = last, rest = rest))
    }
  }
  none
}

#' Locate the street type among the remaining tokens
#'
#' @description French types lead the name (`RUE NOTRE-DAME`, `CH DU LAC`) while
#' English types trail it (`QUEEN ST`), so the two languages are scanned from
#' opposite ends. A type is never taken from the only token left -- a street
#' genuinely named `PARK` or `GREEN` has to keep its name.
#'
#' Where several positions are structurally valid, as happens in a comma-less
#' string whose municipality contains a street-type word, the tie breaks on how
#' often each type occurs in NAR.
#' @param toks A character vector of tokens
#' @param lang `"en"` or `"fr"`
#' @return A list with `type`, the preceding `rest`, and any tokens `after` it
#' @keywords internal
nar_take_type <- function(toks, lang = "en") {
  none <- list(type = NA_character_, rest = toks, after = character(0),
               leads = FALSE)
  n <- length(toks)
  if (n < 2) return(none)

  # French: the type leads, so check the first token before anything else.
  if (identical(lang, "fr")) {
    lead <- nar_lex_lookup(nar_fold(toks[1]), nar_lex_types, lang)
    if (!is.na(lead)) {
      return(list(type = lead, rest = toks[-1], after = character(0),
                  leads = TRUE))
    }
  }

  cand <- integer(0)
  for (i in 2:n) if (nar_is_street_type(nar_fold(toks[i]), lang)) cand <- c(cand, i)
  if (!length(cand)) {
    # An English string can still lead with a French type, as bilingual
    # municipalities do; take it rather than losing the type altogether.
    lead <- nar_lex_lookup(nar_fold(toks[1]), nar_lex_types, lang)
    if (n >= 2 && !is.na(lead)) {
      return(list(type = lead, rest = toks[-1], after = character(0),
                  leads = TRUE))
    }
    return(none)
  }

  # Prefer a position with nothing but directions after it -- that is the end
  # of the street proper. Otherwise fall back on the most common type.
  tail_ok <- vapply(cand, function(i) {
    if (i == n) return(TRUE)
    all(nar_is_street_dir(nar_fold(toks[(i + 1):n]), lang))
  }, logical(1))

  pick <- if (any(tail_ok)) max(cand[tail_ok]) else {
    freq <- vapply(cand, function(i) {
      idx <- match(nar_fold(toks[i]), nar_lex_types$surface_fold)
      if (is.na(idx)) 0 else nar_lex_types$freq[idx]
    }, numeric(1))
    cand[which.max(freq)]
  }

  list(type  = nar_lex_lookup(nar_fold(toks[pick]), nar_lex_types, lang),
       rest  = utils::head(toks, pick - 1),
       after = if (pick < n) toks[(pick + 1):n] else character(0),
       leads = FALSE)
}

#' Score how completely the rules parsed each address
#'
#' @description A blunt completeness score, not a probability: the share of the
#' components that a joinable address needs which actually came out populated.
#' Layer 2 replaces it with a match score where it can.
#' @param res A tibble of parsed components
#' @return A numeric vector in `[0, 1]`
#' @keywords internal
nar_rules_confidence <- function(res) {
  have <- function(x) as.integer(!is.na(x) & (is.numeric(x) | nzchar(as.character(x))))
  score <- have(res$CIVIC_NO) * 3 + have(res$STREET_NAME) * 3 +
    have(res$STREET_TYPE) + have(res$MUN_NAME) * 2 +
    have(res$PROV_ABVN) + have(res$POSTAL_CODE)
  round(score / 11, 3)
}
