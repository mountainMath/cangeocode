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
#' @param prov Optional two-letter province code (recycled against `x`) to use
#' when the string does not name one. Canonicalization is language-conditioned,
#' so this materially changes the result: `"avenue"` normalizes to `AVE` in
#' Ontario and `AV` in Quebec.
#' @param con An open NAR connection. Supplying one enables gazetteer
#' resolution; without it parsing is lexicon-only. The caller keeps ownership --
#' a connection passed here is left open, matching [reverse_geocode()].
#' @param ... Additional arguments (currently unused)
#'
#' @return A tibble with one row per element of `x`, carrying the NAR-shaped
#' columns `APT_NO_LABEL`, `CIVIC_NO`, `CIVIC_NO_SUFFIX`, `STREET_NAME`,
#' `STREET_TYPE`, `STREET_DIR`, `MUN_NAME`, `PROV_ABVN` and `POSTAL_CODE`,
#' alongside the original `input`, the structural `pattern` it parsed as (see
#' [address_pattern()] for the buckets), a `confidence` in `[0, 1]` and a
#' `parse_source` of `"rules"` or `"gazetteer"`.
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
#'
#' \dontrun{
#' con <- nar_connection()
#' normalize_address("100 queen st w toronto on", con = con)
#' DBI::dbDisconnect(con)
#' }
normalize_address <- function(x, prov = NULL, con = NULL, ...) {
  if (!is.character(x)) {
    if (is.factor(x)) x <- as.character(x) else
      stop("`x` must be a character vector of address strings.")
  }
  if (!is.null(prov)) prov <- rep_len(as.character(prov), length(x))

  out <- nar_parse_rules(x, prov = prov)

  if (!is.null(con)) out <- nar_resolve_gazetteer(out, con)

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
  parts <- vector("list", n)
  for (i in seq_len(n)) parts[[i]] <- nar_parse_one(txt[i], lang[i], province[i])
  parts <- do.call(rbind, c(parts, list(stringsAsFactors = FALSE)))

  res <- dplyr::tibble(
    input            = x,
    APT_NO_LABEL     = parts$unit,
    CIVIC_NO         = suppressWarnings(as.numeric(parts$civic)),
    CIVIC_NO_SUFFIX  = parts$suffix,
    STREET_NAME      = parts$name,
    STREET_TYPE      = parts$type,
    STREET_DIR       = parts$dir,
    MUN_NAME         = parts$mun,
    PROV_ABVN        = province,
    POSTAL_CODE      = postal
  )
  res$pattern      <- nar_address_pattern(res, parts$traits, marks)
  res$confidence   <- nar_rules_confidence(res)
  res$parse_source <- "rules"
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
  toks[nzchar(toks)]
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
#' @return A one-row data frame of components
#' @keywords internal
nar_parse_one <- function(s, lang = "en", prov = NA_character_) {
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
  mun <- NA_character_
  if (length(segs) >= 2) {
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
  dir <- NA_character_
  if (length(toks) >= 3) {
    cand <- nar_lex_lookup(nar_fold(toks[1]), nar_lex_dirs, lang)
    if (!is.na(cand)) { dir <- cand; toks <- toks[-1] }
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

  name <- if (length(toks)) paste(toks, collapse = " ") else NA_character_

  data.frame(unit = unit, civic = civic, suffix = suffix, name = name,
             type = type, dir = dir, mun = mun,
             traits = paste(traits, collapse = ","), stringsAsFactors = FALSE)
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
  # Only for those words is the value required to look like a unit number -- a
  # digit ("600", "4B", "5TH") or a lone letter ("A"). Every other designator
  # is unambiguous and keeps taking whatever follows it, because "Apt Bsmt"
  # and "Apt Trlr" are real units whose value is a word rather than a number.
  is_unit_value <- function(v) grepl("[0-9]", v) | grepl("^[A-Z]$", v)
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
      if (!needs_number(f) || all(is_unit_value(nar_fold(value))))
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
  if (nar_fold(first) %in% nar_lex_unit_words && length(toks) >= 3 &&
      (!nar_fold(first) %in% nar_lex_unit_ambiguous ||
       grepl("[0-9]|^[A-Z]$", nar_fold(toks[2])))) {
    return(list(unit = toks[2], civic = NA_character_, rest = toks[-(1:2)]))
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

#' Take a trailing unit designator off the end of a street
#' @param toks A character vector of tokens
#' @return A list with `unit` and `rest`
#' @keywords internal
nar_take_trailing_unit <- function(toks) {
  none <- list(unit = NA_character_, rest = toks)
  n <- length(toks)
  if (!n) return(none)
  last <- toks[n]

  if (grepl("^#.+", last)) {
    return(list(unit = sub("^#", "", last), rest = utils::head(toks, -1)))
  }
  if (n >= 3 && nar_fold(toks[n - 1]) %in% nar_lex_unit_words) {
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
