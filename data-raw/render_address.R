# The noise grammar: NAR rows -> the messy surface forms people actually type.
#
# Sourced rather than run. data-raw/eval_normalize.R uses it to generate labelled
# eval inputs; a fine-tune generator would use the same grammar so the model is
# trained on the distribution it is measured against.
#
# Every knob is recorded alongside the rendered string, so a failure can be
# attributed to the transformation that caused it rather than guessed at.

# The long form of a canonical token is simply the longest surface that maps to
# it, which is what data-raw/street_types.csv already encodes: ST <- STREET,
# BOUL <- BOULEVARD, O <- OUEST.
nar_long_forms <- function(lex) {
  ok <- lex$lang %in% c("en", "fr", "both")
  key <- paste(lex$canonical[ok], lex$lang[ok])
  surf <- lex$surface[ok]
  best <- tapply(surf, key, function(s) s[which.max(nchar(s))])
  stats::setNames(as.character(best), names(best))
}

nar_long_form <- function(canonical, lang, table) {
  hit <- table[paste(canonical, lang)]
  hit[is.na(hit)] <- table[paste(canonical[is.na(hit)], "both")]
  ifelse(is.na(hit), canonical, hit)
}

# A single-character substitution with a physically adjacent key -- the typo a
# person actually makes, as opposed to a uniformly random letter.
nar_qwerty <- local({
  rows <- c("qwertyuiop", "asdfghjkl", "zxcvbnm")
  nb <- list()
  for (r in rows) {
    ch <- strsplit(r, "")[[1]]
    for (i in seq_along(ch)) nb[[ch[i]]] <- ch[setdiff(c(i - 1, i + 1), c(0, length(ch) + 1))]
  }
  nb
})

nar_typo <- function(x) {
  vapply(x, function(s) {
    ch <- strsplit(tolower(s), "")[[1]]
    at <- which(ch %in% names(nar_qwerty))
    if (!length(at)) return(s)
    i <- at[sample.int(length(at), 1)]
    alt <- nar_qwerty[[ch[i]]]
    ch[i] <- alt[sample.int(length(alt), 1)]
    toupper(paste(ch, collapse = ""))
  }, character(1), USE.NAMES = FALSE)
}

nar_titlecase <- function(x) {
  gsub("\\b([a-z])", "\\U\\1", tolower(x), perl = TRUE)
}

#' Render NAR rows into noisy surface strings
#'
#' @param rows A data frame of NAR Addresses rows
#' @param p Named list of probabilities for the optional transformations
#' @return `rows` with a `text` column and one column per knob that fired
nar_render_surface <- function(rows,
                               p = list(typo = 0.10, canada = 0.05, unit = 0.25)) {
  n <- nrow(rows)
  types <- utils::read.csv("data-raw/street_types.csv", encoding = "UTF-8",
                           stringsAsFactors = FALSE)
  long_type <- nar_long_forms(types)
  long_dir  <- nar_long_forms(cangeocode:::nar_lex_dirs)

  fr <- rows$MAIL_PROV_ABVN == "QC"
  lang <- ifelse(fr, "fr", "en")

  pick <- function(opts, prob) opts[sample.int(length(opts), n, replace = TRUE, prob = prob)]
  knob <- data.frame(
    type_form   = pick(c("canon", "long", "drop"),          c(0.45, 0.40, 0.15)),
    dir_form    = pick(c("canon", "long", "drop"),          c(0.50, 0.35, 0.15)),
    unit_form   = pick(c("dash", "prefix", "hash", "suffix"), rep(0.25, 4)),
    mun_form    = pick(c("keep", "fold", "drop"),           c(0.55, 0.30, 0.15)),
    prov_form   = pick(c("abbr", "long", "drop"),           c(0.55, 0.20, 0.25)),
    postal_form = pick(c("spaced", "tight", "drop"),        c(0.30, 0.25, 0.45)),
    case_form   = pick(c("upper", "lower", "title"),        c(0.30, 0.35, 0.35)),
    has_typo    = stats::runif(n) < p$typo,
    has_canada  = stats::runif(n) < p$canada,
    has_unit    = stats::runif(n) < p$unit & nzchar(rows$APT_NO_LABEL),
    stringsAsFactors = FALSE
  )
  # A unit that is not rendered cannot be recovered, so it is not a label either.
  knob$unit_form[!knob$has_unit] <- NA_character_

  name <- rows$OFFICIAL_STREET_NAME
  name[knob$has_typo] <- nar_typo(name[knob$has_typo])

  type <- ifelse(knob$type_form == "drop", "",
                 ifelse(knob$type_form == "long",
                        nar_long_form(rows$OFFICIAL_STREET_TYPE, lang, long_type),
                        rows$OFFICIAL_STREET_TYPE))
  type[!nzchar(rows$OFFICIAL_STREET_TYPE)] <- ""

  dir <- ifelse(knob$dir_form == "drop", "",
                ifelse(knob$dir_form == "long",
                       nar_long_form(rows$OFFICIAL_STREET_DIR, lang, long_dir),
                       rows$OFFICIAL_STREET_DIR))
  dir[!nzchar(rows$OFFICIAL_STREET_DIR)] <- ""

  # French puts the type in front of the name, English behind it. Getting this
  # backwards is the single most common way a naive parser fails in Quebec.
  street <- ifelse(fr, trimws(paste(type, name, dir)),
                   trimws(paste(name, type, dir)))
  street <- gsub(" +", " ", street)

  civic <- paste0(rows$CIVIC_NO, rows$CIVIC_NO_SUFFIX)
  unit <- rows$APT_NO_LABEL
  head <- ifelse(!knob$has_unit, paste(civic, street),
          ifelse(knob$unit_form == "dash",   paste0(unit, "-", civic, " ", street),
          ifelse(knob$unit_form == "hash",   paste0("#", unit, " ", civic, " ", street),
          ifelse(knob$unit_form == "prefix", paste0(ifelse(fr, "App ", "Apt "), unit,
                                                    ", ", civic, " ", street),
                                             paste0(civic, " ", street, ", ",
                                                    ifelse(fr, "Bureau ", "Suite "), unit)))))

  mun <- ifelse(knob$mun_form == "drop", "",
                ifelse(knob$mun_form == "fold",
                       cangeocode:::nar_fold(rows$MAIL_MUN_NAME), rows$MAIL_MUN_NAME))

  prov_long <- c(NL = "Newfoundland and Labrador", PE = "Prince Edward Island",
                 NS = "Nova Scotia", NB = "New Brunswick", QC = "Quebec",
                 ON = "Ontario", MB = "Manitoba", SK = "Saskatchewan",
                 AB = "Alberta", BC = "British Columbia", YT = "Yukon",
                 NT = "Northwest Territories", NU = "Nunavut")
  prov <- ifelse(knob$prov_form == "drop", "",
                 ifelse(knob$prov_form == "long",
                        prov_long[rows$MAIL_PROV_ABVN], rows$MAIL_PROV_ABVN))
  prov[is.na(prov)] <- ""

  pc <- rows$MAIL_POSTAL_CODE
  postal <- ifelse(knob$postal_form == "drop" | nchar(pc) != 6, "",
                   ifelse(knob$postal_form == "spaced",
                          paste0(substr(pc, 1, 3), " ", substr(pc, 4, 6)), pc))

  tail <- ifelse(knob$has_canada, "Canada", "")
  parts <- cbind(head, mun, trimws(paste(prov, postal)), tail)
  text <- apply(parts, 1, function(r) paste(r[nzchar(trimws(r))], collapse = ", "))

  text <- ifelse(knob$case_form == "lower", tolower(text),
                 ifelse(knob$case_form == "title", nar_titlecase(text), toupper(text)))

  cbind(rows, knob, text = text, stringsAsFactors = FALSE)
}
