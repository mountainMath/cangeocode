# Is deepparse better at this than the parser in this package?
#
# deepparse (github.com/GRAAL-Research/deepparse) is a neural address *tagger*
# trained on 20 countries' worth of OpenAddresses, and Canada is one of them.
# It is the strongest off-the-shelf answer to the same question this package
# answers with rules and a gazetteer, so it sets the bar that a fine-tune or a
# from-scratch model would have to clear before either is worth building.
#
# The comparison is not symmetric, and pretending it is would flatter one side
# or the other. deepparse assigns each token of the input a tag. It never
# expands `st`, never picks between Street and Saint, never decides that
# `NOTRE-DAME RUE O` and `Rue Notre-Dame Ouest` are the same street, and never
# consults a register. So it is measured three ways, and all three are reported:
#
#   cangeocode  normalize_address() as shipped.
#   deepparse   the tagger alone.
#   dp -> norm  deepparse used as a *segmenter*: its tags are reassembled into a
#               clean comma-delimited string and handed to normalize_address().
#               This is what someone would actually build if the tagger were
#               good, and it is the configuration that says whether its
#               segmentation is worth anything on top of ours.
#
# and on four corpora, because they fail differently:
#
#   A       eval_normalize.R's rendered NAR rows. Labelled, and the mess we
#           imagined.
#   llm     data-raw/dirty_corpus.R's generated half. Labelled, and the mess a
#           model imagined, which is at least not ours.
#   odhf    the two real halves of the same corpus. Unlabelled.
#   B       Corporations Canada, as in eval_normalize.R Part B. Unlabelled.
#
# On the labelled corpora the score is per field. On the unlabelled ones it is
# the Part B test -- the parse has to join a row NAR actually holds, and the
# postal code, which the join never uses, has to agree.
#
# Run with:  Rscript data-raw/eval_deepparse.R
#   DP_PYTHON   interpreter with deepparse installed
#               (default $NAR_CACHE_PATH/eval/deepparse-venv/bin/python)
#   DP_MODEL    bpemb | fasttext | bpemb_attention | fasttext_attention
#   DP_CORPORA  comma-separated subset of A,llm,odhf,B   (default all)
#   EVAL_N, EVAL_CACHE, EVAL_VERSION as in eval_normalize.R.
#
# Set up the interpreter once with:
#   uv venv --python 3.12 $NAR_CACHE_PATH/eval/deepparse-venv
#   uv pip install --python $NAR_CACHE_PATH/eval/deepparse-venv/bin/python deepparse
# The pretrained weights download themselves into ~/.cache/deepparse on first
# use: about 110 MB for bpemb, several GB for fasttext.

suppressMessages(pkgload::load_all(".", quiet = TRUE))
source("data-raw/render_address.R")

N        <- as.integer(Sys.getenv("EVAL_N", "5000"))
CACHE    <- Sys.getenv("EVAL_CACHE", file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
PY       <- Sys.getenv("DP_PYTHON", file.path(CACHE, "deepparse-venv/bin/python"))
DP_MODEL <- Sys.getenv("DP_MODEL", "bpemb")
CORPORA  <- strsplit(Sys.getenv("DP_CORPORA", "A,llm,odhf,B"), ",")[[1]]
CORP_URL <- "https://d4bf66bykfyaf.cloudfront.net/corporations-active-cbca-en.csv"
SEED     <- 20260821

stopifnot(file.exists(PY))
con  <- nar_connection(Sys.getenv("EVAL_VERSION", "latest"))
on.exit(DBI::dbDisconnect(con), add = TRUE)
fold  <- cangeocode:::nar_fold
mfold <- cangeocode:::nar_match_fold

pct  <- function(x) sprintf("%5.1f%%", 100 * mean(x, na.rm = TRUE))
rule <- function(t) cat("\n", t, "\n", strrep("-", nchar(t)), "\n", sep = "")
agree <- function(got, want) {
  fold(ifelse(is.na(got), "", as.character(got))) ==
    fold(ifelse(is.na(want), "", as.character(want)))
}

# ------------------------------------------------------------- the bridge ---

# Tabs and newlines would desynchronise the TSV that comes back, and a line the
# caller silently dropped would misalign every row after it, so they are
# flattened here rather than in Python.
deepparse <- function(text, tag) {
  f_in  <- file.path(CACHE, sprintf("dp-%s-in.txt", tag))
  f_out <- file.path(CACHE, sprintf("dp-%s-%s.tsv", tag, DP_MODEL))
  clean <- gsub("[\t\r\n]", " ", text)
  writeLines(clean, f_in, useBytes = TRUE)
  t0 <- Sys.time()
  st <- system2(PY, c("data-raw/deepparse/parse.py", shQuote(f_in), shQuote(f_out),
                      "--model", DP_MODEL), stdout = TRUE, stderr = TRUE)
  secs <- as.numeric(Sys.time() - t0, units = "secs")
  if (!file.exists(f_out)) stop(paste(st, collapse = "\n"))
  # quote = "" because an address may legitimately contain a lone apostrophe
  # or double quote, and na.strings = character(0) because an empty field means
  # "the tagger did not use this tag", never a missing value.
  d <- utils::read.delim(f_out, quote = "", colClasses = "character",
                         na.strings = character(0), fileEncoding = "UTF-8")
  stopifnot(nrow(d) == length(text))
  cat(sprintf("  deepparse(%s): %d rows in %.0fs (%.0f/s)\n",
              DP_MODEL, nrow(d), secs, nrow(d) / secs))
  d
}

# deepparse's StreetNumber runs on for anything it reads as part of the number,
# `apt 4b-1234` included, so the civic number is the last all-digit run in it.
# Taking the first would answer with the unit on every dash form.
dp_civic <- function(x) {
  m <- regmatches(x, gregexpr("[0-9]+", x))
  suppressWarnings(as.integer(vapply(m, function(v)
    if (length(v)) v[length(v)] else NA_character_, character(1))))
}

# The tagger returns the province exactly as written, so `ontario` and `on` are
# different strings for the same thing. Resolving that with the package's own
# province lexicon is a table lookup, not parsing, and refusing it would score
# a spelling convention rather than the model.
dp_prov <- function(x) {
  out <- cangeocode:::nar_lex_lookup(fold(x), cangeocode:::nar_lex_prov)
  ifelse(is.na(out), "", out)
}

dp_postal <- function(x) {
  p <- gsub("[^A-Z0-9]", "", toupper(x))
  ifelse(grepl("^[A-Z][0-9][A-Z][0-9][A-Z][0-9]$", p), p, "")
}

# Reassemble the tags into the shape normalize_address() reads best: one comma
# between each field it is trying to find. If the tagger's segmentation carries
# information ours does not, this is where it shows up.
dp_string <- function(d) {
  street <- trimws(paste(d$StreetNumber, d$StreetName, d$Orientation))
  unit <- ifelse(nzchar(d$Unit), paste0(", ", d$Unit), "")
  tail <- trimws(paste(d$Province, d$PostalCode))
  parts <- cbind(trimws(paste0(street, unit)), d$Municipality, tail)
  apply(parts, 1, function(r) paste(r[nzchar(trimws(r))], collapse = ", "))
}

# --------------------------------------------------------------- labelled ---

# Both systems are scored on the same street measure, and it is the generous
# one: does NAR's street name appear as a whole word inside what was returned?
# cangeocode returns the name alone and mostly satisfies it by equality;
# deepparse returns the name with its type and direction still attached and
# could not satisfy an equality test at all. Scoring the two differently would
# make the comparison meaningless, and scoring deepparse strictly would measure
# the fact that it is a tagger rather than how well it tags.
street_hit <- function(got, want) {
  a <- paste0(" ", mfold(ifelse(is.na(got), "", as.character(got))), " ")
  b <- mfold(ifelse(is.na(want), "", as.character(want)))
  nzchar(b) & mapply(grepl, paste0(" ", b, " "), a, fixed = TRUE)
}

score_labelled <- function(name, text, lab) {
  cat(sprintf("\n[%s] %d rows\n", name, length(text)))
  t0 <- Sys.time(); ours <- normalize_address(text, con = con)
  cat(sprintf("  cangeocode: %d rows in %.0fs (%.0f/s)\n", length(text),
              as.numeric(Sys.time() - t0, units = "secs"),
              length(text) / as.numeric(Sys.time() - t0, units = "secs")))
  dp <- deepparse(text, name)
  t0 <- Sys.time(); via <- normalize_address(dp_string(dp), con = con)
  cat(sprintf("  dp -> norm: %d rows in %.0fs\n", length(text),
              as.numeric(Sys.time() - t0, units = "secs")))

  cfg <- list(
    cangeocode = data.frame(
      CIVIC  = agree(ours$CIVIC_NO, lab$CIVIC_NO),
      STREET = street_hit(ours$STREET_NAME, lab$OFFICIAL_STREET_NAME),
      MUN    = agree(ours$MUN_NAME, lab$MAIL_MUN_NAME),
      PROV   = agree(ours$PROV_ABVN, lab$MAIL_PROV_ABVN),
      POSTAL = agree(ours$POSTAL_CODE, lab$MAIL_POSTAL_CODE)),
    deepparse = data.frame(
      CIVIC  = agree(dp_civic(dp$StreetNumber), lab$CIVIC_NO),
      STREET = street_hit(dp$StreetName, lab$OFFICIAL_STREET_NAME),
      MUN    = agree(dp$Municipality, lab$MAIL_MUN_NAME),
      PROV   = agree(dp_prov(dp$Province), lab$MAIL_PROV_ABVN),
      POSTAL = agree(dp_postal(dp$PostalCode), lab$MAIL_POSTAL_CODE)),
    `dp -> norm` = data.frame(
      CIVIC  = agree(via$CIVIC_NO, lab$CIVIC_NO),
      STREET = street_hit(via$STREET_NAME, lab$OFFICIAL_STREET_NAME),
      MUN    = agree(via$MUN_NAME, lab$MAIL_MUN_NAME),
      PROV   = agree(via$PROV_ABVN, lab$MAIL_PROV_ABVN),
      POSTAL = agree(via$POSTAL_CODE, lab$MAIL_POSTAL_CODE)))

  rule(sprintf("%s -- per field", name))
  tab <- do.call(rbind, lapply(names(cfg), function(k) {
    o <- cfg[[k]]
    data.frame(config = k, CIVIC = pct(o$CIVIC), STREET = pct(o$STREET),
               CORE = pct(o$CIVIC & o$STREET), MUN = pct(o$MUN),
               PROV = pct(o$PROV), POSTAL = pct(o$POSTAL))
  }))
  print(tab, row.names = FALSE)
  cat("\n  CORE = civic number and street name together.\n")
  cat("  STREET = NAR's street name appears as a whole word in the answer;\n")
  cat("  MUN/PROV/POSTAL are exact, since all three systems return them verbatim.\n")
  cat("  A field the writer dropped is charged to every configuration alike, so read\n")
  cat("  these columns against each other rather than as absolute accuracies.\n")
  invisible(cfg)
}

# ------------------------------------------------------------- unlabelled ---

# Same test as eval_normalize.R Part B: join Addresses on the strong keys and
# leave the postal code out of the join so it is free to confirm it. A raw
# tagger has no field that can join -- its street still has the type in it --
# so it gets the containment join through Streets instead, which is looser than
# what the other two configurations are held to, deliberately.
join_rate <- function(name_fold, mun_fold, prov, civic, postal, contained = FALSE) {
  probe <- data.frame(row_id = seq_along(name_fold), name_fold = name_fold,
                      mun_fold = mun_fold, prov = prov, civic = civic,
                      postal = postal, stringsAsFactors = FALSE)
  DBI::dbWriteTable(con, "dp_probe", probe, temporary = TRUE, overwrite = TRUE)
  sql <- if (!contained) "
    SELECT p.row_id, count(*) > 0 AS joined,
           max(CASE WHEN a.MAIL_POSTAL_CODE = p.postal THEN 1 ELSE 0 END) = 1 AS postal_ok
      FROM dp_probe p
      JOIN Addresses a
        ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = p.name_fold
       AND strip_accents(upper(a.MAIL_MUN_NAME)) = p.mun_fold
       AND a.MAIL_PROV_ABVN = p.prov AND a.CIVIC_NO = p.civic
     WHERE p.name_fold <> '' AND p.civic IS NOT NULL
     GROUP BY p.row_id" else "
    WITH cand AS (
      SELECT DISTINCT p.row_id, s.NAME_FOLD
        FROM dp_probe p
        JOIN Streets s
          ON strip_accents(upper(s.MAIL_MUN_NAME)) = p.mun_fold
         AND s.MAIL_PROV_ABVN = p.prov
       WHERE p.name_fold <> '' AND p.civic IS NOT NULL
         AND s.NAME_FOLD <> ''
         AND position(' ' || s.NAME_FOLD || ' ' IN ' ' || p.name_fold || ' ') > 0)
    SELECT p.row_id, count(*) > 0 AS joined,
           max(CASE WHEN a.MAIL_POSTAL_CODE = p.postal THEN 1 ELSE 0 END) = 1 AS postal_ok
      FROM dp_probe p JOIN cand c ON c.row_id = p.row_id
      JOIN Addresses a
        ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = c.NAME_FOLD
       AND strip_accents(upper(a.MAIL_MUN_NAME)) = p.mun_fold
       AND a.MAIL_PROV_ABVN = p.prov AND a.CIVIC_NO = p.civic
     GROUP BY p.row_id"
  hits <- DBI::dbGetQuery(con, sql)
  out <- data.frame(joined = rep(FALSE, length(name_fold)),
                    postal_ok = rep(FALSE, length(name_fold)))
  out$joined[hits$row_id] <- hits$joined
  out$postal_ok[hits$row_id] <- hits$postal_ok
  out
}

score_unlabelled <- function(name, text, postal, group = NULL) {
  cat(sprintf("\n[%s] %d rows\n", name, length(text)))
  ours <- normalize_address(text, con = con)
  dp   <- deepparse(text, name)
  via  <- normalize_address(dp_string(dp), con = con)
  blank <- function(x) ifelse(is.na(x), "", x)

  res <- list(
    cangeocode = join_rate(fold(blank(ours$STREET_NAME)), fold(blank(ours$MUN_NAME)),
                           blank(ours$PROV_ABVN), ours$CIVIC_NO, postal),
    deepparse  = join_rate(fold(dp$StreetName), fold(dp$Municipality),
                           dp_prov(dp$Province), dp_civic(dp$StreetNumber),
                           postal, contained = TRUE),
    `dp -> norm` = join_rate(fold(blank(via$STREET_NAME)), fold(blank(via$MUN_NAME)),
                             blank(via$PROV_ABVN), via$CIVIC_NO, postal))

  rule(sprintf("%s -- resolved against NAR", name))
  print(do.call(rbind, lapply(names(res), function(k)
    data.frame(config = k, joined = pct(res[[k]]$joined),
               postal_confirmed = pct(res[[k]]$postal_ok)))), row.names = FALSE)
  cat("\n  deepparse's street still carries its type, so it cannot join on equality;\n")
  cat("  it gets a containment join through the gazetteer instead -- a looser test\n")
  cat("  than the other two rows are held to.\n")

  if (!is.null(group)) {
    rule(sprintf("%s -- postal-confirmed by %s", name, attr(group, "label")))
    print(data.frame(
      grp = names(split(seq_along(text), group)),
      n = lengths(split(seq_along(text), group)),
      cangeocode = vapply(split(res$cangeocode$postal_ok, group), pct, character(1)),
      deepparse = vapply(split(res$deepparse$postal_ok, group), pct, character(1)),
      `dp -> norm` = vapply(split(res$`dp -> norm`$postal_ok, group), pct, character(1)),
      check.names = FALSE), row.names = FALSE)
  }
  invisible(res)
}

# ------------------------------------------------------------------- run ---

cat(sprintf("deepparse model: %s   corpora: %s\n", DP_MODEL, paste(CORPORA, collapse = " ")))

if ("A" %in% CORPORA) {
  rows <- DBI::dbGetQuery(con, sprintf("
    SELECT OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
           CIVIC_NO, CIVIC_NO_SUFFIX, APT_NO_LABEL,
           MAIL_MUN_NAME, MAIL_PROV_ABVN, MAIL_POSTAL_CODE
      FROM Addresses
     WHERE length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
       AND length(MAIL_MUN_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
     USING SAMPLE reservoir(%d ROWS) REPEATABLE (%d)", N, SEED))
  set.seed(SEED)
  r <- nar_render_surface(rows)
  score_labelled("A", r$text, r)
}

corpus <- NULL
if (any(c("llm", "odhf") %in% CORPORA)) {
  f <- file.path(CACHE, "dirty_corpus.csv")
  if (!file.exists(f)) stop("run data-raw/dirty_corpus.R first -- no ", f)
  corpus <- utils::read.csv(f, stringsAsFactors = FALSE, colClasses = c(CIVIC_NO = "integer"))
  corpus[] <- lapply(corpus, function(x) if (is.character(x)) ifelse(is.na(x), "", x) else x)
}

if ("llm" %in% CORPORA) {
  d <- corpus[corpus$source == "llm", ]
  cfg <- score_labelled("llm", d$text, d)
  rule("llm -- CORE by transformation")
  g <- factor(d$transform)
  print(data.frame(
    transform = levels(g), n = as.integer(table(g)),
    cangeocode = vapply(split(cfg$cangeocode$CIVIC & cfg$cangeocode$STREET, g), pct, character(1)),
    deepparse = vapply(split(cfg$deepparse$CIVIC & cfg$deepparse$STREET, g), pct, character(1)),
    `dp -> norm` = vapply(split(cfg$`dp -> norm`$CIVIC & cfg$`dp -> norm`$STREET, g), pct, character(1)),
    check.names = FALSE), row.names = FALSE)
}

if ("odhf" %in% CORPORA) {
  d <- corpus[corpus$source %in% c("odhf_full", "odhf_street"), ]
  g <- factor(d$source); attr(g, "label") <- "sub-source"
  score_unlabelled("odhf", d$text, d$MAIL_POSTAL_CODE, g)
}

if ("B" %in% CORPORA) {
  csv <- file.path(CACHE, basename(CORP_URL))
  if (!file.exists(csv)) {
    to <- options("timeout"); options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
    utils::download.file(CORP_URL, csv, mode = "wb"); options(to)
  }
  set.seed(SEED)
  corp <- as.data.frame(arrow::read_csv_arrow(
    csv, col_select = c("Street", "Street 2", "City/town",
                        "Province/territory", "Postal code"), as_data_frame = TRUE))
  names(corp) <- c("street", "street2", "city", "prov", "postal")
  corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))
  corp <- corp[nzchar(corp$street) & nzchar(corp$city) &
                 corp$prov %in% names(cangeocode:::nar_prov_lang) &
                 grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal), ]
  corp <- corp[sample.int(nrow(corp), min(N, nrow(corp))), ]
  parts <- cbind(corp$street, corp$street2, corp$city,
                 trimws(paste(corp$prov, corp$postal)))
  text <- apply(parts, 1, function(x) paste(x[nzchar(x)], collapse = ", "))
  g <- factor(ifelse(corp$prov == "QC", "QC", "rest of Canada"))
  attr(g, "label") <- "province"
  score_unlabelled("B", text, gsub(" ", "", toupper(corp$postal)), g)
}

cat("\n")
