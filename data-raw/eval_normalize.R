# Measures normalize_address() two ways, because neither is sufficient alone.
#
#   Part A  Round-trip from NAR. Take real rows, render them into noisy surface
#           forms (data-raw/render_address.R), normalize, and compare field by
#           field. Fully labelled, so it gives per-field accuracy -- but only
#           over the noise we thought to generate.
#
#   Part B  Corporations Canada. Real registered-office addresses, typed by
#           people into a form. No labels, so accuracy is measured by whether
#           the output resolves to an address NAR actually holds, and confirmed
#           against the postal code the filing supplied -- a field the join
#           never uses, so agreement is independent evidence the match is right.
#
# Part A says how well the parser handles the mess we imagined. Part B says
# whether we imagined the right mess. Diverging numbers mean the noise grammar
# needs work, not the parser.
#
# Run with:  Rscript data-raw/eval_normalize.R  (needs NAR_CACHE_PATH)
#   EVAL_N        rows to sample per part          (default 5000)
#   EVAL_CACHE    where to keep the downloaded CSV (default <NAR_CACHE_PATH>/eval)
#   EVAL_PARTS    "A", "B" or "AB"                 (default AB)
#   EVAL_VERSION  which cached release to open     (default "latest")
#
# EVAL_VERSION names a release already in the cache -- naming one explicitly
# skips the StatCan lookup entirely, which is what lets the harness run against
# a hand-built subset while a full rebuild is still pending.

# Deliberately the working tree rather than the installed package: an eval
# harness exists to measure the code you just changed.
if (requireNamespace("pkgload", quietly = TRUE) && file.exists("DESCRIPTION")) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(cangeocode)
}
source("data-raw/render_address.R")

N        <- as.integer(Sys.getenv("EVAL_N", "5000"))
PARTS    <- toupper(Sys.getenv("EVAL_PARTS", "AB"))
CACHE    <- Sys.getenv("EVAL_CACHE", file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
CORP_URL <- "https://d4bf66bykfyaf.cloudfront.net/corporations-active-cbca-en.csv"
VERSION  <- Sys.getenv("EVAL_VERSION", "latest")

set.seed(20260821)
con <- nar_connection(version = VERSION)
on.exit(DBI::dbDisconnect(con), add = TRUE)
fold <- cangeocode:::nar_fold

if (!cangeocode:::nar_has_streets(con)) {
  cat("\n!! This database predates the street gazetteer (schema version 4).\n",
      "!! Layer 2 is absent, so everything below measures the rules layer alone.\n",
      "!! Rebuild with nar_connection(refresh = TRUE) for the real numbers.\n", sep = "")
}

pct  <- function(x) sprintf("%5.1f%%", 100 * mean(x, na.rm = TRUE))
rule <- function(title) cat("\n", title, "\n", strrep("-", nchar(title)), "\n", sep = "")

# Compare on the folded form: the normalizer returns NAR's own casing and
# accents once the gazetteer has resolved a row, and the parser's uppercase
# before that. Neither is a mistake, so neither should count as one.
agree <- function(got, want) {
  a <- fold(ifelse(is.na(got), "", as.character(got)))
  b <- fold(ifelse(is.na(want), "", as.character(want)))
  a == b
}

# ---------------------------------------------------------------- Part A ----

if (grepl("A", PARTS)) {
  rule(sprintf("Part A -- round-trip over %s rendered NAR addresses", format(N, big.mark = ",")))

  rows <- DBI::dbGetQuery(con, sprintf("
    SELECT OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
           CIVIC_NO, CIVIC_NO_SUFFIX, APT_NO_LABEL,
           MAIL_MUN_NAME, MAIL_PROV_ABVN, MAIL_POSTAL_CODE
      FROM Addresses
     WHERE length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
       AND length(MAIL_MUN_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
     USING SAMPLE %d ROWS", N))

  r <- nar_render_surface(rows)
  t0 <- Sys.time()
  got <- normalize_address(r$text, con = con)
  cat(sprintf("normalized %d in %.1fs (%.0f/s)\n", nrow(r),
              as.numeric(Sys.time() - t0, units = "secs"),
              nrow(r) / as.numeric(Sys.time() - t0, units = "secs")))

  ok <- data.frame(
    CIVIC_NO    = agree(got$CIVIC_NO, r$CIVIC_NO),
    SUFFIX      = agree(got$CIVIC_NO_SUFFIX, r$CIVIC_NO_SUFFIX),
    UNIT        = agree(got$APT_NO_LABEL, ifelse(r$has_unit, r$APT_NO_LABEL, "")),
    STREET_NAME = agree(got$STREET_NAME, r$OFFICIAL_STREET_NAME),
    STREET_TYPE = agree(got$STREET_TYPE, r$OFFICIAL_STREET_TYPE),
    STREET_DIR  = agree(got$STREET_DIR, r$OFFICIAL_STREET_DIR),
    MUN_NAME    = agree(got$MUN_NAME, r$MAIL_MUN_NAME),
    PROV_ABVN   = agree(got$PROV_ABVN, r$MAIL_PROV_ABVN),
    POSTAL_CODE = agree(got$POSTAL_CODE, r$MAIL_POSTAL_CODE))

  # A field the surface form dropped is scored, but only in the recovery table
  # below -- charging it against the whole-address rate would measure the noise
  # grammar's drop probabilities rather than the normalizer.
  dropped <- data.frame(
    STREET_TYPE = r$type_form == "drop", STREET_DIR = r$dir_form == "drop",
    MUN_NAME = r$mun_form == "drop", PROV_ABVN = r$prov_form == "drop",
    POSTAL_CODE = r$postal_form == "drop")
  supplied <- ok
  for (f in names(dropped)) supplied[[f]][dropped[[f]]] <- TRUE
  ok$ALL <- Reduce(`&`, supplied)
  # The parser's actual job, before any of the optional fields.
  ok$CORE <- ok$CIVIC_NO & ok$STREET_NAME

  rule("per field")
  print(data.frame(field = names(ok), exact = vapply(ok, pct, character(1))),
        row.names = FALSE)
  cat("\n  ALL  = every field the surface form actually carried\n")
  cat("  CORE = civic number and street name\n")

  # The interesting split: a field the string never carried can only be filled
  # in by the gazetteer, so these two columns separate parsing from resolution.
  rule("recovered when the surface form dropped it")
  print(data.frame(
    field    = names(dropped),
    supplied = vapply(names(dropped), function(f) pct(ok[[f]][!dropped[[f]]]), character(1)),
    dropped  = vapply(names(dropped), function(f) pct(ok[[f]][dropped[[f]]]), character(1))),
    row.names = FALSE)

  # A misspelling is only ever fixable by the gazetteer, so this line is the
  # cleanest single read on whether Layer 2 is pulling its weight.
  rule("street name under a keyboard typo")
  print(data.frame(typo = c("clean", "typo"),
                   n = c(sum(!r$has_typo), sum(r$has_typo)),
                   name_exact = c(pct(ok$STREET_NAME[!r$has_typo]),
                                  pct(ok$STREET_NAME[r$has_typo]))), row.names = FALSE)

  rule("by layer")
  print(as.data.frame(table(source = got$parse_source)), row.names = FALSE)
  print(data.frame(source = names(tapply(ok$ALL, got$parse_source, mean)),
                   all_fields = tapply(ok$ALL, got$parse_source, pct)), row.names = FALSE)

  rule("by province")
  by_p <- split(ok$ALL, r$MAIL_PROV_ABVN)
  print(data.frame(prov = names(by_p), n = lengths(by_p),
                   all_fields = vapply(by_p, pct, character(1))), row.names = FALSE)

  # Which structural forms the normalizer is good and bad at, rather than which
  # provinces -- the buckets cut closer to the cause than a province does, since
  # a province mixes several conventions together.
  rule("by pattern")
  by_pat <- split(ok$ALL, got$pattern)
  by_pat <- by_pat[lengths(by_pat) > 0]
  print(data.frame(pattern = names(by_pat), n = lengths(by_pat),
                   all_fields = vapply(by_pat, pct, character(1))),
        row.names = FALSE)

  rule("15 misses")
  bad <- which(!ok$ALL)
  if (length(bad)) {
    show <- bad[seq_len(min(15, length(bad)))]
    print(data.frame(input = substr(r$text[show], 1, 52),
                     got = substr(paste(got$CIVIC_NO[show], got$STREET_NAME[show],
                                        got$STREET_TYPE[show], got$STREET_DIR[show]), 1, 34),
                     want = substr(paste(r$CIVIC_NO[show], r$OFFICIAL_STREET_NAME[show],
                                         r$OFFICIAL_STREET_TYPE[show],
                                         r$OFFICIAL_STREET_DIR[show]), 1, 34)),
          row.names = FALSE)
  }
}

# ---------------------------------------------------------------- Part B ----

if (grepl("B", PARTS)) {
  rule("Part B -- Corporations Canada registered offices")

  dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
  csv <- file.path(CACHE, basename(CORP_URL))
  if (!file.exists(csv)) {
    cat("downloading", CORP_URL, "(~100 MB)\n")
    to <- options("timeout")
    options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
    utils::download.file(CORP_URL, csv, mode = "wb")
    options(to)
  }

  # Read whole then sample: the file is ordered by corporation number, which
  # tracks era and region, so anything head-shaped is a Quebec sample.
  corp <- as.data.frame(arrow::read_csv_arrow(
    csv, col_select = c("Street", "Street 2", "City/town",
                        "Province/territory", "Postal code"),
    as_data_frame = TRUE))
  names(corp) <- c("street", "street2", "city", "prov", "postal")
  corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))

  keep <- nzchar(corp$street) & nzchar(corp$city) &
    corp$prov %in% names(cangeocode:::nar_prov_lang) &
    grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal)
  corp <- corp[keep, , drop = FALSE]
  cat(sprintf("%s usable Canadian addresses; sampling %s\n",
              format(nrow(corp), big.mark = ","), format(N, big.mark = ",")))
  corp <- corp[sample.int(nrow(corp), min(N, nrow(corp))), , drop = FALSE]

  # Street 2 is where the filer put the unit, when they used it at all.
  parts <- cbind(corp$street, corp$street2, corp$city,
                 trimws(paste(corp$prov, corp$postal)))
  corp$text <- apply(parts, 1, function(x) paste(x[nzchar(x)], collapse = ", "))

  got <- normalize_address(corp$text, con = con)

  rule("parse outcome")
  parsed <- !is.na(got$STREET_NAME) & !is.na(got$CIVIC_NO)
  cat("street name and civic number found: ", pct(parsed), "\n", sep = "")
  print(as.data.frame(table(source = got$parse_source)), row.names = FALSE)

  # No labels, so the database is the judge. Join on the strong keys only and
  # keep the postal code out of it -- that leaves it free to confirm the match.
  probe <- data.frame(
    row_id = seq_len(nrow(got)),
    name_fold = fold(ifelse(is.na(got$STREET_NAME), "", got$STREET_NAME)),
    mun_fold  = fold(ifelse(is.na(got$MUN_NAME), "", got$MUN_NAME)),
    prov      = ifelse(is.na(got$PROV_ABVN), "", got$PROV_ABVN),
    civic     = got$CIVIC_NO,
    type      = ifelse(is.na(got$STREET_TYPE), "", got$STREET_TYPE),
    dir       = ifelse(is.na(got$STREET_DIR), "", got$STREET_DIR),
    postal    = gsub(" ", "", toupper(corp$postal)),
    stringsAsFactors = FALSE)

  DBI::dbWriteTable(con, "eval_probe", probe, temporary = TRUE, overwrite = TRUE)
  hits <- DBI::dbGetQuery(con, "
    SELECT p.row_id,
           count(*) > 0 AS joined,
           max(CASE WHEN a.MAIL_POSTAL_CODE = p.postal THEN 1 ELSE 0 END) = 1
             AS postal_ok,
           max(CASE WHEN p.type IN (a.OFFICIAL_STREET_TYPE, a.MAIL_STREET_TYPE)
                    THEN 1 ELSE 0 END) = 1 AS type_ok,
           max(CASE WHEN p.dir IN (a.OFFICIAL_STREET_DIR, a.MAIL_STREET_DIR)
                    THEN 1 ELSE 0 END) = 1 AS dir_ok
      FROM eval_probe p
      JOIN Addresses a
        ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = p.name_fold
       AND strip_accents(upper(a.MAIL_MUN_NAME)) = p.mun_fold
       AND a.MAIL_PROV_ABVN = p.prov
       AND a.CIVIC_NO = p.civic
     WHERE p.name_fold <> '' AND p.civic IS NOT NULL
     GROUP BY p.row_id")

  for (f in c("joined", "postal_ok", "type_ok", "dir_ok")) {
    got[[f]] <- FALSE
    got[[f]][hits$row_id] <- hits[[f]]
  }

  rule("resolved against NAR")
  cat("joins a real NAR address:      ", pct(got$joined), "\n", sep = "")
  cat("  ... and its postal confirms: ", pct(got$postal_ok), "\n", sep = "")
  cat("  ... type agrees (of joined): ", pct(got$type_ok[got$joined]), "\n", sep = "")
  cat("  ... dir agrees  (of joined): ", pct(got$dir_ok[got$joined]), "\n", sep = "")

  # Why the misses miss. NAR's MAIL_MUN_NAME is the *postal* municipality, which
  # in much of the country predates amalgamation -- SCARBOROUGH not TORONTO,
  # NEPEAN not OTTAWA, WOODBRIDGE not VAUGHAN -- while people write the city they
  # live in today. Re-joining on the postal code instead of the municipality
  # isolates how much of the shortfall is that, and nothing to do with parsing.
  # Part A cannot see this at all: it renders the municipality out of NAR, so it
  # always agrees. This is the whole reason Part B exists.
  loose <- DBI::dbGetQuery(con, "
    SELECT DISTINCT p.row_id
      FROM eval_probe p
      JOIN Addresses a
        ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = p.name_fold
       AND a.MAIL_POSTAL_CODE = p.postal
       AND a.CIVIC_NO = p.civic
     WHERE p.name_fold <> '' AND p.civic IS NOT NULL")
  got$postal_join <- FALSE
  got$postal_join[loose$row_id] <- TRUE

  rule("where the shortfall goes")
  cat("confirmed via municipality:    ", pct(got$postal_ok), "\n", sep = "")
  cat("confirmed via postal code:     ", pct(got$postal_join), "\n", sep = "")
  cat("  street right, municipality\n")
  cat("  disagrees with NAR:          ",
      pct(got$postal_join & !got$postal_ok), "\n", sep = "")
  cat("neither -- a genuine parse or\n")
  cat("  coverage failure:            ",
      pct(!got$postal_join & !got$postal_ok), "\n", sep = "")

  rule("by province")
  by_p <- split(got$postal_ok, corp$prov)
  print(data.frame(prov = names(by_p), n = lengths(by_p),
                   confirmed = vapply(by_p, pct, character(1))), row.names = FALSE)

  # The payoff: real strings the normalizer could not place. This is the list
  # that tells you what the noise grammar in Part A is still missing.
  rule("by pattern")
  by_pat <- split(got$postal_ok | got$postal_join, got$pattern)
  by_pat <- by_pat[lengths(by_pat) > 0]
  print(data.frame(pattern = names(by_pat), n = lengths(by_pat),
                   confirmed = vapply(by_pat, pct, character(1))),
        row.names = FALSE)

  rule("20 genuinely unresolved inputs -- calibrate the noise grammar on these")
  bad <- which(!got$postal_ok & !got$postal_join)
  if (length(bad)) {
    show <- bad[sample.int(length(bad), min(20, length(bad)))]
    print(data.frame(input = substr(corp$text[show], 1, 56),
                     parsed = substr(paste(got$CIVIC_NO[show], got$STREET_NAME[show],
                                           got$STREET_TYPE[show], got$STREET_DIR[show]), 1, 30),
                     src = got$parse_source[show]),
          row.names = FALSE)
  }
}

cat("\n")
