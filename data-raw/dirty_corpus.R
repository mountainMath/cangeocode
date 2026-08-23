# Builds the dirty corpus: address strings that are hard on purpose.
#
# eval_normalize.R's two parts share a blind spot. Part A only ever contains the
# mess `render_address.R` was written to generate, and Part B is one registry,
# whose filers fill one form, in one shape, with commas between the fields. Both
# were used to tune the parser, so neither can be trusted to say what happens on
# input nobody here has seen.
#
# This corpus has two halves, and they fail differently on purpose:
#
#   llm   Labelled. A local model is handed the *fields* of a real NAR row --
#         never our rendered string, or the corpus would inherit the grammar it
#         exists to escape -- plus one named transformation, and writes the line
#         a person would have typed. The NAR row is the label. Every row is
#         checked before it is kept: the civic number has to survive verbatim
#         and a distinctive word of the street name has to survive a single
#         edit, or the model changed the address rather than the writing of it
#         and the label would be a lie. Rejections are counted per
#         transformation and printed, because a transformation that mostly
#         fails its own check is one the model cannot do, and that is a result.
#
#   odhf  Real and unlabelled: the free-text `source_format_str_address` column
#         of StatCan's Open Database of Healthcare Facilities, which is what a
#         dozen provincial custodians handed over before anyone tidied it.
#         Mostly *comma-free* -- `8512 164th st surrey bc v4n 1e5` -- which is
#         the case Part B never presents, since the corporate form supplies the
#         separators. Judged the way Part B is: the parse must join a real NAR
#         address, and the file's own postal code, which the join never uses,
#         has to agree.
#
# Run with:  Rscript data-raw/dirty_corpus.R      (needs NAR_CACHE_PATH, Ollama)
#   DIRTY_N        LLM rows to generate       (default 1000)
#   DIRTY_MODEL    Ollama model               (default qwen3:8b)
#   DIRTY_REFRESH  "1" to regenerate          (default: reuse the cached CSV)
#   LLM_HOST, EVAL_CACHE, EVAL_VERSION as in the other harnesses.
#
# Writes <EVAL_CACHE>/dirty_corpus.csv, which data-raw/eval_deepparse.R reads.
# Generation is the slow part -- it is one model call per row -- so the CSV is
# the artefact and this script is idempotent.

suppressMessages(pkgload::load_all(".", quiet = TRUE))

N       <- as.integer(Sys.getenv("DIRTY_N", "1000"))
MODEL   <- Sys.getenv("DIRTY_MODEL", "qwen3:8b")
HOST    <- Sys.getenv("LLM_HOST", "http://localhost:11434")
CACHE   <- Sys.getenv("EVAL_CACHE", file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
REFRESH <- Sys.getenv("DIRTY_REFRESH", "") == "1"
ODHF_URL <- "https://www150.statcan.gc.ca/n1/pub/13-26-0001/2020001/ODHF_v1.1.zip"
SEED    <- 20260822
OUT     <- file.path(CACHE, "dirty_corpus.csv")

dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
rule <- function(title) cat("\n", title, "\n", strrep("-", nchar(title)), "\n", sep = "")

# ------------------------------------------------------------ the LLM half ---

# One line each, and each names a *way of writing*, never a way of being wrong.
# The distinction matters: a transformation that licenses changing the address
# produces rows this script then throws away, so it costs a model call and buys
# nothing.
TRANSFORMS <- c(
  abbrev    = "Abbreviate hard, the way a clerk keying hundreds of these does. Shorten the street name itself as well as its type.",
  building  = "Put the name of the building or business first, then a floor or suite, then the address.",
  careof    = "Write it as it would appear on an envelope with a care-of or attention line.",
  runon     = "Remove every comma and period so the fields run together with only spaces between them.",
  verbose   = "Write it the way someone types into a web form that gave them one long box, with extra words like 'located at', 'unit', 'across from'.",
  ocr       = "Write it as a bad scan or a broken CSV export would leave it: doubled spaces, a stray character or two, inconsistent capitals.",
  bilingual = "Mix French and English conventions in the one line, the way bilingual addresses in Canada actually get typed.",
  terse     = "Strip it to the fewest characters that still identify the place, dropping whatever is redundant."
)

`%||%` <- function(a, b) if (is.null(a)) b else a

ask <- function(prompt) {
  body <- list(model = MODEL, prompt = prompt, stream = FALSE, think = FALSE,
               format = list(type = "object", required = list("address"),
                             properties = list(address = list(type = "string"))),
               options = list(temperature = 0.7, seed = SEED, num_predict = 96))
  tryCatch({
    j <- httr2::request(paste0(HOST, "/api/generate")) |>
      httr2::req_body_json(body) |> httr2::req_timeout(180) |>
      httr2::req_perform() |> httr2::resp_body_json()
    as.character(jsonlite::fromJSON(j$response)$address %||% "")
  }, error = function(e) "")
}

build_llm <- function(con) {
  rows <- DBI::dbGetQuery(con, sprintf("
    SELECT OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
           CIVIC_NO, CIVIC_NO_SUFFIX, APT_NO_LABEL,
           MAIL_MUN_NAME, MAIL_PROV_ABVN, MAIL_POSTAL_CODE
      FROM Addresses
     WHERE length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
       AND length(MAIL_MUN_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
     -- Same REPEATABLE discipline as eval_normalize.R, and a different seed:
     -- the corpus should not be the rows Part A already reports on.
     USING SAMPLE reservoir(%d ROWS) REPEATABLE (%d)", N, SEED))

  set.seed(SEED)
  rows$transform <- sample(names(TRANSFORMS), nrow(rows), replace = TRUE)

  field <- function(label, value) {
    ifelse(nzchar(value), paste0("  ", label, ": ", value, "\n"), "")
  }
  prompt <- paste0(
    "Here is one Canadian address, already broken into its parts:\n",
    field("civic number", paste0(rows$CIVIC_NO, rows$CIVIC_NO_SUFFIX)),
    field("unit", rows$APT_NO_LABEL),
    field("street name", rows$OFFICIAL_STREET_NAME),
    field("street type", rows$OFFICIAL_STREET_TYPE),
    field("street direction", rows$OFFICIAL_STREET_DIR),
    field("municipality", rows$MAIL_MUN_NAME),
    field("province", rows$MAIL_PROV_ABVN),
    field("postal code", rows$MAIL_POSTAL_CODE),
    "\nWrite this same address as a single line of text, in this style:\n",
    TRANSFORMS[rows$transform],
    "\n\nIt must stay the same address: do not change the civic number and do ",
    "not put it on a different street. You may drop a part, reorder, ",
    "abbreviate, misspell or add words. Return only the line.")

  cat(sprintf("generating %d rows with %s\n", nrow(rows), MODEL))
  t0 <- Sys.time()
  text <- character(nrow(rows))
  for (i in seq_along(prompt)) {
    text[i] <- ask(prompt[i])
    if (i %% 50 == 0) cat(sprintf("  %d/%d  %.0fs\n", i, nrow(rows),
                                  as.numeric(Sys.time() - t0, units = "secs")))
  }
  # A model asked for one line occasionally sends several.
  rows$text <- trimws(gsub("[[:space:]]+", " ", text))
  rows
}

# A generated row is only usable if it still *is* the address it is labelled
# with. Two independent traces, both cheap, both computed in the database
# because that is where the fuzzy functions live:
#
#   civic  the number survives as a whole token. Not a substring: `12` inside
#          `128` would pass and mean nothing.
#   trace  the street name's most distinctive word survives one edit, or
#          survives as a prefix of at least three characters -- which is what
#          lets the `abbrev` and `terse` transformations through, since
#          `catherine` legitimately becomes `cath`.
#
# Rows failing either are dropped, not repaired. A repaired row is a row whose
# difficulty we chose, which is the thing this corpus exists to avoid.
check_llm <- function(con, d) {
  fold <- cangeocode:::nar_match_fold
  words <- strsplit(fold(d$OFFICIAL_STREET_NAME), " ", fixed = TRUE)
  key <- vapply(words, function(w) {
    w <- w[nchar(w) >= 4]
    if (!length(w)) return("")
    w[which.max(nchar(w))]
  }, character(1))

  probe <- data.frame(id = seq_len(nrow(d)), out = fold(d$text),
                      word = key, civic = as.character(d$CIVIC_NO),
                      stringsAsFactors = FALSE)
  DBI::dbWriteTable(con, "dirty_check", probe, temporary = TRUE, overwrite = TRUE)
  chk <- DBI::dbGetQuery(con, "
    SELECT p.id,
           max(CASE WHEN t.tok = p.civic THEN 1 ELSE 0 END) = 1 AS civic_ok,
           max(CASE WHEN p.word = '' THEN 1
                    WHEN damerau_levenshtein(t.tok, p.word) <= 1 THEN 1
                    WHEN length(t.tok) >= 3 AND starts_with(p.word, t.tok) THEN 1
                    ELSE 0 END) = 1 AS trace_ok
      FROM dirty_check p, unnest(string_split(p.out, ' ')) AS t(tok)
     GROUP BY p.id")
  d$civic_ok <- FALSE; d$trace_ok <- FALSE
  d$civic_ok[chk$id] <- chk$civic_ok
  d$trace_ok[chk$id] <- chk$trace_ok
  d$keep <- nzchar(d$text) & d$civic_ok & d$trace_ok
  d
}

# ----------------------------------------------------------- the ODHF half ---

build_odhf <- function() {
  zip <- file.path(CACHE, "ODHF_v1.1.zip")
  if (!file.exists(zip)) {
    cat("downloading", ODHF_URL, "\n")
    utils::download.file(ODHF_URL, zip, mode = "wb", quiet = TRUE)
  }
  ex <- file.path(CACHE, "odhf")
  csv <- list.files(ex, "\\.csv$", recursive = TRUE, full.names = TRUE)
  if (!length(csv)) {
    utils::unzip(zip, exdir = ex)
    csv <- list.files(ex, "\\.csv$", recursive = TRUE, full.names = TRUE)
  }
  # cp1252, not UTF-8: the file carries Windows en-dashes, and read.csv on a
  # mislabelled encoding fails on the first one rather than at parse time.
  o <- utils::read.csv(csv[1], fileEncoding = "cp1252", stringsAsFactors = FALSE)
  o[] <- lapply(o, function(x) ifelse(is.na(x), "", trimws(as.character(x))))
  keep <- nzchar(o$source_format_str_address) &
    toupper(o$province) %in% names(cangeocode:::nar_prov_lang) &
    grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", o$postal_code)
  o <- o[keep, , drop = FALSE]

  # The column holds two different things and they are hard for opposite
  # reasons, so they are two sub-sources rather than one average. Which one a
  # row is depends on whether the string already names its own municipality --
  # asked of the folded forms, since the file's own casing and accents do not
  # agree between the two columns.
  fold <- cangeocode:::nar_match_fold
  full <- nzchar(fold(o$city)) &
    mapply(grepl, fold(o$city), fold(o$source_format_str_address), fixed = TRUE)

  #   odhf_full    a whole address with no commas in it -- `8512 164th st surrey
  #                bc v4n 1e5`. Part B never presents this, because the
  #                corporate form supplies the separators, so every rule that
  #                anchors the municipality on a comma is untested until now.
  #   odhf_street  a street and nothing else, overwhelmingly Quebec and French,
  #                with the type in front and a comma after the civic number.
  #                The municipality and province are appended from the file's
  #                own columns, or the row would be unanswerable rather than
  #                hard, and that is stated in the source name.
  text <- ifelse(full, o$source_format_str_address,
                 paste0(o$source_format_str_address, ", ", o$city, ", ",
                        toupper(o$province)))
  data.frame(
    source = ifelse(full, "odhf_full", "odhf_street"),
    transform = "", text = text,
    CIVIC_NO = NA_integer_, CIVIC_NO_SUFFIX = "", APT_NO_LABEL = "",
    OFFICIAL_STREET_NAME = "", OFFICIAL_STREET_TYPE = "", OFFICIAL_STREET_DIR = "",
    MAIL_MUN_NAME = "", MAIL_PROV_ABVN = toupper(o$province),
    MAIL_POSTAL_CODE = gsub(" ", "", toupper(o$postal_code)),
    # The custodian's own splitting of the same string, kept as a weak second
    # opinion rather than a label: it is itself a parse, by whoever ran the
    # provincial extract, and it disagrees with NAR often enough to matter.
    src_civic = o$street_no, src_street = o$street_name, src_city = o$city,
    stringsAsFactors = FALSE)
}

# -------------------------------------------------------------------- run ---

if (file.exists(OUT) && !REFRESH) {
  cat("reusing", OUT, "-- set DIRTY_REFRESH=1 to rebuild\n")
} else {
  con <- nar_connection(Sys.getenv("EVAL_VERSION", "latest"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  if (!requireNamespace("httr2", quietly = TRUE)) stop("httr2 is needed to generate the LLM half")

  d <- check_llm(con, build_llm(con))
  rule("generated rows kept, by transformation")
  print(data.frame(
    transform = names(TRANSFORMS),
    n = as.integer(table(factor(d$transform, names(TRANSFORMS)))),
    kept = sprintf("%5.1f%%", 100 * tapply(d$keep, factor(d$transform, names(TRANSFORMS)), mean)),
    lost_civic = as.integer(tapply(!d$civic_ok, factor(d$transform, names(TRANSFORMS)), sum)),
    lost_street = as.integer(tapply(!d$trace_ok, factor(d$transform, names(TRANSFORMS)), sum))),
    row.names = FALSE)

  llm <- data.frame(source = "llm", transform = d$transform, text = d$text,
                    d[, c("CIVIC_NO", "CIVIC_NO_SUFFIX", "APT_NO_LABEL",
                          "OFFICIAL_STREET_NAME", "OFFICIAL_STREET_TYPE",
                          "OFFICIAL_STREET_DIR", "MAIL_MUN_NAME",
                          "MAIL_PROV_ABVN", "MAIL_POSTAL_CODE")],
                    src_civic = "", src_street = "", src_city = "",
                    stringsAsFactors = FALSE)[d$keep, ]

  corpus <- rbind(llm, build_odhf())
  corpus$text <- gsub("[\t\r\n]", " ", corpus$text)
  utils::write.csv(corpus, OUT, row.names = FALSE, na = "")
  cat(sprintf("\nwrote %s: %d rows\n", OUT, nrow(corpus)))
  print(table(source = corpus$source))
}

corpus <- utils::read.csv(OUT, stringsAsFactors = FALSE, colClasses = c(CIVIC_NO = "integer"))
corpus[] <- lapply(corpus, function(x) if (is.character(x)) ifelse(is.na(x), "", x) else x)
rule("corpus")
print(as.data.frame(table(source = corpus$source, transform = corpus$transform)),
      row.names = FALSE)
rule("10 examples")
print(data.frame(src = corpus$source, text = substr(corpus$text, 1, 78))[
  sort(sample.int(nrow(corpus), 10)), ], row.names = FALSE)
cat("\n")
