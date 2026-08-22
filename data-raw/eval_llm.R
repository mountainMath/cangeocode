# Does a locally-run LLM add anything to address normalization?
#
# Companion to eval_normalize.R, which measures the pipeline. This one measures
# the *residual* -- the only place a model could add value -- and then asks two
# off-the-shelf models to work it.
#
# Both experiments are pick-from-shortlist: the model never emits a string, it
# picks a real NAR row. That is the shape that neutralizes every structural
# failure mode free-form parsing showed during planning, so it is the model's
# best case rather than a straw man.
#
#   Rscript data-raw/eval_llm.R                 # ceilings only, no Ollama needed
#   LLM_MODEL=qwen3:8b Rscript data-raw/eval_llm.R
#
# Env: EVAL_N (default 5000), EVAL_VERSION, LLM_MODEL, LLM_HOST.
# Numbers from this script are written up in
# inst/notes/address-normalization-status.md, "What a local LLM adds, measured".

suppressMessages(pkgload::load_all(".", quiet = TRUE))
source("data-raw/render_address.R")

N       <- as.integer(Sys.getenv("EVAL_N", "5000"))
MODEL   <- Sys.getenv("LLM_MODEL", "gemma4:e2b")
HOST    <- Sys.getenv("LLM_HOST", "http://localhost:11434")
SEED    <- 20260821

con  <- nar_connection(Sys.getenv("EVAL_VERSION", "latest"))
fold <- cangeocode:::nar_fold
on.exit(DBI::dbDisconnect(con), add = TRUE)

agree <- function(got, want) {
  fold(ifelse(is.na(got), "", as.character(got))) ==
    fold(ifelse(is.na(want), "", as.character(want)))
}

# ---- the residual ---------------------------------------------------------
# Recovery here is strict: a field the surface form dropped counts as a miss.
# That is the right denominator for this question -- recovering what is *not in
# the string* is exactly what a model would be for -- and it is a different
# measure from eval_normalize.R's headline, which excludes dropped fields.

rows <- DBI::dbGetQuery(con, sprintf("
  SELECT OFFICIAL_STREET_NAME, OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR,
         CIVIC_NO, CIVIC_NO_SUFFIX, APT_NO_LABEL,
         MAIL_MUN_NAME, MAIL_PROV_ABVN, MAIL_POSTAL_CODE
    FROM Addresses
   WHERE length(OFFICIAL_STREET_NAME) > 0 AND CIVIC_NO IS NOT NULL
     AND length(MAIL_MUN_NAME) > 0 AND length(MAIL_PROV_ABVN) > 0
   USING SAMPLE reservoir(%d ROWS) REPEATABLE (%d)", N, SEED))
set.seed(SEED)
r   <- nar_render_surface(rows)
got <- normalize_address(r$text, con = con)

ok <- data.frame(
  CIVIC_NO = agree(got$CIVIC_NO,    r$CIVIC_NO),
  NAME     = agree(got$STREET_NAME, r$OFFICIAL_STREET_NAME),
  TYPE     = agree(got$STREET_TYPE, r$OFFICIAL_STREET_TYPE),
  DIR      = agree(got$STREET_DIR,  r$OFFICIAL_STREET_DIR),
  MUN      = agree(got$MUN_NAME,    r$MAIL_MUN_NAME),
  PROV     = agree(got$PROV_ABVN,   r$MAIL_PROV_ABVN))
ok$ALL <- Reduce(`&`, ok)
miss <- !ok$ALL
cat(sprintf("strict recovery over %d rendered rows: %.1f%%  (%d misses)\n\n",
            nrow(r), 100 * mean(ok$ALL), sum(miss)))

# ---- ceiling 1: how much of the municipality residual is even knowable? ----
mm <- which(miss & !ok$MUN & ok$NAME & ok$PROV)
d <- data.frame(i = mm, name = fold(r$OFFICIAL_STREET_NAME[mm]),
                prov = r$MAIL_PROV_ABVN[mm], truth = fold(r$MAIL_MUN_NAME[mm]),
                text = r$text[mm])
DBI::dbWriteTable(con, "pm", d, temporary = TRUE, overwrite = TRUE)
d <- merge(d, DBI::dbGetQuery(con, "
  SELECT p.i, count(DISTINCT upper(a.MAIL_MUN_NAME)) AS n_mun,
         max(CASE WHEN upper(a.MAIL_MUN_NAME) = p.truth THEN 1 ELSE 0 END) AS truth_in,
         string_agg(DISTINCT upper(a.MAIL_MUN_NAME), '|') AS muns
    FROM pm p JOIN Addresses a
      ON upper(a.OFFICIAL_STREET_NAME) = p.name AND a.MAIL_PROV_ABVN = p.prov
   GROUP BY p.i"), by = "i", all.x = TRUE)
d$n_mun[is.na(d$n_mun)] <- 0; d$truth_in[is.na(d$truth_in)] <- 0

cat("municipality misses where the street name and province are right:", nrow(d), "\n")
cat("  determined -- exactly one municipality has that street:", sum(d$n_mun == 1), "\n")
cat(sprintf("  2-5 candidates: %d   6-20: %d   >20: %d\n",
            sum(d$truth_in == 1 & d$n_mun %in% 2:5),
            sum(d$truth_in == 1 & d$n_mun %in% 6:20),
            sum(d$truth_in == 1 & d$n_mun > 20)))

# ---- ceiling 2: where does the truth rank by jaro-winkler? ----------------
nm <- which(miss & !ok$NAME)
e <- data.frame(i = nm, truth = fold(r$OFFICIAL_STREET_NAME[nm]),
                mun = fold(r$MAIL_MUN_NAME[nm]), prov = r$MAIL_PROV_ABVN[nm],
                typo = r$has_typo[nm], mun_dropped = r$mun_form[nm] == "drop",
                got = fold(ifelse(is.na(got$STREET_NAME[nm]), "", got$STREET_NAME[nm])),
                text = r$text[nm])
e <- e[nzchar(e$got) & !e$mun_dropped, ]
DBI::dbWriteTable(con, "pn", e, temporary = TRUE, overwrite = TRUE)
e <- merge(e, DBI::dbGetQuery(con, "
  WITH c AS (
    SELECT p.i, s.NAME_FOLD, s.OFFICIAL_STREET_NAME,
           jaro_winkler_similarity(s.NAME_FOLD, p.got) AS sim, p.truth
      FROM pn p JOIN Streets s
        ON upper(s.MAIL_MUN_NAME) = p.mun AND s.MAIL_PROV_ABVN = p.prov),
  ranked AS (
    SELECT i, NAME_FOLD, OFFICIAL_STREET_NAME, sim, truth,
           row_number() OVER (PARTITION BY i ORDER BY sim DESC, NAME_FOLD) AS rk
      FROM c)
  SELECT i, min(CASE WHEN NAME_FOLD = truth THEN rk END) AS truth_rank,
         max(rk) AS n_cand,
         string_agg(CASE WHEN rk <= 10 THEN OFFICIAL_STREET_NAME END, '|' ORDER BY rk) AS top10
    FROM ranked GROUP BY i"), by = "i", all.x = TRUE)

cat("\nstreet-name misses with a municipality to restrict on:", nrow(e), "\n")
cat(sprintf("  truth is the top jaro-winkler candidate anyway: %d of %d (%.0f%%)\n",
            sum(e$truth_rank == 1, na.rm = TRUE), nrow(e),
            100 * sum(e$truth_rank == 1, na.rm = TRUE) / nrow(e)))
cat(sprintf("  top 5: %d   top 10: %d   top 20: %d   absent: %d   median candidates: %d\n",
            sum(e$truth_rank <= 5, na.rm = TRUE), sum(e$truth_rank <= 10, na.rm = TRUE),
            sum(e$truth_rank <= 20, na.rm = TRUE), sum(is.na(e$truth_rank)),
            as.integer(stats::median(e$n_cand, na.rm = TRUE))))

# The 55-row class where the truth is our answer plus a word ranks terribly by
# similarity (679th for `5` -> `NO. 5`), so it is outside any shortlist a model
# would be shown. Containment reaches it for nothing.
esc <- function(x) gsub("([][.^$|()\\\\*+?{}])", "\\\\\\1", x, perl = TRUE)
ct <- vapply(seq_len(nrow(e)), function(k) {
  cand <- DBI::dbGetQuery(con, "
    SELECT DISTINCT NAME_FOLD FROM Streets
     WHERE replace(upper(MAIL_MUN_NAME), '.', '') = replace(?, '.', '')
       AND MAIL_PROV_ABVN = ? AND NAME_FOLD IS NOT NULL
       AND regexp_matches(NAME_FOLD, '(^|[^A-Z0-9])' || ? || '([^A-Z0-9]|$)')",
    params = list(e$mun[k], e$prov[k], esc(e$got[k])))$NAME_FOLD
  c(n = length(cand), hit = e$truth[k] %in% cand,
    uniq = length(cand) == 1 && e$truth[k] %in% cand)
}, numeric(3))
cat(sprintf("\nwhole-word containment over the same misses: truth found %d, unique %d, wrong %d\n",
            sum(ct["hit", ]), sum(ct["uniq", ]), sum(ct["n", ] > 0 & !ct["hit", ])))

# ---- the two experiments --------------------------------------------------

`%||%` <- function(a, b) if (is.null(a)) b else a

ask <- function(prompt) {
  body <- list(model = MODEL, prompt = prompt, stream = FALSE, think = FALSE,
               format = list(type = "object", required = list("choice"),
                             properties = list(choice = list(type = "integer"))),
               options = list(temperature = 0, num_predict = 24))
  out <- tryCatch({
    j <- httr2::request(paste0(HOST, "/api/generate")) |>
      httr2::req_body_json(body) |> httr2::req_timeout(120) |>
      httr2::req_perform() |> httr2::resp_body_json()
    list(choice = jsonlite::fromJSON(j$response)$choice,
         ms = (j$total_duration %||% 0) / 1e6)
  }, error = function(e) list(choice = NA_integer_, ms = NA_real_))
  if (length(out$choice) != 1 || is.na(out$choice)) out$choice <- NA_integer_
  out
}

run <- function(items, label) {
  t0 <- Sys.time()
  res <- lapply(items$prompt, ask)
  picked <- vapply(res, function(a) a$choice, integer(1))
  ms <- vapply(res, function(a) a$ms, numeric(1))
  cat(sprintf("\n%s -- %s\n  n=%d  correct=%d (%.1f%%)  invalid=%d  median %.0f ms/call  %.1fs total\n",
              label, MODEL, nrow(items),
              sum(!is.na(picked) & picked == items$answer),
              100 * mean(!is.na(picked) & picked == items$answer),
              sum(is.na(picked)), stats::median(ms, na.rm = TRUE),
              as.numeric(Sys.time() - t0, units = "secs")))
}

if (!requireNamespace("httr2", quietly = TRUE)) {
  cat("\nhttr2 not installed -- ceilings only, skipping the model experiments.\n")
} else {
  m <- d[d$truth_in == 1 & d$n_mun >= 2 & d$n_mun <= 20, ]
  items <- do.call(rbind, lapply(seq_len(nrow(m)), function(k) {
    opts <- sort(strsplit(m$muns[k], "|", fixed = TRUE)[[1]])
    data.frame(prompt = paste0(
      "This is a Canadian address:\n\"", m$text[k], "\"\n\n",
      "It is in ", m$prov[k], ". The address does not name its municipality. ",
      "Exactly one of these municipalities has a street by that name:\n",
      paste0(seq_along(opts), ". ", opts, collapse = "\n"),
      "\n\nWhich one is the address in? Reply with the option number only."),
      answer = match(m$truth[k], opts), n_opts = length(opts))
  }))
  cat("\nmunicipality pick from a 2-20 shortlist\n")
  cat("  baseline: the pipeline returns NA for every one of these, so 0%\n")
  cat(sprintf("  chance:   %.1f%%\n", 100 * mean(1 / items$n_opts)))
  run(items, "municipality shortlist")

  f <- e[!is.na(e$top10), ]
  items <- do.call(rbind, lapply(seq_len(nrow(f)), function(k) {
    opts <- strsplit(f$top10[k], "|", fixed = TRUE)[[1]]
    data.frame(prompt = paste0(
      "A Canadian address was typed as:\n\"", f$text[k], "\"\n\n",
      "The street name may be misspelled. These are the real street names in ",
      f$mun[k], ", ", f$prov[k], " that are closest to what was typed:\n",
      paste0(seq_along(opts), ". ", opts, collapse = "\n"),
      "\n\nWhich one did the writer mean? Reply with the option number only."),
      answer = match(f$truth[k], toupper(opts)), n_opts = length(opts))
  }))
  cat("\nstreet-name pick from the 10 nearest real streets\n")
  cat(sprintf("  in shortlist: %d of %d\n", sum(!is.na(items$answer)), nrow(items)))
  # Scored over all rows, as the model is: a shortlist that does not contain
  # the truth is a row neither can get right, and dropping those flatters both.
  cat(sprintf("  baseline: take the top jaro-winkler candidate = %d / %d (%.1f%%)\n",
              sum(items$answer == 1, na.rm = TRUE), nrow(items),
              100 * sum(items$answer == 1, na.rm = TRUE) / nrow(items)))
  items$answer[is.na(items$answer)] <- -1L
  run(items, "street-name shortlist")
}
