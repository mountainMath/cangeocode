# Layer 2 of address normalization: resolving parsed components against the
# streets NAR actually contains.
#
# The parser in normalize_address.R works from a closed vocabulary and knows
# nothing about which streets exist. This layer supplies that, which is what
# corrects misspellings, fills in a type or direction the string omitted, and
# settles the comma-less splits the parser could only guess at.

#' Does this database carry the street gazetteer?
#'
#' @description `Streets` and `PostalMun` arrived in schema version 4, and
#' `MunAlias` in version 5. Databases
#' built before it are still perfectly usable -- they just skip this layer -- so
#' the check is a capability probe rather than an error.
#' @param con A NAR connection
#' @return `TRUE` when both gazetteer tables are present
#' @keywords internal
nar_has_streets <- function(con) {
  all(c("Streets", "MunAlias", "PostalMun") %in% DBI::dbListTables(con))
}

#' Resolve parsed address components against the NAR street gazetteer
#'
#' @description Restricts candidate streets to the locality -- recovering it
#' from the postal code when the string never named one -- then scores each
#' candidate on name similarity, agreement of street type, and agreement of
#' direction. Restriction is what makes this affordable: Vancouver has 1,175
#' distinct streets against 374k nationally, so the fuzzy comparison runs over a
#' candidate set three orders of magnitude smaller than the gazetteer.
#'
#' The locality is resolved through `MunAlias`, not by matching `MAIL_MUN_NAME`
#' directly. Mailing cities and census subdivisions overlap without nesting: a
#' mailing city can span jurisdictions, a jurisdiction carries many mailing
#' cities, and amalgamation left retired names in use on both sides. Matching a
#' single canonical string therefore drops real addresses, so a name resolves to
#' every jurisdiction it can denote and the candidate set is their union.
#'
#' A consequence worth knowing: `MUN_NAME` comes back as the matched street\'s
#' own `MAIL_MUN_NAME`, which may not be the name that was written. `Toronto`
#' can resolve to `SCARBOROUGH`. That is the value NAR is keyed on, and so the
#' one a join needs.
#'
#' A postal code is never required. It only ever stands in for a municipality
#' the string did not name, and when neither is present the query falls back to
#' an exact, indexed name match -- discounted in confidence, and returning no
#' municipality rather than inventing one. Only fuzzy correction genuinely needs
#' a locality, since there is nothing else to bound the candidate set with.
#'
#' Both NAR name families are matched against. Neither is complete on its own:
#' `MAIL_STREET_NAME` is empty for 957k addresses, `OFFICIAL_STREET_NAME` for 95,
#' and where both exist they differ beyond case for a further 530k.
#'
#' When [rqa_import()] has been run, a **second pass** offers Quebec's own
#' register the rows NAR could not resolve -- and only those, so no answer that
#' already worked can change. A match there comes back with
#' `parse_source = \"rqa\"` rather than `\"gazetteer\"`, because the street was
#' canonicalized against a register NAR does not carry it in, and a join against
#' `Addresses` will still not find it.
#'
#' @param res A tibble from [nar_parse_rules()]
#' @param con An open NAR connection
#' @param threshold Minimum combined score for a match to be accepted
#' @param name_threshold Minimum name similarity, applied as a gate before the
#' combined score. Without it the type and direction credit alone carries a
#' weak name over the line: `MAIN` against `MAITLAND` scores only 0.88 on the
#' name, but a matching type and an absent direction would still clear a
#' combined 0.85 and silently substitute the wrong street.
#' @param mun_swap_penalty Score multiplier applied to a NAR candidate whose
#' municipality is not the one the string named and is not an attested alias of
#' it -- see the penalty section of [nar_gazetteer_sql()]. Not applied to the
#' RQA pass, which files under census subdivisions rather than postal cities
#' and so changes the municipality by design.
#' @param keep_refused Whether to report the matches the combined `threshold`
#' turned away instead of discarding them. See the section below.
#' @section Reporting what was refused: A match that scores below `threshold` is
#' normally dropped, and the row comes back parsed but unresolved -- which from
#' the outside is indistinguishable from the street not existing. That is a
#' false negative with nothing to read: not the answer that was rejected, not
#' its score, not the evidence class that sank it.
#'
#' `keep_refused = TRUE` adopts the best sub-threshold match anyway and adds a
#' `refused_for` column naming the gate it failed:
#'
#' * `"mun_swap"` -- the score cleared `threshold` before the municipality-swap
#'   multiplier and not after. The street matched; the municipality is one NAR
#'   files mail under, shares no postal code with the answer, and is not its
#'   census subdivision\'s own name. A caller holding locality evidence of its
#'   own is exactly the caller who can overrule this.
#' * `"score"` -- everything else: a weak name, a disagreeing type or direction,
#'   a civic number outside every candidate\'s range.
#' * `NA` -- the row was not refused.
#'
#' `confidence` carries the sub-threshold score, and `mun_evidence` the class,
#' so the row can be re-filtered on either. Rows that cleared `name_threshold`
#' are the whole of what can be reported: that gate is applied inside the query,
#' so a name too far from every candidate never comes back at all.
#' @return `res` with matched rows replaced by their canonical values,
#' `confidence` set to the match score and `parse_source` set to `"gazetteer"`
#' or `"rqa"` according to which register answered
#' @keywords internal
nar_resolve_gazetteer <- function(res, con, threshold = 0.85, name_threshold = 0.90,
                                  mun_swap_penalty = 0.88, keep_refused = FALSE) {
  if (!nar_has_streets(con)) {
    warning("This NAR database predates the street gazetteer (schema version 4); ",
            "returning rules-only results. Rebuild with ",
            "nar_connection(refresh = TRUE) to enable gazetteer resolution.",
            call. = FALSE)
    return(res)
  }

  res$.row <- seq_len(nrow(res))
  # Created here rather than inside the pass, because the column list the
  # result is cut back to is taken now.
  if (keep_refused) res$refused_for <- NA_character_
  out_cols <- setdiff(names(res), ".row")

  # Every reading nar_parse_rules() produced is probed, not just the one its
  # own arbitration preferred -- this layer is the better evidence, and asking
  # it to rule on a single reading throws away the question. The candidates
  # arrive as an attribute; a caller that built `res` some other way is treated
  # as one candidate per row, which is what this function always did.
  cand <- attr(res, "nar_candidates")
  if (is.null(cand)) {
    cand <- res
    cand$.cand <- 1L
  }
  cand$.probe <- seq_len(nrow(cand))

  res <- nar_gazetteer_pass(res, cand, con,
                            eligible = !is.na(cand$STREET_NAME),
                            sql_fn = function(probe, name_threshold)
                              nar_gazetteer_sql(probe, name_threshold,
                                                mun_swap_penalty),
                            source = "gazetteer",
                            threshold = threshold, name_threshold = name_threshold,
                            prepare = nar_gazetteer_prepare,
                            keep_refused = keep_refused,
                            mun_swap_penalty = mun_swap_penalty)

  # Quebec's own register, second and only over what NAR left. Priority is
  # running order, exactly as in geocode(): no row NAR resolved can be
  # displaced by one of these, so importing RQA cannot change an answer that
  # already worked -- it can only fill in one that did not.
  if (nar_has_rqa(con)) {
    # A row NAR only refused is still unresolved as far as this pass is
    # concerned: the flagged answer is a report, not a claim strong enough to
    # keep Quebec's own register from answering the same row properly.
    refused <- if (!"refused_for" %in% names(res)) rep(FALSE, nrow(res))
               else !is.na(res$refused_for)
    unresolved <- res$parse_source != "gazetteer" | refused
    res <- nar_gazetteer_pass(
      res, cand, con,
      eligible = !is.na(cand$STREET_NAME) &
        unresolved[match(cand$.row, res$.row)] &
        (is.na(cand$PROV_ABVN) | cand$PROV_ABVN == "QC"),
      sql_fn = nar_rqa_gazetteer_sql, source = "rqa",
      threshold = threshold, name_threshold = name_threshold,
      keep_refused = keep_refused)
  }

  res <- res[, out_cols, drop = FALSE]
  attr(res, "nar_candidates") <- NULL
  res
}

#' Score one gazetteer against every candidate reading, and adopt the winner
#'
#' @description The machinery both passes share: build the probe, score it
#' database-side, take one winner per input row, and write the canonical values
#' back. Only the eligible rows, the query and the `parse_source` label differ,
#' which is why they are arguments rather than two copies of this.
#'
#' @param res The rows being resolved, carrying `.row`
#' @param cand Every candidate reading, carrying `.row`, `.cand` and `.probe`
#' @param con An open NAR connection
#' @param eligible Logical over `cand`: which readings this pass may probe
#' @param sql_fn A function of `(probe_table, name_threshold)` returning SQL
#' @param source The `parse_source` value a match from this pass earns
#' @param threshold Minimum combined score for a match to be accepted
#' @param name_threshold Minimum name similarity, passed to `sql_fn`
#' @param prepare Optional function of `con`, run once there is work to do
#' @param keep_refused Whether to also adopt the best sub-threshold match,
#' flagged in `refused_for`
#' @param mun_swap_penalty The multiplier this pass\'s query applied, used only
#' to tell a refusal the penalty caused from a refusal it did not
#' @return `res`, with matched rows replaced by their canonical values
#' @keywords internal
nar_gazetteer_pass <- function(res, cand, con, eligible, sql_fn, source,
                               threshold = 0.85, name_threshold = 0.90,
                               prepare = NULL, keep_refused = FALSE,
                               mun_swap_penalty = 1) {
  todo <- cand[eligible, , drop = FALSE]
  if (!nrow(todo)) return(res)

  # The municipality the *string* yielded, which is not the same as the one the
  # reading being scored yielded. An alternative reading may re-segment the
  # trailing run and hand back a shorter name -- `HOWIE CENTRE` read as
  # `CENTRE`, which is itself a Nova Scotia municipality. Scoring the swap
  # against that would let a truncation manufacture its own attestation: NAR
  # files no mail to `HOWIE CENTRE`, but `CENTRE` shares a postal code with
  # `LUNENBURG`, so the reading that broke the name would be the one credited
  # with matching it. The baseline reading is the anchor -- it is the one that
  # took the string's own delimiters at face value.
  base <- cand[order(cand$.row, cand$.cand), , drop = FALSE]
  base <- base[!duplicated(base$.row), , drop = FALSE]
  mun_input <- base$MUN_NAME[match(todo$.row, base$.row)]

  probe <- data.frame(
    row_id    = todo$.probe,
    name_fold = nar_fold(todo$STREET_NAME),
    match_fold = nar_match_fold(todo$STREET_NAME),
    mun_fold  = nar_fold(ifelse(is.na(todo$MUN_NAME), "", todo$MUN_NAME)),
    mun_match = nar_match_fold(ifelse(is.na(todo$MUN_NAME), "", todo$MUN_NAME)),
    mun_input = nar_match_fold(ifelse(is.na(mun_input), "", mun_input)),
    prov      = ifelse(is.na(todo$PROV_ABVN), "", todo$PROV_ABVN),
    fsa       = ifelse(is.na(todo$POSTAL_CODE), "", substr(todo$POSTAL_CODE, 1, 3)),
    civic     = todo$CIVIC_NO,
    type      = ifelse(is.na(todo$STREET_TYPE), "", todo$STREET_TYPE),
    dir       = ifelse(is.na(todo$STREET_DIR), "", todo$STREET_DIR),
    stringsAsFactors = FALSE
  )

  tmp <- paste0("nar_probe_", as.integer(stats::runif(1) * 1e9))
  DBI::dbWriteTable(con, tmp, probe, temporary = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, tmp), silent = TRUE), add = TRUE)

  if (!is.null(prepare)) prepare(con)
  best <- DBI::dbGetQuery(con, sql_fn(tmp, name_threshold))

  if (!nrow(best)) return(res)
  best$.w    <- match(best$row_id, cand$.probe)
  best$.row  <- cand$.row[best$.w]
  best$.cand <- cand$.cand[best$.w]

  acc <- nar_gazetteer_winner(best[best$score >= threshold, , drop = FALSE])
  res <- nar_gazetteer_adopt(res, cand, acc, source)
  if (!keep_refused) return(res)

  # What the combined threshold turned away. Adopted rather than dropped, but
  # never silently: `confidence` carries the sub-threshold score and
  # `refused_for` names the gate, so a match the pipeline would have thrown
  # away -- indistinguishable, from the outside, from no such street existing
  # -- comes back as a flagged answer the caller can drop again.
  #
  # Only rows this pass did not already answer, and only rows no earlier pass
  # answered: a NAR refusal outranks an RQA one, which is the same running-order
  # priority the accepted matches follow.
  ref <- nar_gazetteer_winner(
    best[best$score < threshold & !best$.row %in% acc$.row, , drop = FALSE])
  if (nrow(ref)) {
    ref <- ref[res$parse_source[match(ref$.row, res$.row)] == "rules", ,
               drop = FALSE]
  }
  if (!nrow(ref)) return(res)

  # Which gate, and only one of the two is worth naming separately. A score
  # that clears the threshold before the swap multiplier and not after says the
  # street matched and the municipality did not -- a different failure, and one
  # a caller with its own locality evidence can act on. It is recoverable here
  # rather than needing a second column out of the query because the penalty
  # applies to exactly one evidence class.
  penalised <- !is.na(ref$mun_evidence) & ref$mun_evidence == "unattested" &
    ref$score / mun_swap_penalty >= threshold
  nar_gazetteer_adopt(res, cand, ref, source,
                      ifelse(penalised, "mun_swap", "score"))
}

#' One winner per input row
#'
#' @description The highest-scoring reading; on a tie, the one that answered in
#' the municipality the string wrote; and on a tie in that too, the earliest
#' candidate -- which is the baseline parse. A reading only displaces the
#' baseline by resolving to a street this one does not, or to the same quality
#' of street without moving the town.
#'
#' The middle term is not a preference for tidiness. A municipality swap is
#' normally fined 0.88, so a reading that moved the town rarely ties one that
#' did not -- but the fine exempts an *attested* swap, and an attested swap is
#' exactly what a real place name buys: `170 North Park St, Brantford` resolves
#' at 1.0 to `Park Rd N` in HAMILTON, because Brantford and Hamilton share a
#' census subdivision name, while the reading that keeps the compass word in
#' the name resolves at 1.0 to `North Park St` in Brantford itself. Two
#' readings, both perfect, and the exemption is what let the wrong one through.
#' Score cannot separate them and the swap penalty has already declined to, so
#' the only remaining question is which one stayed where it was told.
#'
#' It cannot fire on a row with one candidate, and it cannot fire where the
#' string named no municipality -- there `mun_kept` is `FALSE` for every
#' reading of the row, since it compares against the *baseline* reading's
#' municipality, which is the same value for all of them. That anchoring is
#' also why the term is inert on the municipality-anchored variants: those
#' exist to re-segment a trailing run into a town the baseline missed, so their
#' answers are the ones that differ from the baseline's, and the term points
#' back at the baseline they were generated to challenge. It bites in one
#' place, which is the place it was measured on.
#' @param best Scored rows carrying `.row`, `.cand`, `score` and `mun_kept`
#' @return `best`, one row per `.row`
#' @keywords internal
nar_gazetteer_winner <- function(best) {
  if (!nrow(best)) return(best)
  o <- order(best$.row, -best$score, -(best$mun_kept %in% TRUE), best$.cand)
  best[o[!duplicated(best$.row[o])], , drop = FALSE]
}

#' Write a gazetteer winner back over the parse it corrects
#'
#' @param res The rows being resolved, carrying `.row`
#' @param cand Every candidate reading, carrying `.probe`
#' @param best One winning row per input, from [nar_gazetteer_winner()]
#' @param source The `parse_source` value a match from this pass earns
#' @param refused_for `NA`, or the gate these rows failed
#' @return `res`, with the matched rows replaced by their canonical values
#' @keywords internal
nar_gazetteer_adopt <- function(res, cand, best, source,
                                refused_for = NA_character_) {
  if (!nrow(best)) return(res)
  w <- best$.w
  i <- match(best$.row, res$.row)

  # Adopt the winning reading's own parse before correcting it. The readings
  # disagree about more than the street: the unit and the civic number move
  # with the municipality, so applying the gazetteer's corrections on top of a
  # different reading's columns would mix two parses into one row.
  for (col in setdiff(nar_normalized_columns(), c("PROV_ABVN", "POSTAL_CODE"))) {
    res[[col]][i] <- cand[[col]][w]
  }
  res$pattern[i] <- as.character(cand$pattern[w])

  res$STREET_NAME[i]  <- best$STREET_NAME
  res$STREET_TYPE[i]  <- nar_blank_to_na(best$STREET_TYPE)
  res$STREET_DIR[i]   <- nar_blank_to_na(best$STREET_DIR)
  # coalesce, not assign: the exact-name branch resolves a street without any
  # locality, and returns NULL for both rather than guessing one.
  res$MUN_NAME[i]     <- ifelse(is.na(best$MAIL_MUN_NAME), res$MUN_NAME[i],
                                best$MAIL_MUN_NAME)
  res$PROV_ABVN[i]    <- ifelse(is.na(best$MAIL_PROV_ABVN), res$PROV_ABVN[i],
                                best$MAIL_PROV_ABVN)
  res$confidence[i]   <- round(best$score, 3)
  res$parse_source[i] <- source
  # TRUE only where the gazetteer actually wrote a municipality *and* it is not
  # the one the string named. Where MAIL_MUN_NAME came back NA the line above
  # kept the parse's own value, which is the input's and so was not remapped.
  res$mun_remapped[i] <- !is.na(best$MAIL_MUN_NAME) & !best$mun_kept
  # Same gate as mun_remapped, for the same reason: where the branch returned no
  # municipality the row kept the parse's own, and there is nothing to classify.
  res$mun_evidence[i] <- ifelse(is.na(best$MAIL_MUN_NAME), NA_character_,
                                best$mun_evidence)
  # Present only when the caller asked for the refusals. An accepted match
  # clears it, so a later pass answering a row an earlier one only refused
  # leaves no stale reason behind.
  if ("refused_for" %in% names(res)) res$refused_for[i] <- refused_for
  res
}

#' Fold a street name to the form the fuzzy branch compares on
#'
#' @description [nar_fold()] settles case and accents, which is enough for an
#' equality join. The fuzzy branch needs more, because two of Quebec's spelling
#' conventions put a correct parse and NAR's own spelling of the same street on
#' opposite sides of the name gate:
#'
#' * **The hyphen is not a distinguishing character.** NAR writes
#'   `du Square-Victoria`, `du Curé-Labelle`, `Alexis-Nihon`; people write the
#'   words with spaces, and usually without the leading particule. Whole-word
#'   containment is exactly the rule that should catch `VICTORIA` inside
#'   `du Square-Victoria` -- and it does not, because with the hyphen in place
#'   `SQUARE-VICTORIA` is one word. Folding it to a space is what lets the rule
#'   fire. This is not a Quebec-only change: `du Bord-du-Lac--Lakeshore` and
#'   `Grande Côte` are the same problem, and English Canada's hyphenated names
#'   gain the same way.
#' * **`ST` and `STE` are abbreviations of `SAINT` and `SAINTE`**, and NAR
#'   spells them out. `ST-JACQUES` against `Saint-Jacques` is six edits on a
#'   thirteen-character string -- nowhere near the gate, and nowhere near the
#'   top of a similarity ranking either. Expanding both sides is the only thing
#'   that makes them meet, and applying it to both sides is what keeps it safe:
#'   a name that really does contain a bare `ST` still matches itself.
#'
#' The apostrophe goes the same way as the hyphen, for the same reason:
#' `de l'Orme` and `DE L ORME` are one street.
#'
#' Applied to the *probe* it produces `match_fold`, which is deliberately a
#' second column rather than a replacement for `name_fold` -- the exact branch
#' joins on `name_fold` through an index, and this expression would defeat it.
#' @param x A character vector
#' @return A character vector folded for comparison
#' @keywords internal
nar_match_fold <- function(x) {
  # The padding below is `paste0(" ", ..., " ")`, and paste0() with a
  # zero-length argument returns one element rather than none -- so without
  # this the fold would answer a one-row vector to an empty query, and the
  # caller building a data frame around it would fail on the length mismatch
  # rather than on anything to do with addresses.
  if (!length(x)) return(character(0))
  x <- gsub("[.]", "", nar_fold(x))
  x <- gsub("[-']", " ", x)
  x <- paste0(" ", trimws(gsub("[[:space:]]+", " ", x)), " ")
  x <- gsub(" STE ", " SAINTE ", x, fixed = TRUE)
  x <- gsub(" ST ", " SAINT ", x, fixed = TRUE)
  trimws(x)
}

#' [nar_match_fold()] as a SQL expression over one column
#'
#' @description The same transform DuckDB-side, so the gazetteer's own spelling
#' is folded the way the probe was. It has to stay in step with
#' [nar_match_fold()] character for character, which `test-normalize.R` asserts
#' over a fixture of the shapes it exists for.
#' @param col A SQL column reference
#' @return A SQL expression string
#' @keywords internal
nar_match_fold_sql <- function(col) {
  # Padded with spaces so the word replacements need no anchors, and trimmed
  # again on the way out.
  #
  # The dash class carries the en and em dash beside the hyphen, and that is
  # the one place the two halves could silently drift apart. R's half never
  # sees either, because stringi's Latin-ASCII transliteration inside
  # nar_fold() has already turned them into a hyphen; DuckDB's strip_accents()
  # leaves both alone. Quebec's register writes a dual name with an en dash --
  # `Bord-du-Lac-Lakeshore` in 11 street names over 2,472 addresses -- where
  # NAR transliterates the same names to a double hyphen, so without this the
  # two spellings of the same street fold apart and never meet.
  quo <- "''''"
  dash <- "'[-\u2013\u2014]'"
  inner <- paste0("regexp_replace(regexp_replace(replace(replace(", col,
                  ", '.', ''), ", quo, ", ' '), ", dash,
                  ", ' ', 'g'), '\\s+', ' ', 'g')")
  paste0("trim(replace(replace(' ' || trim(", inner,
         ") || ' ', ' STE ', ' SAINTE '), ' ST ', ' SAINT '))")
}

#' Fold every gazetteer name once per connection
#'
#' @description [nar_match_fold_sql()] is six string operations, and the fuzzy
#' branch evaluates it against every candidate street of every probe row. That
#' is the same 511,848 names folded over and over: measured on the Part A
#' sample it cost 45% of the normalizer's throughput, 399 rows a second down to
#' 217. Folding the whole gazetteer once instead takes 68 ms.
#'
#' So it is done once per connection and kept, the same way the spatial macros
#' are -- a TEMP table, invisible to other sessions, dropped when the connection
#' closes. `Streets` is written once at import and never updated, so `rowid` is
#' a stable key to join it back on.
#'
#' The alternative was a stored column and a schema bump, which would make every
#' database built before it slower rather than merely different. This costs
#' nothing at import and needs no re-import.
#' @param con An open NAR connection
#' @return Invisibly `TRUE` when the table is present
#' @keywords internal
nar_street_fold <- function(con) {
  if ("StreetFold" %in% DBI::dbListTables(con)) return(invisible(TRUE))
  DBI::dbExecute(con, paste0(
    "CREATE TEMP TABLE StreetFold AS SELECT rowid AS SID, ",
    nar_match_fold_sql("NAME_FOLD"), " AS S_FOLD, ",
    nar_match_fold_sql("MAIL_NAME_FOLD"), " AS S_MAIL_FOLD FROM Streets"))
  invisible(TRUE)
}

#' The municipality co-occurrence tables the swap penalty is scored against
#'
#' @description Two TEMP tables, built once per connection like [nar_street_fold()]
#' and for the same reason -- they are derived from `Addresses` and would
#' otherwise be recomputed on every call.
#'
#' * **`MunMail`** -- every name NAR files postal-coded mail under, folded. It is
#'   what separates "this name was swapped for another" from "this name is not a
#'   mailing municipality at all", which is the ordinary case of a jurisdiction
#'   name being resolved to the mailing city NAR files under. Only the first is
#'   penalised; the second is what `MunAlias` exists to do. Read off the same
#'   rows as `MunCoPostal` on purpose: a name that could never have appeared in
#'   the pair table must not be scored as a name that could have and did not.
#' * **`MunCoPostal`** -- pairs of mailing municipalities that share at least one
#'   **full** postal code. This is the alias evidence, and it is read off Canada
#'   Post's own delivery geography rather than guessed: two names that receive
#'   mail at the same postal code are two names for the same place, whatever the
#'   census thinks. `HOWIE CENTER` and `SYDNEY` share three, which is exactly the
#'   rural-community-to-mailing-city remap the gazetteer exists to perform.
#'   `MILFORD` and `MIDDLE SACKVILLE` share none, and that is the swap that puts
#'   an address 60 km away.
#'
#' The full postal code and not the FSA. An FSA is a forward sortation area and
#' in rural Nova Scotia one of them covers most of a county, so `PostalMun` --
#' which is FSA-keyed and already present -- would attest nearly every pair in
#' the province and the penalty would never fire.
#'
#' 32,216 directed pairs nationally, 0.2 s to build. 95% of them rest on a single
#' shared postal code, which is why the default `min_pc` is 1: raising it to 2
#' discards 30,694 of the 32,216 and turns a permissive check into a strict one.
#' @param con A NAR connection
#' @param min_pc Shared full postal codes required to attest a pair
#' @return Invisibly `TRUE` when the tables are present
#' @keywords internal
nar_mun_copostal <- function(con, min_pc = 1) {
  have <- DBI::dbListTables(con)
  if (all(c("MunMail", "MunCoPostal") %in% have)) return(invisible(TRUE))
  fold <- nar_match_fold_sql("strip_accents(upper(MAIL_MUN_NAME))")
  DBI::dbExecute(con, paste0(
    "CREATE TEMP TABLE MunPostalFold AS
       SELECT DISTINCT MAIL_POSTAL_CODE AS PC, MAIL_PROV_ABVN AS PR, ", fold, " AS MN
         FROM Addresses
        WHERE length(MAIL_POSTAL_CODE) = 6 AND MAIL_MUN_NAME <> ''"))
  DBI::dbExecute(con,
    "CREATE TEMP TABLE MunMail AS SELECT DISTINCT PR, MN FROM MunPostalFold")
  # Directed, so the join in the scoring query needs no OR: the written name is
  # always MN_A and the candidate street's always MN_B.
  DBI::dbExecute(con, paste0(
    "CREATE TEMP TABLE MunCoPostal AS
       SELECT a.PR, a.MN AS MN_A, b.MN AS MN_B, count(*) AS N_PC
         FROM MunPostalFold a JOIN MunPostalFold b USING (PC, PR)
        WHERE a.MN <> b.MN
        GROUP BY 1, 2, 3 HAVING count(*) >= ", as.integer(min_pc)))
  DBI::dbExecute(con, "DROP TABLE MunPostalFold")
  invisible(TRUE)
}

#' Everything the NAR gazetteer pass needs in place before it runs
#' @param con A NAR connection
#' @return Invisibly `TRUE`
#' @keywords internal
nar_gazetteer_prepare <- function(con) {
  nar_street_fold(con)
  nar_mun_copostal(con)
}

#' Empty strings back to NA
#'
#' @description NAR stores an absent street type or direction as `''` rather
#' than NULL, but the normalizer's contract is `NA` for absent.
#' @param x A character vector
#' @return A character vector
#' @keywords internal
nar_blank_to_na <- function(x) ifelse(is.na(x) | !nzchar(x), NA_character_, x)

#' The gazetteer scoring query
#'
#' @description Kept as its own function so the scoring can be read and tested
#' without a database. Two branches, selected by whether the row has a locality
#' to restrict candidates to:
#'
#' * **fuzzy**, when a municipality was named or a postal code supplies one --
#'   name similarity (weight 0.72) plus agreement on street type (0.10),
#'   direction (0.06) and the civic number falling inside the street's range
#'   (0.12). The last three only ever add: a string that omitted them should not
#'   be penalised for it, but one that supplied them and agrees should outrank a
#'   competing street that does not.
#' * **exact**, when it has neither -- an indexed equality on either name family,
#'   scored the same way but discounted for the absent locality, and answering
#'   only with what every candidate of that name agrees on.
#'
#' @section The penalty for changing the municipality: The fuzzy branch widens
#' the candidate set from the municipality that was written to every
#' municipality sharing its census subdivision, which is how a misspelt street
#' in a small community is found at all. But the widening is coarse -- `MILFORD,
#' NS` reaches 166 communities over 127 km through Halifax Regional Municipality
#' alone -- and inside that set a street in the *wrong* community can outscore
#' the right one on a single edit, because agreement on the street type (0.10)
#' buys more than one Damerau-Levenshtein step costs (0.072 at the gate).
#'
#' So a candidate whose mailing municipality is not the one the string named has
#' its whole score multiplied by `mun_swap_penalty` -- unless the two names are
#' *attested aliases*, meaning they share a full postal code somewhere in NAR
#' (see [nar_mun_copostal()]). The penalty is multiplicative rather than a
#' subtraction so that it scales with how good the match otherwise was, and it
#' reorders as well as refuses: an unattested swap now loses to a lower-scoring
#' candidate in the municipality that was actually written.
#'
#' Two cases are exempt and both matter. A string that named no municipality at
#' all has nothing to have swapped -- its locality came from the postal code.
#' And a name NAR files no mail under (`p.mun_testable` false) cannot be checked
#' for attestation either way, so penalising it would fine the string for being
#' unusual rather than for being wrong.
#'
#' **Why 0.88.** With the 0.85 acceptance threshold it means an unattested swap
#' has to score 0.966 before the penalty, which is a street name that is exact or
#' one keystroke from it *and* agreement on everything else the string supplied:
#' the rule is that two uncertainties at once is one too many. Swept against
#' Nova Scotia's PVSC points, over 32,887 exact unambiguous building matches
#' carrying 98 errors past 5 km:
#'
#' | penalty | matches lost | of those, past 5 km | 5 km errors removed |
#' | --- | --- | --- | --- |
#' | 0.92 | 107 | 15.9% | 17 |
#' | 0.90 | 151 | 12.6% | 19 |
#' | **0.88** | **503** | **9.2%** | **46** |
#' | 0.86 | 566 | 8.3% | 47 |
#' | 0.85 | 1126 | 5.1% | 57 |
#'
#' 0.88 is the knee and the exchange rate is what picks it, not the error count:
#' 0.90 to 0.88 buys 27 gross errors for 352 matches, 0.88 to 0.86 buys one more
#' for 63. Below 0.85 the penalty stops discriminating at all -- every unattested
#' swap is refused whatever else it got right, and 85% of the 1,126 rows that
#' costs were within 100 m of an independent reading. The base rate makes the
#' column in the middle the one to read: 0.3% of all matches are past 5 km, so a
#' cut that is 9.2% gross is thirty times enriched in exactly what it is for.
#'
#' One province, and the knee is what is provincial about it -- the mechanism a
#' fuzzy name compounding an unattested swap is not. `1` restores the behaviour
#' before this existed and `0.85` refuses every unattested swap outright.
#'
#' @param probe Name of the temp table holding the parsed components
#' @param name_threshold Minimum name similarity for the fuzzy branch
#' @param mun_swap_penalty Score multiplier for a candidate in a municipality
#'   other than the one the string named, where the two are not attested
#'   aliases. `1` disables it.
#' @return A single SQL string
#' @keywords internal
nar_gazetteer_sql <- function(probe, name_threshold = 0.90,
                              mun_swap_penalty = 0.88) {
  # Placeholders rather than sprintf: the template is past sprintf's 8192-byte
  # format limit, and this way a literal % in a LIKE pattern needs no doubling.
  sql <- "
    WITH probe AS (
      SELECT p.*,
             -- A municipality named in the string wins; otherwise the postal
             -- code supplies one, taking the busiest municipality in the FSA.
             -- Both may be absent, which the exact branch below picks up.
             coalesce(nullif(p.mun_match, ''),
                      (SELECT {fold_pm}
                         FROM PostalMun pm
                        WHERE pm.FSA = p.fsa AND p.fsa <> ''
                        ORDER BY pm.N_ADDRESSES DESC LIMIT 1)) AS mun_use,
             -- Whether the written name is one NAR files mail under at all. The
             -- swap penalty below rests on an *absence* of shared postal codes,
             -- and an absence only means something for a name that could have
             -- had some. `HOWIE CENTER` is a mailing municipality and shares
             -- three postal codes with `SYDNEY`; a name no letter is addressed
             -- to shares none with anything, and that is not evidence.
             EXISTS (SELECT 1 FROM MunMail mm
                      WHERE mm.MN = p.mun_input
                        AND (p.prov = '' OR mm.PR = p.prov)) AS mun_testable
        FROM {probe} p
    ),
    scored AS (
      SELECT p.row_id,
             s.OFFICIAL_STREET_NAME AS STREET_NAME,
             s.OFFICIAL_STREET_TYPE AS STREET_TYPE,
             s.OFFICIAL_STREET_DIR  AS STREET_DIR,
             s.MAIL_MUN_NAME, s.MAIL_PROV_ABVN, s.N_ADDRESSES,
             -- NAR's own spelling, folded the way the probe was --
             -- see nar_match_fold(). Read off StreetFold rather than
             -- computed here: the same names would otherwise be folded
             -- once per probe row that reaches them.
             f.S_FOLD AS s_fold,
             f.S_MAIL_FOLD AS s_mail_fold,
             -- Two kinds of evidence Jaro-Winkler cannot express, each worth
             -- a flat 0.90 rather than given a branch of its own, so
             -- name_threshold keeps meaning one thing -- and so raising it above
             -- 0.90 turns both off, which is what asking for stricter means.
             --
             -- A *single edit* is one keystroke, and that is what the residual
             -- is made of: 69 of the 77 correct answers the 0.90 gate was
             -- throwing away sat exactly one Damerau-Levenshtein step from the
             -- input. Jaro-Winkler cannot see this because it pays a prefix
             -- bonus, so the same one-key slip scores 0.89 in `NARTIN`/`MARTIN`
             -- and 0.83 in `QALL`/`WALL`. The length floor is load-bearing and
             -- not a tidiness rule: at two characters one edit is the whole
             -- word, and `5W` against `5E` is a different street, not a typo.
             --
             -- *Whole-word containment* is the other. It catches the words a
             -- parse rule ate -- `5` for `NO. 5`, `772` for `ROUTE 772`, `PARK`
             -- for `PARK LAWN` -- which similarity ranks nowhere near the top
             -- (679th, in the first of those). It cannot displace a street
             -- actually called `PARK`: that scores an exact 1.0 and wins.
             greatest(
               jaro_winkler_similarity(p.match_fold, s_fold),
               jaro_winkler_similarity(p.match_fold, s_mail_fold)
             ) AS jw_sim,
             greatest(
               jw_sim,
               -- The 0.70 floor is a cheap prefilter, not a second threshold:
               -- one edit cannot drag Jaro-Winkler below it. The worst case is
               -- a substituted first character of a three-letter word, which
               -- scores 0.778 -- and edit distance is far dearer than the
               -- similarity already computed, so it is only worth asking about
               -- candidates that are already close. Without the guard this
               -- query runs 3.5x slower for the same answers.
               -- A length gate before the edit distance, and free: one
               -- Damerau-Levenshtein step cannot bridge a length difference
               -- greater than one, so any pair that fails this would have been
               -- rejected anyway. It matters because folding the hyphen out
               -- moved many more pairs past the 0.70 similarity prefilter --
               -- `COTE-DES-NEIGES` and `COTE DES NEIGES` are near-identical
               -- strings -- and the distance itself is the dear part.
               CASE WHEN jw_sim >= 0.70 AND length(p.match_fold) >= 3
                     AND least(abs(length(p.match_fold) - length(s_fold)),
                               abs(length(p.match_fold) - length(s_mail_fold))) <= 1
                     AND least(
                      damerau_levenshtein(p.match_fold, s_fold),
                      damerau_levenshtein(p.match_fold, s_mail_fold)) <= 1
                    THEN 0.90 ELSE 0 END,
               -- Same idea, and the same reason: a candidate no longer than
               -- the probe can only contain it by being equal to it, which the
               -- similarity above already scores 1.0.
               CASE WHEN p.match_fold <> ''
                     AND greatest(length(s_fold), length(s_mail_fold))
                           > length(p.match_fold)
                     AND (' ' || s_fold || ' '
                            LIKE '% ' || p.match_fold || ' %'
                       OR ' ' || s_mail_fold || ' '
                            LIKE '% ' || p.match_fold || ' %')
                    THEN 0.90 ELSE 0 END) AS name_sim,
             (0.72 * name_sim
             + 0.10 * CASE WHEN p.type = '' THEN 1
                           WHEN p.type IN (s.OFFICIAL_STREET_TYPE, s.MAIL_STREET_TYPE) THEN 1
                           ELSE 0 END
             + 0.06 * CASE WHEN p.dir = '' THEN 1
                           WHEN p.dir IN (s.OFFICIAL_STREET_DIR, s.MAIL_STREET_DIR) THEN 1
                           ELSE 0 END
             -- The civic number is the tie-breaker the name alone cannot give.
             -- \"1055 Georgea St\" is closer to George (0.971) than to Georgia
             -- (0.943), but George runs 970-1010 and Georgia 89-1798, so the
             -- number settles it. An address with no civic number keeps full
             -- credit rather than being penalised for what it did not say.
             + 0.12 * CASE WHEN p.civic IS NULL THEN 1
                           WHEN p.civic BETWEEN s.MIN_CIVIC_NO AND s.MAX_CIVIC_NO THEN 1
                           ELSE 0 END)
             -- The order of the branches is the argument, and it is the same
             -- order the mun_evidence column below reports: keep it if the name
             -- survived, exempt it if there was no name to swap, credit it if a
             -- shared postal code attests the swap, credit it if one side is
             -- the census subdivision's own name (Toronto for North York --
             -- amalgamation, and 0 of 91 such rows in Nova Scotia land even a
             -- kilometre out), exempt it if the written name is one NAR files
             -- no mail under at all, and only then fine it. c.MN_A comes from
             -- the LEFT JOIN below and is non-NULL exactly when the two names
             -- share a postal code.
             --
             -- Fining the untestable arm was measured and rejected: it refuses
             -- 119 exact matches to remove 2 errors past 5 km, against the 13
             -- matches per error the swap arm buys at its knee. Those rows are
             -- the worst-placed class in the package -- p90 327 m, p99 32 km --
             -- but the answer to that is to *report* it, which is what the
             -- uncertainty floor keyed on mun_evidence does, not to refuse a
             -- street match that is usually right.
             * CASE WHEN {fold_smun} = p.mun_input THEN 1
                    WHEN p.mun_input = ''           THEN 1
                    WHEN c.MN_A IS NOT NULL         THEN 1
                    WHEN p.mun_input = {fold_csd}
                      OR {fold_smun} = {fold_csd}   THEN 1
                    WHEN NOT p.mun_testable         THEN 1
                    ELSE {swap} END
               AS score
             , {fold_smun} = p.mun_use AS mun_exact
             -- Not the same question as mun_exact, which asks whether the
             -- winner sits in the locality the candidates were restricted to --
             -- and that locality may itself have come from the postal code
             -- rather than from the string. This asks the caller's question:
             -- is the municipality being handed back the one that was written?
             -- It leaves the query as `mun_remapped` and reaches uncertainty_m,
             -- so it compares against p.mun_input and nothing else.
             , {fold_smun} = p.mun_input AS mun_kept
             -- The same four questions the CASE above asks, kept instead of
             -- collapsed into a multiplier. The order is the precedence: a
             -- string that named nothing is not a swap, evidence outranks the
             -- absence of evidence, and `unattested` is what is left -- a name
             -- NAR does file mail under, which shares no postal code with the
             -- answer and is not its census subdivision's own name. That last
             -- class is the one that carries the gross errors, and it is the
             -- reason this leaves the query rather than staying a multiplier.
             , CASE WHEN {fold_smun} = p.mun_input  THEN 'kept'
                    WHEN p.mun_input = ''           THEN 'inferred'
                    WHEN c.MN_A IS NOT NULL         THEN 'copostal'
                    WHEN p.mun_input = {fold_csd}
                      OR {fold_smun} = {fold_csd}   THEN 'csd'
                    WHEN NOT p.mun_testable         THEN 'untestable'
                    ELSE 'unattested' END AS mun_evidence
        FROM probe p
        -- Through the alias set rather than straight at MAIL_MUN_NAME: the name
        -- someone writes and the name NAR files under are often different names
        -- for overlapping places, in both directions.
        JOIN MunAlias m
          -- Both sides go through nar_match_fold(). Periods are the
          -- original reason -- NAR files ST. JOHN'S, SAULT STE. MARIE and
          -- ST. ALBERT with them, 1,027,129 addresses' worth, while
          -- nar_norm_text() strips them from input as mere abbreviation marks,
          -- so without this those cities resolve to nothing. Saint is the same
          -- problem one step further on: NAR files SAINT-LAURENT and
          -- SAINTE-FOY, people write ST-LAURENT and STE-FOY, and a
          -- municipality that fails to resolve takes the street with it,
          -- because the candidate set is what the municipality restricts.
          -- MunAlias is 18,313 rows, so folding it on every call costs
          -- nothing -- do not move this onto Streets, where it would cost the
          -- index, and where StreetFold already does the job.
          ON {fold_mun} = p.mun_use
         AND (p.prov = '' OR m.PROV_ABVN = p.prov)
        JOIN Streets s
          ON s.MUN_KEY = m.MUN_KEY
        JOIN StreetFold f
          ON f.SID = s.rowid
         AND (p.prov = '' OR s.MAIL_PROV_ABVN = p.prov)
        -- Directed on purpose: written name to candidate name, so no OR is
        -- needed here and the pair is looked up once. At most one row per key,
        -- so this cannot multiply candidates.
        LEFT JOIN MunCoPostal c
          ON c.PR = s.MAIL_PROV_ABVN
         AND c.MN_A = p.mun_input
         AND c.MN_B = {fold_smun}
       WHERE p.mun_use IS NOT NULL
    ),
    -- No municipality, and no postal code to recover one from. Fuzzy matching is
    -- off the table here -- there is no locality to restrict candidates to, and
    -- scanning 374k names would be both slow and a coin toss -- but an *exact*
    -- name match is an indexed equality lookup, and it still earns its keep: it
    -- canonicalizes NAR's own spelling, casing and accents, and settles a type
    -- or direction the string omitted whenever every street of that name agrees.
    exact AS (
      SELECT p.row_id,
             arg_max(s.OFFICIAL_STREET_NAME, s.N_ADDRESSES) AS STREET_NAME,
             -- Unanimity or nothing: candidates spread over many municipalities
             -- only settle a field when they all settle it the same way. Blanks
             -- abstain rather than veto -- NAR carries rows whose OFFICIAL type
             -- is empty (and one whose MAIL name is QUEEN while its OFFICIAL
             -- name is The Queensway), and letting those count as a dissenting
             -- value would suppress a type every street that states one agrees on.
             CASE WHEN count(DISTINCT nullif(s.OFFICIAL_STREET_TYPE, '')) = 1
                  THEN max(nullif(s.OFFICIAL_STREET_TYPE, '')) ELSE '' END AS STREET_TYPE,
             CASE WHEN count(DISTINCT nullif(s.OFFICIAL_STREET_DIR, '')) = 1
                  THEN max(nullif(s.OFFICIAL_STREET_DIR, '')) ELSE '' END AS STREET_DIR,
             -- Unanimity or nothing, as above. One municipality carrying the
             -- only street of that name has *determined* it -- there is nothing
             -- left to guess, and withholding it would be a different kind of
             -- wrong answer. Two or more and it stays NULL: the busiest city
             -- with a street of this name is a guess, not a resolution.
             CASE WHEN count(DISTINCT s.MAIL_MUN_NAME) = 1
                  THEN any_value(s.MAIL_MUN_NAME) END AS MAIL_MUN_NAME,
             -- The province the caller supplied wins; failing that, the one the
             -- candidates agree on, which a determined municipality always has.
             coalesce(nullif(any_value(p.prov), ''),
                      CASE WHEN count(DISTINCT s.MAIL_PROV_ABVN) = 1
                           THEN any_value(s.MAIL_PROV_ABVN) END) AS MAIL_PROV_ABVN,
             sum(s.N_ADDRESSES) AS N_ADDRESSES,
             1.0 AS name_sim,
             -- Discounted for the locality this match never had: 0.92 with a
             -- province to restrict to, 0.88 without. Both clear a 0.85
             -- threshold on their own, but neither does once the civic number
             -- falls outside every candidate's range -- 0.81 and 0.77 -- so a
             -- number that fits no street of that name still declines.
             (CASE WHEN any_value(p.prov) <> '' THEN 0.92 ELSE 0.88 END)
             * (0.88 + 0.12 * max(CASE WHEN p.civic IS NULL THEN 1
                                       WHEN p.civic BETWEEN s.MIN_CIVIC_NO
                                                        AND s.MAX_CIVIC_NO THEN 1
                                       ELSE 0 END)) AS score,
             -- Last, and in this order, because UNION ALL below lines the two
             -- branches up by position and not by name.
             FALSE AS mun_exact,
             -- This branch only runs when the string named no municipality at
             -- all, so there is nothing for the answer to have kept.
             FALSE AS mun_kept,
             -- Same reason: nothing was named, so nothing was swapped for it.
             'inferred' AS mun_evidence
        FROM probe p
        JOIN Streets s
          ON (p.name_fold = s.NAME_FOLD OR p.name_fold = s.MAIL_NAME_FOLD)
         AND (p.prov = '' OR s.MAIL_PROV_ABVN = p.prov)
         AND (p.type = '' OR p.type IN (s.OFFICIAL_STREET_TYPE, s.MAIL_STREET_TYPE))
         AND (p.dir  = '' OR p.dir  IN (s.OFFICIAL_STREET_DIR,  s.MAIL_STREET_DIR))
       WHERE p.mun_use IS NULL
       GROUP BY p.row_id
    )
    SELECT * FROM (
      -- jw_sim is scaffolding for the guard above, not an output column, and
      -- the UNION lines the two branches up by position.
      SELECT * EXCLUDE (jw_sim, s_fold, s_mail_fold) FROM scored
       WHERE name_sim >= {name_threshold}
      UNION ALL
      SELECT * FROM exact
    )
    -- Widening to the jurisdiction pulls in streets whose mailing city is not
    -- the one that was written, which is the point -- but where the evidence is
    -- otherwise equal, the street that also matches the name as written wins.
    -- Only then does the busier street take it.
    -- STREET_TYPE last, and only to make the result reproducible: Calgary has
    -- a Castleglen Rd NE and a Castleglen Way NE with 98 addresses each, so a
    -- string that drops the type leaves the three real criteria tied and
    -- DuckDB free to return either. Neither answer is more correct, but a
    -- harness that measures a change cannot also be measuring which one came
    -- back this time.
    QUALIFY row_number() OVER (PARTITION BY row_id
                               ORDER BY score DESC, mun_exact DESC,
                                        N_ADDRESSES DESC, STREET_TYPE) = 1"
  sql <- gsub("{probe}", probe, sql, fixed = TRUE)
  sql <- gsub("{fold_mun}", nar_match_fold_sql("m.NAME_FOLD"), sql, fixed = TRUE)
  sql <- gsub("{fold_pm}",
              nar_match_fold_sql("strip_accents(upper(pm.MAIL_MUN_NAME))"),
              sql, fixed = TRUE)
  sql <- gsub("{fold_smun}",
              nar_match_fold_sql("strip_accents(upper(s.MAIL_MUN_NAME))"),
              sql, fixed = TRUE)
  sql <- gsub("{fold_csd}",
              nar_match_fold_sql("strip_accents(upper(s.CSD_ENG_NAME))"),
              sql, fixed = TRUE)
  sql <- gsub("{swap}", format(mun_swap_penalty), sql, fixed = TRUE)
  gsub("{name_threshold}", format(name_threshold), sql, fixed = TRUE)
}

#' The RQA gazetteer scoring query
#'
#' @description The second pass's half of [nar_gazetteer_sql()], over
#' `RqaStreets` instead of `Streets`. Same weights and the same name evidence,
#' so a `confidence` means the same thing whichever register produced it --
#' what differs is which register the street was found in, and that is reported
#' as `parse_source` rather than folded into the score.
#'
#' Two things are deliberately not carried over:
#'
#' * **There is no exact branch.** Without a municipality or a postal code there
#'   is no locality to restrict candidates to, and NAR's exact branch earns its
#'   keep by canonicalizing a spelling every candidate of that name agrees on.
#'   RQA covers one province, so an unrestricted match there would assert Quebec
#'   about a string that never said so. Rows with neither locality are left to
#'   the rules parse.
#' * **Only one name family**, because RQA publishes one. `MATCH_FOLD` is
#'   compared against, never `NAME_FOLD`: the addresses this pass exists for are
#'   the ones NAR could not resolve, so the probe carries the *writer's*
#'   spelling and not a register's.
#'
#' @section How the municipality is resolved: Through NAR's `MunAlias`, which is
#' the load-bearing part. RQA files an address under its **census subdivision**
#' -- `Montréal`, never `Verdun` -- while people write the postal city, and RQA
#' publishes no alias table of its own. But `MunAlias` already keys a written
#' name to a CSD, and `MUN_KEY` carries that CSD's name (`24:V:Montréal`), which
#' is the name RQA files under. So `ANJOU`, `LASALLE`, `SAINT-LAURENT` and
#' `VERDUN` all reach Montreal's 4,581 RQA streets for free, and the borough
#' column RQA does carry is not needed here at all.
#'
#' The written name is also matched directly, for a municipality NAR has no
#' addresses in and therefore no alias for -- which is a coverage gap of exactly
#' the kind this pass exists for. The two are a `UNION` and not an `OR`: matching
#' two ways with `OR` is the 99x pattern recorded in `.claude/geocoding.md`.
#'
#' @param probe Name of the temp table holding the parsed components
#' @param name_threshold Minimum name similarity
#' @return A single SQL string
#' @keywords internal
nar_rqa_gazetteer_sql <- function(probe, name_threshold = 0.90) {
  sql <- "
    WITH probe AS (
      SELECT p.*,
             -- Same rule as the NAR pass: a municipality named in the string
             -- wins, otherwise the postal code supplies one. PostalMun is NAR's
             -- and is used as-is -- an FSA denotes the same place whoever is
             -- listing its addresses.
             coalesce(nullif(p.mun_match, ''),
                      (SELECT {fold_pm}
                         FROM PostalMun pm
                        WHERE pm.FSA = p.fsa AND p.fsa <> ''
                        ORDER BY pm.N_ADDRESSES DESC LIMIT 1)) AS mun_use
        FROM {probe} p
       WHERE p.prov = '' OR p.prov = 'QC'
    ),
    muns AS (
      SELECT p.*, p.mun_use AS mun_join
        FROM probe p
       WHERE p.mun_use IS NOT NULL
      UNION
      SELECT p.*, {fold_csd} AS mun_join
        FROM probe p
        JOIN MunAlias m
          ON {fold_mun} = p.mun_use
         AND m.PROV_ABVN = 'QC'
       WHERE p.mun_use IS NOT NULL
    ),
    scored AS (
      SELECT p.row_id,
             -- Case is left alone. NAR's own OFFICIAL_STREET_NAME is title
             -- case with the accents kept (`G.-E.-Cyr`, `118e`)
             -- because its Quebec rows come from this register in the first
             -- place, so RQA's spelling already *is* the convention. The
             -- municipality is the one that differs: NAR upper-cases
             -- MAIL_MUN_NAME, so this does too.
             s.STREET_NAME,
             coalesce(s.STREET_TYPE, '') AS STREET_TYPE,
             coalesce(s.STREET_DIR, '')  AS STREET_DIR,
             upper(s.MUN_NAME) AS MAIL_MUN_NAME,
             s.PROV_ABVN AS MAIL_PROV_ABVN,
             s.N_ADDRESSES,
             jaro_winkler_similarity(p.match_fold, s.MATCH_FOLD) AS jw_sim,
             -- The single edit and the whole-word containment, exactly as in
             -- nar_gazetteer_sql() and for the same reasons -- including the
             -- 0.70 prefilter and the length gate, which are there to keep the
             -- edit distance off pairs that cannot pass it.
             greatest(
               jw_sim,
               CASE WHEN jw_sim >= 0.70 AND length(p.match_fold) >= 3
                     AND abs(length(p.match_fold) - length(s.MATCH_FOLD)) <= 1
                     AND damerau_levenshtein(p.match_fold, s.MATCH_FOLD) <= 1
                    THEN 0.90 ELSE 0 END,
               CASE WHEN p.match_fold <> ''
                     AND length(s.MATCH_FOLD) > length(p.match_fold)
                     AND ' ' || s.MATCH_FOLD || ' '
                           LIKE '% ' || p.match_fold || ' %'
                    THEN 0.90 ELSE 0 END) AS name_sim,
             0.72 * name_sim
             + 0.10 * CASE WHEN p.type = '' THEN 1
                           WHEN p.type = coalesce(s.STREET_TYPE, '') THEN 1
                           ELSE 0 END
             + 0.06 * CASE WHEN p.dir = '' THEN 1
                           WHEN p.dir = coalesce(s.STREET_DIR, '') THEN 1
                           ELSE 0 END
             + 0.12 * CASE WHEN p.civic IS NULL THEN 1
                           WHEN p.civic BETWEEN s.MIN_CIVIC_NO AND s.MAX_CIVIC_NO THEN 1
                           ELSE 0 END
               AS score,
             p.mun_join = p.mun_use AS mun_exact,
             -- As in the NAR pass, and against the written name rather than
             -- against mun_join: an RQA street is filed under its CSD, so
             -- `VERDUN` comes back as the city of Montreal and that is a
             -- substitution whoever made it.
             {fold_smun} = p.mun_input AS mun_kept,
             -- RqaStreets is filed under the CSD, so every substitution this
             -- pass can make is a borough-for-city one -- `VERDUN` answered as
             -- Montreal. That is the amalgamation class by construction, and
             -- there is no weaker arm for it to fall into.
             CASE WHEN {fold_smun} = p.mun_input THEN 'kept'
                  WHEN p.mun_input = ''          THEN 'inferred'
                  ELSE 'csd' END AS mun_evidence
        FROM muns p
        JOIN RqaStreets s
          ON {fold_smun} = p.mun_join
    )
    SELECT * EXCLUDE (jw_sim) FROM scored
     WHERE name_sim >= {name_threshold}
    -- As in the NAR pass: the street that also matches the municipality as
    -- written wins a tie, then the busier street, then STREET_TYPE only to make
    -- the answer reproducible.
    QUALIFY row_number() OVER (PARTITION BY row_id
                               ORDER BY score DESC, mun_exact DESC,
                                        N_ADDRESSES DESC, STREET_TYPE) = 1"
  sql <- gsub("{probe}", probe, sql, fixed = TRUE)
  sql <- gsub("{fold_mun}", nar_match_fold_sql("m.NAME_FOLD"), sql, fixed = TRUE)
  sql <- gsub("{fold_pm}",
              nar_match_fold_sql("strip_accents(upper(pm.MAIL_MUN_NAME))"),
              sql, fixed = TRUE)
  sql <- gsub("{fold_csd}",
              nar_match_fold_sql(
                "strip_accents(upper(split_part(m.MUN_KEY, ':', 3)))"),
              sql, fixed = TRUE)
  sql <- gsub("{fold_smun}", nar_match_fold_sql("s.MUN_FOLD"), sql, fixed = TRUE)
  gsub("{name_threshold}", format(name_threshold), sql, fixed = TRUE)
}
