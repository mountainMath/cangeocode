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
#' @param res A tibble from [nar_parse_rules()]
#' @param con An open NAR connection
#' @param threshold Minimum combined score for a match to be accepted
#' @param name_threshold Minimum name similarity, applied as a gate before the
#' combined score. Without it the type and direction credit alone carries a
#' weak name over the line: `MAIN` against `MAITLAND` scores only 0.88 on the
#' name, but a matching type and an absent direction would still clear a
#' combined 0.85 and silently substitute the wrong street.
#' @return `res` with matched rows replaced by their canonical NAR values,
#' `confidence` set to the match score and `parse_source` set to `"gazetteer"`
#' @keywords internal
nar_resolve_gazetteer <- function(res, con, threshold = 0.85, name_threshold = 0.90) {
  if (!nar_has_streets(con)) {
    warning("This NAR database predates the street gazetteer (schema version 4); ",
            "returning rules-only results. Rebuild with ",
            "nar_connection(refresh = TRUE) to enable gazetteer resolution.",
            call. = FALSE)
    return(res)
  }

  res$.row <- seq_len(nrow(res))
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

  todo <- cand[!is.na(cand$STREET_NAME), , drop = FALSE]
  if (!nrow(todo)) return(res[, out_cols, drop = FALSE])

  probe <- data.frame(
    row_id    = todo$.probe,
    name_fold = nar_fold(todo$STREET_NAME),
    mun_fold  = nar_fold(ifelse(is.na(todo$MUN_NAME), "", todo$MUN_NAME)),
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

  best <- DBI::dbGetQuery(con, nar_gazetteer_sql(tmp, name_threshold))

  if (nrow(best)) {
    ok <- best$score >= threshold
    best <- best[ok, , drop = FALSE]
  }
  if (!nrow(best)) return(res[, out_cols, drop = FALSE])

  # One winner per input: the highest-scoring reading, and on a tie the
  # earliest candidate -- which is the baseline parse. A reading only displaces
  # it by resolving to a street this one does not.
  w <- match(best$row_id, cand$.probe)
  best$.row  <- cand$.row[w]
  best$.cand <- cand$.cand[w]
  o <- order(best$.row, -best$score, best$.cand)
  keep <- o[!duplicated(best$.row[o])]
  best <- best[keep, , drop = FALSE]
  w <- w[keep]

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
  res$parse_source[i] <- "gazetteer"

  res <- res[, out_cols, drop = FALSE]
  attr(res, "nar_candidates") <- NULL
  res
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
#' @param probe Name of the temp table holding the parsed components
#' @param name_threshold Minimum name similarity for the fuzzy branch
#' @return A single SQL string
#' @keywords internal
nar_gazetteer_sql <- function(probe, name_threshold = 0.90) {
  # Placeholders rather than sprintf: the template is past sprintf's 8192-byte
  # format limit, and this way a literal % in a LIKE pattern needs no doubling.
  sql <- "
    WITH probe AS (
      SELECT p.*,
             -- A municipality named in the string wins; otherwise the postal
             -- code supplies one, taking the busiest municipality in the FSA.
             -- Both may be absent, which the exact branch below picks up.
             coalesce(nullif(p.mun_fold, ''),
                      (SELECT replace(strip_accents(upper(pm.MAIL_MUN_NAME)), '.', '')
                         FROM PostalMun pm
                        WHERE pm.FSA = p.fsa AND p.fsa <> ''
                        ORDER BY pm.N_ADDRESSES DESC LIMIT 1)) AS mun_use
        FROM {probe} p
    ),
    scored AS (
      SELECT p.row_id,
             s.OFFICIAL_STREET_NAME AS STREET_NAME,
             s.OFFICIAL_STREET_TYPE AS STREET_TYPE,
             s.OFFICIAL_STREET_DIR  AS STREET_DIR,
             s.MAIL_MUN_NAME, s.MAIL_PROV_ABVN, s.N_ADDRESSES,
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
               jaro_winkler_similarity(p.name_fold, replace(s.NAME_FOLD, '.', '')),
               jaro_winkler_similarity(p.name_fold, replace(s.MAIL_NAME_FOLD, '.', ''))
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
               CASE WHEN jw_sim >= 0.70 AND length(p.name_fold) >= 3 AND least(
                      damerau_levenshtein(p.name_fold, replace(s.NAME_FOLD, '.', '')),
                      damerau_levenshtein(p.name_fold, replace(s.MAIL_NAME_FOLD, '.', ''))) <= 1
                    THEN 0.90 ELSE 0 END,
               -- Same idea, and the same reason: a candidate no longer than
               -- the probe can only contain it by being equal to it, which the
               -- similarity above already scores 1.0.
               CASE WHEN p.name_fold <> ''
                     AND greatest(length(s.NAME_FOLD), length(s.MAIL_NAME_FOLD))
                           > length(p.name_fold)
                     AND (' ' || replace(s.NAME_FOLD, '.', '') || ' '
                            LIKE '% ' || p.name_fold || ' %'
                       OR ' ' || replace(s.MAIL_NAME_FOLD, '.', '') || ' '
                            LIKE '% ' || p.name_fold || ' %')
                    THEN 0.90 ELSE 0 END) AS name_sim,
             0.72 * name_sim
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
                           ELSE 0 END
               AS score
             , replace(strip_accents(upper(s.MAIL_MUN_NAME)), '.', '')
                 = p.mun_use AS mun_exact
        FROM probe p
        -- Through the alias set rather than straight at MAIL_MUN_NAME: the name
        -- someone writes and the name NAR files under are often different names
        -- for overlapping places, in both directions.
        JOIN MunAlias m
          -- Periods come off both sides. NAR files ST. JOHN'S, SAULT STE.
          -- MARIE and ST. ALBERT with them, 1,027,129 addresses' worth, while
          -- nar_norm_text() strips them from input as mere abbreviation marks.
          -- Without this the two never meet and those cities resolve to
          -- nothing. MunAlias is 18,313 rows, so scanning it costs nothing --
          -- do not move this onto Streets, where it would cost the index.
          ON replace(m.NAME_FOLD, '.', '') = p.mun_use
         AND (p.prov = '' OR m.PROV_ABVN = p.prov)
        JOIN Streets s
          ON s.MUN_KEY = m.MUN_KEY
         AND (p.prov = '' OR s.MAIL_PROV_ABVN = p.prov)
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
             FALSE AS mun_exact
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
      SELECT * EXCLUDE (jw_sim) FROM scored WHERE name_sim >= {name_threshold}
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
  gsub("{name_threshold}", format(name_threshold), sql, fixed = TRUE)
}
