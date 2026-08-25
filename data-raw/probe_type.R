# A name-final type word: is it the street's type, or the end of its name?
#
# `nar_parse_one()` takes the last recognized type surface in the string as the
# street type. When the input *carries* a type that is unambiguous -- `Aspen
# Cove Rd` splits at RD and COVE stays in the name. When the input *omits* it,
# the same rule eats the name: `Aspen Cove` parses as ASPEN + type COVE.
#
# This measures failure mode 3 the way probe_direction.R measured mode 2, and
# it is the same fair-reference argument: it asks whether the parser reproduces
# the decomposition NAR itself records -- name here, type there -- and NAR is
# by definition authoritative about its own columns, so the "NAR is not ground
# truth" caveat that qualifies every coordinate comparison in this package does
# not apply.
#
#   Rscript data-raw/probe_type.R
#
# Needs NAR_CACHE_PATH and an imported release. Runs in about three minutes.

library(cangeocode)
library(dplyr)

con <- nar_connection()
on.exit(close_nar(), add = TRUE)

# The parser's own vocabulary defines the risk: a name-final word is only eaten
# if nar_parse_one() would recognize it. Surfaces rather than canonical forms --
# COVE and CV are two surfaces of one type and NAR names end in both.
surf <- unique(cangeocode:::nar_lex_types$surface_fold)
IN_SQL <- paste0("('", paste(gsub("'", "''", surf), collapse = "','"), "')")

# The population, and each clause is doing work. A type of NAR's own is what
# makes the name-final word unambiguously *name* -- NAR had the column and put
# something else in it. A multi-word name excludes the streets that are only a
# type word, which is a different problem. Direction-free on both name families
# for the reason probe_direction.R gives: with a direction in play two rules
# compete and the measurement stops being about one of them.
where <- sprintf("
  WHERE coalesce(a.OFFICIAL_STREET_TYPE,'') <> ''
    AND coalesce(a.OFFICIAL_STREET_DIR,'') = ''
    AND coalesce(a.MAIL_STREET_DIR,'') = ''
    AND a.CIVIC_NO IS NOT NULL
    AND coalesce(a.MAIL_MUN_NAME,'') <> ''
    AND contains(a.OFFICIAL_STREET_NAME, ' ')
    AND upper(strip_accents(
          regexp_extract(a.OFFICIAL_STREET_NAME, '([^ ]+)$', 1))) IN %s", IN_SQL)

pool <- DBI::dbGetQuery(con, paste(
  "SELECT count(*) n, count(DISTINCT a.OFFICIAL_STREET_NAME) s FROM Addresses a",
  where))
message("Addresses whose street NAME ends in a type word: ",
        format(pool$n, big.mark = ","), " over ",
        format(pool$s, big.mark = ","), " distinct names")

# USING SAMPLE wraps the filtered relation, never sits beside the WHERE -- on
# the flat form DuckDB samples the table first and the filter then leaves a
# handful of rows.
samp <- DBI::dbGetQuery(con, sprintf("
  SELECT * FROM (
    SELECT a.CIVIC_NO, a.OFFICIAL_STREET_NAME nm, a.OFFICIAL_STREET_TYPE ty,
           a.MAIL_MUN_NAME mun, a.MAIL_PROV_ABVN pr, a.ADDR_GUID,
           upper(regexp_extract(a.OFFICIAL_STREET_NAME, '([^ ]+)$', 1)) lastw
      FROM Addresses a %s
  ) USING SAMPLE 2500 ROWS (reservoir, 11)", where))

tidy <- function(x) trimws(gsub("\\s+", " ", x))
# Two renderings of the same address. The control carries NAR's own type, which
# is the form the note claims is safe; the probe drops it, which is the only
# form the rule can misfire on.
with_ty <- tidy(sprintf("%s %s %s, %s, %s", samp$CIVIC_NO, samp$nm, samp$ty,
                        samp$mun, samp$pr))
no_ty   <- tidy(sprintf("%s %s, %s, %s", samp$CIVIC_NO, samp$nm,
                        samp$mun, samp$pr))

score <- function(addr) {
  n <- normalize_address(addr, con = con)
  g <- geocode(addr, con = con)
  tibble(kept   = toupper(n$STREET_NAME) == toupper(samp$nm),
         placed = !is.na(g$lon),
         right  = !is.na(g$ADDR_GUID) & g$ADDR_GUID == samp$ADDR_GUID,
         name   = n$STREET_NAME, type = n$STREET_TYPE, src = n$parse_source)
}
ctl <- score(with_ty)
prb <- score(no_ty)

message("\n--- the type present is not at risk (control)")
print(ctl |> summarise(n = n(),
                       name_kept  = sprintf("%.1f%%", 100 * mean(kept)),
                       placed     = sprintf("%.1f%%", 100 * mean(placed)),
                       right_addr = sprintf("%.1f%%", 100 * mean(right))))

message("\n--- the type dropped, which is the at-risk form")
print(prb |> summarise(n = n(),
                       name_kept  = sprintf("%.1f%%", 100 * mean(kept)),
                       placed     = sprintf("%.1f%%", 100 * mean(placed)),
                       right_addr = sprintf("%.1f%%", 100 * mean(right))))

# The signature of the mode: the last word of the name became the type. Anything
# else in the loss set failed for another reason and should not be counted here.
eaten <- !prb$kept &
  !is.na(prb$type) & toupper(prb$type) != "" &
  toupper(prb$name) == toupper(trimws(sub("\\s+\\S+$", "", samp$nm)))

message("\n--- what the losses are")
print(table(name_lost = !prb$kept, last_word_eaten_as_type = eaten))

message("\n--- and what a loss costs")
print(tibble(lost = !prb$kept, placed = prb$placed, right = prb$right) |>
        summarise(n = n(),
                  placed     = sprintf("%.1f%%", 100 * mean(placed)),
                  right_addr = sprintf("%.1f%%", 100 * mean(right)),
                  .by = lost))

message("\n--- where the gazetteer answers anyway (a confident wrong street)")
print(table(parse_source = prb$src[!prb$kept], placed = prb$placed[!prb$kept]))

message("\n--- which name-final words cost the most")
print(tibble(w = samp$lastw, lost = !prb$kept) |>
        summarise(n = n(), lost = sum(lost),
                  rate = sprintf("%.0f%%", 100 * mean(lost)), .by = w) |>
        arrange(desc(lost)) |> head(20), n = 20)

# The decisive block, and the reason this probe exists. A loss that falls to
# `rules` looks like "the gazetteer found nothing". Re-asking with keep_refused
# shows what it actually found and threw away -- and for this mode the answer is
# usually the right street, declined by hundredths.
ref <- normalize_address(no_ty, con = con, keep_refused = TRUE)
found <- toupper(ref$STREET_NAME) == toupper(samp$nm)

message("\n--- of the ", sum(!prb$kept), " losses, what the gazetteer had found")
print(table(right_street_found = found[!prb$kept],
            gate = ifelse(is.na(ref$refused_for[!prb$kept]), "accepted",
                          ref$refused_for[!prb$kept])))

message("\n--- the refused score, where the street was right")
rs <- ref$confidence[!prb$kept & found & !is.na(ref$refused_for)]
print(summary(rs))
print(table(sprintf("%.3f", rs)) |> sort(decreasing = TRUE) |> head(8))

message("\n--- a sample of the residual")
print(tibble(input = substr(no_ty, 1, 44), nar = paste(samp$nm, samp$ty),
             parsed = paste(prb$name, prb$type), src = prb$src,
             right = prb$right)[!prb$kept, ] |> head(30), n = 30)
