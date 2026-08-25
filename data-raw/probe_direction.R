# A leading compass word: is it the street's name, or a direction?
#
# `nar_parse_one()` strips a leading NORTH/SOUTH/EAST/WEST into STREET_DIR
# unconditionally. For ~90,000 Canadian addresses that word is the name, and
# this measures what the unconditional strip costs.
#
# NAR is a fair reference here, which is unusual in this package and worth
# stating. Every accuracy measurement elsewhere carries the "NAR is not ground
# truth" caveat because it compares a *coordinate* to NAR's coordinate. This
# asks a different question -- does the parser reproduce the decomposition NAR
# itself records, name here and direction there -- and NAR is by definition
# authoritative about its own columns.
#
#   Rscript data-raw/probe_direction.R
#
# Needs NAR_CACHE_PATH and an imported release. Runs in about two minutes.

library(cangeocode)
library(dplyr)

con <- nar_connection()
on.exit(close_nar(), add = TRUE)

COMPASS <- c("EAST", "WEST", "NORTH", "SOUTH", "NORD", "SUD", "EST", "OUEST")
IN_SQL  <- paste0("('", paste(COMPASS, collapse = "','"), "')")

# The population: addresses whose OFFICIAL street name begins with a spelled-out
# compass word and which carry no direction on *either* name family. That last
# condition is what makes NAR's opinion unambiguous -- it had a direction column
# available and left it empty, so it is asserting the word is part of the name.
# Rows where NAR sets a direction as well are excluded on purpose: South
# Terwillegar Dr NW is a neighbourhood in a quadrant and both halves are real.
where <- sprintf("
   WHERE coalesce(a.OFFICIAL_STREET_DIR,'') = ''
     AND coalesce(a.MAIL_STREET_DIR,'') = ''
     AND a.CIVIC_NO IS NOT NULL
     AND coalesce(a.MAIL_MUN_NAME,'') <> ''
     AND upper(split_part(a.OFFICIAL_STREET_NAME,' ',1)) IN %s
     AND upper(a.OFFICIAL_STREET_NAME)
         <> upper(split_part(a.OFFICIAL_STREET_NAME,' ',1))", IN_SQL)

pool <- DBI::dbGetQuery(con, paste("SELECT count(*) n FROM Addresses a", where))$n
message("Compass-led, direction-free addresses in NAR: ", format(pool, big.mark = ","))

# USING SAMPLE has to wrap the filtered relation, not sit beside the WHERE --
# on the flat form DuckDB samples the table first and the filter then leaves a
# handful of rows.
samp <- DBI::dbGetQuery(con, sprintf("
  SELECT * FROM (
    SELECT a.CIVIC_NO, a.OFFICIAL_STREET_NAME nm, a.OFFICIAL_STREET_TYPE ty,
           a.MAIL_MUN_NAME mun, a.MAIL_PROV_ABVN pr, a.ADDR_GUID
      FROM Addresses a %s
  ) USING SAMPLE 2500 ROWS (reservoir, 11)", where))

# Written the way NAR spells it, so nothing but the leading word is in question.
samp$addr <- trimws(gsub("\\s+", " ",
  paste0(samp$CIVIC_NO, " ", samp$nm, " ", samp$ty, ", ", samp$mun, ", ", samp$pr)))

norm <- normalize_address(samp$addr, con = con)
geo  <- geocode(samp$addr, con = con)

kept  <- toupper(norm$STREET_NAME) == toupper(samp$nm)
right <- !is.na(geo$ADDR_GUID) & geo$ADDR_GUID == samp$ADDR_GUID

message("\n--- did the compass word survive the parse?")
print(table(kept_in_name = kept, stripped_to_dir = !is.na(norm$STREET_DIR)))

message("\n--- and what that costs")
print(tibble(kept, placed = !is.na(geo$lon), right) |>
        summarise(n = n(),
                  placed     = sprintf("%.1f%%", 100 * mean(placed)),
                  right_addr = sprintf("%.1f%%", 100 * mean(right)),
                  .by = kept))

# The half that matters. A row that loses the word and is still resolved by the
# gazetteer is not an unplaced row -- it is a confident answer on a different
# street, very often the mirror-image one, and no output column says so.
lost <- !kept & is.na(norm$STREET_DIR)
message("\n--- lost the word but NOT into STREET_DIR: ", sum(lost),
        " (of ", sum(!kept), " total losses)")
print(table(parse_source = norm$parse_source[lost]))

# Printed whole rather than sampled: since nar_dir_lead_variant() and the
# municipality tie-break went in, the entire residual is small enough to read,
# and reading all of it is what showed it is no longer about directions.
message("\n--- what the gazetteer answered instead")
print(tibble(input = substr(samp$addr, 1, 46),
             nar_name = samp$nm, parsed = norm$STREET_NAME)[lost, ], n = Inf)

# The other half: the word did go into STREET_DIR, so the restored reading was
# offered and lost. Since nar_dir_lead_variant() went in this is the larger of
# the two residual classes, and it is the benign one -- an unplaced row, or one
# placed on the same street NAR names, rather than a confident wrong answer.
held <- !kept & !is.na(norm$STREET_DIR)
message("\n--- kept in STREET_DIR anyway: ", sum(held),
        " (of ", sum(!kept), " total losses)")
print(table(parse_source = norm$parse_source[held],
            placed = !is.na(geo$lon[held])))

message("\n--- what those resolved to")
print(tibble(input = substr(samp$addr, 1, 46), nar_name = samp$nm,
             parsed = norm$STREET_NAME, dir = norm$STREET_DIR,
             src = norm$parse_source, mun = norm$MUN_NAME)[held, ], n = Inf)
