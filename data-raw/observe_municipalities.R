# Regenerates data-raw/nar_municipalities_observed.csv: every municipality name
# NAR files an address under, with the province and how many addresses carry it.
#
# The parser needs this to arbitrate a comma-less split. "100 Mile House" and
# "TH25 Vancouver" are structurally identical -- a token that is not a street
# word, followed by a place -- and only an inventory of real places tells them
# apart. `MunAlias` is the source rather than `MAIL_MUN_NAME` because it already
# carries the retired and alternate names amalgamation left in circulation.
#
# Run with:  Rscript data-raw/observe_municipalities.R  (needs NAR_CACHE_PATH)
library(cangeocode)

con <- nar_connection()
on.exit(DBI::dbDisconnect(con), add = TRUE)

d <- DBI::dbGetQuery(con, "
  SELECT NAME_FOLD AS surface, PROV_ABVN AS prov, sum(N_ADDRESSES)::BIGINT AS n
  FROM MunAlias
  WHERE length(NAME_FOLD) > 0
  GROUP BY 1, 2
  ORDER BY n DESC")

write.csv(d, "data-raw/nar_municipalities_observed.csv", row.names = FALSE)
cat("Wrote", nrow(d), "municipality names across",
    length(unique(d$prov)), "provinces\n")
