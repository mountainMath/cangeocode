# Regenerates data-raw/nar_street_types_observed.csv: the street-type tokens
# that actually occur in a NAR release, with how Quebec-concentrated each one
# is. `qc_share` is what tells you whether a token is the French or English
# canonical form -- RUE/AV/BOUL/CH/RANG sit at 0.94-1.00, everything else ~0.
#
# Run with:  Rscript data-raw/observe_street_types.R  (needs NAR_CACHE_PATH)
library(cangeocode)

con <- nar_connection()
on.exit(DBI::dbDisconnect(con), add = TRUE)

d <- DBI::dbGetQuery(con, "
  SELECT OFFICIAL_STREET_TYPE AS t,
         sum(CASE WHEN MAIL_PROV_ABVN = 'QC' THEN 1 ELSE 0 END) AS qc,
         count(*) AS n
  FROM Addresses
  WHERE length(OFFICIAL_STREET_TYPE) > 0
  GROUP BY 1 ORDER BY n DESC")
d$qc_share <- round(d$qc / d$n, 3)

write.csv(d, "data-raw/nar_street_types_observed.csv", row.names = FALSE)
cat("Wrote", nrow(d), "street types\n")
