# Splits Quebec's Part B failures between the parser and NAR's coverage.
#
# `eval_normalize.R` Part B reports one number per province -- the share of
# Corporations Canada filings that normalize to an address NAR holds, confirmed
# by the filing's own postal code. Quebec's is the lowest in the country, and
# that number alone cannot say why: a filing can fail to resolve because the
# parser read it wrong, because NAR spells the street differently, or because
# NAR simply does not carry the address. Only the third is a coverage problem,
# and only the first is ours to fix.
#
# Quebec is the one province where the difference is measurable, because the
# Repertoire quebecois des adresses (RQA) is the register NAR's Quebec rows are
# derived from and is published in full -- see `compare_rqa.R`, which builds the
# `rqa.duckdb` this script reads, and `inst/notes/quebec-addresses.md`.
#
# Two lookups, not one, because they answer different questions. The **key** is
# (postal code, civic number) -- what an import would anti-join on, and the only
# key NAR and RQA both produce without a street name. The **address** is (forward
# sortation area, civic number, street name): postal-free enough to survive a
# filing whose postal code is stale, and still tight enough not to match a
# different town. Both are needed, and keying on the full postal code alone gets
# the answer wrong -- `1255 Rue Peel, Montreal H3B 2T6` is in both registers, at
# H3B 2T9 and H3B 4V4, so a postal-keyed test calls an address both registers
# hold a coverage gap.
#
# Each failure falls in exactly one class, first match winning:
#
#   no_civic   the parse found no civic number or no street, so nothing can be
#              looked up. Ours.
#   spelling   NAR holds the street at that exact postal code and civic number.
#              The parse was right and Part B's strict join is what missed it.
#              Ours, and the cheapest kind to fix.
#   postal     NAR holds the address in the same FSA, at a different postal
#              code. The parse was right and the filing's postal code is stale
#              or wrong -- nobody's, and unreachable by any join that confirms
#              on the postal code, which is what Part B does by construction.
#   coverage   RQA holds the address and NAR does not. The coverage gap, and
#              the only class an import can recover. Reported split by whether
#              a postal + civic anti-join would carry it, since that is the
#              cheap import key and it does not catch a street NAR is missing
#              at a key it already has.
#   parse      RQA holds the filed key under some other street, and neither
#              register holds ours. We read the street wrong, or the filer
#              wrote a street that is not there.
#   neither    no register holds the key or the address. A filing that is
#              stale, wrong, or not an address. Not diagnosable from here.
#
# RQA_PART picks the section:
#
#   split  the classification above, with examples per class.
#   gain   what loading the RQA-only addresses would actually buy for address
#          matching, priced against both candidate import keys, plus what those
#          addresses are.
#   interp what it would buy for geocoding, which is a different and much
#          smaller thing: most of the addresses NAR lacks sit on a street NAR
#          has, so `nar_interpolate` already places them. This measures how far
#          from RQA's own coordinate it puts them.
#
# Prerequisites: NAR_CACHE_PATH with an imported release, `rqa.duckdb` already
# built by `compare_rqa.R`, and the Corporations Canada CSV that Part B caches
# (this script downloads it on the same terms if it is absent).
#
# Usage:
#   RQA_PART=all Rscript data-raw/diagnose_quebec.R

suppressMessages(pkgload::load_all(".", quiet = TRUE))

part     <- Sys.getenv("RQA_PART", "all")
rqa_dir  <- Sys.getenv("RQA_DIR", path.expand("~/data/rqa"))
nar_ver  <- Sys.getenv("EVAL_VERSION", "latest")
N        <- as.integer(Sys.getenv("EVAL_N", "4000"))
CACHE    <- Sys.getenv("EVAL_CACHE", file.path(Sys.getenv("NAR_CACHE_PATH"), "eval"))
CORP_URL <- "https://d4bf66bykfyaf.cloudfront.net/corporations-active-cbca-en.csv"

rqa_db <- file.path(rqa_dir, "rqa.duckdb")
stopifnot(file.exists(rqa_db))

fold  <- cangeocode:::nar_fold
mfold <- cangeocode:::nar_match_fold
msql  <- cangeocode:::nar_match_fold_sql

# NAR is the primary connection, not RQA as in `compare_rqa.R`: this script
# runs the parser, and `normalize_address()` writes its macros and its folded
# gazetteer into whatever connection it is handed.
con <- nar_connection(version = nar_ver)
on.exit(DBI::dbDisconnect(con), add = TRUE)
DBI::dbExecute(con, sprintf("ATTACH '%s' AS rqa (READ_ONLY)", rqa_db))

hdr <- function(x) cat("\n== ", x, " ==\n", sep = "")
pct <- function(x) sprintf("%5.1f%%", 100 * mean(x, na.rm = TRUE))
q   <- function(sql, ...) DBI::dbGetQuery(con, sprintf(sql, ...))

# ---- the Quebec sample, and Part B's own two joins ---------------------------

dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
csv <- file.path(CACHE, basename(CORP_URL))
if (!file.exists(csv)) {
  cat("downloading", CORP_URL, "(~100 MB)\n")
  to <- options("timeout")
  options(timeout = max(1200, as.numeric(unlist(to)), na.rm = TRUE))
  utils::download.file(CORP_URL, csv, mode = "wb")
  options(to)
}

set.seed(20260821)
corp <- as.data.frame(arrow::read_csv_arrow(
  csv, col_select = c("Street", "Street 2", "City/town",
                      "Province/territory", "Postal code"), as_data_frame = TRUE))
names(corp) <- c("street", "street2", "city", "prov", "postal")
corp[] <- lapply(corp, function(x) ifelse(is.na(x), "", trimws(x)))
corp <- corp[nzchar(corp$street) & nzchar(corp$city) & corp$prov == "QC" &
               grepl("^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$", corp$postal), ]
cat(sprintf("%s usable Quebec filings; sampling %s\n",
            format(nrow(corp), big.mark = ","), format(N, big.mark = ",")))
corp <- corp[sample.int(nrow(corp), min(N, nrow(corp))), , drop = FALSE]

parts <- cbind(corp$street, corp$street2, corp$city,
               trimws(paste(corp$prov, corp$postal)))
corp$text <- apply(parts, 1, function(x) paste(x[nzchar(x)], collapse = ", "))
corp$postal <- gsub(" ", "", toupper(corp$postal))

got <- normalize_address(corp$text, con = con)

probe <- data.frame(
  row_id     = seq_len(nrow(got)),
  name_fold  = fold(ifelse(is.na(got$STREET_NAME), "", got$STREET_NAME)),
  match_fold = mfold(ifelse(is.na(got$STREET_NAME), "", got$STREET_NAME)),
  mun_fold   = fold(ifelse(is.na(got$MUN_NAME), "", got$MUN_NAME)),
  civic      = got$CIVIC_NO,
  postal     = corp$postal,
  stringsAsFactors = FALSE)
DBI::dbWriteTable(con, "qc_probe", probe, temporary = TRUE, overwrite = TRUE)

# Part B's two joins, unchanged: the strict one that scores the province, and
# the loose one that forgives a municipality NAR spells pre-amalgamation.
strict <- q("
  SELECT DISTINCT p.row_id FROM qc_probe p JOIN Addresses a
      ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = p.name_fold
     AND strip_accents(upper(a.MAIL_MUN_NAME)) = p.mun_fold
     AND a.MAIL_PROV_ABVN = 'QC' AND a.CIVIC_NO = p.civic
     AND a.MAIL_POSTAL_CODE = p.postal
   WHERE p.name_fold <> '' AND p.civic IS NOT NULL")$row_id
loose <- q("
  SELECT DISTINCT p.row_id FROM qc_probe p JOIN Addresses a
      ON strip_accents(upper(a.OFFICIAL_STREET_NAME)) = p.name_fold
     AND a.MAIL_POSTAL_CODE = p.postal AND a.CIVIC_NO = p.civic
   WHERE p.name_fold <> '' AND p.civic IS NOT NULL")$row_id

ok   <- seq_len(nrow(got)) %in% union(strict, loose)
fail <- which(!ok)
hdr("the Quebec sample, scored the way Part B scores it")
cat("confirmed via municipality:  ", pct(seq_len(nrow(got)) %in% strict), "\n", sep = "")
cat("confirmed either way:        ", pct(ok), "\n", sep = "")
cat("failures to classify:        ", length(fail), "\n", sep = "")

# Whole-word containment either way, on the match fold. NAR keeps the leading
# particule in the name (`du Square-Victoria`) where a filer usually drops it,
# and RQA's recomposed odonyme carries the generique (`Boulevard Newman`) where
# the parser has split it off, so neither side can be tested for equality.
contains <- function(a, b) sprintf(
  "(' ' || %s || ' ' LIKE '%% ' || %s || ' %%' OR ' ' || %s || ' ' LIKE '%% ' || %s || ' %%')",
  a, b, b, a)

nar_name <- sprintf("(%s OR %s)",
  contains(msql("upper(strip_accents(a.OFFICIAL_STREET_NAME))"), "p.match_fold"),
  contains(msql("upper(strip_accents(a.MAIL_STREET_NAME))"), "p.match_fold"))
rqa_name <- contains(msql("upper(strip_accents(a.odonyme_recompose_normal))"),
                     "p.match_fold")

# Both sides joined on FSA + civic, with the full postal code and the street
# tested inside the aggregate rather than in the join, so each is one pass.
cls <- q("
  WITH n AS (
    SELECT p.row_id,
           max(CASE WHEN a.MAIL_POSTAL_CODE = p.postal THEN 1 ELSE 0 END) = 1 AS key_pc,
           max(CASE WHEN %s AND a.MAIL_POSTAL_CODE = p.postal THEN 1 ELSE 0 END) = 1
             AS street_pc,
           max(CASE WHEN %s THEN 1 ELSE 0 END) = 1 AS street_fsa
      FROM qc_probe p JOIN Addresses a
        ON a.CIVIC_NO = p.civic AND a.PROV_CODE = '24'
       AND substr(a.MAIL_POSTAL_CODE, 1, 3) = substr(p.postal, 1, 3)
     WHERE p.civic IS NOT NULL GROUP BY p.row_id
  ), r AS (
    SELECT p.row_id,
           max(CASE WHEN replace(upper(a.code_postal), ' ', '') = p.postal
                    THEN 1 ELSE 0 END) = 1 AS key_pc,
           max(CASE WHEN %s THEN 1 ELSE 0 END) = 1 AS street_fsa,
           -- The row that confirms the street where there is one, so the class
           -- can be read off the example; any row at the key otherwise, which
           -- is what says what is actually there instead.
           coalesce(any_value(a.adresse_formatee) FILTER (WHERE %s),
                    any_value(a.adresse_formatee)) AS ex
      FROM qc_probe p JOIN rqa.rqa a
        ON TRY_CAST(a.numero_municipal AS INT) = p.civic
       AND substr(replace(upper(a.code_postal), ' ', ''), 1, 3) = substr(p.postal, 1, 3)
     WHERE p.civic IS NOT NULL AND a.etat = 'Certifiée' GROUP BY p.row_id
  )
  SELECT p.row_id,
         coalesce(n.key_pc, false)     AS nar_key,
         coalesce(n.street_pc, false)  AS nar_street_pc,
         coalesce(n.street_fsa, false) AS nar_street,
         coalesce(r.key_pc, false)     AS rqa_key,
         coalesce(r.street_fsa, false) AS rqa_street,
         r.ex AS rqa_example
    FROM qc_probe p LEFT JOIN n USING (row_id) LEFT JOIN r USING (row_id)
   ORDER BY p.row_id", nar_name, nar_name, rqa_name, rqa_name)

classify <- function(i) {
  if (is.na(probe$civic[i]) || !nzchar(probe$name_fold[i])) return("no_civic")
  z <- cls[i, ]
  if (z$nar_street_pc) return("spelling")
  if (z$nar_street)    return("postal")
  if (z$rqa_street)    return("coverage")
  if (z$rqa_key)       return("parse")
  "neither"
}

# ---- split -------------------------------------------------------------------

if (part %in% c("split", "all")) {
  klass <- vapply(fail, classify, character(1))
  lev <- c("spelling", "no_civic", "parse", "postal", "coverage", "neither")
  tab <- table(factor(klass, levels = lev))
  hdr(sprintf("what the %d failures are", length(fail)))
  print(data.frame(class = names(tab), n = as.integer(tab),
                   pct = sprintf("%5.1f%%", 100 * as.integer(tab) / length(fail))),
        row.names = FALSE)
  cat("\n  ours:      spelling + no_civic + parse = ",
      pct(klass %in% c("spelling", "no_civic", "parse")), "\n", sep = "")
  cat("  coverage:  RQA has it, NAR does not    = ", pct(klass == "coverage"), "\n", sep = "")
  cat("  the filing's:  postal + neither        = ",
      pct(klass %in% c("postal", "neither")), "\n", sep = "")

  cov <- fail[klass == "coverage"]
  hdr("the coverage class, by the import key that would catch it")
  print(data.frame(
    key = c("postal + civic (an anti-join import)", "street as well (a merge)"),
    n   = c(sum(cls$rqa_key[cov] & !cls$nar_key[cov]), length(cov))),
    row.names = FALSE)

  # Inside the classes that are ours, the two shapes worth separating. A street
  # type the parser never found is the cheap one: it is a missing surface form
  # in `data-raw/street_types.csv`, not a rule. A name that is a whole-word
  # sub- or superstring of NAR's is the other -- the parse is right and the
  # gazetteer failed to snap it to NAR's spelling.
  ours <- fail[klass %in% c("spelling", "no_civic", "parse")]
  hdr("inside the classes that are ours")
  cat("no street type found:        ", sum(is.na(got$STREET_TYPE[ours])), " of ",
      length(ours), "\n", sep = "")
  lead <- vapply(ours[is.na(got$STREET_TYPE[ours])], function(i) {
    t <- cangeocode:::nar_tokens(sub("^[0-9]+[A-Za-z]?[ ,-]+", "", corp$street[i]))
    if (length(t)) cangeocode:::nar_fold(t[1]) else ""
  }, character(1))
  cat("the token that follows the civic number, where no type was found:\n")
  print(utils::head(sort(table(lead[nzchar(lead)]), decreasing = TRUE), 12))

  for (k in lev) {
    idx <- fail[klass == k]
    if (!length(idx)) next
    hdr(sprintf("%s -- %d rows", k, length(idx)))
    show <- idx[seq_len(min(8, length(idx)))]
    print(data.frame(
      input  = substr(corp$text[show], 1, 44),
      parsed = substr(paste(got$CIVIC_NO[show], got$STREET_NAME[show],
                            got$STREET_TYPE[show], "|", got$MUN_NAME[show]), 1, 38),
      rqa    = substr(ifelse(is.na(cls$rqa_example[show]), "-",
                             cls$rqa_example[show]), 1, 38)), row.names = FALSE)
  }
}

# ---- gain --------------------------------------------------------------------

if (part %in% c("gain", "all")) {
  klass <- vapply(fail, classify, character(1))
  n <- nrow(got)
  cov <- fail[klass == "coverage"]
  cheap <- cov[cls$rqa_key[cov] & !cls$nar_key[cov]]

  hdr("what loading the RQA-only addresses would buy on this sample")
  cat("Part B Quebec now:                  ", pct(ok), "\n", sep = "")
  cat("+ import keyed on postal + civic:   ", pct(ok | seq_len(n) %in% cheap), "\n", sep = "")
  cat("+ merged on the street as well:     ", pct(ok | seq_len(n) %in% cov), "\n", sep = "")
  cat("\n(An upper bound. It credits every row RQA confirms, and says nothing\n",
      "about whether the parser would then find it -- the fold that confirmed\n",
      "it here is looser than the join Part B scores with.)\n", sep = "")

  # How big the gap is on a key that uses the street name. The postal + civic
  # key in `compare_rqa.R` is coarse enough to hide a street NAR lacks at a key
  # it already has, and that difference is what the two prices above straddle.
  #
  # Two things this key has to get right, and both were wrong before they were
  # measured. NAR stores the leading particule inside the street name
  # (`de la Cote-de-Liesse`) where RQA keeps it in its own column and the
  # `specifique` has none, so without stripping it the two spellings of one
  # street never meet and the gap reads 1.27 million instead of 358 thousand.
  # And the key has to be checked in BOTH directions: NAR-only is the noise
  # floor, since NAR's Quebec rows come from RQA and anything NAR has that RQA
  # does not is the key failing rather than a real difference.
  sp <- function(x) {
    pat <- "'^(DE LA |DES |DU |DE |LA |LE |LES |L |D |AUX |AU |A |EN )'"
    for (i in 1:3) x <- sprintf("regexp_replace(%s, %s, '')", x, pat)
    x
  }
  addr_keys <- sprintf("
    WITH ns AS (
      SELECT DISTINCT substr(replace(upper(MAIL_POSTAL_CODE), ' ', ''), 1, 3) fsa,
             CIVIC_NO::INT cn, %s nm
        FROM Addresses WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> ''
         AND CIVIC_NO IS NOT NULL
    ), rs AS (
      SELECT DISTINCT substr(replace(upper(code_postal), ' ', ''), 1, 3) fsa,
             TRY_CAST(numero_municipal AS INT) cn, %s nm
        FROM rqa.rqa WHERE etat = 'Certifiée' AND code_postal <> ''
    )", sp(msql("upper(strip_accents(OFFICIAL_STREET_NAME))")),
        sp(msql("upper(strip_accents(specifique_odonyme))")))

  hdr("the coverage gap on two keys, over the whole province")
  print(q("
    WITH nk AS (
      SELECT DISTINCT replace(upper(MAIL_POSTAL_CODE), ' ', '') pc, CIVIC_NO::INT cn
        FROM Addresses WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> ''
         AND CIVIC_NO IS NOT NULL
    ), rk AS (
      SELECT DISTINCT replace(upper(code_postal), ' ', '') pc,
             TRY_CAST(numero_municipal AS INT) cn
        FROM rqa.rqa WHERE etat = 'Certifiée' AND code_postal <> ''
    )
    SELECT (SELECT count(*) FROM rk) AS rqa_keys,
           (SELECT count(*) FROM rk ANTI JOIN nk USING (pc, cn)) AS rqa_only_key,
           (SELECT count(*) FROM nk ANTI JOIN rk USING (pc, cn)) AS nar_only_key"))

  print(q(paste0(addr_keys, "
    SELECT (SELECT count(*) FROM ns) AS nar_addrs,
           (SELECT count(*) FROM rs) AS rqa_addrs,
           (SELECT count(*) FROM rs ANTI JOIN ns USING (fsa, cn, nm)) AS rqa_only,
           (SELECT count(*) FROM ns ANTI JOIN rs USING (fsa, cn, nm)) AS nar_only")))

  # What is left of the gap once whole-word containment is allowed instead of
  # equality -- the residue of name forms the particule strip does not reconcile.
  hdr("the address gap, corrected for names that merely contain one another")
  # dbGetQuery directly, not q(): the LIKE patterns carry bare percent signs.
  print(DBI::dbGetQuery(con, paste0(addr_keys, ", gap AS (SELECT * FROM rs ANTI JOIN ns USING (fsa, cn, nm))
    SELECT count(*) AS gap,
           count(*) FILTER (WHERE EXISTS (
             SELECT 1 FROM ns n2 WHERE n2.fsa = g.fsa AND n2.cn = g.cn
              AND (' ' || n2.nm || ' ' LIKE '% ' || g.nm || ' %'
                OR ' ' || g.nm || ' ' LIKE '% ' || n2.nm || ' %'))) AS nar_has_by_containment,
           count(*) FILTER (WHERE NOT EXISTS (
             SELECT 1 FROM ns n2 WHERE n2.fsa = g.fsa AND n2.cn = g.cn))
             AS nar_has_no_civic_there
      FROM gap g")))

  hdr("what the postal + civic gap is made of")
  print(q("
    WITH nk AS (
      SELECT DISTINCT replace(upper(MAIL_POSTAL_CODE), ' ', '') pc, CIVIC_NO::INT cn
        FROM Addresses WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> ''
         AND CIVIC_NO IS NOT NULL
    ), r AS (
      SELECT replace(upper(code_postal), ' ', '') pc,
             TRY_CAST(numero_municipal AS INT) cn,
             any_value(qualite_positionnement_geometrique) qual,
             any_value(nom_region_administrative) reg
        FROM rqa.rqa WHERE etat = 'Certifiée' AND code_postal <> '' GROUP BY 1, 2
    )
    SELECT qual, count(*) n,
           round(100.0 * count(*) / sum(count(*)) OVER (), 1) pct
      FROM r ANTI JOIN nk USING (pc, cn) GROUP BY 1 ORDER BY n DESC"))

  print(q("
    WITH nk AS (
      SELECT DISTINCT replace(upper(MAIL_POSTAL_CODE), ' ', '') pc, CIVIC_NO::INT cn
        FROM Addresses WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> ''
         AND CIVIC_NO IS NOT NULL
    ), r AS (
      SELECT replace(upper(code_postal), ' ', '') pc,
             TRY_CAST(numero_municipal AS INT) cn,
             any_value(nom_region_administrative) reg
        FROM rqa.rqa WHERE etat = 'Certifiée' AND code_postal <> '' GROUP BY 1, 2
    )
    SELECT reg, count(*) n FROM r ANTI JOIN nk USING (pc, cn)
     GROUP BY 1 ORDER BY n DESC LIMIT 10"))
}

# ---- interp -------------------------------------------------------------------

if (part %in% c("interp", "all")) {
  # Sampled from the addresses NAR does not carry, rendered back into surface
  # form and run through the local tiers only. No network: the point of the
  # measurement is what the package already does offline.
  hdr("interpolation over the addresses NAR does not carry")
  sp <- function(x) {
    pat <- "'^(DE LA |DES |DU |DE |LA |LE |LES |L |D |AUX |AU |A |EN )'"
    for (i in 1:3) x <- sprintf("regexp_replace(%s, %s, '')", x, pat)
    x
  }
  miss <- q("
    WITH ns AS (
      SELECT DISTINCT substr(replace(upper(MAIL_POSTAL_CODE), ' ', ''), 1, 3) fsa,
             CIVIC_NO::INT cn, %s nm
        FROM Addresses WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> ''
         AND CIVIC_NO IS NOT NULL
    ), rs AS (
      SELECT substr(replace(upper(code_postal), ' ', ''), 1, 3) fsa,
             TRY_CAST(numero_municipal AS INT) cn, %s nm,
             any_value(numero_municipal) civic,
             any_value(odonyme_recompose_normal) odo,
             any_value(nom_municipalite) mun, any_value(code_postal) pc,
             any_value(longitude::DOUBLE) lon, any_value(latitude::DOUBLE) lat,
             any_value(qualite_positionnement_geometrique) qual
        FROM rqa.rqa WHERE etat = 'Certifiée' AND code_postal <> ''
       GROUP BY 1, 2, 3 HAVING count(DISTINCT (longitude, latitude)) = 1
    )
    SELECT * FROM rs ANTI JOIN ns USING (fsa, cn, nm)
     USING SAMPLE reservoir(%d ROWS) REPEATABLE (20260821)",
    sp(msql("upper(strip_accents(OFFICIAL_STREET_NAME))")),
    sp(msql("upper(strip_accents(specifique_odonyme))")), N)

  txt <- sprintf("%s %s, %s, QC %s", miss$civic, miss$odo, miss$mun, miss$pc)
  g <- geocode(txt, con = con)
  cat(sprintf("%s addresses NAR does not carry, geocoded on the local tiers\n",
              format(nrow(miss), big.mark = ",")))
  print(as.data.frame(table(match_method = g$match_method)), row.names = FALSE)

  # Whether an unplaced row is NAR missing the street or the parser missing the
  # address. The strings here are RQA's own canonical spelling, so a parse
  # failure on one is the parser's, and it has to be netted off before the
  # unplaced share can be read as coverage.
  none <- is.na(g$lon)
  nz <- normalize_address(txt[none], con = con)
  cat("of the unplaced, parsed a civic number and a street: ",
      pct(!is.na(nz$CIVIC_NO) & !is.na(nz$STREET_NAME)), "\n", sep = "")
  cat("  ... and the gazetteer resolved it:                 ",
      pct(nz$parse_source == "gazetteer"), "\n", sep = "")

  d <- data.frame(method = as.character(g$match_method), qual = miss$qual,
                  lon = g$lon, lat = g$lat, rlon = miss$lon, rlat = miss$lat)
  d <- d[!is.na(d$lon), , drop = FALSE]
  DBI::dbWriteTable(con, "qc_interp", d, temporary = TRUE, overwrite = TRUE)
  hdr("how far the interpolated point falls from RQA's own")
  print(q("SELECT method, count(*) n,
      round(median(m), 1) p50, round(quantile_cont(m, 0.9), 1) p90,
      round(quantile_cont(m, 0.99), 1) p99,
      round(100.0 * count(*) FILTER (WHERE m < 50) / count(*), 1) under_50m,
      round(100.0 * count(*) FILTER (WHERE m > 500) / count(*), 1) over_500m
    FROM (SELECT method, ST_Distance(
        ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:3347', always_xy := true),
        ST_Transform(ST_Point(rlon, rlat), 'EPSG:4269', 'EPSG:3347', always_xy := true)) m
      FROM qc_interp) GROUP BY 1 ORDER BY n DESC"))
}

cat("\n")
