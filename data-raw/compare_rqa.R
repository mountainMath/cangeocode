# Measures Statistics Canada's NAR against Quebec's own address register.
#
# The Repertoire quebecois des adresses (RQA) is published by the Ministere des
# Ressources naturelles et des Forets under CC-BY 4.0, as a single ~3 GB CSV
# plus `Odonymes_renvois.csv`. It is the register NAR's Quebec rows are
# ultimately derived from, and it is also what the `qc` geocoding tier queries
# -- see `data-raw/probe_qc.R`. That makes this comparison a check on how much
# of RQA survives the trip into NAR, NOT an accuracy benchmark: neither side is
# independent of the other, and the numbers below are what establishes that.
#
# Four things are measured, and RQA_PART picks which:
#
#   count  -- row counts, unit coverage, distinct civic addresses, and what
#             each side calls a building point.
#   gap    -- the addresses one side carries and the other does not, keyed on
#             postal code + civic number, and what RQA says about the quality
#             of the ones NAR is missing.
#   dist   -- point-to-point disagreement for the addresses both sides carry,
#             split by RQA's own positional-quality flag. This is the table
#             that shows NAR is carrying RQA's coordinates.
#   odo    -- the odonyme decomposition, which NAR has no analogue for and
#             which is the reason to keep RQA around for the parser.
#
# Two traps, both of which produced wrong numbers on the way here:
#
#   * NAR's `x`/`y` are stored PROJECTED, in the storage CRS (EPSG:3347), not
#     as lon/lat. RQA's `longitude`/`latitude` are degrees on NAD83
#     (EPSG:4269). Transform the RQA side TO 3347, never the NAR side FROM
#     4269 -- doing the latter silently yields NA distances, not an error.
#     `always_xy := true` is required, as everywhere else in this package.
#   * RQA's non-current rows must be excluded (`etat = 'Certifiee'`, with the
#     accent). Included, they add ~7.5k rows and points that were retired.
#
# The comparison keys on (postal code, civic number) and keeps only keys that
# are unique on BOTH sides. That is a coarse key -- it will not distinguish two
# streets that share a postal code and a civic number -- so restricting to
# unique-on-both is what keeps it honest, at the cost of dropping the ambiguous
# tail. Street names are deliberately NOT part of the key: NAR and RQA spell
# them differently, and using them would measure the parser, not the geometry.
#
# Prerequisites: NAR_CACHE_PATH with an imported release, and RQA extracted to
# RQA_DIR (default ~/data/rqa). Download, once:
#
#   https://diffusion.mern.gouv.qc.ca/Diffusion/RQA/RQA_CSV.zip
#
# Usage:
#   RQA_PART=all Rscript data-raw/compare_rqa.R
#
# The first run builds `<RQA_DIR>/rqa.duckdb` from the CSV, which takes a few
# minutes and about 2 GB; later runs reuse it.

suppressMessages(devtools::load_all(quiet = TRUE))

part    <- Sys.getenv("RQA_PART", "all")
rqa_dir <- Sys.getenv("RQA_DIR", path.expand("~/data/rqa"))
nar_ver <- Sys.getenv("RQA_NAR_VERSION", "2026-06")

rqa_csv <- file.path(rqa_dir, "RQA.csv")
rqa_db  <- file.path(rqa_dir, "rqa.duckdb")
nar_db  <- file.path(Sys.getenv("NAR_CACHE_PATH"), paste0(nar_ver, ".duckdb"))

stopifnot(nzchar(Sys.getenv("NAR_CACHE_PATH")), file.exists(nar_db))

con <- DBI::dbConnect(duckdb::duckdb(), rqa_db)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
q <- function(sql) DBI::dbGetQuery(con, sql)

if (!"rqa" %in% DBI::dbListTables(con)) {
  stopifnot(file.exists(rqa_csv))
  message("building ", rqa_db, " from ", rqa_csv, " ...")
  # all_varchar: the register carries empty strings rather than nulls, and
  # letting DuckDB infer types turns `numero_municipal` into a number in some
  # partitions and a string in others.
  DBI::dbExecute(con, sprintf(
    "CREATE TABLE rqa AS SELECT * FROM read_csv('%s', header = true,
       sample_size = 200000, all_varchar = true)", rqa_csv))
}
DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")
DBI::dbExecute(con, sprintf("ATTACH '%s' AS nar (READ_ONLY)", nar_db))

hdr <- function(x) cat("\n== ", x, " ==\n", sep = "")

# ---- count ------------------------------------------------------------------

if (part %in% c("count", "all")) {
  hdr("NAR Quebec")
  print(q("SELECT count(*) AS n_rows,
      count(*) FILTER (WHERE APT_NO_LABEL <> '') AS with_unit,
      count(DISTINCT (CIVIC_NO, CIVIC_NO_SUFFIX, OFFICIAL_STREET_NAME,
                      OFFICIAL_STREET_TYPE, OFFICIAL_STREET_DIR, CSD_FRE_NAME))
        AS distinct_civic,
      count(*) FILTER (WHERE geom_source = 'building') AS building
    FROM nar.Addresses WHERE PROV_CODE = '24'"))

  hdr("RQA, certified rows only")
  print(q("SELECT count(*) AS n_rows,
      count(*) FILTER (WHERE numero_unite <> '') AS with_unit,
      count(DISTINCT (numero_municipal, numero_municipal_suffixe, seqodo,
                      code_municipalite)) AS distinct_civic,
      count(*) FILTER (WHERE qualite_positionnement_geometrique = 'Bâtiment')
        AS building
    FROM rqa WHERE etat = 'Certifiée'"))

  hdr("RQA positional quality, which NAR has no analogue for")
  print(q("SELECT qualite_positionnement_geometrique AS qual, count(*) AS n,
      round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
    FROM rqa WHERE etat = 'Certifiée' GROUP BY 1 ORDER BY n DESC"))
}

# ---- gap --------------------------------------------------------------------

nar_keys <- "
  SELECT DISTINCT upper(replace(MAIL_POSTAL_CODE, ' ', '')) AS pc,
                  CIVIC_NO::INT AS cn
  FROM nar.Addresses
  WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> '' AND CIVIC_NO IS NOT NULL"

rqa_keys <- "
  SELECT upper(replace(code_postal, ' ', '')) AS pc,
         TRY_CAST(numero_municipal AS INT) AS cn,
         any_value(qualite_positionnement_geometrique) AS qual,
         any_value(adresse_formatee) AS ex
  FROM rqa WHERE etat = 'Certifiée' AND code_postal <> '' GROUP BY 1, 2"

if (part %in% c("gap", "all")) {
  hdr("postal + civic keys on each side")
  print(q(sprintf("WITH n AS (%s), r AS (%s)
    SELECT (SELECT count(*) FROM n) AS nar_keys,
           (SELECT count(*) FROM r) AS rqa_keys,
           (SELECT count(*) FROM n JOIN r USING (pc, cn)) AS shared",
    nar_keys, rqa_keys)))

  hdr("RQA keys with no NAR counterpart, by RQA's quality flag")
  print(q(sprintf("WITH n AS (%s), r AS (%s)
    SELECT qual, count(*) AS n FROM r ANTI JOIN n USING (pc, cn)
    GROUP BY 1 ORDER BY n DESC", nar_keys, rqa_keys)))

  hdr("a sample of what RQA has and NAR does not")
  print(q(sprintf("WITH n AS (%s), r AS (%s)
    SELECT ex FROM r ANTI JOIN n USING (pc, cn) USING SAMPLE 8 ROWS",
    nar_keys, rqa_keys)))
}

# ---- dist -------------------------------------------------------------------

if (part %in% c("dist", "all")) {
  # One row per (postal, civic) on each side, keeping only keys unique on BOTH
  # -- an ambiguous key would compare two different addresses to each other.
  DBI::dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE pair AS
    WITH n AS (
      SELECT upper(replace(MAIL_POSTAL_CODE, ' ', '')) pc, CIVIC_NO::INT cn,
             any_value(x) nx, any_value(y) ny
      FROM nar.Addresses
      WHERE PROV_CODE = '24' AND MAIL_POSTAL_CODE <> '' AND CIVIC_NO IS NOT NULL
            AND geom_source = 'building'
      GROUP BY 1, 2 HAVING count(DISTINCT (x, y)) = 1
    ), r AS (
      SELECT upper(replace(code_postal, ' ', '')) pc,
             TRY_CAST(numero_municipal AS INT) cn,
             any_value(longitude::DOUBLE) lon, any_value(latitude::DOUBLE) lat,
             any_value(qualite_positionnement_geometrique) qual
      FROM rqa WHERE etat = 'Certifiée' AND code_postal <> ''
      GROUP BY 1, 2 HAVING count(DISTINCT (longitude, latitude)) = 1
    )
    SELECT n.pc, n.cn, r.qual,
           ST_Distance(ST_Point(n.nx, n.ny),
                       ST_Transform(ST_Point(r.lon, r.lat),
                                    'EPSG:4269', 'EPSG:3347',
                                    always_xy := true)) AS d
    FROM n JOIN r USING (pc, cn)")

  hdr("NAR building point vs RQA point, same postal + civic")
  print(q("SELECT count(*) AS n,
      round(median(d), 2) AS p50, round(quantile_cont(d, 0.9), 1) AS p90,
      round(quantile_cont(d, 0.99), 1) AS p99,
      round(100.0 * count(*) FILTER (WHERE d < 1) / count(*), 1) AS under_1m,
      round(100.0 * count(*) FILTER (WHERE d > 100) / count(*), 1) AS over_100m
    FROM pair"))

  hdr("the same, split by RQA's positional-quality flag")
  print(q("SELECT qual, count(*) AS n,
      round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct,
      round(median(d), 2) AS p50, round(quantile_cont(d, 0.9), 1) AS p90,
      round(100.0 * count(*) FILTER (WHERE d < 1) / count(*), 1) AS under_1m
    FROM pair GROUP BY 1 ORDER BY n DESC"))
}

# ---- odo --------------------------------------------------------------------

if (part %in% c("odo", "all")) {
  hdr("the odonyme decomposition")
  print(q("SELECT generique_odonyme AS generique, particule_odonyme AS particule,
      specifique_odonyme AS specifique, point_cardinal_odonyme AS cardinal,
      odonyme_recompose_normal AS normal, odonyme_recompose_court AS court
    FROM rqa USING SAMPLE 6 ROWS"))

  hdr("its shape")
  print(q("SELECT
      count(DISTINCT generique_odonyme) AS generiques,
      count(DISTINCT particule_odonyme) AS particules,
      round(100.0 * count(*) FILTER (WHERE particule_odonyme <> '')
            / count(*), 1) AS pct_with_particule,
      count(DISTINCT seqodo) AS odonymes,
      count(*) FILTER (WHERE renvoi_seqodo <> '') AS with_renvoi
    FROM rqa"))
}
