test_that("the lexicon map resolves the French reading", {
  types <- rqa_lex_map(nar_lex_types)
  # AVENUE is AVE in Ontario and AV in Quebec; this map is Quebec's.
  expect_equal(types$canonical[types$surface_fold == "AVENUE"], "AV")
  expect_equal(types$canonical[types$surface_fold == "BOULEVARD"], "BOUL")
  expect_equal(types$canonical[types$surface_fold == "CHEMIN"], "CH")

  dirs <- rqa_lex_map(nar_lex_dirs)
  expect_equal(dirs$canonical[dirs$surface_fold == "OUEST"], "O")
  expect_equal(dirs$canonical[dirs$surface_fold == "EST"], "E")
})

test_that("RQA's positional classes become match_method labels", {
  expect_equal(rqa_method_label(c("Bâtiment", "Géocodée", "Incertaine",
                                  "Centre lot", "Front lot")),
               c("rqa_building", "rqa_geocoded", "rqa_uncertain",
                 "rqa_lot", "rqa_lot"))
  # A class a future release invents still places its addresses.
  expect_equal(rqa_method_label("Site / Accès propriété"), "rqa_other")
})

test_that("the RQA query joins on the match fold, not the plain one", {
  sql <- rqa_geocode_sql("probe")
  expect_match(sql, "a.MATCH_FOLD = p.match_fold", fixed = TRUE)
  expect_false(grepl("a.NAME_FOLD = p.name_fold", sql, fixed = TRUE))
  # No MunAlias route: RQA has no alias set, so an authoritative municipality
  # is compared against the municipality and the borough directly.
  expect_false(grepl("MunAlias", sql, fixed = TRUE))
  expect_match(sql, "BOROUGH", fixed = TRUE)
})

test_that("the import shapes RQA into NAR's conventions", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()

  expect_true(nar_has_rqa(con))
  a <- DBI::dbGetQuery(con, "SELECT * FROM RqaAddresses ORDER BY RQA_ID")

  # The retired row is dropped.
  expect_equal(nrow(a), 4)
  expect_false("rqa5" %in% a$RQA_ID)

  # Particule plus specifique, which is where NAR keeps it.
  expect_equal(a$STREET_NAME[a$RQA_ID == "rqa3"], "du Curé-Labelle")
  expect_equal(a$STREET_SPECIFIC[a$RQA_ID == "rqa3"], "Curé-Labelle")

  # The generique becomes a canonical type, in French.
  expect_equal(a$STREET_TYPE, c("RUE", "RUE", "BOUL", "RUE"))
  expect_equal(a$STREET_DIR, c(NA, NA, NA, "O"))

  # The folds the tier and the gazetteer join on.
  expect_equal(a$NAME_FOLD[a$RQA_ID == "rqa4"], "SAINT-JACQUES")
  expect_equal(a$MATCH_FOLD[a$RQA_ID == "rqa4"], "SAINT JACQUES")
  expect_equal(a$MATCH_FOLD[a$RQA_ID == "rqa3"], "DU CURE LABELLE")

  expect_equal(a$PROV_ABVN, rep("QC", 4))
  expect_equal(a$FSA[a$RQA_ID == "rqa2"], "H1B")
  expect_true(all(!is.na(a$x)) && all(!is.na(a$y)))
})

test_that("the imported points land where they were written", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  # Storage is projected and untagged, so the round trip goes back through the
  # macros rather than reading x/y as degrees.
  ll <- DBI::dbGetQuery(con, "
    SELECT RQA_ID, nar_lon(geom) AS lon, nar_lat(geom) AS lat
      FROM RqaAddresses WHERE RQA_ID = 'rqa1'")
  expect_equal(ll$lon, -73.5730, tolerance = 1e-6)
  expect_equal(ll$lat, 45.4995, tolerance = 1e-6)
})

test_that("IN_NAR marks the addresses NAR already carries", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  a <- DBI::dbGetQuery(con, "SELECT RQA_ID, IN_NAR FROM RqaAddresses ORDER BY RQA_ID")
  # 1255 Peel is the one address in both fixtures.
  expect_equal(a$IN_NAR, c(TRUE, FALSE, FALSE, FALSE))

  s <- DBI::dbGetQuery(con, "SELECT * FROM RqaStreets ORDER BY SEQODO")
  expect_equal(nrow(s), 4)
  expect_equal(s$N_NOT_IN_NAR, c(0, 1, 1, 1))
  expect_equal(s$MIN_CIVIC_NO[s$SEQODO == "2"], 431)
})

test_that("the import records what it loaded", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  meta <- nar_metadata(con)
  expect_equal(unname(meta["rqa_version"]), "20260801")
  expect_equal(unname(meta["rqa_rows"]), "4")
  expect_equal(unname(meta["rqa_not_in_nar"]), "3")
  expect_equal(unname(meta["rqa_licence"]), "CC-BY 4.0")
  # NAR's own metadata survives untouched.
  expect_equal(unname(meta["version"]), "test-01")
})

test_that("the rqa tier places an address NAR does not carry", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  parsed <- data.frame(CIVIC_NO = 431L, STREET_NAME = "COURTEMANCHE",
                       STREET_TYPE = "RUE", MUN_NAME = "MONTREAL-EST",
                       PROV_ABVN = "QC", stringsAsFactors = FALSE)

  none <- geocode(parsed, method = "nar", con = con)
  expect_equal(none$match_method, "none")

  hit <- geocode(parsed, method = c("nar", "rqa"), con = con)
  expect_equal(hit$match_method, "rqa_building")
  expect_equal(hit$uncertainty_m, 0)
  expect_equal(hit$lon, -73.5100, tolerance = 1e-5)
  expect_equal(hit$lat, 45.6300, tolerance = 1e-5)
})

test_that("the tier folds the particule and the hyphen the way RQA needs", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  # NAR's spelling of this street carries the particule; a user's does not, and
  # writes the hyphen as a space. Only the match fold joins the two.
  parsed <- data.frame(CIVIC_NO = 100L, STREET_NAME = "DU CURE LABELLE",
                       STREET_TYPE = "BOUL", MUN_NAME = "SAINT-JEROME",
                       PROV_ABVN = "QC", stringsAsFactors = FALSE)
  hit <- geocode(parsed, method = c("nar", "rqa"), con = con)
  # Incertaine is not a building placement, and nothing here has measured what
  # it is worth, so the distance is withheld rather than guessed.
  expect_equal(hit$match_method, "rqa_uncertain")
  expect_true(is.na(hit$uncertainty_m))
})

test_that("an authoritative municipality reaches a borough", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  parsed <- data.frame(CIVIC_NO = 5510L, STREET_NAME = "SAINT-JACQUES",
                       STREET_DIR = "O", PROV_ABVN = "QC",
                       MUN_NAME = NA_character_, stringsAsFactors = FALSE)
  hit <- geocode(parsed, mun = "Le Sud-Ouest", method = c("nar", "rqa"),
                 con = con)
  expect_equal(hit$match_method, "rqa_geocoded")
})

test_that("the tier is Quebec-only and declines everything else", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()
  # A street name that exists in Quebec, on an address that says Ontario.
  parsed <- data.frame(CIVIC_NO = 431L, STREET_NAME = "COURTEMANCHE",
                       MUN_NAME = "OTTAWA", PROV_ABVN = "ON",
                       stringsAsFactors = FALSE)
  hit <- geocode(parsed, method = c("nar", "rqa"), con = con)
  expect_equal(hit$match_method, "none")
})

test_that("the tier refuses to run against a database with no RQA", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection()
  expect_false(nar_has_rqa(con))
  expect_error(geocode("4001 W King Edward Ave, Vancouver BC",
                       method = c("nar", "rqa"), con = con),
               "rqa_import")
})

test_that("rqa is a known method and keeps its priority position", {
  expect_equal(nar_geocode_methods(c("nar", "rqa", "nar_interpolate")),
               c("nar", "rqa", "nar_interpolate"))
  # Order is the caller's, not the registry's -- priority is running order.
  expect_equal(nar_geocode_methods(c("rqa", "nar")), c("rqa", "nar"))
  expect_error(nar_geocode_methods("rqa_"), "Unknown geocoding method")
})

test_that("the RQA gazetteer query has no exact branch and never leaves Quebec", {
  sql <- nar_rqa_gazetteer_sql("probe")

  # The NAR query answers a string that named no municipality from an exact,
  # indexed name match. This one must not: RQA covers one province, so an
  # unrestricted match here would assert Quebec about a string that never said
  # so. A row with no locality at all simply drops out of the `muns` CTE.
  expect_match(sql, "p.prov = '' OR p.prov = 'QC'", fixed = TRUE)
  expect_match(sql, "WHERE p.mun_use IS NOT NULL", fixed = TRUE)

  # One name family, and the comparison is the match fold throughout -- RQA
  # keeps the particule in a column of its own, so its plain fold and NAR's are
  # not the same string for the same street.
  expect_match(sql, "s.MATCH_FOLD", fixed = TRUE)
  expect_false(grepl("s.NAME_FOLD", sql, fixed = TRUE))

  # The municipality resolves through NAR's alias table, restricted to Quebec,
  # and MUN_KEY's third field is the CSD name RQA files under.
  expect_match(sql, "JOIN MunAlias m", fixed = TRUE)
  expect_match(sql, "m.PROV_ABVN = 'QC'", fixed = TRUE)
  expect_match(sql, "split_part(m.MUN_KEY, ':', 3)", fixed = TRUE)

  # The same weights as the NAR pass, so `confidence` means one thing whichever
  # register answered.
  for (w in c("0.72 * name_sim", "0.10 * CASE", "0.06 * CASE", "0.12 * CASE")) {
    expect_match(sql, w, fixed = TRUE)
  }
})

test_that("normalization falls through to the Quebec register", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()

  # Rue Courtemanche in Montreal-Est is the fixture's one address NAR does not
  # carry, so it is the whole reason the second pass exists.
  out <- normalize_address("431 rue Courtemanche, Montreal-Est, QC", con = con)
  expect_equal(out$parse_source, "rqa")
  expect_equal(out$STREET_NAME, "Courtemanche")
  expect_equal(out$STREET_TYPE, "RUE")
  expect_equal(out$MUN_NAME, "MONTRÉAL-EST")
  expect_equal(out$PROV_ABVN, "QC")
})

test_that("the Quebec register cannot displace an answer NAR already gave", {
  skip_if_no_duckdb_spatial()
  con <- local_rqa_connection()

  # Rue Peel is the one address both fixtures carry, and the two registers
  # spell its municipality differently -- MONTREAL in NAR, Montreal in RQA.
  # Priority is running order, exactly as in geocode(): NAR goes first, so
  # importing RQA cannot change an answer that already worked, and the
  # municipality that comes back is the proof of which pass answered.
  out <- normalize_address("1255 rue Peel, Montreal, QC", con = con)
  expect_equal(out$parse_source, "gazetteer")
  expect_equal(out$MUN_NAME, "MONTREAL")

  # And a province RQA does not cover is never offered to it at all.
  out <- normalize_address("4001 W King Edward Ave, Vancouver, BC", con = con)
  expect_equal(out$parse_source, "gazetteer")
  expect_equal(out$MUN_NAME, "VANCOUVER")
})
