# The Quebec geocoder binding. Nothing here touches the network: every test
# runs against a response captured from the live service and saved under
# fixtures/, which is the only way the floors stay checkable once the service
# changes what it answers.
#
#   qc-batch        -- three addresses in one request, and the response comes
#                      back in the order 2, 1, 3. A civic match, a street-only
#                      match, and one the service would not match at all.
#   qc-abbrev       -- `1 RUE NOTRE-DAME O`: the abbreviated direction, which
#                      the locator answers with a street centroid scoring 92.4
#                      where the correct civic point scores 82.5. This is the
#                      fixture behind nar_qc_query().
#   qc-wrongstreet  -- `12 RUE SAINT-JEAN, GATINEAU` answered with
#                      `12 Rue Saint-Jean-Bosco, Gatineau` as RQA_Adresse:
#                      right municipality, right number, wrong street. The
#                      locator floor passes it and only the address floor does
#                      not.
#   qc-wrongmun     -- `77 RUE SAINTE-CATHERINE OUEST, SHERBROOKE` answered
#                      with the Montréal street, 130 km away.
#   qc-reverse      -- one reverse-geocode answer.
#   qc-reverse-none -- the refusal a point with nothing near it gets back.

qc_fixture <- function(name) {
  path <- testthat::test_path("fixtures", paste0("qc-", name, ".json"))
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

# The components a query would have carried, without needing a gazetteer.
qc_parts <- function(civic, name, type = NA, dir = NA, mun = NA, prov = "QC") {
  data.frame(CIVIC_NO = civic, STREET_NAME = name, STREET_TYPE = type,
             STREET_DIR = dir, MUN_NAME = mun, PROV_ABVN = prov,
             stringsAsFactors = FALSE)
}

test_that("results are placed by ResultID, not by position", {
  skip_if_not_installed("jsonlite")
  raw <- qc_fixture("batch")
  # The premise of the test: the service really did answer out of order.
  expect_equal(vapply(raw$locations, function(l) l$attributes$ResultID, 1L),
               c(2L, 1L, 3L))

  r <- nar_qc_locations(raw, 3)
  expect_equal(nrow(r), 3L)
  # Row 1 was the address the service could not match, and it stays row 1.
  expect_equal(r$qc_status, c("U", "M", "M"))
  expect_equal(r$qc_locator, c(NA, "RQA_Adresse", "RQA_Rue"))
  expect_true(grepl("Gauchet", r$qc_address[2]))
})

test_that("an id the service dropped still gets a row", {
  skip_if_not_installed("jsonlite")
  # Three sent, one answer: the other two are unmatched rows, not a short frame.
  raw <- qc_fixture("batch")
  raw$locations <- raw$locations[1]
  r <- nar_qc_locations(raw, 3)

  expect_equal(nrow(r), 3L)
  expect_equal(which(!is.na(r$qc_locator)), 2L)
  expect_true(all(is.na(r$qc_status[c(1, 3)])))
})

test_that("coordinates come from location, never from the Latitude attribute", {
  skip_if_not_installed("jsonlite")
  raw <- qc_fixture("batch")
  hit <- raw$locations[[1]]
  # The trap: the attribute is a French-locale string, and as.numeric() on it
  # is NA at best. If this stops being true the test below stops proving much.
  expect_true(is.character(hit$attributes$Latitude))
  expect_match(hit$attributes$Latitude, ",")

  r <- nar_qc_locations(raw, 3)
  expect_equal(r$lat[2], hit$location$y)
  expect_equal(r$lon[2], hit$location$x)
  expect_false(is.na(r$lat[2]))
})

test_that("a street-only answer is not an address", {
  skip_if_not_installed("jsonlite")
  loc <- nar_qc_locations(qc_fixture("batch"), 3)
  q <- qc_parts(c(1000, 1000, NA), c("DE LA GAUCHETIERE ROBERT-BOURASSA",
                                     "DE LA GAUCHETIERE", "SAINT-JEAN"),
                type = "RUE", dir = c("O", "O", NA),
                mun = c("MONTREAL", "MONTREAL", "QUEBEC"))
  out <- nar_qc_floors(loc, q)

  expect_equal(out$match_method, c("none", "qc_address", "none"))
  expect_equal(out$qc_reject[1], "unmatched")
  expect_equal(out$qc_reject[3], "street only, no civic number")
  # A rejected row keeps what was thrown away.
  expect_equal(out$qc_locator[3], "RQA_Rue")
  expect_true(is.na(out$uncertainty_m[3]))
  expect_true(is.na(out$lon[3]))
})

test_that("the address floor catches the right number on the wrong street", {
  skip_if_not_installed("jsonlite")
  loc <- nar_qc_locations(qc_fixture("wrongstreet"), 1)
  # The locator was happy: it resolved a civic number, in the right place.
  expect_equal(loc$qc_locator, "RQA_Adresse")
  expect_equal(loc$qc_status, "M")

  out <- nar_qc_floors(loc, qc_parts(12, "SAINT-JEAN", "RUE", mun = "GATINEAU"))
  expect_equal(out$match_method, "none")
  expect_false(is.na(out$qc_reject))

  # And it accepts the street it actually answered about.
  ok <- nar_qc_floors(loc, qc_parts(12, "SAINT-JEAN-BOSCO", "RUE",
                                    mun = "GATINEAU"))
  expect_equal(ok$match_method, "qc_address")
})

test_that("a street in the wrong municipality is rejected", {
  skip_if_not_installed("jsonlite")
  loc <- nar_qc_locations(qc_fixture("wrongmun"), 1)
  expect_true(grepl("Montr", loc$qc_address))

  out <- nar_qc_floors(loc, qc_parts(77, "SAINTE-CATHERINE", "RUE", "O",
                                     mun = "SHERBROOKE"))
  expect_equal(out$match_method, "none")
})

test_that("the abbreviated direction buys a confident wrong answer", {
  skip_if_not_installed("jsonlite")
  loc <- nar_qc_locations(qc_fixture("abbrev"), 1)
  # A street centroid, and it outscores the civic match the spelled-out form
  # gets back -- which is the whole argument for nar_qc_query().
  expect_equal(loc$qc_locator, "RQA_Rue")
  expect_gt(loc$qc_score, 90)

  out <- nar_qc_floors(loc, qc_parts(1, "NOTRE-DAME", "RUE", "O",
                                     mun = "MONTREAL"))
  expect_equal(out$match_method, "none")
  expect_equal(out$qc_reject, "street only, no civic number")
})

test_that("the query is rendered French-canonical, not NAR-canonical", {
  p <- qc_parts(c(1, 1000, 875), c("NOTRE-DAME", "DE LA GAUCHETIERE",
                                   "HYPOLITE-BERNIER"),
                type = c("RUE", "BOUL", "AV"), dir = c("O", NA, "NE"),
                mun = c("MONTREAL", "MONTREAL", "LEVIS"))
  q <- nar_qc_query(p)

  # Type first, direction spelled out, both in French.
  expect_equal(q[1], "1 RUE NOTRE-DAME OUEST, MONTREAL, QC")
  expect_equal(q[2], "1000 BOULEVARD DE LA GAUCHETIERE, MONTREAL, QC")
  expect_equal(q[3], "875 AVENUE HYPOLITE-BERNIER NORD-EST, LEVIS, QC")
  # Which is not what the NAR renderer produces.
  expect_false(identical(q[1], nar_address_string(p)[1]))
})

test_that("the query renderer survives missing components", {
  p <- qc_parts(c(NA, 12, 3), c("PRINCIPALE", NA, "DU MOULIN"),
                type = c("RUE", "RUE", NA), mun = c("LAVAL", NA, "MAGOG"))
  q <- nar_qc_query(p)

  expect_equal(q[1], "RUE PRINCIPALE, LAVAL, QC")
  expect_equal(q[2], "12 RUE, QC")
  expect_equal(q[3], "3 DU MOULIN, MAGOG, QC")
  # The suffix belongs to the number, with no space.
  p2 <- qc_parts(990, "PRINCIPALE", "RUE", mun = "LAVAL")
  p2$CIVIC_NO_SUFFIX <- "A"
  expect_equal(nar_qc_query(p2), "990A RUE PRINCIPALE, LAVAL, QC")
})

test_that("an unknown street type or direction is sent unchanged", {
  expect_equal(nar_qc_query(qc_parts(5, "SOMETHING", "ZZZ", "QQ", "LAVAL")),
               "5 ZZZ SOMETHING QQ, LAVAL, QC")
  # But the ones that matter are expanded.
  expect_equal(unname(nar_qc_types()[c("BOUL", "CH", "AV")]),
               c("BOULEVARD", "CHEMIN", "AVENUE"))
  expect_equal(unname(nar_qc_dirs()[c("O", "W", "SO")]),
               c("OUEST", "OUEST", "SUD-OUEST"))
})

test_that("Loc_name drives the precision, and Addr_type does not", {
  expect_equal(nar_qc_precision("RQA_Adresse")$match_method, "qc_address")
  expect_equal(nar_qc_precision("RQA_Rue")$match_method, "qc_street")
  expect_gt(nar_qc_precision("RQA_Rue")$uncertainty_m,
            nar_qc_precision("RQA_Adresse")$uncertainty_m)
  # Anything else, including an empty Loc_name, is not an answer.
  expect_equal(nar_qc_precision(NULL)$match_method, "none")
  expect_equal(nar_qc_precision("Feature")$match_method, "none")
})

test_that("a reverse answer is read, and a refusal is not an error", {
  skip_if_not_installed("jsonlite")
  r <- nar_qc_reverse_row(qc_fixture("reverse"))
  expect_equal(r$qc_city, "Montréal")
  expect_equal(r$qc_postal, "H3H2S1")
  expect_false(is.na(r$lon))
  # The endpoint reports no distance; qc_reverse_geocode() measures it.
  expect_true(is.na(r$qc_dist_m))

  none <- nar_qc_reverse_row(qc_fixture("reverse-none"))
  expect_equal(nrow(none), 1L)
  expect_true(is.na(none$qc_address))
  expect_true(is.na(none$lon))
  expect_equal(nrow(nar_qc_reverse_row(NULL)), 1L)
})

test_that("the tier only sends Quebec rows", {
  res <- qc_parts(c(1, 2, 3), c("A", "B", "C"),
                  mun = c("MONTREAL", "TORONTO", "VANCOUVER"),
                  prov = c("QC", "ON", "BC"))
  out <- data.frame(x = NA_real_, y = NA_real_, match_method = "none",
                    uncertainty_m = NA_real_, n_matches = NA_integer_)[rep(1, 3), ]
  # Called with no Quebec row left to do, it must not reach the network at all.
  expect_identical(nar_geocode_tier_qc(res, out, todo = 2:3, con = NULL), out)
})

test_that("no rows to send means no request", {
  # ceiling() over an empty vector asks for zero batches, and rbind over an
  # empty list is NULL rather than a zero-row frame. Reached with a parsed
  # frame, since normalize_address() does not accept a zero-length vector.
  skip_if_not_installed("httr2")
  skip_if_not_installed("jsonlite")
  out <- qc_geocode(qc_parts(1, "A", mun = "LAVAL")[0, , drop = FALSE])

  expect_equal(nrow(out), 0L)
  expect_true(all(c("input", "match_method", "uncertainty_m", "qc_locator",
                    "qc_score", "qc_status", "qc_address", "qc_postal",
                    "qc_reject", "lon", "lat") %in% names(out)))
})
