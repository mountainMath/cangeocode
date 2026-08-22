# The NRCan geolocator binding. Nothing here touches the network: every test
# runs against a response captured from the live service and saved under
# fixtures/, which is also the only way the floors stay checkable once the
# service changes what it ranks first.
#
# The fixtures are the four outcomes that matter, and three of them are wrong
# answers that a naive binding would accept:
#
#   position  -- a real interpolated civic position. The only class that passes.
#   centroid  -- INTERPOLATED_CENTROID: the street was found, the number was not.
#   geoname   -- garbage in, a populated place out, still ranked first.
#   wrongmun  -- a real Rue Notre-Dame Ouest, in the wrong town, 500 km away.
#   wrongtype -- Spadina Road answered with Spadina Avenue, 3 km away.
#   error     -- `{"message": ...}` under an HTTP 200.

nrcan_fixture <- function(name) {
  path <- testthat::test_path("fixtures", paste0("nrcan-", name, ".json"))
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

# The components a query would have carried, without needing a gazetteer.
nrcan_parts <- function(civic, name, type = NA, dir = NA, mun = NA, prov = NA) {
  data.frame(CIVIC_NO = civic, STREET_NAME = name, STREET_TYPE = type,
             STREET_DIR = dir, MUN_NAME = mun, PROV_ABVN = prov,
             stringsAsFactors = FALSE)
}

test_that("the top result is read, and only the top result", {
  skip_if_not_installed("jsonlite")
  r <- nar_nrcan_top(nrcan_fixture("position"))

  expect_equal(r$nrcan_kind, "Street")
  expect_equal(r$nrcan_qualifier, "INTERPOLATED_POSITION")
  expect_equal(r$nrcan_title, "100 Water Street, Charlottetown, Prince Edward Island")
  expect_equal(round(r$lon, 4), -63.1241)
  expect_equal(round(r$lat, 4), 46.2325)
  # One row per response, however many results the service ranked.
  expect_equal(nrow(r), 1L)
})

test_that("the fully qualified type is reduced to its leaf", {
  skip_if_not_installed("jsonlite")
  # `ca.gc.nrcan.geoloc.data.model.Street` is what the wire carries.
  expect_equal(nar_nrcan_top(nrcan_fixture("geoname"))$nrcan_kind, "Geoname")
})

test_that("a JSON object body is an absent answer, not a result", {
  skip_if_not_installed("jsonlite")
  # The 500-inside-a-200. An unnamed list is the results array; a named one is
  # this. Reading it as a result would take `message` for a title.
  r <- nar_nrcan_top(nrcan_fixture("error"))

  expect_true(is.na(r$nrcan_title))
  expect_true(is.na(r$lon))
  expect_equal(nrow(r), 1L)
})

test_that("an empty body is an absent answer too", {
  r <- nar_nrcan_top(list())
  expect_true(is.na(r$nrcan_title))
  expect_equal(nrow(r), 1L)
})

test_that("an interpolated civic position passes every floor", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(100, "WATER", "ST", NA, "CHARLOTTETOWN", "PE")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("position")), q)

  expect_equal(r$match_method, "nrcan")
  expect_true(is.na(r$nrcan_reject))
  expect_equal(r$uncertainty_m, nar_nrcan_uncertainty_m())
  expect_false(is.na(r$lon))
})

test_that("a street centroid is rejected: the civic number was not resolved", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(1155, "ROBSON", "ST", NA, "VANCOUVER", "BC")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("centroid")), q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "INTERPOLATED_CENTROID")
  # A rejected row offers no point, so it carries no error bar -- but what was
  # thrown away stays visible.
  expect_true(is.na(r$uncertainty_m))
  expect_true(is.na(r$lon))
  expect_equal(r$nrcan_title, "Robson Street, Vancouver, British Columbia")
})

test_that("garbage degrades to a populated place and is rejected", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(NA, "ZZZZQQQ")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("geoname")), q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "Geoname")
})

test_that("the right street in the wrong municipality is rejected", {
  skip_if_not_installed("jsonlite")
  # Ranked first, INTERPOLATED_POSITION, and a real address -- 500 km from the
  # one that was asked for. Nothing in the response says so.
  q <- nrcan_parts(1, "NOTRE-DAME", "RUE", "O", "MONTREAL", "QC")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("wrongmun")), q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "^municipality MONTREAL")
})

test_that("a silently substituted street type is rejected", {
  skip_if_not_installed("jsonlite")
  # Toronto has both a Spadina Road and a Spadina Avenue; the service picked
  # the other one and said nothing.
  q <- nrcan_parts(330, "SPADINA", "RD", NA, "TORONTO", "ON")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("wrongtype")), q)

  expect_equal(r$match_method, "none")
  expect_equal(r$nrcan_reject, "street type RD != AVE")
})

test_that("the incorporated form of a municipality still matches", {
  skip_if_not_installed("jsonlite")
  # Same fixture, asked for with the type the service returned: the only thing
  # that must not now reject it is `TORONTO` against `City Of Toronto`.
  q <- nrcan_parts(330, "SPADINA", "AVE", NA, "TORONTO", "ON")
  r <- nar_nrcan_floors(nar_nrcan_top(nrcan_fixture("wrongtype")), q)

  expect_equal(r$match_method, "nrcan")
})

test_that("a municipality that only appears inside the street name is rejected", {
  # The bug the whole-title substring check had: `Brook` is in `Brook Street`,
  # so `CORNER BROOK` appeared to match a title about a different street.
  q <- nrcan_parts(28, "SILVER", "ST", NA, "CORNER BROOK", "NL")
  t <- nrcan_parts(28, "BROOK", "ST", NA, "CORNER BROOK", "NL")

  expect_equal(nar_nrcan_agreement(q, t), "street name SILVER != BROOK")
})

test_that("an absent component cannot contradict, but an absent street name can", {
  # A type the query did not carry is not evidence of disagreement...
  expect_true(is.na(nar_nrcan_agreement(
    nrcan_parts(12, "MAIN", NA, NA, "MONCTON", "NB"),
    nrcan_parts(12, "MAIN", "ST", NA, "MONCTON", "NB"))))
  # ...but the street name and the civic number are what was being asked, so a
  # missing one means nothing was verified rather than nothing disagreed.
  expect_match(nar_nrcan_agreement(
    nrcan_parts(12, "MAIN", "ST", NA, "MONCTON", "NB"),
    nrcan_parts(NA, "MAIN", "ST", NA, "MONCTON", "NB")), "^civic number")
})

test_that("agreement is vectorized and keeps input order", {
  q <- nrcan_parts(c(1, 2, 3), c("MAIN", "MAIN", "MAIN"), mun = "X", prov = "NS")
  t <- nrcan_parts(c(1, 9, 3), c("MAIN", "MAIN", "OAK"), mun = "X", prov = "NS")
  r <- nar_nrcan_agreement(q, t)

  expect_length(r, 3L)
  expect_true(is.na(r[1]))
  expect_match(r[2], "^civic number")
  expect_match(r[3], "^street name")
})

test_that("the floors report the first disagreement, not all of them", {
  # Otherwise the reason string grows with the number of ways an answer is
  # wrong, and the common case reads worse than the rare one.
  q <- nrcan_parts(5, "OAK", "AVE", NA, "PARIS", "ON")
  t <- nrcan_parts(5, "PARIS", "AVE", NA, "HAMILTON", "ON")
  expect_equal(nar_nrcan_agreement(q, t), "street name OAK != PARIS")
})

test_that("`nrcan` is a tier geocode() accepts, and order is preserved", {
  expect_equal(nar_geocode_methods(c("nrcan", "nar")), c("nrcan", "nar"))
  expect_equal(nar_geocode_methods("nrcan"), "nrcan")
  expect_error(nar_geocode_methods("nrcam"), "nrcam")
})

test_that("only the arguments the geolocator declares reach it", {
  # `...` in geocode() has to serve two online services with different
  # vocabularies. min_score is the BC service's and would be an unused-argument
  # error here; rate is understood by both.
  expect_equal(nar_nrcan_dots(list(min_score = 80, rate = 2)), list(rate = 2))
  expect_length(nar_nrcan_dots(list(min_score = 80)), 0L)
  expect_length(nar_nrcan_dots(list()), 0L)
  # The tier supplies these itself, so a caller's copy must not override it.
  expect_length(nar_nrcan_dots(list(crs = 3347, con = 1, geometry = TRUE)), 0L)
})

test_that("the endpoint is the geo.ca host", {
  expect_match(nar_nrcan_url(), "^https://geolocator\\.api\\.geo\\.ca/")
})
