# The NRCan geolocator binding. Nothing here touches the network: every test
# runs against a response captured from the live service and saved under
# fixtures/, which is also the only way the floors stay checkable once the
# service changes what it ranks first.
#
# Each fixture is the FULL response -- all 25 results -- because the binding
# reads all of them and the rank of the one that survives is part of what is
# under test. The six are the outcomes that matter, and four of them are wrong
# answers that a naive binding would accept:
#
#   position  -- a real interpolated civic position, ranked first.
#   centroid  -- INTERPOLATED_CENTROID: the street was found, the number was not.
#   geoname   -- garbage in, a populated place out, still ranked first.
#   wrongmun  -- a real Rue Notre-Dame Ouest in Lorrainville ranked first, 500 km
#                from the one asked for. The right one is ranked SEVENTH.
#   wrongtype -- Spadina Road asked for, Spadina Avenue ranked first, 3 km off.
#                The right one is ranked SEVENTH.
#   error     -- `{"message": ...}` under an HTTP 200.
#
# The last two are the reason the whole list is scanned, so they are tested
# twice each: once as the recovery they now are, and once with a query nothing
# in the response can satisfy, which is what keeps the rejection paths honest.

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

# One address, every candidate the service returned for it.
nrcan_floors <- function(name, q) nar_nrcan_floors(nar_nrcan_candidates(
  nrcan_fixture(name)), q)

test_that("every result is read, in the order the service ranked them", {
  skip_if_not_installed("jsonlite")
  r <- nar_nrcan_candidates(nrcan_fixture("position"))

  expect_equal(nrow(r), 25L)
  expect_equal(r$nrcan_kind[1], "Street")
  expect_equal(r$nrcan_qualifier[1], "INTERPOLATED_POSITION")
  expect_equal(r$nrcan_title[1],
               "100 Water Street, Charlottetown, Prince Edward Island")
  expect_equal(round(r$lon[1], 4), -63.1241)
  expect_equal(round(r$lat[1], 4), 46.2325)
  # Rank is positional and nothing may reorder it -- "best" is read from it.
  expect_equal(r$nrcan_title[17],
               "100 Edward Street, Charlottetown, Prince Edward Island")
})

test_that("the fully qualified type is reduced to its leaf", {
  skip_if_not_installed("jsonlite")
  # `ca.gc.nrcan.geoloc.data.model.Street` is what the wire carries.
  expect_equal(nar_nrcan_candidates(nrcan_fixture("geoname"))$nrcan_kind[1],
               "Geoname")
})

test_that("a JSON object body is an absent answer, not a result", {
  skip_if_not_installed("jsonlite")
  # The 500-inside-a-200. An unnamed list is the results array; a named one is
  # this. Reading it as a result would take `message` for a title.
  r <- nar_nrcan_candidates(nrcan_fixture("error"))

  expect_equal(nrow(r), 0L)
  expect_equal(names(r), c("nrcan_kind", "nrcan_qualifier", "nrcan_title",
                           "lon", "lat"))
})

test_that("an empty body is an absent answer too", {
  expect_equal(nrow(nar_nrcan_candidates(list())), 0L)
})

test_that("no answer at all is a row, and says so", {
  q <- nrcan_parts(100, "WATER", "ST", NA, "CHARLOTTETOWN", "PE")
  r <- nar_nrcan_floors(nar_nrcan_candidates(list()), q)

  expect_equal(nrow(r), 1L)
  expect_equal(r$match_method, "none")
  expect_equal(r$nrcan_reject, "no answer")
  expect_equal(r$n_matches, 0L)
  expect_true(is.na(r$nrcan_title))
  expect_true(is.na(r$lon))
})

test_that("an interpolated civic position passes every floor", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(100, "WATER", "ST", NA, "CHARLOTTETOWN", "PE")
  r <- nrcan_floors("position", q)

  expect_equal(r$match_method, "nrcan")
  expect_true(is.na(r$nrcan_reject))
  expect_equal(r$uncertainty_m, nar_nrcan_uncertainty_m())
  expect_equal(round(r$lon, 4), -63.1241)
  # The other five interpolated positions in the response are other streets.
  expect_equal(r$n_matches, 1L)
})

test_that("a street centroid is rejected: the civic number was not resolved", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(1155, "ROBSON", "ST", NA, "VANCOUVER", "BC")
  r <- nrcan_floors("centroid", q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "INTERPOLATED_CENTROID")
  # A rejected row offers no point, so it carries no error bar -- but what was
  # thrown away stays visible.
  expect_true(is.na(r$uncertainty_m))
  expect_true(is.na(r$lon))
  expect_equal(r$n_matches, 0L)
  expect_equal(r$nrcan_title, "Robson Street, Vancouver, British Columbia")
})

test_that("garbage degrades to a populated place and is rejected", {
  skip_if_not_installed("jsonlite")
  q <- nrcan_parts(NA, "ZZZZQQQ")
  r <- nrcan_floors("geoname", q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "Geoname")
})

test_that("the right municipality is found below the wrong one", {
  skip_if_not_installed("jsonlite")
  # Lorrainville is ranked first, INTERPOLATED_POSITION, and a real address --
  # 500 km from the one asked for, with nothing in the response saying so. The
  # Montreal answer is ranked seventh in the same body, and the floor is what
  # tells them apart, so reading only the top result threw it away.
  q <- nrcan_parts(1, "NOTRE-DAME", "RUE", "O", "MONTREAL", "QC")
  r <- nrcan_floors("wrongmun", q)

  expect_equal(r$match_method, "nrcan")
  expect_true(is.na(r$nrcan_reject))
  expect_equal(r$nrcan_title, "1 Rue Notre-Dame Ouest, Montréal, Quebec")
  expect_equal(round(r$lon, 4), -73.5556)
  # Victoriaville and Trois-Pistoles are interpolated positions on the same
  # street name and must not count as alternatives.
  expect_equal(r$n_matches, 1L)
})

test_that("a wrong municipality is still rejected when no result has the right one", {
  skip_if_not_installed("jsonlite")
  # Same 25 candidates, asked for a town none of them is in. The reason names
  # the best-ranked one that got past the class floor.
  q <- nrcan_parts(1, "NOTRE-DAME", "RUE", "O", "QUEBEC", "QC")
  r <- nrcan_floors("wrongmun", q)

  expect_equal(r$match_method, "none")
  expect_match(r$nrcan_reject, "^municipality QUEBEC")
  expect_equal(r$n_matches, 0L)
})

test_that("the right street type is found below a substituted one", {
  skip_if_not_installed("jsonlite")
  # Toronto has both a Spadina Road and a Spadina Avenue; the service ranks the
  # Avenue first and says nothing. The Road is ranked seventh.
  q <- nrcan_parts(330, "SPADINA", "RD", NA, "TORONTO", "ON")
  r <- nrcan_floors("wrongtype", q)

  expect_equal(r$match_method, "nrcan")
  expect_equal(r$nrcan_title, "330 Spadina Road, City Of Toronto, Ontario")
  expect_equal(round(r$lon, 4), -79.4105)
  expect_equal(r$n_matches, 1L)
})

test_that("a silently substituted street type is rejected", {
  skip_if_not_installed("jsonlite")
  # No Spadina Crescent anywhere in the response, so the substitution is all
  # that is on offer and it is refused.
  q <- nrcan_parts(330, "SPADINA", "CRES", NA, "TORONTO", "ON")
  r <- nrcan_floors("wrongtype", q)

  expect_equal(r$match_method, "none")
  expect_equal(r$nrcan_reject, "street type CRES != AVE")
})

test_that("the incorporated form of a municipality still matches", {
  skip_if_not_installed("jsonlite")
  # Same fixture, asked for with the type the service ranked first: the only
  # thing that must not now reject it is `TORONTO` against `City Of Toronto`.
  q <- nrcan_parts(330, "SPADINA", "AVE", NA, "TORONTO", "ON")
  r <- nrcan_floors("wrongtype", q)

  expect_equal(r$match_method, "nrcan")
  expect_equal(round(r$lon, 4), -79.3986)
})

test_that("a lost request is not the same as an empty answer", {
  # Both are zero candidates, and only one of them is about the address. A
  # coverage figure that folds them together understates the tier.
  q <- rbind(nrcan_parts(100, "WATER", "ST", NA, "CHARLOTTETOWN", "PE"),
             nrcan_parts(100, "WATER", "ST", NA, "CHARLOTTETOWN", "PE"))
  r <- nar_nrcan_floors(nar_nrcan_candidates(list()), q, integer(0),
                        failed = c(TRUE, FALSE))

  expect_equal(r$nrcan_reject, c("request failed", "no answer"))
  expect_equal(r$match_method, c("none", "none"))
})

test_that("the service's dropped requests are recognized as transient", {
  skip_if_not_installed("httr2")
  json <- function(status, body) httr2::response(
    status_code = status, headers = list(`content-type` = "application/json"),
    body = charToRaw(body))

  # The measured failure: a clean 500, ~8% of requests, recovers on a re-send.
  expect_true(nar_nrcan_transient(json(500, "")))
  expect_true(nar_nrcan_transient(json(503, "")))
  expect_true(nar_nrcan_transient(json(429, "")))
  # The same error escaping through a gateway that did not label it.
  expect_true(nar_nrcan_transient(
    json(200, '{"message": "Internal server error"}')))

  # An empty array is the service answering "nothing", which is an answer.
  expect_false(nar_nrcan_transient(json(200, "[]")))
  expect_false(nar_nrcan_transient(json(200, '[{"title": "x"}]')))
  # A 404 is not going to become a 200 by asking again.
  expect_false(nar_nrcan_transient(json(404, "")))
})

test_that("candidates are kept with the address they answer", {
  skip_if_not_installed("jsonlite")
  # Two addresses in one call: one that survives at rank 7, one that has no
  # civic position at all. Nothing may leak between them.
  a <- nar_nrcan_candidates(nrcan_fixture("wrongtype"))
  b <- nar_nrcan_candidates(nrcan_fixture("centroid"))
  q <- rbind(nrcan_parts(330, "SPADINA", "RD", NA, "TORONTO", "ON"),
             nrcan_parts(1155, "ROBSON", "ST", NA, "VANCOUVER", "BC"))
  r <- nar_nrcan_floors(rbind(a, b), q,
                        c(rep(1L, nrow(a)), rep(2L, nrow(b))))

  expect_equal(nrow(r), 2L)
  expect_equal(r$match_method, c("nrcan", "none"))
  expect_equal(r$n_matches, c(1L, 0L))
  expect_equal(r$nrcan_title[1], "330 Spadina Road, City Of Toronto, Ontario")
  expect_match(r$nrcan_reject[2], "INTERPOLATED_CENTROID")
})

test_that("a municipality that only appears inside the street name is rejected", {
  # The bug the whole-title substring check had: `Brook` is in `Brook Street`,
  # so `CORNER BROOK` appeared to match a title about a different street.
  q <- nrcan_parts(28, "SILVER", "ST", NA, "CORNER BROOK", "NL")
  t <- nrcan_parts(28, "BROOK", "ST", NA, "CORNER BROOK", "NL")

  expect_equal(nar_address_agreement(q, t), "street name SILVER != BROOK")
})

test_that("an absent component cannot contradict, but an absent street name can", {
  # A type the query did not carry is not evidence of disagreement...
  expect_true(is.na(nar_address_agreement(
    nrcan_parts(12, "MAIN", NA, NA, "MONCTON", "NB"),
    nrcan_parts(12, "MAIN", "ST", NA, "MONCTON", "NB"))))
  # ...but the street name and the civic number are what was being asked, so a
  # missing one means nothing was verified rather than nothing disagreed.
  expect_match(nar_address_agreement(
    nrcan_parts(12, "MAIN", "ST", NA, "MONCTON", "NB"),
    nrcan_parts(NA, "MAIN", "ST", NA, "MONCTON", "NB")), "^civic number")
})

test_that("agreement is vectorized and keeps input order", {
  q <- nrcan_parts(c(1, 2, 3), c("MAIN", "MAIN", "MAIN"), mun = "X", prov = "NS")
  t <- nrcan_parts(c(1, 9, 3), c("MAIN", "MAIN", "OAK"), mun = "X", prov = "NS")
  r <- nar_address_agreement(q, t)

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
  expect_equal(nar_address_agreement(q, t), "street name OAK != PARIS")
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
