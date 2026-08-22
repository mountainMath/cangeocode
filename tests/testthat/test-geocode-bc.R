# The BC Address Geocoder binding. Nothing here touches the network: every test
# runs against a response captured from the live service and saved under
# fixtures/, which is also the only way the parser's contract stays checkable
# once the service changes its scores.

bc_fixture <- function(name) {
  path <- testthat::test_path("fixtures", paste0("bc-", name, ".json"))
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

test_that("a clean civic match is reported as one", {
  skip_if_not_installed("jsonlite")
  r <- nar_bc_feature(bc_fixture("civic"))

  expect_equal(r$match_method, "bc_civic")
  expect_equal(r$bc_score, 100L)
  expect_equal(r$bc_precision, "CIVIC_NUMBER")
  expect_equal(r$bc_address, "525 Superior St, Victoria, BC")
  expect_true(is.na(r$bc_faults))
  expect_equal(round(r$lon, 4), -123.3709)
  expect_equal(round(r$lat, 4), 48.4177)
})

test_that("a civic number the service could not place degrades to the street", {
  skip_if_not_installed("jsonlite")
  r <- nar_bc_feature(bc_fixture("street"))

  expect_equal(r$match_method, "bc_street")
  expect_equal(r$bc_precision, "STREET")
  expect_equal(r$bc_faults, "CIVIC_NUMBER notInAnyBlock")
  # Coarse, and priced as such: this is a point on Superior St, not 999 of it.
  expect_equal(r$uncertainty_m, 500)
})

test_that("garbage still returns a point, and is still rejected", {
  skip_if_not_installed("jsonlite")
  # The whole reason the binding cannot treat a response as a match: the
  # service answers with the centre of Victoria rather than nothing at all.
  r <- nar_bc_feature(bc_fixture("locality"))

  expect_false(is.na(r$lon))
  expect_equal(r$bc_precision, "LOCALITY")
  expect_equal(r$bc_score, 48L)
  # The score floor is what rejects it -- LOCALITY is a real precision, worth
  # 5 km, and would be reported as such had the service been confident.
  expect_equal(r$match_method, "none")
  expect_true(is.na(r$uncertainty_m))
  expect_equal(nar_bc_precision("LOCALITY")$match_method, "bc_locality")
  expect_equal(nar_bc_precision("LOCALITY")$uncertainty_m, 5000)
})

test_that("a misspelt address still resolves, with the faults reported", {
  skip_if_not_installed("jsonlite")
  r <- nar_bc_feature(bc_fixture("typo"))

  expect_equal(r$match_method, "bc_civic")
  expect_equal(r$bc_score, 87L)
  # Same coordinates as the correctly spelt version.
  expect_equal(round(r$lon, 4), -123.3709)
  expect_match(r$bc_faults, "STREET_NAME spelledWrong")
  expect_match(r$bc_faults, "LOCALITY spelledWrong")
})

test_that("min_score rejects a match the service scored poorly", {
  skip_if_not_installed("jsonlite")
  fx <- bc_fixture("typo")

  expect_equal(nar_bc_feature(fx, min_score = 90)$match_method, "none")
  # The score and precision survive the rejection, so the caller can see what
  # was thrown away and why.
  expect_equal(nar_bc_feature(fx, min_score = 90)$bc_score, 87L)
  expect_equal(nar_bc_feature(fx, min_score = 80)$match_method, "bc_civic")
})

test_that("an empty response is a non-match, not an error", {
  expect_equal(nar_bc_feature(list(features = list()))$match_method, "none")
  expect_true(is.na(nar_bc_feature(list())$lon))
})

test_that("an unknown precision is not silently trusted", {
  expect_equal(nar_bc_precision("SOMETHING_NEW")$match_method, "none")
  expect_true(is.na(nar_bc_precision(NULL)$uncertainty_m))
  # Case-insensitive, since the vocabulary is documented in upper case but the
  # response is not a contract.
  expect_equal(nar_bc_precision("civic_number")$match_method, "bc_civic")
})

test_that("the query string is rebuilt from the components, not the input", {
  res <- data.frame(
    input = c("junk that must not be sent", "b"),
    CIVIC_NO = c("990", "12"), CIVIC_NO_SUFFIX = c("A", NA),
    STREET_NAME = c("GEORGIA", "MAIN"), STREET_TYPE = c("ST", "ST"),
    STREET_DIR = c("W", NA), MUN_NAME = c("VANCOUVER", "VICTORIA"),
    PROV_ABVN = c("BC", "BC"), stringsAsFactors = FALSE)

  s <- nar_bc_address_string(res)
  expect_equal(s[1], "990A GEORGIA ST W, VANCOUVER, BC")
  expect_equal(s[2], "12 MAIN ST, VICTORIA, BC")
  expect_false(any(grepl("junk", s)))
})

test_that("components the caller never supplied are simply absent", {
  res <- data.frame(CIVIC_NO = "12", STREET_NAME = "MAIN",
                    MUN_NAME = "VICTORIA", PROV_ABVN = "BC",
                    stringsAsFactors = FALSE)
  expect_equal(nar_bc_address_string(res), "12 MAIN, VICTORIA, BC")
})

test_that("the fallback only sends the BC rows that failed", {
  res <- data.frame(CIVIC_NO = c("1", "2", "3"),
                    STREET_NAME = c("A", "B", "C"),
                    MUN_NAME = c("VICTORIA", "TORONTO", "VANCOUVER"),
                    PROV_ABVN = c("BC", "ON", "BC"), stringsAsFactors = FALSE)
  hits <- data.frame(ADDR_GUID = NA_character_,
                     match_method = c("nar_building", "none", "none"),
                     uncertainty_m = c(0, NA, NA), n_matches = c(1L, 0L, 0L),
                     x = c(1, NA, NA), y = c(1, NA, NA))

  sent <- NULL
  out <- local({
    local_mocked_bindings(bc_geocode = function(x, ...) {
      sent <<- x
      sf::st_sf(data.frame(match_method = "bc_civic", uncertainty_m = 20,
                           bc_score = 100L),
                geometry = sf::st_sfc(sf::st_point(c(4012345, 2007890)),
                                      crs = 3347))
    })
    nar_geocode_bc_fallback(res, hits, con = NULL, bounds = NULL)
  })

  # Row 1 succeeded and row 2 is in Ontario, so exactly one address is sent.
  expect_equal(sent, "3 C, VANCOUVER, BC")
  expect_equal(out$match_method, c("nar_building", "none", "bc_civic"))
  expect_equal(out$x, c(1, NA, 4012345))
  expect_equal(out$uncertainty_m, c(0, NA, 20))
  # Row 1 is untouched by the fallback.
  expect_equal(out$n_matches, c(1L, 0L, 1L))
})

test_that("a fallback point outside `within` is discarded", {
  res <- data.frame(CIVIC_NO = "3", STREET_NAME = "C", MUN_NAME = "VANCOUVER",
                    PROV_ABVN = "BC", stringsAsFactors = FALSE)
  hits <- data.frame(ADDR_GUID = NA_character_, match_method = "none",
                     uncertainty_m = NA_real_, n_matches = 0L,
                     x = NA_real_, y = NA_real_)
  box <- sf::st_as_sfc(sf::st_bbox(c(xmin = 4000000, ymin = 2000000,
                                     xmax = 4001000, ymax = 2001000),
                                   crs = 3347))

  out <- local({
    local_mocked_bindings(bc_geocode = function(x, ...) {
      sf::st_sf(data.frame(match_method = "bc_civic", uncertainty_m = 20),
                geometry = sf::st_sfc(sf::st_point(c(4012345, 2007890)),
                                      crs = 3347))
    })
    nar_geocode_bc_fallback(res, hits, con = NULL, bounds = box)
  })

  # `within` is authoritative for every tier, including the one that runs
  # outside the database.
  expect_equal(out$match_method, "none")
  expect_true(is.na(out$x))
})

test_that("bc_validate skips the rows the service does not cover", {
  g <- data.frame(input = c("a", "b"), PROV_ABVN = c("ON", "AB"),
                  lon = c(-79, -114), lat = c(43, 51),
                  stringsAsFactors = FALSE)
  out <- bc_validate(g)

  expect_equal(nrow(out), 2)
  expect_true(all(is.na(out$bc_dist_m)))
  expect_true(all(is.na(out$bc_match_method)))
})

test_that("bc_validate measures the separation in metres", {
  g <- data.frame(input = "525 Superior St, Victoria, BC", PROV_ABVN = "BC",
                  lon = -123.3709161, lat = 48.4177006,
                  stringsAsFactors = FALSE)

  out <- local({
    local_mocked_bindings(bc_geocode = function(x, ...) {
      sf::st_sf(data.frame(match_method = "bc_civic", uncertainty_m = 20,
                           bc_score = 100L, bc_precision = "CIVIC_NUMBER"),
                geometry = sf::st_sfc(sf::st_point(c(-123.3709161, 48.4187006)),
                                      crs = 4326))
    })
    bc_validate(g)
  })

  expect_equal(out$bc_precision, "CIVIC_NUMBER")
  # 0.001 degrees of latitude is about 111 m.
  expect_equal(round(out$bc_dist_m), 111)
})

test_that("bc_validate needs coordinates to compare against", {
  g <- data.frame(input = "a", PROV_ABVN = "BC", stringsAsFactors = FALSE)
  expect_error(bc_validate(g), "no coordinates")
})
