# A stand-in for geocode()'s output: one placed row per test the helper knows,
# one row that was never placed, and one clean row that must survive every bar.
accept_fixture <- function() {
  data.frame(
    input = paste0("row", 1:8),
    lon = c(-63.1, -63.2, -63.3, -63.4, -63.5, -63.6, NA, -63.8),
    lat = c(44.1, 44.2, 44.3, 44.4, 44.5, 44.6, NA, 44.8),
    match_method = c("nar_building", "nar_interpolated", "nar_building",
                     "nar_building", "nar_building", "nar_building",
                     "none", "nar_building"),
    uncertainty_m = c(0, 140, 0, 0, 0, 300, NA, 0),
    n_matches = c(1L, 1L, 1L, 3L, 1L, 1L, 0L, 1L),
    confidence = c(1, 1, 0.77, 1, 1, 1, NA, 1),
    POSTAL_CODE = c(NA, NA, NA, NA, "B0N1M0", NA, NA, NA),
    match_postal_code = c(NA, NA, NA, NA, "B4E1A1", NA, NA, NA),
    mun_remapped = c(TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NA, FALSE),
    mun_evidence = c("unattested", "kept", "kept", "kept", "kept", "kept",
                     NA, "copostal"),
    refused_for = c(NA, NA, "score", NA, NA, NA, NA, NA),
    stringsAsFactors = FALSE
  )
}

test_that("each test withdraws the row it is about and no other", {
  g <- accept_fixture()

  expect_equal(geocode_accept(g, attested_only = TRUE)$rejected_for,
               c("unattested_mun", rep(NA_character_, 7)))
  expect_equal(geocode_accept(g, method = "nar_building")$rejected_for,
               c(NA, "method", rep(NA_character_, 6)))
  expect_equal(geocode_accept(g, refused = FALSE)$rejected_for,
               c(NA, NA, "refused", rep(NA_character_, 5)))
  expect_equal(geocode_accept(g, unambiguous = TRUE)$rejected_for,
               c(NA, NA, NA, "ambiguous", rep(NA_character_, 4)))
  expect_equal(geocode_accept(g, postal_code = TRUE)$rejected_for,
               c(rep(NA_character_, 4), "postal_code", NA, NA, NA))
  expect_equal(geocode_accept(g, max_uncertainty = 100)$rejected_for,
               c(NA, "uncertainty", NA, NA, NA, "uncertainty", NA, NA))
  expect_equal(geocode_accept(g, min_confidence = 0.85)$rejected_for,
               c(NA, NA, "confidence", rep(NA_character_, 5)))

  # The last row is clean on every count, and nothing above may touch it.
  expect_true(all(is.na(vapply(
    list(geocode_accept(g, attested_only = TRUE, unambiguous = TRUE,
                        postal_code = TRUE, max_uncertainty = 100,
                        min_confidence = 0.85, refused = FALSE,
                        method = "nar_building")),
    function(a) a$rejected_for[8], character(1)))))
})

test_that("a rejected row loses its coordinates and keeps its evidence", {
  g <- accept_fixture()
  a <- geocode_accept(g, attested_only = TRUE)

  expect_true(is.na(a$lon[1]) && is.na(a$lat[1]))
  # Everything the rejection was argued from survives, so it can be argued with.
  expect_equal(a$match_method[1], "nar_building")
  expect_equal(a$uncertainty_m[1], 0)
  expect_equal(a$mun_evidence[1], "unattested")
  expect_equal(a$lon[-1], g$lon[-1])
})

test_that("a row that was never placed is not a rejection", {
  g <- accept_fixture()
  # Row 7 fails `method` and `unambiguous` on the values it carries, but there
  # is no position to withdraw, so it stays distinguishable from the rows this
  # call turned away.
  a <- geocode_accept(g, method = "nar_building", unambiguous = TRUE)
  expect_true(is.na(a$rejected_for[7]))
})

test_that("a row is charged to the first test it fails", {
  g <- accept_fixture()
  g$n_matches[1] <- 4L
  # attested_only is declared before unambiguous, and row 1 now fails both.
  expect_equal(geocode_accept(g, attested_only = TRUE,
                              unambiguous = TRUE)$rejected_for[1],
               "unattested_mun")
})

test_that("a bar that cannot be evaluated errors rather than passing", {
  g <- accept_fixture()
  expect_error(geocode_accept(g[, setdiff(names(g), "mun_remapped")],
                              attested_only = TRUE),
               "mun_remapped")
  expect_error(geocode_accept(g[, setdiff(names(g), "match_postal_code")],
                              postal_code = TRUE),
               "match_postal_code")
  # refused is the exception: a plain geocode() result has no refusals at all.
  expect_silent(geocode_accept(g[, setdiff(names(g), "refused_for")],
                               refused = FALSE))
})

test_that("an sf result is withdrawn as an empty geometry", {
  skip_if_not_installed("sf")
  g <- accept_fixture()
  s <- sf::st_as_sf(g[!is.na(g$lon), ], coords = c("lon", "lat"), crs = 4326)
  a <- geocode_accept(s, attested_only = TRUE)

  expect_s3_class(a, "sf")
  expect_equal(sf::st_is_empty(sf::st_geometry(a)),
               c(TRUE, rep(FALSE, nrow(s) - 1)))
  expect_equal(a$rejected_for[1], "unattested_mun")
})

test_that("the attested classes are read from the uncertainty floor", {
  g <- accept_fixture()[rep(1, 6), ]
  g$mun_evidence <- names(nar_remap_uncertainty_m())
  # Exactly the classes priced above zero are the ones dropped, so the two
  # cannot drift apart.
  expect_equal(is.na(geocode_accept(g, attested_only = TRUE)$rejected_for),
               nar_remap_uncertainty_m() == 0, ignore_attr = TRUE)
})
