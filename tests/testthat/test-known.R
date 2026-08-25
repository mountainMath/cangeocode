test_that("known refuses a component it does not have", {
  # Naming the valid set in the message is the whole point: a caller who wrote
  # `city =` and got silence would have an unconstrained search reported as a
  # constrained one, which is the failure this argument exists to prevent.
  expect_error(nar_known(list(city = "Toronto"), 1L), "does not take city")
  expect_error(nar_known(list(city = "Toronto"), 1L), "MUN_NAME")
  expect_error(nar_known(list("Toronto"), 1L), "named list")
  expect_error(nar_known(list(MUN_NAME = "A", MUN_NAME = "B"), 1L),
               "more than once")
  expect_error(nar_known(list(MUN_NAME = c("A", "B")), 3L),
               "length 1 or length 3")
})

test_that("known recycles, drops empties, and survives an empty input", {
  k <- nar_known(list(PROV_ABVN = "BC"), 3L)
  expect_equal(k$PROV_ABVN, rep("BC", 3))
  expect_null(nar_known(NULL, 3L))
  expect_null(nar_known(list(PROV_ABVN = character(0)), 3L))
  expect_equal(nrow(nar_known(list(PROV_ABVN = "BC"), 0L)), 0L)

  # A data frame of components is the same thing said another way, which is
  # what lets a caller hand over the columns they already have.
  expect_equal(nar_known(data.frame(PROV_ABVN = c("BC", "ON")), 2L)$PROV_ABVN,
               c("BC", "ON"))
})

test_that("a supplied component is put into the shape the parser produces", {
  k <- nar_known(list(MUN_NAME = "Howie Centre", PROV_ABVN = "British Columbia",
                      POSTAL_CODE = "v6s 1n4", CIVIC_NO = "4001"), 1L)
  # Otherwise the override would be authoritative and unmatchable at once.
  expect_equal(k$MUN_NAME, "HOWIE CENTRE")
  expect_equal(k$PROV_ABVN, "BC")
  expect_equal(k$POSTAL_CODE, "V6S1N4")
  expect_equal(k$CIVIC_NO, 4001)

  # A blank is nothing known about the row, not a component that is blank.
  expect_true(is.na(nar_known(list(MUN_NAME = "   "), 1L)$MUN_NAME))

  # A province code that is not one is passed through rather than refused: it
  # will match nothing, which is the honest answer for a bad code.
  expect_equal(nar_known(list(PROV_ABVN = "ZZ"), 1L)$PROV_ABVN, "ZZ")
})

test_that("an asserted component overrides what the string parsed to", {
  n <- normalize_address("100 Queen St W, Toronto, ON",
                         known = list(PROV_ABVN = "BC", CSD_NAME = "Vancouver"))
  expect_equal(n$PROV_ABVN, "BC")
  expect_equal(n$CSD_NAME, "VANCOUVER")
  # The parsed mailing city contradicted the asserted jurisdiction and comes
  # out rather than staying on to constrain a search it cannot agree with.
  expect_true(is.na(n$MUN_NAME))

  # Both grains supplied is a caller narrowing to one community inside a city,
  # so neither clears the other.
  n <- normalize_address("100 Queen St W, Toronto, ON",
                         known = list(MUN_NAME = "Scarborough",
                                      CSD_NAME = "Toronto"))
  expect_equal(n$MUN_NAME, "SCARBOROUGH")
  expect_equal(n$CSD_NAME, "TORONTO")

  # Per row, and NA means nothing is known about that row.
  n <- normalize_address(rep("100 Queen St W, Toronto, ON", 2),
                         known = list(PROV_ABVN = c("BC", NA)))
  expect_equal(n$PROV_ABVN, c("BC", "ON"))
})

test_that("the offline parse carries the jurisdiction column too", {
  # Present and NA rather than absent, so a frame from normalize_address() has
  # the same shape whether or not a database was open when it was made.
  expect_true("CSD_NAME" %in% names(normalize_address("100 Queen St W")))
})

test_that("an asserted municipality is never priced as a remap", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  n <- normalize_address("4001 King Edward Ave W",
                         known = list(CSD_NAME = "Vancouver"), con = con)
  expect_false(n$mun_remapped)
  expect_equal(n$mun_evidence, "kept")
})

test_that("a reported jurisdiction never becomes a constraint on the next search", {
  # The output CSD_NAME says which census subdivision the match turned out to
  # be in; it does not say the search was restricted to it. Feeding it back in
  # would narrow the second search to something nobody asked for, and
  # geocode(normalize_address(x)) would answer differently from geocode(x) --
  # 5491 Route 11, Brantville NB is the real case, where NAR files that street
  # across three CSD keys and only one of them holds the flanking civics.
  parse <- data.frame(CIVIC_NO = 100, STREET_NAME = "MAIN",
                      CSD_NAME = "Tracadie", parse_source = "gazetteer",
                      stringsAsFactors = FALSE)
  expect_null(nar_known_csd(parse, NULL, from_frame = FALSE))

  # The same frame built by the caller is input, and does constrain.
  expect_equal(nar_known_csd(parse, NULL, from_frame = TRUE), "Tracadie")

  # An assertion outranks the column either way.
  k <- nar_known(list(CSD_NAME = "Toronto"), 1L)
  expect_equal(nar_known_csd(parse, k, from_frame = TRUE), "TORONTO")
})

test_that("a hand-built frame's jurisdiction restricts the search", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  f <- data.frame(CIVIC_NO = 5001, STREET_NAME = "MUSQUEAM",
                  CSD_NAME = "Vancouver", stringsAsFactors = FALSE)
  expect_equal(geocode(f, con = con)$ADDR_GUID, "addr9")

  f$CSD_NAME <- "Toronto"
  expect_equal(geocode(f, con = con)$match_method, "none")

  # And a parse handed straight back is unchanged by the round trip.
  x <- "5001 Musqueam Dr, Vancouver, BC"
  expect_equal(geocode(normalize_address(x, con = con), con = con)$ADDR_GUID,
               geocode(x, con = con)$ADDR_GUID)
})
