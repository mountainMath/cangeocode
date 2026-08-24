test_that("the release URL is the shapefile, for the release asked for", {
  expect_match(rnf_url("25"), "lrnf000r25a_e\\.zip$")
  expect_match(rnf_url("22"), "lrnf000r22a_e\\.zip$")
})

test_that("the RNF query joins on the match fold and resolves both municipality routes", {
  sql <- rnf_geocode_sql("probe")
  expect_match(sql, "s.MATCH_FOLD = p.match_fold", fixed = TRUE)
  expect_false(grepl("s.NAME_FOLD = p.name_fold", sql, fixed = TRUE))

  # Both routes, and both are needed: MunAlias knows that a mailing city is a
  # census subdivision, and the direct comparison reaches the subdivisions NAR
  # does not carry at all.
  expect_match(sql, "MunAlias", fixed = TRUE)
  expect_match(sql, "s.CSD_FOLD_L = p.mun", fixed = TRUE)

  # No bounds asked for, so no spatial predicate is emitted at all.
  expect_false(grepl("st_intersects", sql, fixed = TRUE))
  expect_false(grepl("st_within", sql, fixed = TRUE))
})

test_that("a `within` restriction filters the segment and the placed point", {
  sql <- rnf_geocode_sql("probe", "POLYGON ((0 0, 1 0, 1 1, 0 0))")
  # The segment test is the prefilter the RTREE index can serve; the point test
  # is the exact one, since a segment can cross the boundary.
  expect_match(sql, "st_intersects(s.geom", fixed = TRUE)
  expect_match(sql, "st_within(nar_xy(x, y)", fixed = TRUE)
})

test_that("uncertainty is flat on a short segment and scales on a long one", {
  expect_equal(rnf_uncertainty_m(100), 95)
  expect_equal(rnf_uncertainty_m(200), 95)
  expect_equal(rnf_uncertainty_m(1000), 350)
})

test_that("the import keeps named, ranged segments and folds the municipality key", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  expect_true(nar_has_rnf(con))
  s <- DBI::dbGetQuery(con, "SELECT * FROM RnfSegments ORDER BY NGD_UID")
  expect_equal(nrow(s), 5)

  # The key is spelled the way NAR's own Streets.MUN_KEY is, and folded, so the
  # two join without a crosswalk.
  expect_equal(s$MUN_KEY_L[s$NGD_UID == "ngd1"], "59:CY:VANCOUVER")
  expect_true(all(DBI::dbGetQuery(con, "
    SELECT MUN_KEY FROM Streets WHERE NAME_FOLD = 'GRANT'")$MUN_KEY %in%
      "59:CY:Vancouver"))

  expect_equal(s$PROV_ABVN_L[s$NGD_UID == "ngd1"], "BC")
  expect_equal(s$TYPE_FOLD[s$NGD_UID == "ngd1"], "ST")
  # 'N/A' and NULL both mean absent; neither may survive as a third value.
  expect_true(all(is.na(s$DIR_FOLD)))
  expect_equal(s$len_m[s$NGD_UID == "ngd1"], 200)

  # The gazetteer covers both sides of every segment, so a street on a boundary
  # belongs to both municipalities.
  g <- DBI::dbGetQuery(con, "SELECT * FROM RnfStreets ORDER BY NAME_FOLD")
  expect_true(all(c("GRANT", "COVE", "DUPLEX", "MARINE") %in% g$NAME_FOLD))
  expect_equal(g$MIN_CIVIC_NO[g$NAME_FOLD == "GRANT"], 100)
  expect_equal(g$MAX_CIVIC_NO[g$NAME_FOLD == "GRANT"], 301)
  expect_equal(g$N_SEGMENTS[g$NAME_FOLD == "DUPLEX"], 4)

  meta <- nar_metadata(con)
  expect_equal(unname(meta["rnf_release"]), "25")
  expect_equal(unname(meta["rnf_segments"]), "5")
})

test_that("the tier places a civic number on the side its parity claims", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  # 150 is even, so it belongs to AFR/ATR = 100..300, which is the right of the
  # west-to-east digitizing direction: 13 m SOUTH of the centreline. The
  # position along the line is the 5% setback applied to the range fraction,
  # 0.05 + 0.90 * 0.25 = 0.275, so 55 m along a 200 m segment.
  even <- geocode("150 Grant St, Vancouver, BC", method = "rnf", con = con,
                  crs = nar_storage_crs())
  expect_equal(even$match_method, "rnf_interpolated")
  expect_equal(even$n_matches, 1L)
  expect_equal(even$uncertainty_m, 95)
  expect_equal(even$lon, 4012055)
  expect_equal(even$lat, 2007010 - 13)

  # 151 is odd, the same fraction along, and 13 m north instead.
  odd <- geocode("151 Grant St, Vancouver, BC", method = "rnf", con = con,
                 crs = nar_storage_crs())
  expect_equal(odd$lon, 4012055)
  expect_equal(odd$lat, 2007010 + 13)
})

test_that("the tier refuses to extrapolate past the range", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  # 500 is past the end of every range on the street. It is not placed at the
  # nearest end of the nearest segment; it is not placed.
  g <- geocode("500 Grant St, Vancouver, BC", method = "rnf", con = con)
  expect_equal(g$match_method, "none")
  expect_true(is.na(g$lon))

})

test_that("a parity mismatch is not by itself a refusal", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  # Marine Dr carries 1..99 on the left and nothing on the right, so 50 is an
  # even number inside an odd range. It is still placed, on the only side that
  # claims it. Parity chooses BETWEEN sides; it does not veto one, because a
  # mismatch is not evidence that the range is wrong -- the road may be drawn as
  # a single generalized centreline where there are two carriageways, or the
  # civic number itself may be misfiled. Refusing here would drop real
  # addresses to avoid a placement that is off by the width of a street.
  m <- geocode("50 Marine Dr, Vancouver, BC", method = "rnf", con = con,
               crs = nar_storage_crs())
  expect_equal(m$match_method, "rnf_interpolated")
  expect_equal(m$lat, 2010000 + 13)
})

test_that("more than one matching segment is refused and reported", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  g <- geocode("50 Duplex St, Vancouver, BC", method = "rnf", con = con)
  expect_equal(g$match_method, "rnf_ambiguous")
  expect_equal(g$n_matches, 2L)
  expect_true(is.na(g$lon))
})

test_that("a census subdivision NAR does not carry still resolves", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  # Bowen Island is nowhere in the NAR fixture, so MunAlias cannot map the name
  # to a key. The direct comparison against RNF's own CSD name is what places
  # this, and streets like it are what the tier exists for.
  g <- geocode("50 Cove Rd, Bowen Island, BC", method = "rnf", con = con,
               crs = nar_storage_crs())
  expect_equal(g$match_method, "rnf_interpolated")
  expect_false(is.na(g$lon))
})

test_that("the tier runs below NAR's own interpolation, and only where it fails", {
  skip_if_no_duckdb_spatial()
  con <- local_rnf_connection()

  # NAR carries 100..300 even on Grant St, so its own neighbours place 150 and
  # the road file is never asked. Its answer is the one on the NAR points, not
  # the one offset from the centreline.
  g <- geocode("150 Grant St, Vancouver, BC",
               method = c("nar", "nar_interpolate", "rnf"), con = con,
               crs = nar_storage_crs())
  expect_equal(g$match_method, "nar_interpolated")
  expect_equal(g$lat, 2007000)

  # 350 is past the end of NAR's run but inside RNF's range, which is the
  # division of labour the ordering exists for.
  h <- geocode("250 Grant St, Vancouver, BC", method = c("nar", "rnf"),
               con = con, crs = nar_storage_crs())
  expect_equal(h$match_method, "rnf_interpolated")
})

test_that("naming the tier without the tables is an error, not a silent miss", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  expect_false(nar_has_rnf(con))
  expect_error(geocode("150 Grant St, Vancouver, BC", method = "rnf", con = con),
               "rnf_import")
})

test_that("a soft 404 is not a release", {
  # StatCan answers a missing release with a 302 to a 4 KB HTML error page
  # served as 200 OK, so the status code alone accepts a release that does not
  # exist and the failure surfaces much later as an unzip error.
  html <- charToRaw(paste0("HTTP/1.1 200 OK\r\n",
                           "Content-Type: text/html\r\n",
                           "Content-Length: 4099\r\n\r\n"))
  zip <- charToRaw(paste0("HTTP/1.1 200 OK\r\n",
                          "Content-Type: application/x-zip-compressed\r\n",
                          "Content-Length: 357592695\r\n\r\n"))
  expect_false(rnf_headers_are_a_zip(html))
  expect_true(rnf_headers_are_a_zip(zip))

  # A zip too small to be a release is also refused, and a zip with no declared
  # length is accepted -- the size floor exists to catch an error page.
  small <- charToRaw("HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\nContent-Length: 900\r\n\r\n")
  nolen <- charToRaw("HTTP/1.1 200 OK\r\nContent-Type: application/zip\r\n\r\n")
  expect_false(rnf_headers_are_a_zip(small))
  expect_true(rnf_headers_are_a_zip(nolen))
})
