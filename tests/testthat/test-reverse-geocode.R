# Fixture geometry, in the storage CRS:
#   addr1 building  (4012046.46, 2006868.66)
#   addr2 blockface (4012086.46, 2006838.66)  -- 50 m from addr1
#   addr3 no geometry
addr1_xy <- c(4012046.46456561, 2006868.65510961)

test_that("reverse_geocode finds matches within the radius and none outside", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  near <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 10)
  both <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)

  expect_equal(near$ADDR_GUID, "addr1")
  expect_equal(both$ADDR_GUID, c("addr1", "addr2"))
  # addr3 has no geometry and can never match.
  expect_false("addr3" %in% both$ADDR_GUID)
})

test_that("results are sorted by distance", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  # Sorting happens in R: DuckDB drops ORDER BY in subqueries without LIMIT.
  got <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)

  expect_false(is.unsorted(got$dist))
  expect_equal(round(got$dist), c(0, 50))
})

test_that("matches carry the source of the point they were measured from", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  got <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)

  expect_equal(got$geom_source, c("building", "blockface"))
})

test_that("the internal x/y columns do not leak to the caller", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  plain <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)
  spatial <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, geometry = TRUE)

  # They duplicate geom and would go stale under reprojection.
  expect_false(any(c("x", "y") %in% names(plain)))
  expect_false(any(c("x", "y") %in% names(spatial)))
})

test_that("output types return the documented shapes", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  address <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, output = "address")
  components <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, output = "components")
  multiple <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, output = "multiple")

  expect_type(address, "character")
  expect_length(address, 1)
  expect_match(address, "KING EDWARD")
  expect_equal(nrow(components), 1)
  expect_equal(components$ADDR_GUID, "addr1")
  expect_equal(nrow(multiple), 2)
})

test_that("geometry = TRUE returns sf in the storage CRS at the matched point", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  got <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 10, geometry = TRUE)

  expect_s3_class(got, "sf")
  expect_equal(sf::st_crs(got), sf::st_crs(nar_storage_crs()))
  coords <- sf::st_coordinates(got)
  expect_equal(as.numeric(coords[1, 1:2]), addr1_xy, tolerance = 1e-6)
  # The geometry must agree with the distance reported alongside it.
  expect_equal(got$dist[1], 0, tolerance = 1e-6)
})

test_that("lon/lat input reaches the same rows as storage-CRS input", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  lonlat <- sf::st_coordinates(sf::st_transform(
    sf::st_sfc(sf::st_point(addr1_xy), crs = 3347), 4326))[1, 1:2]

  from_lonlat <- reverse_geocode(as.numeric(lonlat), match_radius = 60)
  from_storage <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)

  expect_equal(from_lonlat$ADDR_GUID, from_storage$ADDR_GUID)
})

test_that("an sf point is accepted as readily as a numeric pair", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  point <- sf::st_sfc(sf::st_point(addr1_xy), crs = 3347)

  expect_equal(reverse_geocode(point, match_radius = 60)$ADDR_GUID,
               reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)$ADDR_GUID)
})

test_that("no match warns and returns NULL", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  expect_warning(got <- reverse_geocode(addr1_xy + 1e5, crs = 3347, match_radius = 100),
                 "No address found within 100 m")
  expect_null(got)

  expect_warning(reverse_geocode(addr1_xy + 1e5, crs = 3347, match_radius = 2000),
                 "2 km")
})

test_that("unsupported arguments are rejected", {
  expect_error(reverse_geocode(c(0, 0), source = "google"), "arg")
  expect_error(reverse_geocode(c(0, 0), output = "everything"), "arg")
})

test_that("a supplied connection is reused and left open", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, con = con)

  expect_equal(got$ADDR_GUID, c("addr1", "addr2"))
  # The caller owns the connection, so it must survive the call.
  expect_true(DBI::dbIsValid(con))
  expect_equal(reverse_geocode(addr1_xy, crs = 3347, match_radius = 10, con = con)$ADDR_GUID,
               "addr1")
})

test_that("a supplied connection means no version lookup at all", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)
  local_mocked_bindings(available_nar_versions = function(...) stop("network used"),
                        .package = "cangeocode")

  expect_no_error(reverse_geocode(addr1_xy, crs = 3347, match_radius = 60, con = con))
})

test_that("address strings are assembled the same way for every row", {
  skip_if_no_duckdb_spatial()
  local_nar_connection(blockface = TRUE)

  got <- reverse_geocode(addr1_xy, crs = 3347, match_radius = 60)

  # No apartment label in the fixture, and every street component present.
  # The direction is placed before the street name, as it always has been.
  expect_equal(got$address,
               c("4001 W KING EDWARD AVE, VANCOUVER V6S1N3",
                 "4002 W KING EDWARD AVE, VANCOUVER V6S1N3"))
  expect_equal(which(names(got) == "address"), which(names(got) == "ADDR_GUID") + 1)
})

test_that("nar_paste_parts matches paste(na.omit(...), collapse = ' ')", {
  reference <- function(...) {
    parts <- lapply(list(...), as.character)
    vapply(seq_along(parts[[1]]), function(i) {
      row <- vapply(parts, `[`, character(1), i)
      paste(row[!is.na(row)], collapse = " ")
    }, character(1))
  }

  a <- c("1", NA, NA, "4", NA)
  b <- c("A", "B", NA, NA, NA)
  d <- c("N", NA, "S", "E", NA)

  expect_equal(nar_paste_parts(a, b, d), reference(a, b, d))
  # Every part missing collapses to an empty string, not NA.
  expect_equal(nar_paste_parts(NA_character_, NA_character_), "")
  # Interior spacing inside a part is preserved rather than reflowed.
  expect_equal(nar_paste_parts("ST  JOHN", "RD"), "ST  JOHN RD")
  # Numeric components are coerced, as the civic number is.
  expect_equal(nar_paste_parts(4001L, "MAIN"), "4001 MAIN")
})
