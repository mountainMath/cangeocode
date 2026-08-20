addr1_xy <- c(4012046.46456561, 2006868.65510961)

test_that("collect_nar returns sf tagged with the database's own CRS", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Addresses") |> collect_nar()

  expect_s3_class(got, "sf")
  # Read from nar_metadata rather than assumed.
  expect_equal(sf::st_crs(got), sf::st_crs(nar_crs(con)))
  expect_equal(nrow(got), 3)
})

test_that("collect_nar drops the internal prefilter columns", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Addresses") |> collect_nar()

  expect_false(any(c("x", "y") %in% names(got)))
  expect_true("geom_source" %in% names(got))
})

test_that("rows without geometry survive as empty points", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Addresses") |> collect_nar()
  empty <- sf::st_is_empty(sf::st_geometry(got))

  # addr3 has neither a building nor a blockface point.
  expect_equal(got$ADDR_GUID[empty], "addr3")
  expect_equal(sum(empty), 1)
})

test_that("collect_nar(crs=) reprojects without transposing the axes", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Locations") |> collect_nar(crs = 4326)
  coords <- sf::st_coordinates(got)

  expect_equal(sf::st_crs(got), sf::st_crs(4326))
  # EPSG:4326 declares its axes lat/lon; without always_xy these come back
  # swapped, putting longitude in the 49 range and latitude at -123.
  expect_true(all(coords[, 1] < -100), label = "longitude in column 1")
  expect_true(all(coords[, 2] > 40 & coords[, 2] < 60), label = "latitude in column 2")
  expect_equal(as.numeric(coords[1, 1]), -123.1999, tolerance = 1e-5)
})

test_that("collect_nar round-trips storage coordinates unchanged", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Addresses") |>
    dplyr::filter(.data$ADDR_GUID == "addr1") |> collect_nar()

  expect_equal(as.numeric(sf::st_coordinates(got)[1, 1:2]), addr1_xy, tolerance = 1e-6)
})

test_that("nar_within_radius keeps exactly the rows inside the radius", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)
  addresses <- con |> dplyr::tbl("Addresses")

  # addr1 and addr2 are 50 m apart.
  at <- function(radius) {
    addresses |> nar_within_radius(addr1_xy[1], addr1_xy[2], radius) |>
      dplyr::pull("ADDR_GUID") |> sort()
  }

  expect_equal(at(10), "addr1")
  expect_equal(at(49), "addr1")
  expect_equal(at(51), c("addr1", "addr2"))
})

test_that("the bounding-box prefilter does not change the rows returned", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)
  addresses <- con |> dplyr::tbl("Addresses")

  # The prefilter is a zonemap optimisation gated on x/y being present; with the
  # columns dropped the same query must still return the same rows.
  with_prefilter <- addresses |>
    nar_within_radius(addr1_xy[1], addr1_xy[2], 60) |>
    dplyr::select(-"x", -"y", -"geom") |> dplyr::collect()
  without_prefilter <- addresses |> dplyr::select(-"x", -"y") |>
    nar_within_radius(addr1_xy[1], addr1_xy[2], 60) |>
    dplyr::select(-"geom") |> dplyr::collect()

  expect_equal(nrow(with_prefilter), 2)
  expect_equal(dplyr::arrange(with_prefilter, .data$ADDR_GUID),
               dplyr::arrange(without_prefilter, .data$ADDR_GUID))
})

test_that("nar_within_radius reports distance in metres", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- con |> dplyr::tbl("Addresses") |>
    nar_within_radius(addr1_xy[1], addr1_xy[2], 60) |>
    dplyr::select("ADDR_GUID", "dist") |> dplyr::collect() |>
    dplyr::arrange(.data$dist)

  # The storage CRS is projected, so no conversion is needed anywhere.
  expect_equal(got$dist, c(0, 50), tolerance = 1e-6)
})

test_that("collect_nar accepts a CRS however the caller spells it", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)
  locations <- con |> dplyr::tbl("Locations")

  numeric_crs <- locations |> collect_nar(crs = 4326)
  string_crs <- locations |> collect_nar(crs = "EPSG:4326")
  object_crs <- locations |> collect_nar(crs = sf::st_crs(4326))

  expect_equal(sf::st_coordinates(numeric_crs), sf::st_coordinates(string_crs))
  expect_equal(sf::st_coordinates(numeric_crs), sf::st_coordinates(object_crs))
})

test_that("collect_nar refuses a table it cannot reproject", {
  # This used to fail with an opaque dplyr error about nar_wkb(), and a `crs`
  # argument was silently discarded.
  local <- sf::st_sf(a = 1, geom = sf::st_sfc(sf::st_point(c(0, 1)), crs = 3347))

  expect_error(collect_nar(local), "needs a lazy table")
  expect_error(collect_nar(local, crs = 4326), "needs a lazy table")
})
