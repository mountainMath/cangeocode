test_that("package CRS constants are the ones the schema depends on", {
  expect_equal(nar_storage_crs(), "EPSG:3347")
  # NAD83, not OGC:CRS84 -- established from NAR's own BG_X/BG_Y, see issue #4.
  expect_equal(nar_lonlat_crs(), "EPSG:4269")
  expect_equal(nar_schema_version(), 5L)
})

test_that("nar_project accepts the three supported input shapes identically", {
  lonlat <- c(-123.1999, 49.2501)
  bare <- nar_project(lonlat)
  point <- nar_project(sf::st_sfc(sf::st_point(lonlat), crs = 4326))
  frame <- nar_project(sf::st_sf(a = 1, geometry = sf::st_sfc(sf::st_point(lonlat), crs = 4326)))

  expect_equal(point, bare)
  expect_equal(frame, bare)
})

test_that("nar_project returns bare unnamed storage coordinates", {
  xy <- nar_project(c(-123.1999, 49.2501))

  expect_type(xy, "double")
  expect_length(xy, 2)
  # Names would leak into dbplyr as `nar_xy(... AS X)` and fail to parse.
  expect_null(names(xy))
  # Statistics Canada Lambert metres, not degrees.
  expect_gt(xy[1], 3e6)
  expect_gt(xy[2], 1e6)
})

test_that("nar_project reprojects from the CRS the object carries, not the argument", {
  lonlat <- c(-123.1999, 49.2501)
  target <- nar_project(lonlat, crs = 4326)
  carried <- sf::st_sfc(sf::st_point(lonlat), crs = 4326)

  # `crs` is ignored when the object states its own.
  expect_equal(nar_project(carried, crs = 3347), target)
  # ... and used when it does not.
  expect_equal(nar_project(sf::st_sfc(sf::st_point(lonlat)), crs = 4326), target)
})

test_that("nar_project is a no-op for coordinates already in the storage CRS", {
  xy <- c(4012046.46456561, 2006868.65510961)

  expect_equal(nar_project(xy, crs = 3347), xy)
})

test_that("nar_project does not silently transpose lon/lat", {
  # EPSG:4269 declares its axes lat/lon. Reading -123 as a latitude yields
  # POINT (inf inf) rather than an error, so guard the result is finite and
  # actually lands in British Columbia.
  xy <- nar_project(c(-123.1999, 49.2501), crs = "EPSG:4269")

  expect_true(all(is.finite(xy)))
  back <- sf::st_coordinates(sf::st_transform(
    sf::st_sfc(sf::st_point(xy), crs = 3347), 4326))
  expect_equal(as.numeric(back[1, 1]), -123.1999, tolerance = 1e-6)
  expect_equal(as.numeric(back[1, 2]), 49.2501, tolerance = 1e-6)
})

test_that("nar_project rejects input it cannot interpret", {
  expect_error(nar_project(c(1, 2, 3)), "length-2 numeric")
  expect_error(nar_project("here"), "length-2 numeric")
  expect_error(nar_project(c(NA_real_, 1)), "length-2 numeric")
  expect_error(
    nar_project(sf::st_sfc(sf::st_point(c(0, 1)), sf::st_point(c(2, 3)), crs = 4326)),
    "exactly one point"
  )
})

test_that("nar_has_xy detects the prefilter columns", {
  expect_true(nar_has_xy(data.frame(x = 1, y = 2, geom = 3)))
  expect_false(nar_has_xy(data.frame(x = 1, geom = 3)))
  expect_false(nar_has_xy(data.frame(geom = 3)))
})

test_that("nar_crs_string renders every CRS spelling for DuckDB", {
  # A bare number is what sf and reverse_geocode() accept, but DuckDB's binder
  # rejects "4326" outright, so it has to become an authority string.
  expect_equal(nar_crs_string(4326), "EPSG:4326")
  expect_equal(nar_crs_string(3347L), "EPSG:3347")
  expect_equal(nar_crs_string("EPSG:4269"), "EPSG:4269")
  expect_equal(nar_crs_string(sf::st_crs(4326)), "EPSG:4326")
  expect_equal(nar_crs_string(nar_storage_crs()), "EPSG:3347")
})

test_that("nar_crs_string falls back to a full definition without an EPSG code", {
  wkt <- nar_crs_string("+proj=laea +lat_0=45 +lon_0=-100 +datum=WGS84")

  expect_type(wkt, "character")
  expect_match(wkt, "laea|Lambert|PROJCRS|CONVERSION")
})

test_that("nar_crs_string rejects a CRS it cannot interpret", {
  # sf raises on an unparseable definition ...
  expect_error(nar_crs_string("not a crs"), "invalid crs")
  # ... and returns an absent CRS for a missing one, which is ours to catch.
  expect_error(nar_crs_string(NA_character_), "Could not interpret crs")
})
