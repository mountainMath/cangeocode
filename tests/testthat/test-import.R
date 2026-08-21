test_that("import builds the expected tables, indexes and metadata", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  expect_setequal(DBI::dbListTables(con),
                  c("Addresses", "Locations", "nar_metadata"))
  expect_setequal(DBI::dbGetQuery(con, "SELECT index_name FROM duckdb_indexes()")$index_name,
                  c("add_geom_idx", "add_loc_guid_idx", "loc_geom_idx", "loc_loc_guid_idx"))
  expect_equal(nar_meta_value(con, "schema_version"), "3")
  expect_equal(nar_meta_value(con, "crs"), nar_storage_crs())
  expect_equal(nar_meta_value(con, "lonlat_crs"), nar_lonlat_crs())
  expect_equal(nar_meta_value(con, "version"), "test-01")
  expect_equal(nar_crs(con), nar_storage_crs())
})

test_that("nar_meta_value falls back for databases predating a key", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  expect_equal(nar_meta_value(con, "no_such_key", default = "fallback"), "fallback")
})

test_that("geom prefers the building point and falls back to the blockface", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- DBI::dbGetQuery(con, "SELECT ADDR_GUID, geom_source, x, y,
                                 BF_REPPOINT_X, geom IS NULL AS no_geom
                               FROM Addresses ORDER BY ADDR_GUID")

  expect_equal(got$geom_source, c("building", "blockface", NA))
  expect_equal(got$no_geom, c(FALSE, FALSE, TRUE))
  # addr1 keeps its own building point rather than the blockface one it also has.
  expect_equal(got$x[1], 4012046.46456561)
  # addr2 has only the blockface point.
  expect_equal(got$x[2], got$BF_REPPOINT_X[2])
})

test_that("x/y always mirror geom, since the prefilter trusts them", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  for (table in c("Addresses", "Locations")) {
    mismatched <- DBI::dbGetQuery(con, sprintf(
      "SELECT count(*) n FROM %s
       WHERE x IS DISTINCT FROM st_x(geom) OR y IS DISTINCT FROM st_y(geom)", table))
    expect_equal(mismatched$n, 0, info = table)
  }
})

test_that("geom_source and geom never disagree about whether a point exists", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  disagreeing <- DBI::dbGetQuery(con,
    "SELECT count(*) n FROM Addresses
     WHERE (geom_source IS NULL) IS DISTINCT FROM (geom IS NULL)")

  expect_equal(disagreeing$n, 0)
})

test_that("BG_X/BG_Y are consumed into x/y rather than duplicated", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  fields <- DBI::dbListFields(con, "Addresses")

  expect_false(any(c("BG_X", "BG_Y") %in% fields))
  expect_true(all(c("x", "y", "geom", "geom_source") %in% fields))
  # The blockface columns stay, so a rebuild can revisit the fallback.
  expect_true(all(c("BF_REPPOINT_X", "BF_REPPOINT_Y") %in% fields))
})

test_that("releases without blockface columns still import", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = FALSE)

  got <- DBI::dbGetQuery(con, "SELECT ADDR_GUID, geom_source FROM Addresses
                               ORDER BY ADDR_GUID")

  expect_false("BF_REPPOINT_X" %in% DBI::dbListFields(con, "Addresses"))
  expect_true("geom_source" %in% DBI::dbListFields(con, "Addresses"))
  # Nothing to fall back to, so the addresses without a building point get none.
  expect_equal(got$geom_source, c("building", NA, NA))
})

test_that("Locations geometry is built from NAR lon/lat without transposing axes", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  got <- DBI::dbGetQuery(con, "SELECT LOC_GUID, nar_lon(geom) lon, nar_lat(geom) lat
                               FROM Locations ORDER BY LOC_GUID")

  expect_true(all(is.finite(got$lon)), label = "lon finite (not POINT(inf inf))")
  expect_equal(got$lon, c(-123.1999, -123.1995, -123.2000), tolerance = 1e-6)
  expect_equal(got$lat, c(49.2501, 49.2504, 49.2500), tolerance = 1e-6)
  expect_false(any(c("BG_LATITUDE", "BG_LONGITUDE") %in% DBI::dbListFields(con, "Locations")))
})

test_that("a cached database is reused rather than reimported", {
  skip_if_no_duckdb_spatial()
  exdir <- local_nar_fixture(blockface = TRUE)
  cache <- local_nar_env(exdir)

  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)
  expect_length(list.files(cache, pattern = "\\.duckdb$"), 1)

  # A second call must not report that it is importing again. Asserting outright
  # silence would be wrong: duckdb informs a machine with no ~/.duckdb that it is
  # keeping downloaded extensions in a temp dir, which says nothing about whether
  # we reimported. Match the import messages instead.
  msgs <- testthat::capture_messages(con <- nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)
  expect_false(any(grepl("Downloading|Importing|Indexing|successfully imported", msgs)))
})

test_that("a failed import leaves no cache behind and spares the caller's exdir", {
  skip_if_no_duckdb_spatial()
  bad <- withr::local_tempdir()
  writeLines(c("LOC_GUID,ADDR_GUID,NOPE", "a,b,c"), file.path(bad, "Address_X.csv"))
  writeLines(c("LOC_GUID,BG_LATITUDE,BG_LONGITUDE", "a,49,-123"),
             file.path(bad, "Location_X.csv"))
  cache <- local_nar_env(bad)

  expect_error(suppressMessages(nar_connection(version = "test-01")),
               "missing expected column")

  expect_length(list.files(cache, pattern = "\\.duckdb$"), 0)
  expect_length(list.files(cache, pattern = "building"), 0)
  # nar_exdir belongs to the caller and must survive the failure.
  expect_setequal(list.files(bad), c("Address_X.csv", "Location_X.csv"))
})

test_that("an incomplete cached database is refused, not silently served", {
  skip_if_no_duckdb_spatial()
  exdir <- local_nar_fixture(blockface = TRUE)
  cache <- local_nar_env(exdir)

  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)

  path <- file.path(cache, "test-01.duckdb")
  broken <- DBI::dbConnect(duckdb::duckdb(dbdir = path))
  DBI::dbExecute(broken, "LOAD spatial;")
  DBI::dbExecute(broken, "DROP TABLE Addresses;")
  DBI::dbExecute(broken, "CHECKPOINT;")
  DBI::dbDisconnect(broken, shutdown = TRUE)

  expect_error(nar_connection(version = "test-01"), "incomplete")
})

test_that("a missing cache path is reported before anything else happens", {
  withr::local_envvar(c(NAR_CACHE_PATH = ""))

  expect_error(nar_connection(), "NAR_CACHE_PATH")
})
