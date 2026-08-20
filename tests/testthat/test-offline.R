# The package must not need StatCan to open a database it already downloaded.
offline <- function(...) {
  stop("Could not resolve host: www150.statcan.gc.ca")
}

test_that("a cached version key opens without touching the network", {
  skip_if_no_duckdb_spatial()
  cache <- local_nar_env(local_nar_fixture(blockface = TRUE))
  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)

  local_mocked_bindings(available_nar_versions = offline, .package = "cangeocode")

  expect_no_error(con <- nar_connection(version = "test-01"))
  expect_equal(nar_meta_value(con, "version"), "test-01")
  DBI::dbDisconnect(con)
})

test_that("'latest' falls back to the newest cached database when offline", {
  skip_if_no_duckdb_spatial()
  cache <- local_nar_env(local_nar_fixture(blockface = TRUE))
  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)
  # A second, older cached database, so "newest" is a real choice.
  file.copy(file.path(cache, "test-01.duckdb"), file.path(cache, "test-00.duckdb"))

  local_mocked_bindings(available_nar_versions = offline, .package = "cangeocode")

  expect_warning(con <- nar_connection(), "newest cached version, test-01")
  DBI::dbDisconnect(con)
})

test_that("an empty cache offline reports the connection failure itself", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture(blockface = TRUE))
  local_mocked_bindings(available_nar_versions = offline, .package = "cangeocode")

  # Nothing cached, so there is nothing to fall back to and the real cause
  # must survive rather than being reported as an invalid version.
  expect_error(nar_connection(), "Could not resolve host")
})

test_that("refresh always resolves against StatCan", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture(blockface = TRUE))
  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)

  local_mocked_bindings(available_nar_versions = offline, .package = "cangeocode")

  # A rebuild needs the download URL, so the cache cannot answer for it.
  expect_error(nar_connection(version = "test-01", refresh = TRUE),
               "Could not resolve host")
})

test_that("nar_cached_versions lists cached databases newest first", {
  cache <- withr::local_tempdir()

  expect_equal(nar_cached_versions(cache), character(0))

  file.create(file.path(cache, c("2024-05.duckdb", "2026-06.duckdb", "2025-12.duckdb")))
  file.create(file.path(cache, "notes.txt"))

  expect_equal(nar_cached_versions(cache), c("2026-06", "2025-12", "2024-05"))
})
