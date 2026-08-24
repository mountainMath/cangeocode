test_that("a bare geocode() parks its connection and the next call reuses it", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture())

  expect_null(nar_session_state())

  first <- suppressMessages(geocode("1 Test St, Testville, BC",
                                    version = "test-01"))
  st <- nar_session_state()
  expect_false(is.null(st))
  expect_identical(st$version, "test-01")
  expect_true(DBI::dbIsValid(st$con))

  # Reused, not reopened -- the same handle answers the second call.
  second <- geocode("2 Test St, Testville, BC", version = "test-01")
  expect_identical(nar_session_state()$con, st$con)
  expect_equal(nrow(first), 1L)
  expect_equal(nrow(second), 1L)
})

test_that("reverse_geocode() shares the same parked connection", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture())

  suppressMessages(geocode("1 Test St, Testville, BC", version = "test-01"))
  con <- nar_session_state()$con

  reverse_geocode(c(-123.1999, 49.2501), match_radius = 100, version = "test-01")
  expect_identical(nar_session_state()$con, con)
})

test_that("an explicit con is used and never parked", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection()

  expect_null(nar_session_state())
  geocode("1 Test St, Testville, BC", con = con)
  expect_null(nar_session_state())
})

test_that("close_nar() reports whether it had anything to close", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture())

  expect_false(close_nar())
  suppressMessages(geocode("1 Test St, Testville, BC", version = "test-01"))
  expect_true(close_nar())
  expect_false(close_nar())
  expect_null(nar_session_state())
})

test_that("a connection closed behind the package's back is reopened", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture())

  suppressMessages(geocode("1 Test St, Testville, BC", version = "test-01"))
  DBI::dbDisconnect(nar_session_state()$con)
  # Invalid is indistinguishable from absent, rather than being handed out.
  expect_null(nar_session_state())

  res <- geocode("1 Test St, Testville, BC", version = "test-01")
  expect_equal(nrow(res), 1L)
  expect_true(DBI::dbIsValid(nar_session_state()$con))
})

test_that("open_nar() parks a connection and returns it invisibly", {
  skip_if_no_duckdb_spatial()
  local_nar_env(local_nar_fixture())

  con <- suppressMessages(open_nar(version = "test-01"))
  expect_true(DBI::dbIsValid(con))
  expect_identical(nar_session_state()$con, con)
  # Idempotent: asking again hands back what is already open.
  expect_identical(suppressMessages(open_nar(version = "test-01")), con)
  expect_identical(suppressMessages(open_nar()), con)
})

test_that("version matching reuses \"latest\" but not a different release", {
  st <- list(con = NULL, version = "2026-06",
             path = file.path("nowhere", "2026-06.duckdb"))

  expect_true(nar_session_matches(st, "latest"))
  expect_true(nar_session_matches(st, "2026-06"))

  # Unresolvable without a cache, which must read as "no match" rather than
  # falling through to the parked database.
  withr::local_envvar(c(NAR_CACHE_PATH = ""))
  expect_false(nar_session_matches(st, "2025-12"))
})

test_that("an import closes the session connection before taking the lock", {
  skip_if_no_duckdb_spatial()
  cache <- local_nar_env(local_nar_fixture())

  suppressMessages(geocode("1 Test St, Testville, BC", version = "test-01"))
  path <- nar_session_state()$path
  expect_identical(path, file.path(cache, "test-01.duckdb"))

  expect_message(nar_session_release(path), "Closing the session NAR connection")
  expect_null(nar_session_state())

  # Nothing parked, and a path that is not the parked one, are both no-ops.
  expect_false(nar_session_release(path))
})
