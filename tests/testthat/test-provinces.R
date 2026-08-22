# The province vocabulary, the coverage bookkeeping, and the partial import.

test_that("provinces are recognized by abbreviation, code or name", {
  expect_equal(nar_normalize_provinces("bc"), "BC")
  expect_equal(nar_normalize_provinces(59), "BC")
  expect_equal(nar_normalize_provinces("59"), "BC")
  expect_equal(nar_normalize_provinces("british columbia"), "BC")
  expect_equal(nar_normalize_provinces(c("BC", "48")), c("BC", "AB"))
  # A leading zero is how the SGC writes the Atlantic codes and how the member
  # files are named, so both spellings have to land on the same province.
  expect_equal(nar_normalize_provinces("10"), "NL")
  expect_null(nar_normalize_provinces(NULL))
  expect_null(nar_normalize_provinces(character(0)))
})

test_that("the whole country has several names and cannot be mixed", {
  for (x in c("all", "ALL", "national", "Canada", "ca")) {
    expect_equal(nar_normalize_provinces(x), nar_all_provinces())
  }
  expect_error(nar_normalize_provinces(c("all", "BC")), "whole country")
})

test_that("an unrecognized province is an error naming the alternatives", {
  # Silently dropping it would produce a database quietly missing the data the
  # caller asked for, and the mistake would only surface as unmatched rows.
  expect_error(nar_normalize_provinces("XX"), "Unrecognized province: XX")
  expect_error(nar_normalize_provinces("XX"), "two-letter abbreviation")
})

test_that("member file names map to provinces, and other members to NA", {
  expect_equal(nar_zip_member_province("NAR/Address_59.csv"), "BC")
  expect_equal(nar_zip_member_province("NAR/Location_24.csv"), "QC")
  # Ontario is large enough that StatCan splits it across several members.
  expect_equal(nar_zip_member_province("NAR/Address_35_part_2.csv"), "ON")
  # Anything the pattern cannot place is shared and travels with every subset.
  expect_equal(nar_zip_member_province("NAR/NAR_User_Guide_EN.pdf"), NA_character_)
  expect_equal(nar_zip_member_province("NAR/"), NA_character_)
})

test_that("coverage sets answer whether anything still has to be downloaded", {
  expect_true(nar_covers_set(nar_all_provinces(), "BC"))
  expect_true(nar_covers_set(nar_all_provinces(), nar_all_provinces()))
  expect_true(nar_covers_set(c("BC", "AB"), "BC"))
  expect_true(nar_covers_set(c("BC", "AB"), c("AB", "BC")))
  expect_false(nar_covers_set(c("BC", "AB"), c("BC", "ON")))
  # A province is not the country, however many of them there are.
  expect_false(nar_covers_set(c("BC", "AB"), nar_all_provinces()))
})

test_that("a national database is planned, prompted for and labelled", {
  # Nothing cached and nothing asked for: the decision is deferred to the
  # caller, which is what the NULL means.
  plan <- suppressMessages(nar_import_plan(tempfile(fileext = ".duckdb"), NULL, FALSE))
  expect_null(plan$fetch)
  expect_false(plan$append)

  plan <- suppressMessages(nar_import_plan(tempfile(fileext = ".duckdb"), "BC", FALSE))
  expect_equal(plan$fetch, "BC")
  expect_false(plan$append)

  expect_equal(nar_coverage_label(nar_all_provinces()), "all provinces")
  expect_equal(nar_coverage_label(c("ON", "BC")), "BC, ON")
})

test_that("a partial import loads only the provinces asked for", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))

  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  withr::defer(DBI::dbDisconnect(con))

  expect_equal(nar_provinces(con), "BC")
  expect_equal(nar_meta_value(con, "provinces", NA), "BC")
  expect_equal(
    tbl(con, "Addresses") |> dplyr::distinct(.data$MAIL_PROV_ABVN) |>
      dplyr::pull(.data$MAIL_PROV_ABVN),
    "BC")
  expect_equal(tbl(con, "Addresses") |> dplyr::count() |> dplyr::pull(.data$n), 3)
  expect_equal(tbl(con, "Locations") |> dplyr::count() |> dplyr::pull(.data$n), 3)
  # The gazetteer is built from whatever was loaded, so it is partial too.
  expect_equal(
    tbl(con, "Streets") |> dplyr::distinct(.data$MAIL_PROV_ABVN) |>
      dplyr::pull(.data$MAIL_PROV_ABVN),
    "BC")
})

test_that("asking for a province a partial database already holds downloads nothing", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  DBI::dbDisconnect(con)

  cache <- Sys.getenv("NAR_CACHE_PATH")
  nar_path <- file.path(cache, "test-01.duckdb")
  before <- file.mtime(nar_path)

  plan <- suppressMessages(nar_import_plan(nar_path, "BC", FALSE))
  expect_equal(plan$fetch, character(0))

  # And the same through the front door: no import runs, so the file is
  # untouched.
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  withr::defer(DBI::dbDisconnect(con))
  expect_equal(file.mtime(nar_path), before)
  expect_equal(nar_provinces(con), "BC")
})

test_that("a second province is added to an existing partial database", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))

  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  DBI::dbDisconnect(con)

  plan <- suppressMessages(nar_import_plan(
    file.path(Sys.getenv("NAR_CACHE_PATH"), "test-01.duckdb"), c("BC", "AB"), FALSE))
  expect_equal(plan$fetch, "AB")
  expect_true(plan$append)

  con <- suppressMessages(nar_connection(version = "test-01",
                                         provinces = c("BC", "AB")))
  withr::defer(DBI::dbDisconnect(con))

  expect_equal(nar_provinces(con), c("AB", "BC"))
  expect_equal(tbl(con, "Addresses") |> dplyr::count() |> dplyr::pull(.data$n), 6)
  expect_equal(tbl(con, "Locations") |> dplyr::count() |> dplyr::pull(.data$n), 6)
  expect_setequal(
    tbl(con, "Addresses") |> dplyr::distinct(.data$MAIL_PROV_ABVN) |>
      dplyr::pull(.data$MAIL_PROV_ABVN),
    c("AB", "BC"))

  # The appended rows have to carry the same geometry decisions the created
  # ones did: x/y mirroring geom rather than BG alone, and geom_source telling
  # the two apart. An appended province whose x/y disagreed with its geom would
  # break the bounding-box prefilter for those rows only.
  ab <- tbl(con, "Addresses") |>
    dplyr::filter(.data$MAIL_PROV_ABVN == "AB") |>
    dplyr::select("ADDR_GUID", "geom_source", "x", "y") |>
    dplyr::arrange(.data$ADDR_GUID) |>
    dplyr::collect()
  expect_equal(ab$geom_source, c("building", "blockface", NA))
  expect_false(anyNA(ab$x[1:2]))
  expect_true(is.na(ab$x[3]))

  # The derived tables are aggregates over the whole address table, so they are
  # rebuilt on an append rather than left describing only the first province.
  expect_setequal(
    tbl(con, "Streets") |> dplyr::distinct(.data$MAIL_PROV_ABVN) |>
      dplyr::pull(.data$MAIL_PROV_ABVN),
    c("AB", "BC"))
  expect_setequal(
    tbl(con, "MunAlias") |> dplyr::distinct(.data$PROV_ABVN) |>
      dplyr::pull(.data$PROV_ABVN),
    c("AB", "BC"))
})

test_that("a national database satisfies any province request", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))

  con <- suppressMessages(nar_connection(version = "test-01", provinces = "all"))
  withr::defer(DBI::dbDisconnect(con))
  expect_equal(nar_meta_value(con, "provinces", NA), nar_all_provinces())
  # Reported as every province rather than as the internal marker, so the
  # answer can be compared against a PROV_ABVN column directly.
  expect_equal(nar_provinces(con), nar_province_table()$abvn)
  expect_equal(tbl(con, "Addresses") |> dplyr::count() |> dplyr::pull(.data$n), 6)

  plan <- suppressMessages(nar_import_plan(
    file.path(Sys.getenv("NAR_CACHE_PATH"), "test-01.duckdb"), "ON", FALSE))
  expect_equal(plan$fetch, character(0))
})

test_that("refresh rebuilds the coverage a database already has", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  DBI::dbDisconnect(con)

  nar_path <- file.path(Sys.getenv("NAR_CACHE_PATH"), "test-01.duckdb")
  # Refreshing a British Columbia database must not silently turn it into a
  # national one.
  plan <- suppressMessages(nar_import_plan(nar_path, NULL, TRUE))
  expect_equal(plan$fetch, "BC")
  expect_false(plan$append)

  con <- suppressMessages(nar_connection(version = "test-01", refresh = TRUE))
  withr::defer(DBI::dbDisconnect(con))
  expect_equal(nar_provinces(con), "BC")
})

test_that("widening a partial database to the whole country rebuilds it", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  DBI::dbDisconnect(con)

  nar_path <- file.path(Sys.getenv("NAR_CACHE_PATH"), "test-01.duckdb")
  plan <- suppressMessages(nar_import_plan(nar_path, nar_all_provinces(), FALSE))
  expect_equal(plan$fetch, nar_all_provinces())
  expect_false(plan$append)

  con <- suppressMessages(nar_connection(version = "test-01", provinces = "all"))
  withr::defer(DBI::dbDisconnect(con))
  # Rebuilt, not appended: British Columbia appears once, not twice.
  expect_equal(tbl(con, "Addresses") |> dplyr::count() |> dplyr::pull(.data$n), 6)
})

test_that("a province the release does not carry is an error, not an empty import", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  expect_error(suppressMessages(nar_connection(version = "test-01", provinces = "ON")),
               "No NAR CSVs for ON")
})

test_that("an address outside a partial database's coverage is not_covered", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "BC"))
  withr::defer(DBI::dbDisconnect(con))

  g <- geocode(c("4001 King Edward Ave W, Vancouver, BC",
                 "4001 King Edward Ave W, Calgary, AB",
                 "9999 Nowhere Ave, Vancouver, BC",
                 "gibberish"),
               con = con)

  # A province this database never downloaded is a different answer from an
  # address it looked for and could not find.
  expect_equal(g$match_method,
               c("nar_building", "not_covered", "none", "none"))
})

test_that("a national database never reports not_covered", {
  skip_if_no_duckdb_spatial()
  local_nar_env(nar_province_fixture(c("BC", "AB")))
  con <- suppressMessages(nar_connection(version = "test-01", provinces = "all"))
  withr::defer(DBI::dbDisconnect(con))

  g <- geocode("4001 King Edward Ave W, Regina, SK", con = con)
  expect_equal(g$match_method, "none")
})
