# Arrow type objects compare by pointer, so match on their printed form.
field_type <- function(schema, name) {
  field <- schema$GetFieldByName(name)
  if (is.null(field)) NA_character_ else field$type$ToString()
}

write_header <- function(header, rows = character()) {
  path <- withr::local_tempfile(fileext = ".csv", .local_envir = parent.frame())
  writeLines(c(header, rows), path)
  path
}

test_that("nar_csv_schema takes column order from the file, not a fixed list", {
  path <- write_header("A,B,C")
  schema <- nar_csv_schema(path, list(B = arrow::float64()))

  expect_equal(names(schema), c("A", "B", "C"))
  expect_equal(field_type(schema, "B"), "double")
  # Everything not named keeps StatCan's original text.
  expect_equal(field_type(schema, "A"), "string")
  expect_equal(field_type(schema, "C"), "string")
})

test_that("types follow the column name when a release moves the column", {
  # The 2026-06 release inserted BF_REPPOINT_X/Y after BG_Y, shifting
  # BU_N_CIVIC_ADD and BU_USE along by two. Arrow maps a declared schema onto
  # CSV columns positionally, so a fixed list extended at the end would have
  # read blockface coordinates into BU_N_CIVIC_ADD without complaint.
  old <- write_header(paste(nar_address_header(blockface = FALSE), collapse = ","))
  new <- write_header(paste(nar_address_header(blockface = TRUE), collapse = ","))

  old_schema <- nar_csv_schema(old, nar_address_types())
  new_schema <- nar_csv_schema(new, nar_address_types())

  expect_length(names(old_schema), 29)
  expect_length(names(new_schema), 31)
  for (schema in list(old_schema, new_schema)) {
    expect_equal(field_type(schema, "BG_X"), "double")
    expect_equal(field_type(schema, "BU_USE"), "int64")
    expect_equal(field_type(schema, "CIVIC_NO"), "int64")
  }
  expect_equal(field_type(new_schema, "BF_REPPOINT_X"), "double")
  expect_equal(field_type(old_schema, "BF_REPPOINT_X"), NA_character_)
})

test_that("a column that disappears is an error, not a silent mis-read", {
  path <- write_header("LOC_GUID,ADDR_GUID,NOPE")

  expect_error(
    nar_csv_schema(path, list(), required = c("LOC_GUID", "BG_X", "BG_Y")),
    "missing expected column"
  )
  expect_error(nar_csv_schema(path, list(), required = "BG_X"), "BG_X")
})

test_that("a UTF-8 BOM does not corrupt the first column name", {
  path <- withr::local_tempfile(fileext = ".csv")
  con <- file(path, open = "wb")
  writeBin(charToRaw("﻿LOC_GUID,BG_X\n"), con)
  close(con)

  schema <- nar_csv_schema(path, list(BG_X = arrow::float64()),
                           required = "LOC_GUID")
  expect_equal(names(schema), c("LOC_GUID", "BG_X"))
})

test_that("quoted and padded headers are normalised", {
  path <- write_header('"LOC_GUID", "BG_X" ,"BG_Y"')

  expect_equal(names(nar_csv_schema(path, list())), c("LOC_GUID", "BG_X", "BG_Y"))
})

test_that("an unreadable header is an error", {
  path <- withr::local_tempfile(fileext = ".csv")
  file.create(path)

  expect_error(nar_csv_schema(path, list()), "column header")
})
