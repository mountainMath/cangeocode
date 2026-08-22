# Reading selected members out of a zip, through the reader seam.

# A NAR-shaped archive: two provinces, one of them split into parts, plus a
# guide that belongs to no province.
local_zip_release <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  members <- c("Addresses/Address_59.csv", "Addresses/Address_35_part_1.csv",
               "Addresses/Address_35_part_2.csv", "Locations/Location_59.csv",
               "Locations/Location_35.csv", "NAR_User_Guide_EN.txt")
  for (m in members) {
    dir.create(file.path(dir, dirname(m)), showWarnings = FALSE, recursive = TRUE)
    # Long and repetitive, so the members actually compress and the copied
    # bytes are deflate streams rather than stored ones.
    writeLines(rep(paste("row for", m), 200), file.path(dir, m))
  }
  zip_path <- withr::local_tempfile(fileext = ".zip", .local_envir = env)
  withr::with_dir(dir, utils::zip(zip_path, members, flags = "-q"))
  list(dir = dir, zip = zip_path, members = members)
}

test_that("little-endian fields survive past the signed 32-bit boundary", {
  # readBin()'s 4-byte integer is signed, and offsets into a 1.7 GB archive are
  # already close to the boundary this is here to clear.
  expect_equal(nar_le(as.raw(c(1, 0, 0, 0)), 0, 4), 1)
  expect_equal(nar_le(as.raw(c(0xff, 0xff, 0xff, 0xff)), 0, 4), 4294967295)
  expect_equal(nar_le(as.raw(c(0, 0, 0, 0x80)), 0, 4), 2147483648)
  expect_equal(nar_le(nar_le_raw(3141592653, 4), 0, 4), 3141592653)
  expect_equal(nar_le(nar_le_raw(2^40 + 7, 8), 0, 8), 2^40 + 7)
})

test_that("the central directory is read out of a real archive", {
  rel <- local_zip_release()
  dir <- nar_zip_directory(nar_file_reader(rel$zip))

  expect_setequal(dir$name, rel$members)
  expect_true(all(dir$usize > 0))
  expect_true(all(dir$csize > 0))
  # Ordered by offset, which is the order the members will be fetched in.
  expect_false(is.unsorted(dir$offset))

  # Each member's stated compressed size has to land exactly on the next
  # member's local header, or the copy would splice the wrong bytes.
  reader <- nar_file_reader(rel$zip)
  for (i in seq_len(nrow(dir))) {
    local <- reader(dir$offset[i], 30)
    expect_equal(local[1:4], as.raw(c(0x50, 0x4b, 0x03, 0x04)))
  }
})

test_that("a subset of members round-trips into a working archive", {
  rel <- local_zip_release()
  reader <- nar_file_reader(rel$zip)
  dir <- nar_zip_directory(reader)

  wanted <- dir[nar_zip_member_province(dir$name) %in% "ON" |
                  is.na(nar_zip_member_province(dir$name)), , drop = FALSE]
  out <- withr::local_tempfile(fileext = ".zip")
  nar_zip_copy_members(reader, wanted, out)

  # unzip() reads it, and the CRCs it checks are the ones copied from the
  # source directory -- so a byte spliced in the wrong place fails here.
  listing <- utils::unzip(out, list = TRUE)
  expect_setequal(listing$Name,
                  c("Addresses/Address_35_part_1.csv",
                    "Addresses/Address_35_part_2.csv",
                    "Locations/Location_35.csv", "NAR_User_Guide_EN.txt"))

  exdir <- withr::local_tempdir()
  utils::unzip(out, exdir = exdir)
  for (m in listing$Name) {
    expect_identical(readLines(file.path(exdir, m)),
                     readLines(file.path(rel$dir, m)),
                     info = m)
  }
})

test_that("copying every member reproduces the whole release", {
  rel <- local_zip_release()
  reader <- nar_file_reader(rel$zip)
  out <- withr::local_tempfile(fileext = ".zip")
  nar_zip_copy_members(reader, nar_zip_directory(reader), out)

  exdir <- withr::local_tempdir()
  utils::unzip(out, exdir = exdir)
  for (m in rel$members) {
    expect_identical(readLines(file.path(exdir, m)),
                     readLines(file.path(rel$dir, m)), info = m)
  }
})

test_that("selecting no members is an error rather than an empty archive", {
  rel <- local_zip_release()
  reader <- nar_file_reader(rel$zip)
  dir <- nar_zip_directory(reader)
  expect_error(nar_zip_copy_members(reader, dir[0, ], tempfile()),
               "No zip members selected")
})

test_that("a file that is not a zip is reported as such", {
  path <- withr::local_tempfile()
  writeLines(rep("not a zip", 100), path)
  expect_error(nar_zip_directory(nar_file_reader(path)),
               "end-of-central-directory")
})
