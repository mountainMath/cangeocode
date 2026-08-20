test_that("version labels parse without consulting the locale", {
  # strptime's %B reads month names through LC_TIME and returns NA for an
  # English name under a non-English locale, which used to poison `path`.
  expect_equal(nar_version_date("May 2024"), as.Date("2024-05-01"))
  expect_equal(nar_version_date("June 2026"), as.Date("2026-06-01"))
  # A bare year is that year's December release.
  expect_equal(nar_version_date("2022"), as.Date("2022-12-01"))
  expect_equal(nar_version_date("12 June 2026"), as.Date("2026-06-12"))
  expect_equal(nar_version_date("2026-06-01"), as.Date("2026-06-01"))
})

test_that("month abbreviations resolve by unambiguous prefix", {
  expect_equal(nar_version_date("Sep 2025"), as.Date("2025-09-01"))
  expect_equal(nar_version_date("Sept. 2025"), as.Date("2025-09-01"))
  expect_equal(nar_version_date("sept 2025"), as.Date("2025-09-01"))
  # "Ju" could be June or July, so it stays unparsed rather than guessing.
  expect_true(is.na(nar_version_date("Ju 2024")))
})

test_that("an unparseable label is NA rather than an error", {
  expect_true(is.na(nar_version_date("sometime")))
  expect_equal(nar_version_date(c("May 2024", "sometime")),
               c(as.Date("2024-05-01"), as.Date(NA)))
})

test_that("nar_version_date is stable across locales", {
  original <- Sys.getlocale("LC_TIME")
  skip_if(suppressWarnings(Sys.setlocale("LC_TIME", "fr_FR.UTF-8")) == "",
          "fr_FR.UTF-8 locale unavailable")
  withr::defer(Sys.setlocale("LC_TIME", original))

  expect_equal(nar_version_date("May 2024"), as.Date("2024-05-01"))
})

fake_page <- function(links) {
  xml2::read_html(paste0("<html><body><section><div><p>",
                         paste0(sprintf('<a href="%s">%s</a>', links, names(links)),
                                collapse = ""),
                         "</p></div></section></body></html>"))
}
overview <- "https://example.org/pub/46-26-0002/462600022022001-eng.htm"

test_that("relative links are resolved against the publication page", {
  page <- fake_page(c(`June 2026` = "2022001/202606.zip"))

  got <- nar_version_table(page, overview)

  expect_equal(got$url, "https://example.org/pub/46-26-0002/2022001/202606.zip")
  expect_equal(got$path, "2026-06")
})

test_that("an absolute link is left alone", {
  # file.path() used to fold these into the publication page's directory.
  page <- fake_page(c(`June 2026` = "https://cdn.example.net/nar/202606.zip"))

  expect_equal(nar_version_table(page, overview)$url,
               "https://cdn.example.net/nar/202606.zip")
})

test_that("versions come back newest first and non-zip links are ignored", {
  page <- fake_page(c(`May 2024` = "a/202405.zip",
                      `June 2026` = "a/202606.zip",
                      `Some guide` = "a/guide.htm",
                      `2022` = "a/2022.zip"))

  got <- nar_version_table(page, overview)

  expect_equal(got$version, c("June 2026", "May 2024", "2022"))
  expect_false(any(grepl("guide", got$url)))
})

test_that("a page with no downloads is an error, not an empty table", {
  # This is how a StatCan layout change surfaces.
  page <- fake_page(c(`Some guide` = "a/guide.htm"))

  expect_error(nar_version_table(page, overview), "page layout has probably changed")
})

test_that("an unrecognised label warns and is dropped, keeping the rest usable", {
  page <- fake_page(c(`June 2026` = "a/202606.zip", `Winter release` = "a/x.zip"))

  expect_warning(got <- nar_version_table(page, overview), "unrecognized date label")
  expect_equal(got$version, "June 2026")
})
