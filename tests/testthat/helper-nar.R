# Fixtures + mocks that let the import path run without touching StatCan.

nar_address_header <- function(blockface = FALSE) {
  cols <- c("LOC_GUID", "ADDR_GUID", "APT_NO_LABEL", "CIVIC_NO", "CIVIC_NO_SUFFIX",
            "OFFICIAL_STREET_NAME", "OFFICIAL_STREET_TYPE", "OFFICIAL_STREET_DIR",
            "PROV_CODE", "CSD_ENG_NAME", "CSD_FRE_NAME", "CSD_TYPE_ENG_CODE",
            "CSD_TYPE_FRE_CODE", "MAIL_STREET_NAME", "MAIL_STREET_TYPE",
            "MAIL_STREET_DIR", "MAIL_MUN_NAME", "MAIL_PROV_ABVN", "MAIL_POSTAL_CODE",
            "BG_DLS_LSD", "BG_DLS_QTR", "BG_DLS_SCTN", "BG_DLS_TWNSHP", "BG_DLS_RNG",
            "BG_DLS_MRD", "BG_X", "BG_Y", "BU_N_CIVIC_ADD", "BU_USE")
  if (!blockface) return(cols)
  # The 2026-06 release inserted the blockface columns *after* BG_Y rather than
  # at the end, which is exactly the shift nar_csv_schema() has to survive.
  append(cols, c("BF_REPPOINT_X", "BF_REPPOINT_Y"), after = which(cols == "BG_Y"))
}

# Three addresses on one blockface:
#   addr1  building point, and (in the blockface layout) a blockface point too
#   addr2  no building point -- falls back to the blockface point when present
#   addr3  neither, so no geometry under either layout
#
# addr3 also sits on a different street under a *different mailing city* inside
# the same CSD -- the many-to-many that MunAlias exists for. Both directions are
# in the fixture: SOUTHLANDS is a mailing city that is not a CSD, and Vancouver
# is a CSD that covers mail addressed to two different cities.
nar_address_rows <- function(blockface = FALSE) {
  base <- function(guid, civic, x, y,
                   street = "KING EDWARD", type = "AVE", dir = "W",
                   mail_mun = "VANCOUVER", postal = "V6S1N3") {
    c(sub("addr", "loc", guid), guid, "", civic, "", street, type, dir,
      "BC", "Vancouver", "Vancouver", "CY", "CV", street, type, dir,
      mail_mun, "BC", postal, "", "", "", "", "", "", x, y, "1", "1")
  }
  rows <- list(base("addr1", "4001", "4012046.46456561", "2006868.65510961"),
               base("addr2", "4002", "", ""),
               base("addr3", "4003", "", "",
                    street = "MUSQUEAM", type = "DR", dir = "",
                    mail_mun = "SOUTHLANDS", postal = "V6N3T7"))
  if (blockface) {
    bf <- list(c("4012086.46456561", "2006838.65510961"),
               c("4012086.46456561", "2006838.65510961"),
               c("", ""))
    rows <- Map(function(r, b) append(r, b, after = 27), rows, bf)
  }
  rows
}

nar_location_lines <- function() {
  c("LOC_GUID,CSD_CODE,FED_CODE,FED_ENG_NAME,FED_FRE_NAME,ER_CODE,ER_ENG_NAME,ER_FRE_NAME,BG_LATITUDE,BG_LONGITUDE",
    "loc1,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2501,-123.1999",
    "loc2,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2504,-123.1995",
    "loc3,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2500,-123.2000")
}

#' Write a miniature NAR release to a directory and return its path
local_nar_fixture <- function(blockface = FALSE, env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  lines <- c(paste(nar_address_header(blockface), collapse = ","),
             vapply(nar_address_rows(blockface), paste, character(1), collapse = ","))
  writeLines(lines, file.path(dir, "Address_BC.csv"))
  writeLines(nar_location_lines(), file.path(dir, "Location_BC.csv"))
  dir
}

# Accepts ... because normalized_nar_version() passes `refresh` through.
nar_fake_versions <- function(...) {
  dplyr::tibble(version = "test", url = "https://example.invalid/nar.zip",
                Date = as.Date("2026-06-01"), path = "test-01")
}

#' Point the package at a throwaway cache and a local extract, with no network
local_nar_env <- function(exdir, env = parent.frame()) {
  cache <- withr::local_tempdir(.local_envir = env)
  withr::local_envvar(c(NAR_CACHE_PATH = cache), .local_envir = env)
  withr::local_options(list(nar_exdir = exdir), .local_envir = env)
  testthat::local_mocked_bindings(available_nar_versions = nar_fake_versions,
                                  .package = "cangeocode", .env = env)
  cache
}

#' Import the fixture and hand back an open connection
local_nar_connection <- function(blockface = TRUE, env = parent.frame()) {
  local_nar_env(local_nar_fixture(blockface, env = env), env = env)
  con <- suppressMessages(nar_connection(version = "test-01"))
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}

skip_if_no_duckdb_spatial <- function() {
  testthat::skip_on_cran()
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  ok <- tryCatch({
    DBI::dbExecute(con, "INSTALL spatial; LOAD spatial;")
    TRUE
  }, error = function(e) FALSE)
  testthat::skip_if_not(ok, "DuckDB spatial extension unavailable")
}
