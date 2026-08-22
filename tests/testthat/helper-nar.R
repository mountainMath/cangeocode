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
#
# `run = TRUE` appends a fourth street carrying a numbered run, which is what
# the interpolation tier needs and the three addresses above cannot give it:
# two flanking civics of the same parity, a gap between them, and an odd side
# offset from the even one. It is opt-in because every other test in the suite
# counts the rows in this table.
nar_address_rows <- function(blockface = FALSE, run = FALSE) {
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
  if (run) {
    # A 100 m stretch of GRANT ST, laid out east-west so the interpolated
    # coordinate is a plain arithmetic check. Even civics run 100, 200 and 300
    # at x = 4012000, 4012100 and 4012200 with y fixed; the odd side sits 20 m
    # north of them. So 150 must land at 4012050 and 250 at 4012150, 400 must be
    # refused as extrapolation, and 151 must come off the *odd* line at y+20
    # rather than splitting the difference with the even one.
    grant <- function(guid, civic, x, y) {
      base(guid, civic, x, y, street = "GRANT", type = "ST", dir = "",
           postal = "V5L1Z9")
    }
    rows <- c(rows, list(
      grant("addr4", "100", "4012000", "2007000"),
      grant("addr5", "200", "4012100", "2007000"),
      grant("addr6", "300", "4012200", "2007000"),
      grant("addr7", "101", "4012000", "2007020"),
      grant("addr8", "301", "4012200", "2007020"),
      # Mailed to SOUTHLANDS, which is not a CSD, but inside the Vancouver CSD
      # and -- unlike addr3 -- actually carrying coordinates. This is what the
      # authoritative `mun` argument has to reach through the alias set.
      base("addr9", "5001", "4012300", "2007100", street = "MUSQUEAM",
           type = "DR", dir = "", mail_mun = "SOUTHLANDS", postal = "V6N3T7")))
  }
  if (blockface) {
    bf <- list(c("4012086.46456561", "2006838.65510961"),
               c("4012086.46456561", "2006838.65510961"),
               c("", ""))
    bf <- c(bf, rep(list(c("", "")), length(rows) - length(bf)))
    rows <- Map(function(r, b) append(r, b, after = 27), rows, bf)
  }
  rows
}

nar_location_lines <- function(run = FALSE) {
  lines <- c("LOC_GUID,CSD_CODE,FED_CODE,FED_ENG_NAME,FED_FRE_NAME,ER_CODE,ER_ENG_NAME,ER_FRE_NAME,BG_LATITUDE,BG_LONGITUDE",
    "loc1,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2501,-123.1999",
    "loc2,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2504,-123.1995",
    "loc3,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2500,-123.2000")
  if (!run) return(lines)
  # The run's locations, emitted only alongside the run's addresses. Not
  # unconditionally: test-import.R reads every lon/lat out of this table.
  c(lines,
    "loc4,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2502,-123.1997",
    "loc5,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2502,-123.1996",
    "loc6,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2502,-123.1995",
    "loc7,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2503,-123.1997",
    "loc8,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2503,-123.1995",
    "loc9,5915022,59001,Van,Van,5920,Mainland,Mainland,49.2504,-123.1994")
}

#' Write a miniature NAR release to a directory and return its path
local_nar_fixture <- function(blockface = FALSE, run = FALSE, env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  lines <- c(paste(nar_address_header(blockface), collapse = ","),
             vapply(nar_address_rows(blockface, run), paste, character(1),
                    collapse = ","))
  writeLines(lines, file.path(dir, "Address_BC.csv"))
  writeLines(nar_location_lines(run), file.path(dir, "Location_BC.csv"))
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
local_nar_connection <- function(blockface = TRUE, run = FALSE,
                                 env = parent.frame()) {
  local_nar_env(local_nar_fixture(blockface, run, env = env), env = env)
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

#' A three-row gazetteer in a throwaway writable database
#'
#' The real fixture connection is read-only, which is right for what it tests
#' but leaves no way to exercise the gazetteer SQL against contrived rows. This
#' builds just the three tables nar_gazetteer_sql() reads.
local_mini_gazetteer <- function(env = parent.frame()) {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  DBI::dbExecute(con, "CREATE TABLE Streets AS SELECT
      'Doyle' AS OFFICIAL_STREET_NAME, 'ST' AS OFFICIAL_STREET_TYPE,
      '' AS OFFICIAL_STREET_DIR, 'DOYLE' AS MAIL_STREET_NAME,
      'ST' AS MAIL_STREET_TYPE, '' AS MAIL_STREET_DIR,
      'ST. JOHN''S' AS MAIL_MUN_NAME, 'NL' AS MAIL_PROV_ABVN,
      '10' AS PROV_CODE, 'St. John''s' AS CSD_ENG_NAME,
      '10:CY:St. John''s' AS MUN_KEY, 'DOYLE' AS NAME_FOLD,
      'DOYLE' AS MAIL_NAME_FOLD, 120 AS N_ADDRESSES,
      1 AS MIN_CIVIC_NO, 400 AS MAX_CIVIC_NO
    UNION ALL SELECT
      -- A second Doyle St, in a second city. Nothing in the string can choose
      -- between them, which is what the exact branch has to decline on.
      'Doyle', 'ST', '', 'DOYLE', 'ST', '',
      'MOUNT PEARL', 'NL', '10', 'Mount Pearl', '10:CY:Mount Pearl',
      'DOYLE', 'DOYLE', 40, 1, 90
    UNION ALL SELECT
      -- Kenmount Rd is in one city only, so it is determined rather than
      -- guessed, and the exact branch may answer with it.
      'Kenmount', 'RD', '', 'KENMOUNT', 'RD', '',
      'MOUNT PEARL', 'NL', '10', 'Mount Pearl', '10:CY:Mount Pearl',
      'KENMOUNT', 'KENMOUNT', 60, 1, 900")
  DBI::dbExecute(con, "CREATE TABLE MunAlias AS SELECT
      'ST. JOHN''S' AS NAME_FOLD, 'NL' AS PROV_ABVN,
      '10:CY:St. John''s' AS MUN_KEY, 120 AS N_ADDRESSES
    UNION ALL SELECT 'MOUNT PEARL', 'NL', '10:CY:Mount Pearl', 40")
  DBI::dbExecute(con, "CREATE TABLE PostalMun AS SELECT
      'A1E' AS FSA, 'ST. JOHN''S' AS MAIL_MUN_NAME, 120 AS N_ADDRESSES")
  con
}

#' A miniature release split by province, named the way StatCan names its own
#'
#' The real bulk zip carries one `Address_<SGC code>.csv` per province, and
#' that naming is what both the partial download and the append path key off.
#' `local_nar_fixture()` writes `Address_BC.csv`, which deliberately does not
#' match it -- a file the pattern cannot place is treated as shared and always
#' loaded, which is how the guides and the readme survive a partial download.
#' So the province tests need a fixture of their own.
nar_province_fixture <- function(provinces = c("BC", "AB"), env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  tbl <- nar_province_table()
  for (abvn in provinces) {
    code <- tbl$code[match(abvn, tbl$abvn)]
    rows <- lapply(nar_address_rows(blockface = TRUE), function(r) {
      # Province columns, plus a GUID prefix so the two provinces cannot
      # collide on a key.
      r[9] <- code
      r[18] <- abvn
      r[1] <- paste0(abvn, "-", r[1])
      r[2] <- paste0(abvn, "-", r[2])
      r
    })
    lines <- c(paste(nar_address_header(TRUE), collapse = ","),
               vapply(rows, paste, character(1), collapse = ","))
    writeLines(lines, file.path(dir, paste0("Address_", code, ".csv")))

    loc <- nar_location_lines()
    loc[-1] <- paste0(abvn, "-", loc[-1])
    writeLines(loc, file.path(dir, paste0("Location_", code, ".csv")))
  }
  # A member the province pattern cannot place, standing in for the release's
  # user guide.
  writeLines("not a province file", file.path(dir, "NAR_User_Guide.txt"))
  dir
}
