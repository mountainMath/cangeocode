# A miniature road network file, with the release's own column names.
#
# Four streets, each carrying one thing the tier has to get right:
#
#   GRANT ST, Vancouver -- the same 200 m stretch the NAR fixture's `run`
#     addresses sit on, but drawn as a centreline midway between the two sides
#     and digitized west to east. Left of that direction is north, which is
#     where the odd civics are, so AFL/ATL is the odd range and AFR/ATR the even
#     one. This is what pins the side logic down: an even number has to come out
#     13 m SOUTH of the line and an odd one 13 m north, and getting the
#     digitizing convention backwards swaps them.
#   COVE RD, Bowen Island -- a street in a census subdivision the NAR fixture
#     has never heard of, so `MunAlias` has nothing to say about it. It is what
#     the direct CSD-name comparison exists for, and it is the case the tier was
#     built for: 8.3% of RNF's ranged street/CSD pairs are absent from NAR.
#   DUPLEX ST, Vancouver -- two segments whose ranges both contain 50. The tier
#     has to refuse rather than choose.
#   MARINE DR, Vancouver -- a segment with a left range only, so the right-hand
#     side is null and an even number on it must not be placed.
rnf_fixture_columns <- function() {
  c("NGD_UID", "NAME", "TYPE", "DIR", "AFL_VAL", "ATL_VAL", "AFR_VAL",
    "ATR_VAL", "CSDUID_L", "CSDNAME_L", "CSDTYPE_L", "CSDUID_R", "CSDNAME_R",
    "CSDTYPE_R", "PRUID_L", "PRUID_R", "CLASS", "RANK")
}

rnf_fixture_rows <- function() {
  seg <- function(uid, name, type, afl, atl, afr, atr, csd = "Vancouver",
                  csduid = "5915022", csdtype = "CY") {
    stats::setNames(data.frame(uid, name, type, NA_character_,
                               afl, atl, afr, atr,
                               csduid, csd, csdtype, csduid, csd, csdtype,
                               "59", "59", "23", "4",
                               stringsAsFactors = FALSE),
                    rnf_fixture_columns())
  }
  rbind(
    seg("ngd1", "Grant", "ST", "101", "301", "100", "300"),
    seg("ngd2", "Cove", "RD", "1", "99", "2", "98",
        csd = "Bowen Island", csduid = "5915055", csdtype = "IM"),
    seg("ngd3", "Duplex", "ST", "1", "99", "2", "98"),
    seg("ngd4", "Duplex", "ST", "1", "99", "2", "98"),
    seg("ngd5", "Marine", "DR", "1", "99", NA_character_, NA_character_))
}

# The geometries, in the storage CRS. Grant is the one that matters: 200 m
# west to east at y = 2007010, which is 10 m north of the NAR fixture's even
# civics and 10 m south of its odd ones.
rnf_fixture_geometry <- function() {
  ln <- function(x1, y1, x2, y2) {
    sf::st_linestring(matrix(c(x1, y1, x2, y2), ncol = 2, byrow = TRUE))
  }
  sf::st_sfc(
    ln(4012000, 2007010, 4012200, 2007010),
    ln(4013000, 2008000, 4013100, 2008000),
    ln(4014000, 2009000, 4014100, 2009000),
    ln(4014500, 2009000, 4014600, 2009000),
    ln(4015000, 2010000, 4015100, 2010000),
    crs = sf::st_crs(nar_storage_crs()))
}

#' Write the miniature road network file and return the path to its shapefile
#'
#' Named the way StatCan names its own, because [rnf_resolve_shp()] reads the
#' release out of the file name.
local_rnf_fixture <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  shp <- file.path(dir, "lrnf000r25a_e.shp")
  x <- sf::st_sf(rnf_fixture_rows(), geometry = rnf_fixture_geometry())
  suppressWarnings(sf::st_write(x, shp, quiet = TRUE))
  shp
}

#' Import both fixtures and hand back an open read-only connection
#'
#' The NAR import has to finish and release the file before the road network
#' file can be written into it, exactly as in [local_rqa_connection()]: DuckDB
#' takes an exclusive lock for a writer and `nar_connection()` holds a reader.
local_rnf_connection <- function(run = TRUE, env = parent.frame()) {
  local_nar_env(local_nar_fixture(blockface = TRUE, run = run, env = env),
                env = env)
  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)

  suppressMessages(rnf_import(version = "test-01",
                              shp = local_rnf_fixture(env = env)))

  con <- suppressMessages(nar_connection(version = "test-01"))
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}
