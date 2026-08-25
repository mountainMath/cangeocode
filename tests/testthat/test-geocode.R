test_that("an address NAR carries resolves to its own building point", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  g <- geocode("4001 King Edward Ave W, Vancouver, BC", con = con)

  expect_equal(g$match_method, "nar_building")
  expect_equal(g$uncertainty_m, 0)
  expect_equal(g$n_matches, 1L)
  expect_equal(g$ADDR_GUID, "addr1")
})

test_that("an address with only a blockface point says so, and prices it", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  g <- geocode("4002 King Edward Ave W, Vancouver, BC", con = con)

  # addr2 has no building point, so the fallback fires and the uncertainty is
  # the measured blockface constant rather than zero. Reporting 0 here -- the
  # value an exact civic match would otherwise imply -- is the failure this
  # guards: the point is a segment centroid shared with the whole blockface.
  expect_equal(g$match_method, "nar_blockface")
  expect_equal(g$uncertainty_m, nar_blockface_uncertainty_m())
})

test_that("a remapped municipality lifts uncertainty_m by what attested it", {
  # The floor is applied to the parse, not to a tier, so it is tested that way:
  # 0 is the value an exact building match reports, 176 a blockface, NA an
  # unplaced row -- and only the first is wrong on a row whose municipality the
  # gazetteer chose *and could not attest*. A bigger uncertainty is never talked
  # down, and an unplaced row stays unplaced rather than acquiring a precision it
  # has no point for.
  floors <- nar_remap_uncertainty_m()
  out <- data.frame(uncertainty_m = c(0, 176, NA, 0, 0, 0))
  res <- data.frame(
    mun_remapped = c(TRUE, TRUE, TRUE, FALSE, TRUE, TRUE),
    mun_evidence = c("unattested", "untestable", "unattested", "kept",
                     "copostal", "csd"))

  # The last two are the point of the split: a swap a postal code or a census
  # subdivision vouches for measures no worse than a municipality the input got
  # right, so it is left at the 0 m its tier reported.
  expect_equal(nar_geocode_remap_floor(out, res)$uncertainty_m,
               c(unname(floors[["unattested"]]), 176, NA, 0, 0, 0))

  # A `res` carrying the flag but not the evidence -- a frame from an older
  # parse -- is priced as unattested rather than waved through.
  bare <- data.frame(mun_remapped = c(TRUE, TRUE, TRUE, FALSE, TRUE, TRUE))
  expect_equal(nar_geocode_remap_floor(out, bare)$uncertainty_m,
               c(rep(unname(floors[["unattested"]]), 1), 176, NA, 0,
                 rep(unname(floors[["unattested"]]), 2)))

  # A `res` that never carried either column -- a caller passing their own
  # parsed frame -- reads as "nothing was remapped" rather than erroring.
  expect_equal(nar_geocode_remap_floor(out, data.frame(x = 1:6))$uncertainty_m,
               out$uncertainty_m)
})

test_that("a municipality the caller asserted is never reported as remapped", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # A municipality in `known` is a constraint on the search, not a reading of
  # the string, so there is nothing for the gazetteer to have substituted and
  # nothing to price. Either grain settles it.
  g <- geocode("4001 King Edward Ave W",
               known = list(CSD_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)
  expect_false(g$mun_remapped)
  expect_equal(g$uncertainty_m, 0)

  g <- geocode("4001 King Edward Ave W",
               known = list(MUN_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)
  expect_false(g$mun_remapped)
  expect_equal(g$uncertainty_m, 0)
})

test_that("a civic number NAR lacks is interpolated between its flanks", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # 150 sits midway between 100 at x = 4012000 and 200 at x = 4012100, so it
  # belongs at 4012050 exactly, and the 100 m flanking span prices it at 50 m.
  g <- geocode("150 Grant St, Vancouver, BC", con = con, crs = NULL,
               geometry = TRUE)

  expect_equal(g$match_method, "nar_interpolated")
  expect_equal(g$uncertainty_m, 50)
  expect_equal(as.numeric(sf::st_coordinates(g)), c(4012050, 2007000))
})

test_that("interpolation uses only the same side of the street", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # The odd side is 20 m north of the even one, and far more sparsely numbered:
  # 151 has to come off 101 and 301, a quarter of the way along, rather than off
  # the even civics that bracket it much more tightly. So y = 2007020 -- the odd
  # line -- and the 200 m odd-side span prices it at 100 m rather than the 50 m
  # the even side would have implied. Pooling both sides is the mistake this
  # catches, and nationally it is a 35.2 m median error against 4.2 m.
  g <- geocode("151 Grant St, Vancouver, BC", con = con, crs = NULL,
               geometry = TRUE)

  expect_equal(g$match_method, "nar_interpolated")
  expect_equal(as.numeric(sf::st_coordinates(g)), c(4012050, 2007020))
  expect_equal(g$uncertainty_m, 100)
})

test_that("a civic number past the end of the run is refused, not extrapolated", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # 400 is above every even civic on the street, so there is no upper flank.
  # Continuing the run's spacing would put it somewhere plausible-looking and
  # be wrong by a 90th-percentile 237 m, so nothing is returned at all.
  g <- geocode("400 Grant St, Vancouver, BC", con = con)

  expect_equal(g$match_method, "none")
  expect_true(is.na(g$uncertainty_m))
  expect_true(is.na(g$lon))
})

test_that("dropping the interpolation tier from `method` skips it entirely", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  g <- geocode("150 Grant St, Vancouver, BC", con = con, method = "nar")

  expect_equal(g$match_method, "none")
})

test_that("`method` order decides which tier answers", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # 200 Grant St is in NAR with its own point, and is also flanked by 100 and
  # 300, so either tier can answer it. Whichever runs first is the one that does.
  a <- "200 Grant St, Vancouver, BC"
  expect_equal(geocode(a, con = con)$match_method, "nar_building")
  expect_equal(geocode(a, con = con,
                       method = c("nar_interpolate", "nar"))$match_method,
               "nar_interpolated")
})

test_that("a tier that never runs reports nothing, ADDR_GUID included", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  a <- "200 Grant St, Vancouver, BC"
  expect_false(is.na(geocode(a, con = con)$ADDR_GUID))
  # Interpolation does not look the record up, so putting it first costs the
  # identifier the exact tier would have found.
  expect_true(is.na(geocode(a, con = con,
                            method = c("nar_interpolate", "nar"))$ADDR_GUID))
})

test_that("`method` is validated, deduplicated, and order-preserving", {
  expect_equal(nar_geocode_methods("nar"), "nar")
  expect_equal(nar_geocode_methods(c("bc", "nar")), c("bc", "nar"))
  expect_equal(nar_geocode_methods(c("nar", "nar")), "nar")
  # Exact matches beat prefixes, so "nar" is not ambiguous against
  # "nar_interpolate".
  expect_equal(nar_geocode_methods(c("nar", "nar_i")),
               c("nar", "nar_interpolate"))

  expect_equal(nar_geocode_methods(c("nar", "rnf")), c("nar", "rnf"))

  expect_error(nar_geocode_methods("osm"), 'Unknown geocoding method "osm"')
  expect_error(nar_geocode_methods(character()), "must be one or more of")
  expect_error(nar_geocode_methods(1), "must be one or more of")
})

test_that("interpolation never uses a blockface point as a flank", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # 4001 and 4002 bracket nothing usable: 4001 is a building point but 4002 has
  # only a blockface centroid, and interpolating off that would compound its
  # 176 m error into a result presented as precise.
  g <- geocode("4005 King Edward Ave W, Vancouver, BC", con = con)

  expect_equal(g$match_method, "none")
})

test_that("an ambiguous address reports how many points it could have been", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # King Edward Ave W is filed under VANCOUVER; naming no municipality leaves
  # the query unrestricted, which is allowed rather than refused -- but the
  # count and the widened uncertainty have to say so.
  g <- geocode("4001 King Edward Ave W", con = con)

  expect_equal(g$match_method, "nar_building")
  expect_equal(g$n_matches, 1L)
})

test_that("known overrides what the string said", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # The string names the wrong province and the wrong city outright. Because
  # `known` is authoritative rather than a fallback, the search runs in BC and
  # the result reports BC -- a row whose PROV_ABVN disagreed with the point
  # returned would misdescribe what was actually searched. The mailing city the
  # string supplied is *cleared* rather than left to constrain: TORONTO and the
  # asserted Vancouver jurisdiction do not overlap, so keeping both would let
  # the contradicted reading veto the assertion. What comes back in MUN_NAME is
  # then the mailing city of the record that matched, not the string's.
  g <- geocode("4001 King Edward Ave W, Toronto, ON",
               known = list(CSD_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)

  expect_equal(g$match_method, "nar_building")
  expect_equal(g$PROV_ABVN, "BC")
  expect_equal(g$CSD_NAME, "VANCOUVER")
  expect_equal(g$MUN_NAME, "VANCOUVER")

  # An asserted mailing city is not cleared -- it is the caller's own reading
  # of the same grain, and it lands on the row.
  g <- geocode("4001 King Edward Ave W, Toronto, ON",
               known = list(MUN_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)

  expect_equal(g$match_method, "nar_building")
  expect_equal(g$MUN_NAME, "VANCOUVER")
})

test_that("CSD_NAME resolves through the alias set and MUN_NAME does not", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # addr9 is mailed to SOUTHLANDS, which is not a CSD, inside the Vancouver CSD.
  # Asking for the jurisdiction has to reach it. Matching MAIL_MUN_NAME directly
  # would not, and that is the Toronto/Scarborough problem in miniature.
  g <- geocode("5001 Musqueam Dr",
               known = list(CSD_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)
  expect_equal(g$ADDR_GUID, "addr9")

  # The other grain, on the same row: the record is mailed to SOUTHLANDS, so
  # Vancouver as a *mailing city* finds nothing and SOUTHLANDS does.
  expect_equal(geocode("5001 Musqueam Dr",
                       known = list(MUN_NAME = "Vancouver", PROV_ABVN = "BC"),
                       con = con)$match_method, "none")
  expect_equal(geocode("5001 Musqueam Dr",
                       known = list(MUN_NAME = "Southlands", PROV_ABVN = "BC"),
                       con = con)$ADDR_GUID, "addr9")

  # Asymmetric on purpose. MunAlias carries mailing names as routes *into* a
  # jurisdiction, so SOUTHLANDS as a CSD_NAME resolves to the Vancouver CSD and
  # answers -- a caller who names a place that is not itself a census
  # subdivision is asking about the jurisdiction it sits in, and that is a
  # question with an answer. It is the reverse direction, a jurisdiction used as
  # a mailing city, that has none.
  expect_equal(geocode("5001 Musqueam Dr",
                       known = list(CSD_NAME = "Southlands", PROV_ABVN = "BC"),
                       con = con)$ADDR_GUID, "addr9")
})

test_that("within restricts the search and refuses what falls outside it", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  inside  <- c(4011900, 2006900, 4012300, 2007100)
  outside <- c(4000000, 2000000, 4001000, 2001000)

  expect_equal(geocode("100 Grant St", within = inside, crs = NULL,
                       con = con)$match_method, "nar_building")
  expect_equal(geocode("100 Grant St", within = outside, crs = NULL,
                       con = con)$match_method, "none")
})

test_that("within also constrains the flanks interpolation may use", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # A box that holds 100 and 200 but stops short of 300. 150 is still
  # interpolable inside it; 250 is not, because the flank above it was excluded
  # and what remains would be an extrapolation.
  box <- c(4011900, 2006900, 4012150, 2007100)

  expect_equal(geocode("150 Grant St", within = box, crs = NULL,
                       con = con)$match_method, "nar_interpolated")
  expect_equal(geocode("250 Grant St", within = box, crs = NULL,
                       con = con)$match_method, "none")
})

test_that("a parsed data frame can be geocoded directly", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  parsed <- normalize_address("4001 King Edward Ave W, Vancouver, BC", con = con)
  g <- geocode(parsed, con = con)

  expect_equal(g$ADDR_GUID, "addr1")
  expect_error(geocode(data.frame(a = 1), con = con), "STREET_NAME")
})

test_that("an address NAR carries but cannot place says so", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # addr3 is a real NAR record with no coordinates of either kind, and none of
  # its neighbours on Musqueam Dr can bracket it. Reporting `none` would say the
  # address does not exist, which is a different and wrong claim -- the record
  # is named, it simply has no point.
  g <- geocode("4003 Musqueam Dr, Vancouver, BC", con = con)

  expect_equal(g$match_method, "nar_no_geometry")
  expect_equal(g$ADDR_GUID, "addr3")
  expect_true(is.na(g$lon))
})

test_that("results come back in input order, one row per input", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  x <- c("400 Grant St, Vancouver, BC",           # refused
         "4001 King Edward Ave W, Vancouver, BC", # exact
         "not an address at all",                 # unparseable
         "150 Grant St, Vancouver, BC")           # interpolated

  g <- geocode(x, con = con)

  expect_equal(nrow(g), 4)
  expect_equal(g$input, x)
  expect_equal(g$match_method,
               c("none", "nar_building", "none", "nar_interpolated"))
})

test_that("an authoritative constraint must be length 1 or length(x)", {
  expect_error(nar_recycle(c("BC", "ON"), 3, "prov"), "length 1 or length 3")
  expect_equal(nar_recycle("BC", 3, "prov"), rep("BC", 3))
})

test_that("within rejects a shape it cannot read", {
  expect_error(nar_geocode_bounds("somewhere", 4326, NULL), "length-4 numeric")
})

test_that("a matched record reports its own postal code, not the input's", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  g <- geocode("4001 King Edward Ave W, Vancouver, BC", con = con)

  # The string carried no postal code, so the parsed column stays empty. The
  # two columns are not two attempts at the same thing: POSTAL_CODE is what was
  # said and match_postal_code is what was found.
  expect_true(is.na(g$POSTAL_CODE))
  expect_equal(g$match_postal_code, "V6S1N3")

  # An address NAR holds without coordinates still has a postal code, so the
  # tier that reports nar_no_geometry reports one too.
  g3 <- geocode("4003 Musqueam Dr, Southlands, BC", con = con,
                method = "nar")
  expect_equal(g3$match_method, "nar_no_geometry")
  expect_equal(g3$match_postal_code, "V6N3T7")
})

test_that("only a tier that matched a record fills the postal code", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # Interpolation places the address between two records rather than resolving
  # to one, and the flanks need not share a postal code, so it reports none.
  g <- geocode("150 Grant St, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_interpolated")
  expect_true(is.na(g$match_postal_code))

  expect_true(is.na(geocode("1 Nowhere Rd, Vancouver, BC", con = con)$match_postal_code))
})

test_that("candidates that disagree on the postal code report none", {
  skip_if_no_duckdb_spatial()
  # The fixture's `units` knob adds a second unit at addr1's civic number, at
  # the same point but in a different postal code -- the split-building case.
  con <- local_nar_connection(units = TRUE)

  # n_matches still says 1, because there is only one *point*; the postal code
  # is a separate question, and nothing in the query says which unit was meant.
  g <- geocode("4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_building")
  expect_equal(g$n_matches, 1L)
  expect_true(is.na(g$match_postal_code))
})

test_that("n_records counts addresses where n_matches counts points", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  # The two are the whole reason both columns exist. One place, two addresses:
  # the units of a multi-unit building are separate NAR records sharing a
  # coordinate, and n_matches alone reports that as unambiguous -- which it is,
  # spatially, and is not as an answer to "which address is this".
  g <- geocode("4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(g$n_matches, 1L)
  expect_equal(g$n_records, 2L)

  # A record the fixture holds once is 1 and not 0: n_records is a count of
  # what matched, not a count of what was surplus.
  g2 <- geocode("4003 Musqueam Dr, Southlands, BC", con = con, method = "nar")
  expect_equal(g2$match_method, "nar_no_geometry")
  expect_equal(g2$n_records, 1L)
})

test_that("a supplied unit narrows the records it matches", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  # Naming the unit answers the question the two records disagreed on, so the
  # postal code the aggregate had to decline on is now reportable.
  g <- geocode("202-4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_building")
  expect_equal(g$n_records, 1L)
  expect_equal(g$match_postal_code, "V6S1N4")

  # The other unit is the blank one, which is a unit like any other here.
  expect_equal(nrow(geocode_matches("202-4001 King Edward Ave W",
                                    known = list(PROV_ABVN = "BC"),
                                    con = con)), 1L)
})

test_that("a unit NAR does not carry narrows nothing", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  # 27.5% of the units supplied by real filings are not in NAR at that civic
  # number. Narrowing unconditionally would take every one of those addresses
  # from placed to unplaced, so a unit that matches nothing is dropped rather
  # than enforced -- the filter narrows or it does nothing.
  g <- geocode("999-4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_building")
  expect_equal(g$n_records, 2L)
  expect_true(is.na(g$match_postal_code))

  # And the point is the one the address would have had anyway.
  base <- geocode("4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(g$x, base$x)
})

test_that("the unit fold spells the words NAR abbreviates", {
  # NAR writes a basement as BSMT and a filing writes it as Basement, or as
  # Sous-sol in Quebec. Comparing the two raw would match neither, and these
  # are the only unit labels that are words rather than numbers.
  expect_equal(nar_unit_fold(c("202", "2 02", "Apt. 3")), c("202", "202", "APT3"))
  expect_equal(nar_unit_fold(c("Basement", "SOUS-SOL", "Sous sol")),
               c("BSMT", "BSMT", "BSMT"))
  expect_equal(nar_unit_fold(c("Upper", "lower")), c("UPPR", "LWR"))
  # An absent unit stays absent; the probe blanks it, and a blank one is what
  # the filter reads as "no unit was supplied".
  expect_true(is.na(nar_unit_fold(NA_character_)))
})

test_that("a tier that matched no record reports no records", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # Interpolation stands between two records rather than on one, so there is
  # nothing to count -- 0, the same as an address that was not placed at all.
  g <- geocode("150 Grant St, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_interpolated")
  expect_equal(g$n_records, 0L)

  expect_equal(geocode("1 Nowhere Rd, Vancouver, BC", con = con)$n_records, 0L)
})

test_that("the exact query counts points and records separately", {
  sql <- nar_geocode_exact_sql("probe")
  expect_match(sql, "count(DISTINCT c.x::VARCHAR || ',' || c.y::VARCHAR) AS n_points",
               fixed = TRUE)
  expect_match(sql, "count(DISTINCT c.ADDR_GUID) AS n_records", fixed = TRUE)
})

test_that("the postal-code aggregate folds a missing value into the agreement", {
  # count(DISTINCT) skips NULLs, so without the coalesce a set of one value and
  # one NULL would report the value as agreed.
  sql <- nar_geocode_postal_sql("c.PC")
  expect_match(sql, "count\\(DISTINCT coalesce\\(c\\.PC, ''\\)\\)")
  expect_match(sql, "nullif\\(min\\(coalesce\\(c\\.PC, ''\\)\\), ''\\)")
  expect_match(sql, "AS match_postal_code$")
})

test_that("a batch nothing in it can be looked up returns none, not an error", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # A civic number and a municipality but no street, so nothing is keepable and
  # the probe has zero rows. Every tier has to be able to decline that: the
  # probe's unconstrained columns were length-one literals, which do not
  # recycle down to zero rows, so building it raised a data.frame() error about
  # differing row counts rather than reporting a failure to match.
  g <- geocode("49321, BRAZEAU COUNTY, AB", con = con)
  expect_equal(g$match_method, "none")
  expect_true(is.na(g$lon))

  # The same, with the municipality authoritative -- which swaps which of the
  # two probe columns is the literal.
  expect_equal(geocode("49321, SOMEWHERE, AB", con = con,
                       known = list(CSD_NAME = "Vancouver"))$match_method,
               "none")

  # And mixed, since a batch is only as parseable as its worst row.
  g2 <- geocode(c("49321, BRAZEAU COUNTY, AB",
                  "4001 King Edward Ave W, Vancouver, BC"), con = con)
  expect_equal(g2$match_method, c("none", "nar_building"))
})

test_that("an empty input is answered with an empty result", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection()

  # Geocoding a vector a filter emptied is a normal thing to do. rbind() of
  # nothing is a zero-column matrix rather than a zero-row data frame, and a
  # length-one NA does not assign into a zero-row frame, so both ends of the
  # pipeline had to be told what no rows looks like.
  g <- geocode(character(0), con = con)
  expect_equal(nrow(g), 0L)
  expect_true(all(c("match_method", "match_postal_code", "lon", "lat") %in% names(g)))

  norm <- normalize_address(character(0), con = con)
  expect_equal(nrow(norm), 0L)
  expect_equal(names(norm), names(normalize_address("1 Main St, Vancouver, BC",
                                                    con = con)))
})

test_that("geocode_matches returns one row per NAR record", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  m <- geocode_matches("4001 King Edward Ave W, Vancouver, BC", con = con)

  # The fixture's two units at one civic number, which geocode() reports as
  # n_records = 2 and n_matches = 1.
  expect_equal(nrow(m), 2L)
  expect_equal(m$input_id, c(1L, 1L))
  expect_equal(m$match_rank, 1:2)
  expect_setequal(m$MAIL_POSTAL_CODE, c("V6S1N3", "V6S1N4"))
  expect_true(all(c("APT_NO_LABEL", "LOC_GUID", "lon", "lat") %in% names(m)))
})

test_that("the first match is the record geocode() answered with", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  # The invariant the shared rank exists for: both queries collapse the same
  # candidate set in the same order, so rank 1 is not merely usually the same
  # record -- it is the same expression that chose it.
  x <- c("4001 King Edward Ave W, Vancouver, BC",
         "4003 Musqueam Dr, Southlands, BC")
  g <- geocode(x, con = con, method = "nar")
  m <- geocode_matches(x, con = con)
  first <- m[m$match_rank == 1, ]
  expect_equal(first$ADDR_GUID, g$ADDR_GUID[first$input_id])

  # And the count agrees with what geocode() reported without enumerating.
  expect_equal(as.integer(table(factor(m$input_id, levels = seq_along(x)))),
               g$n_records)
})

test_that("an address only another tier can place has no matches", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(run = TRUE)

  # Interpolation resolves to no record, so there is nothing to enumerate --
  # zero rows, not one row of NAs, and not an error.
  g <- geocode("150 Grant St, Vancouver, BC", con = con)
  expect_equal(g$match_method, "nar_interpolated")

  m <- geocode_matches("150 Grant St, Vancouver, BC", con = con)
  expect_equal(nrow(m), 0L)
  # The columns are still the query's own, because the empty probe is run
  # rather than short-circuited.
  expect_true(all(nar_geocode_match_cols() %in% names(m)))

  expect_equal(nrow(geocode_matches(character(0), con = con)), 0L)
  expect_equal(nrow(geocode_matches("49321, BRAZEAU COUNTY, AB", con = con)), 0L)
})

test_that("geocode_matches takes the same constraints and shapes as geocode", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(units = TRUE)

  # A parsed data frame is a supported way in, and carries no `input` column.
  m <- geocode_matches(normalize_address("4001 King Edward Ave W, Vancouver, BC")[
                         , setdiff(names(normalize_address("4001 King Edward Ave W")), "input")],
                       con = con)
  expect_equal(nrow(m), 2L)
  expect_true(all(is.na(m$input)))

  expect_s3_class(geocode_matches("4001 King Edward Ave W, Vancouver, BC",
                                  con = con, geometry = TRUE), "sf")

  # An authoritative municipality constrains the enumeration exactly as it
  # constrains the answer -- same probe, same setup.
  expect_equal(nrow(geocode_matches(
    "4001 King Edward Ave W",
    known = list(CSD_NAME = "Vancouver", PROV_ABVN = "BC"), con = con)), 2L)
  expect_equal(nrow(geocode_matches(
    "4001 King Edward Ave W",
    known = list(CSD_NAME = "Toronto", PROV_ABVN = "ON"), con = con)), 0L)
})

test_that("both readings of the candidate set share their ordering", {
  # Not a behavioural test -- a textual one, because the point of factoring the
  # rank out is that it cannot be edited in one query and not the other.
  rank <- nar_geocode_rank_sql(nar_geocode_nar_rank())
  expect_true(grepl(rank, nar_geocode_exact_sql("probe"), fixed = TRUE))
  expect_true(grepl(rank, nar_geocode_matches_sql("probe"), fixed = TRUE))

  civic <- nar_geocode_civic_key()
  expect_true(grepl(civic, nar_geocode_exact_sql("probe"), fixed = TRUE))
  expect_true(grepl(civic, nar_geocode_matches_sql("probe"), fixed = TRUE))

  # The unit narrowing is shared for the same reason: an enumeration that kept
  # the units the answer dropped would not be showing what was answered from.
  unit <- nar_geocode_unit_hit("a.APT_NO_LABEL")
  expect_true(grepl(unit, nar_geocode_exact_sql("probe"), fixed = TRUE))
  expect_true(grepl(unit, nar_geocode_matches_sql("probe"), fixed = TRUE))

  # The enumeration ranks without collapsing to the best row; the answer
  # collapses on that rank. Both carry the unit filter's own QUALIFY, so what
  # separates them is the collapse and not the keyword.
  collapse <- paste0("QUALIFY ", rank, " = 1")
  expect_false(grepl(collapse, nar_geocode_matches_sql("probe"), fixed = TRUE))
  expect_true(grepl(collapse, nar_geocode_exact_sql("probe"), fixed = TRUE))
})

test_that("gazetteer arguments reach the parse from geocode()'s dots", {
  d <- nar_gazetteer_dots(list(keep_refused = TRUE, mun_swap_penalty = 0.9,
                               rate = 1, api_key = "x"))
  # The gazetteer's own arguments and nothing else -- an online tier's are
  # sorted out separately, and would error here.
  expect_equal(d, list(keep_refused = TRUE, mun_swap_penalty = 0.9)[names(d)])
  expect_setequal(names(d), c("keep_refused", "mun_swap_penalty"))
})
