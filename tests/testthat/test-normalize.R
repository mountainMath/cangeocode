# Layer 1 needs no database at all; only the gazetteer tests take a connection.

test_that("normalize_address splits the unit off the civic number", {
  # Every one of these is a form a local LLM got wrong in testing: the unit is
  # silently swallowed and the civic number lost, which then fails to join.
  r <- normalize_address(c("302-1055 W Georgia St, Vancouver, BC",
                           "Apt 302, 1055 W Georgia St, Vancouver, BC",
                           "#302 1055 W Georgia St, Vancouver, BC",
                           "302 - 1055 W Georgia St, Vancouver, BC",
                           "1055 W Georgia St Suite 302, Vancouver, BC"))
  expect_equal(r$APT_NO_LABEL, rep("302", 5))
  expect_equal(r$CIVIC_NO, rep(1055, 5))
  expect_equal(r$STREET_NAME, rep("GEORGIA", 5))
  expect_equal(r$STREET_TYPE, rep("ST", 5))
  expect_equal(r$STREET_DIR, rep("W", 5))
})

test_that("a designator in front of a hyphenated unit-civic still splits it", {
  # "SUITE 800-666 BURRARD ST" is the standard Canadian office form, and the
  # designator used to make the parse *worse* than leaving it out: the bare
  # "800-666" split correctly while the spelled-out version took the whole
  # hyphenated token as the unit and returned no civic number at all. A civic
  # number lost here cannot be recovered downstream -- the row simply stops
  # joining.
  r <- normalize_address(c("800-666 Burrard St, Vancouver, BC",
                           "Suite 800-666 Burrard St, Vancouver, BC",
                           "Suite 800 - 666 Burrard St, Vancouver, BC",
                           "Unit 800-666 Burrard St, Vancouver, BC",
                           "#800-666 Burrard St, Vancouver, BC"))
  expect_equal(r$APT_NO_LABEL, rep("800", 5))
  expect_equal(r$CIVIC_NO, rep(666, 5))
  expect_equal(r$STREET_NAME, rep("BURRARD", 5))
})

test_that("a designator whose value is a word is untouched by that split", {
  # nar_split_unit_civic() returns the token unchanged when it does not split,
  # so the APT BSMT / APT TRLR forms have to survive the new route through it.
  r <- normalize_address(c("APT BSMT 1055 W Georgia St, Vancouver, BC",
                           "Apt 302, 1055 W Georgia St, Vancouver, BC"))
  expect_equal(r$APT_NO_LABEL, c("BSMT", "302"))
  expect_equal(r$CIVIC_NO, c(1055, 1055))
})

test_that("a hyphen left standing alone is a separator, not part of the name", {
  # nar_norm_text() joins "302 - 1055" because a bare number follows, but
  # declines on "1688 - 152nd" because "152ND" is not one. The hyphen then
  # survived as its own token and became the first word of the street name.
  r <- normalize_address(c("1688 - 152nd Street, Surrey, BC",
                           "100 Main St - Apt 5, Toronto, ON"))
  expect_equal(r$STREET_NAME, c("152ND", "MAIN"))
  expect_equal(r$CIVIC_NO, c(1688, 100))
  expect_equal(r$APT_NO_LABEL, c(NA, "5"))

  # Hyphens inside a token are untouched -- that is most of Quebec.
  expect_equal(normalize_address("12 St-Jean, Quebec, QC")$STREET_NAME,
               "ST-JEAN")
})

test_that("street type and direction canonicalize by province language", {
  # The same word normalizes differently either side of the Ottawa river:
  # NAR writes AVE/BLVD/W in Ontario and AV/BOUL/O in Quebec.
  on <- normalize_address("100 Principale Avenue West, Ottawa, ON")
  qc <- normalize_address("100 Principale Avenue West, Gatineau, QC")
  expect_equal(on$STREET_TYPE, "AVE")
  expect_equal(qc$STREET_TYPE, "AV")
  expect_equal(on$STREET_DIR, "W")
  expect_equal(qc$STREET_DIR, "O")

  expect_equal(normalize_address("1 Boulevard Charest, Quebec, QC")$STREET_TYPE, "BOUL")
  expect_equal(normalize_address("1 Boulevard Main, Toronto, ON")$STREET_TYPE, "BLVD")
})

test_that("French street types lead the name and English types trail it", {
  fr <- normalize_address("12 Rue Notre-Dame Est, Montreal, QC")
  expect_equal(fr$STREET_NAME, "NOTRE-DAME")
  expect_equal(fr$STREET_TYPE, "RUE")
  expect_equal(fr$STREET_DIR, "E")

  en <- normalize_address("12 Queen Street East, Toronto, ON")
  expect_equal(en$STREET_NAME, "QUEEN")
  expect_equal(en$STREET_TYPE, "ST")
  expect_equal(en$STREET_DIR, "E")
})

test_that("a street type word inside the name is not mistaken for the type", {
  # PARK and GREEN are both real NAR types, so a naive left-to-right scan takes
  # them and orphans the actual type.
  expect_equal(normalize_address("44 High Park Ave, Toronto, ON")$STREET_NAME, "HIGH PARK")
  expect_equal(normalize_address("44 High Park Ave, Toronto, ON")$STREET_TYPE, "AVE")
  expect_equal(normalize_address("44 Green Lane, Thornhill, ON")$STREET_NAME, "GREEN")
  expect_equal(normalize_address("44 Green Lane, Thornhill, ON")$STREET_TYPE, "LANE")
  # A street whose whole name is a type word keeps its name.
  expect_equal(normalize_address("44 Park, Toronto, ON")$STREET_NAME, "PARK")
})

test_that("civic suffixes are read only in the forms NAR stores", {
  # CIVIC_NO_SUFFIX holds a single letter or 1/2 and nothing else.
  a <- normalize_address("990A King Edward Ave, Ottawa, ON")
  expect_equal(a$CIVIC_NO, 990)
  expect_equal(a$CIVIC_NO_SUFFIX, "A")

  h <- normalize_address("12 1/2 Rue Notre-Dame, Montreal, QC")
  expect_equal(h$CIVIC_NO, 12)
  expect_equal(h$CIVIC_NO_SUFFIX, "1/2")

  # A *spaced* letter is a direction, not a suffix -- NAR has 235 W suffixes
  # against hundreds of thousands of W directions.
  w <- normalize_address("1055 W Georgia St, Vancouver, BC")
  expect_true(is.na(w$CIVIC_NO_SUFFIX))
  expect_equal(w$STREET_DIR, "W")
})

test_that("postal codes are extracted to NAR's six-character form", {
  r <- normalize_address(c("1055 W Georgia St, Vancouver, BC V6E 3P3",
                           "12 Rue Notre-Dame, Montreal, QC H2Y1C6",
                           "1 Main St, Halifax, NS"))
  expect_equal(r$POSTAL_CODE, c("V6E3P3", "H2Y1C6", NA))
})

test_that("a postal code lands on the row it came from", {
  # regmatches() returns one element per match rather than per input, so a
  # careless subset shifts every code onto a later row.
  r <- normalize_address(c("1 Main St, Halifax, NS",
                           "2 Main St, Halifax, NS B3H 1A1",
                           "3 Main St, Halifax, NS"))
  expect_equal(r$POSTAL_CODE, c(NA, "B3H1A1", NA))
})

test_that("province names are read only where a province can be", {
  expect_equal(normalize_address("1 Main St, Kingston, Ontario")$PROV_ABVN, "ON")
  expect_equal(normalize_address("1 Main St, Montreal, Quebec")$PROV_ABVN, "QC")
  expect_equal(normalize_address("1 Main St, Vancouver, British Columbia")$PROV_ABVN, "BC")
  # "Ontario Street" in Kingston is a street, not a province.
  r <- normalize_address("55 Ontario Street, Kingston, ON")
  expect_equal(r$STREET_NAME, "ONTARIO")
  expect_equal(r$MUN_NAME, "KINGSTON")
})

test_that("accents survive normalization", {
  # NAR stores MONTÉE and CÔTE accented, so folding is a matching device only.
  expect_equal(normalize_address("45 Montee du Lac, Saint-Sauveur, QC")$STREET_TYPE, "MONTÉE")
  expect_equal(normalize_address("45 Montée du Lac, Saint-Sauveur, QC")$STREET_TYPE, "MONTÉE")
  expect_equal(normalize_address("45 Cote Sainte-Catherine, Montreal, QC")$STREET_TYPE, "CÔTE")
})

test_that("comma-less addresses still split street from municipality", {
  r <- normalize_address("unit 4b 100 queen street west toronto on")
  expect_equal(r$APT_NO_LABEL, "4B")
  expect_equal(r$CIVIC_NO, 100)
  expect_equal(r$STREET_NAME, "QUEEN")
  expect_equal(r$STREET_TYPE, "ST")
  expect_equal(r$STREET_DIR, "W")
  expect_equal(r$MUN_NAME, "TORONTO")
  expect_equal(r$PROV_ABVN, "ON")
})

test_that("bare unit labels are kept verbatim", {
  # BSMT alone accounts for 137,413 NAR rows.
  expect_equal(normalize_address("BSMT 44 High Park Ave, Toronto, ON")$APT_NO_LABEL, "BSMT")
})

test_that("normalize_address returns one row per input and the documented shape", {
  x <- c("1 Main St, Halifax, NS", "", NA_character_, "not an address at all")
  r <- normalize_address(x)
  expect_equal(nrow(r), length(x))
  expect_equal(r$input, x)
  expect_true(all(nar_normalized_columns() %in% names(r)))
  expect_true(all(c("confidence", "parse_source") %in% names(r)))
  expect_true(all(r$confidence >= 0 & r$confidence <= 1))
})

test_that("normalize_address rejects non-character input", {
  expect_error(normalize_address(list(1, 2)), "character vector")
})

# --- Layer 2 ---------------------------------------------------------------

test_that("the gazetteer resolves against the streets NAR actually has", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # The fixture holds three addresses on KING EDWARD AVE W in Vancouver.
  r <- normalize_address("4001 King Edward Avenue West, Vancouver, BC", con = con)
  expect_equal(r$STREET_NAME, "KING EDWARD")
  expect_equal(r$STREET_TYPE, "AVE")
  expect_equal(r$STREET_DIR, "W")
  expect_equal(r$MUN_NAME, "VANCOUVER")
  expect_equal(r$parse_source, "gazetteer")

  # A misspelling the rules cannot see is corrected by the gazetteer.
  m <- normalize_address("4001 King Edwrd Avenue West, Vancouver, BC", con = con)
  expect_equal(m$STREET_NAME, "KING EDWARD")
  expect_equal(m$parse_source, "gazetteer")
})

test_that("the gazetteer declines rather than substituting a wrong street", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # Nothing in the fixture resembles this, so the rules result must stand.
  r <- normalize_address("100 Zzyzx Boulevard, Vancouver, BC", con = con)
  expect_equal(r$parse_source, "rules")
  expect_equal(r$STREET_NAME, "ZZYZX")
})

test_that("a postal code supplies the municipality when the string omits it", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  r <- normalize_address("4001 King Edward Avenue West, V6S 1N3", con = con)
  expect_equal(r$MUN_NAME, "VANCOUVER")
  expect_equal(r$STREET_NAME, "KING EDWARD")
})

test_that("gazetteer resolution is skipped, with a warning, on older databases", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)
  # The connection is read-only, so the table cannot actually be dropped; mock
  # the capability probe instead.
  local_mocked_bindings(nar_has_streets = function(con) FALSE)

  expect_warning(r <- normalize_address("4001 King Edward Ave W, Vancouver, BC", con = con),
                 "schema version 4")
  expect_equal(r$parse_source, "rules")
})

test_that("a missing postal code costs nothing when the string names the place", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # The postal code is never an input the normalizer needs: it only ever stands
  # in for a municipality the string did not name. With one named, dropping it
  # -- and the province with it -- must change nothing.
  full <- normalize_address("4001 King Edward Ave W, Vancouver, BC V6S 1N3", con = con)
  none <- normalize_address("4001 King Edward Ave W, Vancouver", con = con)

  fields <- c("CIVIC_NO", "STREET_NAME", "STREET_TYPE", "STREET_DIR",
              "MUN_NAME", "PROV_ABVN", "confidence", "parse_source")
  expect_equal(none[fields], full[fields])
  expect_equal(none$parse_source, "gazetteer")
  expect_true(is.na(none$POSTAL_CODE))
})

test_that("a street with neither postal code nor municipality still resolves", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  bare <- normalize_address("4001 King Edward Ave W", con = con)

  expect_equal(bare$parse_source, "gazetteer")
  expect_equal(bare$STREET_NAME, "KING EDWARD")
  expect_equal(bare$STREET_TYPE, "AVE")
  expect_equal(bare$STREET_DIR, "W")
  # Scored below a match that had a locality to confirm it against.
  expect_lt(bare$confidence, 1)
  # The locality is not guessed -- but only one municipality in the fixture has
  # a King Edward Ave, so there is nothing left to guess. Determined, not
  # inferred; the ambiguous case is the next test.
  expect_equal(bare$MUN_NAME, "VANCOUVER")
  expect_equal(bare$PROV_ABVN, "BC")
})

test_that("an ambiguous municipality is left absent rather than guessed at", {
  skip_if_no_duckdb_spatial()
  con <- local_mini_gazetteer()

  # Doyle St is in two cities in this gazetteer and Kenmount Rd in one. Neither
  # string names a locality, so both take the exact branch -- and it answers
  # only where the answer is forced.
  two <- nar_resolve_gazetteer(nar_parse_rules("207 Doyle Street"), con)
  expect_equal(two$STREET_NAME, "Doyle")
  expect_true(is.na(two$MUN_NAME))

  one <- nar_resolve_gazetteer(nar_parse_rules("207 Kenmount Road"), con)
  expect_equal(one$STREET_NAME, "Kenmount")
  expect_equal(one$MUN_NAME, "MOUNT PEARL")
  # The province comes with it: a determined municipality always has one, even
  # though the string never said so.
  expect_equal(one$PROV_ABVN, "NL")
})

test_that("without a locality the match must be exact, not merely close", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # With a municipality to restrict candidates to, a misspelling is corrected.
  expect_equal(normalize_address("4001 King Edwrd Ave W, Vancouver", con = con)$STREET_NAME,
               "KING EDWARD")
  # Without one there is nothing to bound a fuzzy search with, so the same
  # misspelling is left alone rather than matched against all of Canada.
  loose <- normalize_address("4001 King Edwrd Ave W", con = con)
  expect_equal(loose$parse_source, "rules")
  expect_equal(loose$STREET_NAME, "KING EDWRD")
})

test_that("a civic number outside every candidate's range declines the match", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # KING EDWARD runs 4001-4002. Without a locality the civic number is the only
  # corroboration left, so one that fits no street of that name must not resolve.
  expect_equal(normalize_address("999999 King Edward Ave W", con = con)$parse_source,
               "rules")
})

test_that("a mailing city and its CSD reach each other in both directions", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # The fixture's CSD is Vancouver; KING EDWARD is mailed to VANCOUVER and
  # MUSQUEAM to SOUTHLANDS. Neither name is the other's, and a match keyed on
  # MAIL_MUN_NAME alone would find only half of each pair.

  # Writing the CSD name for a street NAR files under another mailing city ...
  csd <- normalize_address("4003 Musqueam Dr, Vancouver, BC", con = con)
  expect_equal(csd$parse_source, "gazetteer")
  expect_equal(csd$STREET_NAME, "MUSQUEAM")
  # ... and the answer comes back keyed the way NAR keys it, not the way it was
  # asked, because that is the value a join to Addresses needs.
  expect_equal(csd$MUN_NAME, "SOUTHLANDS")

  # ... and the reverse: a mailing city that is no CSD, naming a street mailed
  # to a different city in the same one.
  mail <- normalize_address("4001 King Edward Ave W, Southlands, BC", con = con)
  expect_equal(mail$parse_source, "gazetteer")
  expect_equal(mail$STREET_NAME, "KING EDWARD")
  expect_equal(mail$MUN_NAME, "VANCOUVER")
})

test_that("the name as written still outranks the jurisdiction it widens to", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection(blockface = TRUE)

  # Widening to the CSD pulls in MUSQUEAM alongside KING EDWARD. A probe that
  # names VANCOUVER exactly must not be handed the neighbouring mailing city's
  # street just because the alias join made it reachable.
  r <- normalize_address("4001 King Edward Ave W, Vancouver, BC", con = con)

  expect_equal(r$MUN_NAME, "VANCOUVER")
})

test_that("a comma-delimited unit is lifted before the municipality is chosen", {
  # The form real filings use constantly: the unit sits in its own segment,
  # which leaves the last segment as the municipality only if it comes out
  # first. Each of these read its street name as "<NAME> <unit>" before.
  r <- normalize_address(c("9320 Boulevard Saint-Laurent, 320, Montreal, QC",
                           "1980 Sherbrooke Street West, # 500, Montreal, QC",
                           "7777 Boul Decarie, 5th Floor, Montreal, QC",
                           "302, 1055 W Georgia St, Vancouver, BC"))

  expect_equal(r$APT_NO_LABEL, c("320", "500", "5TH", "302"))
  expect_equal(r$STREET_NAME, c("SAINT-LAURENT", "SHERBROOKE", "DECARIE", "GEORGIA"))
  expect_equal(r$MUN_NAME, c("MONTREAL", "MONTREAL", "MONTREAL", "VANCOUVER"))
})

test_that("lifting a unit segment never consumes the whole address", {
  # A unit segment is only ever lifted when something is left to be the
  # address. "Suite 302" alone is all there is, so it stays put.
  r <- normalize_address(c("Suite 302", "1055 W Georgia St, Suite 302"))

  expect_true(is.na(r$CIVIC_NO[1]))
  # ... and with a street present it comes out, leaving no municipality behind
  # rather than promoting the unit into one.
  expect_equal(r$APT_NO_LABEL[2], "302")
  expect_equal(r$STREET_NAME[2], "GEORGIA")
  expect_true(is.na(r$MUN_NAME[2]))
})

test_that("a trailing country does not displace the municipality", {
  # "Canada" takes the last comma segment, which is the slot the municipality
  # is read from -- so leaving it in place costs the city, not just the noise.
  r <- normalize_address(c("2805 Cedarwood Dr, Ottawa, ON, Canada",
                           "1871 13th Ave W, Vancouver, British Columbia, Canada",
                           "201-676 Richmond St W, Toronto, M6J 1C3, Canada"))

  expect_equal(r$MUN_NAME, c("OTTAWA", "VANCOUVER", "TORONTO"))
  expect_equal(r$PROV_ABVN, c("ON", "BC", NA))
  expect_equal(r$STREET_NAME, c("CEDARWOOD", "13TH", "RICHMOND"))
})

test_that("a leading bare number is the civic number, not a unit", {
  # The ordinary French form. Only the segment after it separates the two
  # readings: "845, rue de Vernon" has no other candidate for a civic number,
  # while "302, 1055 W Georgia St" already has one.
  r <- normalize_address(c("845, rue de Vernon, Gatineau, QC",
                           "253, Route 105, Chelsea, QC",
                           "302, 1055 W Georgia St, Vancouver, BC"))

  expect_equal(r$CIVIC_NO, c(845, 253, 1055))
  expect_equal(r$APT_NO_LABEL, c(NA, NA, "302"))
  expect_equal(r$MUN_NAME, c("GATINEAU", "CHELSEA", "VANCOUVER"))
})

test_that("a numbered rural road keeps its number and takes no street type", {
  r <- normalize_address(c("385074 Range Road 42, Rocky View County, AB",
                           "12 Township Road 514, Parkland County, AB",
                           "1234 Concession 5, Puslinch, ON",
                           "77 County Rd 21, Prince Edward County, ON",
                           "45 Bruce Road 3, Kincardine, ON"))
  expect_equal(r$STREET_NAME, c("RANGE ROAD 42", "TOWNSHIP ROAD 514",
                                "CONCESSION 5", "COUNTY ROAD 21",
                                "BRUCE ROAD 3"))
  expect_true(all(is.na(r$STREET_TYPE)))
  expect_equal(r$CIVIC_NO, c(385074, 12, 1234, 77, 45))
  expect_true(all(r$pattern == "numbered_road"))
})

test_that("a second number in front of a range road belongs to the name", {
  # NAR really does file 53222 Range Road 272 as one street, whose addresses
  # carry their own small civic numbers. One number is the civic number; two
  # means the first is civic and the second is part of the name.
  r <- normalize_address(c("53222 Range Road 272, Spruce Grove, AB",
                           "73 53279 Range Road 225, Sherwood Park, AB"))
  expect_equal(r$CIVIC_NO, c(53222, 73))
  expect_equal(r$STREET_NAME, c("RANGE ROAD 272", "53279 RANGE ROAD 225"))
})

test_that("Route is a typeless road in New Brunswick and a street type in Quebec", {
  # 51,000 NB addresses against 113,827 QC ones, spelled identically. Only the
  # province tells them apart, so an unknown province keeps the commoner
  # reading rather than guessing.
  r <- normalize_address(c("1585 Route 105, Fredericton, NB",
                           "253, Route 105, Chelsea, QC",
                           "1585 Route 105"))
  expect_equal(r$STREET_NAME, c("ROUTE 105", "105", "105"))
  expect_equal(r$STREET_TYPE, c(NA, "ROUTE", "RTE"))
})

test_that("a numbered highway is left to the ordinary street-type path", {
  # NAR stores Highway 7 as name 7, type HWY -- 115,175 rows. Treating it like
  # a range road would break the commonest numbered road in the country.
  r <- normalize_address("100 Highway 7, Markham, ON")
  expect_equal(r$STREET_NAME, "7")
  expect_equal(r$STREET_TYPE, "HWY")
})

test_that("the recognizer sorts each address into its most specific bucket", {
  cases <- c(
    po_box         = "PO Box 40, Iqaluit, NU X0A 0H0",
    po_box         = "General Delivery, Whitehorse, YT",
    rural_route    = "RR 3, Site 4, Comp 5, Kelowna, BC",
    intersection   = "King St W & Bay St, Toronto, ON",
    numbered_road  = "53222 Range Road 272, Spruce Grove, AB",
    grid           = "9819 96A Street NW, Edmonton, AB",
    numeric_street = "67 West 25th Ave, Vancouver, BC",
    french_street  = "845, rue de Vernon, Gatineau, QC",
    unit_civic     = "302-1055 W Georgia St, Vancouver, BC",
    civic_street   = "100 Queen Street West, Toronto, ON",
    street_only    = "Musqueam Drive, Vancouver, BC",
    postal_only    = "V6E 3P3",
    unparsed       = ""
  )
  expect_equal(as.character(address_pattern(cases)), names(cases))
})

test_that("a street name that opens with a delivery word is not a PO box", {
  # Box Grove Bypass is a real street in Markham; BOX only marks a post office
  # box when a number follows it.
  expect_equal(as.character(address_pattern("Box Grove Bypass, Markham, ON")),
               "street_only")
})

test_that("the pattern survives gazetteer resolution", {
  skip_if_no_duckdb_spatial()
  con <- local_nar_connection()
  r <- normalize_address("4001 King Edward Ave W, Vancouver, BC", con = con)
  expect_equal(r$parse_source, "gazetteer")
  expect_equal(as.character(r$pattern), "civic_street")
})

test_that("Sainte in a place name is not read as a Suite designator", {
  # STE abbreviates Suite, but it is also Sainte. Requiring a unit designator's
  # value to look like a unit number is what tells them apart: "Sault Marie" is
  # a word, "600" is not. Without this the municipality is eaten outright.
  # The requirement is confined to STE on purpose -- see the next test.
  r <- normalize_address(c("29 Hocking Ave, Sault Ste. Marie, ON",
                           "100 Main St, Ste-Foy, QC",
                           "Suite 600, 100 Main St, Calgary, AB",
                           "Ste 4B, 100 Queen St, Toronto, ON"))
  expect_equal(r$MUN_NAME, c("SAULT STE MARIE", "STE-FOY", "CALGARY", "TORONTO"))
  expect_equal(r$APT_NO_LABEL, c(NA, NA, "600", "4B"))
  expect_equal(r$CIVIC_NO, c(29, 100, 100, 100))
})

test_that("Sainte survives in a comma-less string too", {
  # The guard has to sit on both unit paths. Without commas the municipality is
  # not a segment of its own, so "Sault Ste Marie ON" reaches the trailing-unit
  # rule with STE second-from-last -- and that rule used to take whatever
  # followed a designator, leaving a unit called MARIE on a street in SAULT.
  r <- normalize_address(c("123 Main St Sault Ste Marie ON",
                           "12 Ste Anne St Ste Anne MB"))
  expect_equal(r$MUN_NAME, c("SAULT STE MARIE", "STE ANNE"))
  expect_equal(r$APT_NO_LABEL, c(NA_character_, NA_character_))
  expect_equal(r$STREET_NAME, c("MAIN", "STE ANNE"))

  # A real Suite in the same position still resolves, since 400 looks like a
  # unit number and Marie does not.
  r <- normalize_address(c("100 Queen St Ste 400", "100 Queen St Ste 4B"))
  expect_equal(r$APT_NO_LABEL, c("400", "4B"))
  expect_equal(r$STREET_NAME, c("QUEEN", "QUEEN"))
})

test_that("an unambiguous unit designator still accepts a word for a value", {
  # The Sainte guard must not spread to the other designators: APT is never a
  # place name, and the value it introduces is routinely a word rather than a
  # number. Widening the rule to every designator drops the civic number off
  # these outright, because the whole run collapses into the street name.
  r <- normalize_address(c("Apt Bsmt, 2768 Euclid Ave, Vancouver, BC",
                           "Apt Trlr, 22848 Old Yale Rd, Langley, BC"))
  expect_equal(r$APT_NO_LABEL, c("BSMT", "TRLR"))
  expect_equal(r$CIVIC_NO, c(2768, 22848))
  expect_equal(r$STREET_NAME, c("EUCLID", "OLD YALE"))
})

test_that("a period in a municipality name does not block the gazetteer", {
  skip_if_no_duckdb_spatial()
  # NAR files ST. JOHN'S, SAULT STE. MARIE and ST. ALBERT with their periods --
  # 1,027,129 addresses between them -- while nar_norm_text() strips periods
  # from input as abbreviation marks. Both sides have to be folded or those
  # cities resolve to nothing at all.
  con <- local_mini_gazetteer()
  res <- nar_parse_rules("207 Doyle Street, St. John's, NL")
  out <- nar_resolve_gazetteer(res, con)
  expect_equal(out$parse_source, "gazetteer")
  expect_equal(out$STREET_NAME, "Doyle")
  expect_equal(out$MUN_NAME, "ST. JOHN'S")
})

test_that("a municipality is anchored from the end when no comma marks it", {
  # The comma is doing all the work in "... TH25, Vancouver": drop it and the
  # left-to-right walk takes the whole tail as the municipality, because
  # nothing local distinguishes "TH25 VANCOUVER" from "100 MILE HOUSE". The
  # inventory does, so all three of these have to reach the same reading.
  r <- normalize_address(c("100 Main St TH25, Vancouver",
                           "100 Main St TH25 Vancouver",
                           "100 Main #25 Vancouver"))
  expect_equal(r$MUN_NAME, rep("VANCOUVER", 3))
  expect_equal(r$STREET_NAME, rep("MAIN", 3))
  expect_equal(r$APT_NO_LABEL, c("TH25", "TH25", "25"))
})

test_that("a multi-word municipality outranks the shorter one inside it", {
  # Anchoring tries the longest run first, and both lengths are real places --
  # MILE HOUSE is not a municipality but HOUSE is not the point: the failure
  # mode is taking a two- or three-word name apart, so the longest match that
  # leaves a street behind has to win.
  r <- normalize_address(c("100 Main St 100 Mile House BC",
                           "1234 Main St Sault Ste Marie ON"))
  expect_equal(r$MUN_NAME, c("100 MILE HOUSE", "SAULT STE MARIE"))
  expect_equal(r$STREET_NAME, c("MAIN", "MAIN"))
})

test_that("anchoring never fires on a parse that is not broken", {
  # The guard that matters. These strings name no municipality at all, and
  # every one ends in a word that is a real place -- Albanel, Nantes and Trail
  # are all municipalities. Offering an anchored reading here costs the street
  # name, and the gazetteer cannot arbitrate it back because a match restricted
  # to a real municipality outscores an unrestricted one by construction.
  r <- normalize_address(c("80 rue Albanel, QC", "135 de Nantes, QC",
                           "82 Fesroches Trail, ON"))
  expect_equal(r$MUN_NAME, rep(NA_character_, 3))
  expect_equal(r$STREET_NAME, c("ALBANEL", "DE NANTES", "FESROCHES"))
  expect_equal(r$STREET_TYPE, c("RUE", NA, "TRAIL"))

  # Nor may a bare civic number and a place name become an address in that
  # place with no street in it.
  r <- normalize_address("123 Kingston")
  expect_equal(r$STREET_NAME, "KINGSTON")
  expect_true(is.na(r$MUN_NAME))
})

test_that("a spaced hash still introduces a unit", {
  # nar_take_trailing_unit() had a branch for a hash sitting on its own token
  # that could not be reached, so "# 25" fell through to the street name while
  # "#25" resolved. nar_norm_text() splits the two forms identically once a
  # municipality follows, which is how the dead branch surfaced.
  r <- normalize_address(c("100 Main St #25", "100 Main St # 25",
                           "100 Main St # 25 Vancouver"))
  expect_equal(r$APT_NO_LABEL, rep("25", 3))
  expect_equal(r$STREET_NAME, rep("MAIN", 3))
})

test_that("a trailing unit is taken only when it cannot be part of a name", {
  # With the municipality anchored off the end, a lone token can be left over
  # where no designator introduces it. Taking any such token as a unit eats the
  # numbers that belong to numbered rural roads -- "Route 12" and "Highway 20"
  # both end in a bare number -- so only a digit-and-letter mix counts.
  r <- normalize_address(c("100 Main St TH25 Vancouver", "100 Main St PH2 Vancouver",
                           "11735 Cascumpec - Rte 12, Coleman",
                           "997 Chilcotin-Bella Coola Highway 20, Williams Lake"))
  expect_equal(r$APT_NO_LABEL, c("TH25", "PH2", NA, NA))
  expect_equal(r$MUN_NAME, c("VANCOUVER", "VANCOUVER", "COLEMAN", "WILLIAMS LAKE"))
})
