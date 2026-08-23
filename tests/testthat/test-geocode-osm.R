# The OpenStreetMap binding. Nothing here touches the network: every test runs
# against a response captured from the live Canada-hosted Nominatim and saved
# under fixtures/.
#
# The six fixtures are the outcomes that matter, and the interesting thing about
# them is how little they resemble the geolocator's. That service always answers
# and its wrong answers are confident, so four of its six fixtures are wrong
# answers a naive binding would accept. This one refuses instead:
#
#   building  -- a house-level building carrying its own house number.
#   duplicate -- ONE address returned as TWO OSM objects, the building and an
#                office inside it, 8 m apart. Not an ambiguity, and the reason
#                n_matches counts addresses rather than results.
#   french    -- `1 Rue Notre-Dame Ouest, Montreal`, which the geolocator
#                answers with a street 500 km away and this one gets right.
#   road      -- `28 Silver ST, CORNER BROOK`, which the geolocator answers with
#                `28 Brook Street` -- a different street. Here it comes back as
#                the road itself at rank 26 and no house number: a refusal.
#   empty     -- `[]`. The service saying it has nothing, which the geolocator
#                never does.
#   error     -- `{"error": {...}}`, the object body, under an HTTP 400.

osm_fixture <- function(name) {
  path <- testthat::test_path("fixtures", paste0("osm-", name, ".json"))
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

# The components a query would have carried, without needing a gazetteer.
osm_parts <- function(civic, name, type = NA, dir = NA, mun = NA, prov = NA) {
  data.frame(CIVIC_NO = civic, STREET_NAME = name, STREET_TYPE = type,
             STREET_DIR = dir, MUN_NAME = mun, PROV_ABVN = prov,
             stringsAsFactors = FALSE)
}

# One address, every candidate the service returned for it.
osm_floors <- function(name, q) nar_osm_floors(nar_osm_candidates(
  osm_fixture(name)), q)

test_that("the fields the service separated are read as fields", {
  cand <- nar_osm_candidates(osm_fixture("building"))

  expect_equal(nrow(cand), 1)
  expect_equal(cand$osm_rank, 30L)
  expect_equal(cand$osm_house_number, "990")
  expect_equal(cand$osm_road, "Bute Street")
  expect_equal(cand$osm_mun, "Vancouver")
  # From `ISO3166-2-lvl4`, not from the prose `British Columbia`.
  expect_equal(cand$osm_prov, "BC")
  # Strings in the body, numbers here.
  expect_equal(cand$lon, -123.1296635)
  expect_equal(cand$lat, 49.2839931)
  expect_match(cand$osm_licence, "ODbL")
})

test_that("a house-level answer that agrees is accepted", {
  q <- osm_parts(990, "BUTE", "ST", NA, "VANCOUVER", "BC")
  r <- osm_floors("building", q)

  expect_equal(r$match_method, "osm")
  expect_true(is.na(r$osm_reject))
  expect_equal(r$n_matches, 1L)
  expect_equal(r$lon, -123.1296635)
  # Not measured, and so not asserted. See nar_osm_uncertainty_m().
  expect_true(is.na(r$uncertainty_m))
})

test_that("one address returned as two OSM objects counts once", {
  # The building and the law office inside it. Same house number, same road,
  # same city; counting two would report an ambiguity that does not exist.
  cand <- nar_osm_candidates(osm_fixture("duplicate"))
  expect_equal(nrow(cand), 2)
  expect_equal(cand$osm_category, c("building", "office"))
  expect_equal(unique(cand$osm_house_number), "1155")

  q <- osm_parts(1155, "ROBSON", "ST", NA, "VANCOUVER", "BC")
  r <- nar_osm_floors(cand, q)
  expect_equal(r$match_method, "osm")
  expect_equal(r$n_matches, 1L)
  # The best-ranked survivor is the one returned.
  expect_equal(r$lon, cand$lon[1])
})

test_that("a street the service found without the civic number is refused", {
  # The geolocator answers this one with `28 Brook Street, Corner Brook`, a
  # different street. This service returns the road and no house number.
  q <- osm_parts(28, "SILVER", "ST", NA, "CORNER BROOK", "NL")
  r <- osm_floors("road", q)

  expect_equal(r$match_method, "none")
  expect_equal(r$osm_reject, "best result is highway at rank 26")
  expect_equal(r$n_matches, 0L)
  expect_true(is.na(r$lon))
  # What was thrown away stays visible.
  expect_match(r$osm_title, "Silver Street")
})

test_that("no answer at all is a row, and says so", {
  q <- osm_parts(99999, "NOWHERE", "RD", NA, "NOWHEREVILLE", "SK")
  r <- osm_floors("empty", q)

  expect_equal(nrow(r), 1)
  expect_equal(r$match_method, "none")
  expect_equal(r$osm_reject, "no answer")
  expect_true(is.na(r$osm_title))
})

test_that("an error body is no answer rather than one result", {
  # `{"error": {"code": 400, ...}}`. Read as a result it would take the object
  # for a candidate; the named-list test is what separates it from the array.
  cand <- nar_osm_candidates(osm_fixture("error"))
  expect_equal(nrow(cand), 0)

  q <- osm_parts(1, "MAIN", "ST", NA, "OTTAWA", "ON")
  r <- nar_osm_floors(cand, q)
  expect_equal(r$osm_reject, "no answer")
})

test_that("a wrong municipality is rejected even at house level", {
  # The answer is a real 990 Bute Street. It is just not in Burnaby.
  q <- osm_parts(990, "BUTE", "ST", NA, "BURNABY", "BC")
  r <- osm_floors("building", q)

  expect_equal(r$match_method, "none")
  expect_match(r$osm_reject, "^municipality BURNABY")
})

test_that("a wrong civic number is rejected", {
  q <- osm_parts(992, "BUTE", "ST", NA, "VANCOUVER", "BC")
  r <- osm_floors("building", q)

  expect_equal(r$match_method, "none")
  expect_match(r$osm_reject, "^civic number 992")
})

test_that("the municipality is taken from the answer, not from the title", {
  # The display_name runs `The Berkeley, 990, Bute Street, Davie Village, West
  # End, Vancouver, ...`. Parsing that back would have to get past a building
  # name and two sub-municipal localities; the service already separated them.
  q <- osm_parts(990, "BUTE", "ST", NA, "WEST END", "BC")
  r <- osm_floors("building", q)

  expect_equal(r$match_method, "none")
  expect_match(r$osm_reject, "^municipality WEST END")
})

test_that("the locality fields below a municipality are not read as one", {
  expect_equal(nar_osm_mun(list(city = "Vancouver", suburb = "West End")),
               "Vancouver")
  expect_equal(nar_osm_mun(list(town = "Corner Brook")), "Corner Brook")
  expect_equal(nar_osm_mun(list(village = "Field")), "Field")
  # A suburb alone is not a municipality.
  expect_true(is.na(nar_osm_mun(list(suburb = "West End",
                                     neighbourhood = "Davie Village"))))
})

test_that("candidates are kept with the address they answer", {
  cand <- rbind(nar_osm_candidates(osm_fixture("building")),
                nar_osm_candidates(osm_fixture("road")))
  q <- rbind(osm_parts(990, "BUTE", "ST", NA, "VANCOUVER", "BC"),
             osm_parts(28, "SILVER", "ST", NA, "CORNER BROOK", "NL"))
  r <- nar_osm_floors(cand, q, idx = c(1L, 2L))

  expect_equal(r$match_method, c("osm", "none"))
  expect_equal(r$osm_reject, c(NA, "best result is highway at rank 26"))
})

test_that("a lost request is not the same as an empty answer", {
  # Both are zero candidates, and only one of them is about the address.
  q <- rbind(osm_parts(1, "MAIN", "ST", NA, "OTTAWA", "ON"),
             osm_parts(1, "MAIN", "ST", NA, "OTTAWA", "ON"))
  r <- nar_osm_floors(nar_osm_candidates(list()), q, integer(0),
                      failed = c(TRUE, FALSE))

  expect_equal(r$osm_reject, c("request failed", "no answer"))
  expect_equal(r$match_method, c("none", "none"))
})

test_that("a French address is sent in an order the service can match", {
  # Measured: `1 NOTRE-DAME RUE O` returns nothing, `1 RUE NOTRE-DAME OUEST`
  # returns the address. The type leads and the direction is spelled out --
  # nothing in Nominatim expands `O` to `Ouest`, though it does expand `W`.
  fr <- osm_parts(1, "NOTRE-DAME", "RUE", "O", "MONTREAL", "QC")
  expect_equal(nar_osm_street(fr), "1 RUE NOTRE-DAME OUEST")

  en <- osm_parts(100, "QUEEN", "ST", "W", "TORONTO", "ON")
  expect_equal(nar_osm_street(en), "100 QUEEN ST W")

  # The suffix stays glued to the number: OSM stores house_number as free text.
  sfx <- osm_parts(990, "BUTE", "ST", NA, "VANCOUVER", "BC")
  sfx$CIVIC_NO_SUFFIX <- "A"
  expect_equal(nar_osm_street(sfx), "990A BUTE ST")
})

test_that("the French answer that the geolocator gets wrong is accepted", {
  q <- osm_parts(1, "NOTRE-DAME", "RUE", "O", "MONTREAL", "QC")
  r <- osm_floors("french", q)

  expect_equal(r$match_method, "osm")
  # Montreal, not the Lorrainville street 500 km away.
  expect_equal(round(r$lon, 3), -73.556)
})

test_that("the query is structured unless it is asked not to be", {
  res <- osm_parts(1, "NOTRE-DAME", "RUE", "O", "MONTREAL", "QC")

  expect_equal(nar_osm_query(res)[[1]],
               list(street = "1 RUE NOTRE-DAME OUEST", city = "MONTREAL",
                    state = "QC"))
  expect_equal(nar_osm_query(res, structured = FALSE)[[1]],
               list(q = "1 RUE NOTRE-DAME OUEST, MONTREAL, QC"))

  # A supplied element is a requirement, so an absent one must stay absent
  # rather than being sent empty.
  bare <- osm_parts(1, "NOTRE-DAME", "RUE", "O", NA, NA)
  expect_equal(names(nar_osm_query(bare)[[1]]), "street")
})

test_that("a dropped request is recognized as transient and a bad one is not", {
  skip_if_not_installed("httr2")
  json <- function(status, body) httr2::response(
    status_code = status, headers = list(`content-type` = "application/json"),
    body = charToRaw(body))

  expect_true(nar_osm_transient(json(500, "")))
  expect_true(nar_osm_transient(json(429, "")))
  expect_true(nar_osm_transient(json(200, '{"error": {"code": 500}}')))
  # The service labels a malformed query itself, and re-sending it would only
  # get the same answer three times.
  expect_false(nar_osm_transient(json(400, '{"error": {"code": 400}}')))
  # Nothing found is an answer, not a loss.
  expect_false(nar_osm_transient(json(200, "[]")))
  expect_false(nar_osm_transient(json(200, '[{"place_rank": 30}]')))
})
