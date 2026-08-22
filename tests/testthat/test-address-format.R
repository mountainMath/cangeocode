test_that("spellings of one address collapse to one key", {
  # This is the whole point of the key: five ways of writing the same tower,
  # differing in abbreviation, case, punctuation, unit form and postal code.
  x <- c("1055 W Georgia St, Vancouver, BC",
         "1055 West Georgia Street, Vancouver, British Columbia",
         "Suite 1500 - 1055 W Georgia St, Vancouver BC",
         "#1500-1055 west georgia st., vancouver, b.c.",
         "1055 WEST GEORGIA ST, VANCOUVER, BC V6E 3P3")
  expect_equal(length(unique(address_key(x))), 1L)
})

test_that("the unit is in the key only when asked for", {
  x <- c("Suite 1500 - 1055 W Georgia St, Vancouver BC",
         "Suite 800 - 1055 W Georgia St, Vancouver BC")
  expect_equal(length(unique(address_key(x))), 1L)
  expect_equal(length(unique(address_key(x, unit = TRUE))), 2L)
})

test_that("the key folds accents, case and the punctuation NAR disagrees on", {
  # NAR keeps the periods in ST. JOHN'S and SAULT STE. MARIE; nar_norm_text()
  # strips them from input. A key that did not fold both sides would split
  # every gazetteer-resolved row from its rules-only twin.
  pairs <- list(c("12 Water St, St. John's, NL", "12 Water St, ST JOHNS, NL"),
                c("1 Rue Notre-Dame E, Montréal, QC",
                  "1 rue Notre Dame E, Montreal, QC"),
                c("29 Hocking Ave, Sault Ste. Marie, ON",
                  "29 Hocking Ave, Sault Ste Marie, ON"))
  for (p in pairs) expect_equal(length(unique(address_key(p))), 1L, info = p[1])
})

test_that("fields stay in their own slot, broad to narrow", {
  k <- address_key("1055 W Georgia St, Vancouver, BC")
  expect_equal(k, "BC|VANCOUVER|GEORGIA|ST|W|1055|")
  # A missing field is an empty slot rather than a dropped one, so two rows
  # cannot line up by shifting along.
  expect_equal(address_key("1055 Georgia St, Vancouver, BC"),
               "BC|VANCOUVER|GEORGIA|ST||1055|")
})

test_that("a row with no street name has no key", {
  # NA rather than an empty key, or every unparseable row would join to every
  # other unparseable row.
  expect_equal(address_key(c("", "   ", "V6E 3P3")),
               c(NA_character_, NA_character_, NA_character_))
})

test_that("the key and the formatter accept a parse as readily as a string", {
  x <- c("302-1055 w georgia st, vancouver bc v6e3p3",
         "12 1/2 rue notre-dame e, montreal, quebec")
  parsed <- normalize_address(x)
  expect_equal(address_key(parsed), address_key(x))
  expect_equal(format_address(parsed), format_address(x))
})

test_that("parsing arguments are refused for an already-parsed frame", {
  parsed <- normalize_address("100 Queen St W, Toronto, ON")
  expect_error(address_key(parsed, prov = "ON"), "already parsed")
  expect_error(format_address(parsed, prov = "ON"), "already parsed")
  expect_error(address_key(parsed["CIVIC_NO"]), "missing the component column")
  expect_error(address_key(list(1, 2)), "character vector of address strings")
})

test_that("the formatter puts French types in front and English types behind", {
  # The test is the type, not the province: RUE is unambiguously French
  # wherever it turns up, and NAR has plenty of them outside Quebec.
  expect_equal(format_address(c("12 rue notre-dame e, montreal, quebec",
                                "100 queen street west, toronto, ontario",
                                "45 rue principale, hawkesbury, ontario")),
               c("12 RUE NOTRE-DAME E, MONTREAL, QC",
                 "100 QUEEN ST W, TORONTO, ON",
                 "45 RUE PRINCIPALE, HAWKESBURY, ON"))
})

test_that("the formatter hyphenates the unit and spaces the postal code", {
  expect_equal(format_address("apt 302, 1055 w georgia st, vancouver bc v6e3p3"),
               "302-1055 GEORGIA ST W, VANCOUVER, BC V6E 3P3")
  # A suffix carrying punctuation is a fraction, and 12 + 1/2 is not 121/2.
  expect_equal(format_address("12 1/2 Main St, Wawa, ON"),
               "12 1/2 MAIN ST, WAWA, ON")
  expect_equal(format_address("990A Kingsway, Vancouver, BC"),
               "990A KINGSWAY, VANCOUVER, BC")
})

test_that("a formatted address parses back to the same key", {
  # The formatter has to emit something the parser can read, or a cleaned
  # column stops joining to the one it was cleaned from.
  x <- c("302-1055 w georgia st, vancouver bc v6e3p3",
         "12 1/2 rue notre-dame e, montreal, quebec",
         "100 queen street west, toronto, ontario",
         "53222 Range Road 272, Spruce Grove, AB",
         "9819 96A Street NW, Edmonton, AB")
  expect_equal(address_key(format_address(x), unit = TRUE),
               address_key(x, unit = TRUE))
})

test_that("nothing at all formats to NA, not to an empty string", {
  expect_equal(format_address(c("", "  ")), c(NA_character_, NA_character_))
})
