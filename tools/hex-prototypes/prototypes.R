# Prototype hex stickers for the cangeocode package.
#
# The motif is the package's own subject: Canada, and points dropped on it.
# Five directions are drawn; run the whole file and compare the PNGs written
# to tools/hex-prototypes/.  Palette and construction follow the canpumf /
# canivt family — gold on charcoal with a red hex border — and the sticker is
# saved with ggsave() rather than hexSticker::save_sticker(), which is what
# keeps current ggplot2 from painting gridlines through the hexagon.
#
#   A  pins on the map        silhouette + a gold pin over each of ten places
#   B  real NAR points        the country drawn out of a sample of NAR itself
#   C  pin + accuracy rings   one big pin with concentric rings — uncertainty_m
#                            (the chosen design; drawn in four palettes)
#   D  country in a pin       Canada cut into the head of a single map pin
#   E  inverted               gold landmass, red pins
#
# Needs: cancensus (with an API key) for the country geometry, and — for B and
# C only — an imported NAR database via NAR_CACHE_PATH.  Both are cached to
# tools/hex-prototypes/ on first run, so re-runs are offline.

library(ggplot2)
library(sf)
library(hexSticker)
library(dplyr)

out_dir <- if (requireNamespace("here", quietly = TRUE))
  here::here("tools/hex-prototypes") else "tools/hex-prototypes"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
p_ <- function(...) file.path(out_dir, ...)

# Statistics Canada's usual Lambert conformal conic, as in the cansim sticker.
crs <- paste("+proj=lcc +lat_1=50 +lat_2=70 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0",
             "+ellps=GRS80 +datum=NAD83 +units=m no_defs")

charcoal <- "#333333"; famred <- "#E60013"; gold <- "#F2C200"

# hexSticker registers only the regular cut of Aller, so p_fontface = "bold" is
# silently a no-op against it (the two PNGs come out pixel-identical).  Register
# the family properly — the bold cut ships with the package — and ask for it by
# family name instead.
aller <- function(f) system.file(file.path("fonts/Aller", f), package = "hexSticker")
# The family is deliberately NOT called "Aller": hexSticker:::load_font() maps
# that name to "Aller_Rg" and re-registers it from the single regular file, which
# is what makes p_fontface = "bold" a silent no-op.  An unknown name passes
# through untouched.
sysfonts::font_add("AllerHex", regular = aller("Aller_Rg.ttf"), bold = aller("Aller_Bd.ttf"))
showtext::showtext_auto()
# ggsave writes through ragg when it is installed, and ragg resolves fonts with
# systemfonts rather than showtext — so the family has to be registered twice or
# the bold cut is silently dropped.
if (requireNamespace("systemfonts", quietly = TRUE))
  systemfonts::register_font("AllerHex", plain = aller("Aller_Rg.ttf"),
                             bold = aller("Aller_Bd.ttf"),
                             italic = aller("Aller_It.ttf"),
                             bolditalic = aller("Aller_BdIt.ttf"))

## --- data ---------------------------------------------------------------

# The country.  Cached because it needs a cancensus API key.
if (file.exists(p_("ca.rds"))) {
  ca <- readRDS(p_("ca.rds"))
} else {
  ca <- cancensus::get_census("CA21", regions = list(C = "01"),
                              geo_format = "sf", quiet = TRUE) |>
    st_transform(crs)
  saveRDS(ca, p_("ca.rds"))
}
bb <- st_bbox(ca)
XR <- as.numeric(bb$xmax - bb$xmin)
YR <- as.numeric(bb$ymax - bb$ymin)

# A ~0.35% system sample of NAR's own location points — the actual addresses
# the package geocodes, used as the fill of designs B and C.
nar_dots <- function() {
  if (file.exists(p_("nar_dots.rds"))) return(readRDS(p_("nar_dots.rds")))
  con <- cangeocode::nar_connection()
  d <- DBI::dbGetQuery(con, paste(
    "SELECT BF_REPPOINT_LONGITUDE lon, BF_REPPOINT_LATITUDE lat FROM Locations",
    "WHERE BF_REPPOINT_LONGITUDE IS NOT NULL USING SAMPLE 0.35% (system)"))
  xy <- st_as_sf(d, coords = c("lon", "lat"), crs = 4269) |>
    st_transform(crs) |> st_coordinates() |> as.data.frame()
  xy <- xy[is.finite(xy$X) & is.finite(xy$Y), ]
  saveRDS(xy, p_("nar_dots.rds"))
  xy
}
dots <- tryCatch(nar_dots(), error = function(e) {
  message("no NAR database — B and C will be drawn without the point cloud")
  data.frame(X = numeric(), Y = numeric())
})

## --- a map pin, as a polygon --------------------------------------------
# Tip at (x, y); head is a circle of radius r centred hf*r above the tip, and
# the sides are the two tangents from the tip to that circle.  hf sets how
# lanky the pin is: 2.6 for the small pins, ~1.95 for D's squat one.

pin_poly <- function(x, y, r, n = 120, hf = 2.6) {
  h <- hf * r; beta <- acos(r / h)
  th <- seq(-pi/2 + beta, 3*pi/2 - beta, length.out = n)
  data.frame(px = c(x, x + r * cos(th)), py = c(y, y + h + r * sin(th)))
}
pin_hole <- function(x, y, r, n = 80, hf = 2.6) {
  th <- seq(0, 2*pi, length.out = n)
  data.frame(px = x + 0.44 * r * cos(th), py = y + hf * r + 0.44 * r * sin(th))
}
pins_df <- function(ct, r, f = pin_poly)
  do.call(rbind, lapply(seq_len(nrow(ct)), function(i)
    cbind(f(ct$X[i], ct$Y[i], r), id = i)))

## --- places to drop pins on ---------------------------------------------
city_ll <- tribble(
  ~name,          ~lon,     ~lat,
  "Vancouver",  -123.12,   49.28,
  "Calgary",    -114.07,   51.05,
  "Winnipeg",    -97.14,   49.90,
  "Toronto",     -79.38,   43.65,
  "Montreal",    -73.57,   45.50,
  "Halifax",     -63.57,   44.65,
  "StJohns",     -52.71,   47.56,
  "Yellowknife",-114.37,   62.45,
  "Iqaluit",     -68.51,   63.75,
  "Whitehorse", -135.05,   60.72,
  "Thompson",    -97.86,   55.74
)
cities <- st_as_sf(city_ll, coords = c("lon", "lat"), crs = 4269) |>
  st_transform(crs) |> st_coordinates() |> as.data.frame() |>
  bind_cols(name = city_ll$name)
pick <- function(...) cities[match(c(...), cities$name), ]

R <- XR * 0.024   # small-pin radius, in map units

## --- framing ------------------------------------------------------------
# Padding is in fractions of the country's own bbox.  The generous default top
# pad is what keeps the Arctic clear of the wordmark: without it Ellesmere
# lands in the middle of the "g".

fr <- function(p, l = .04, r = .04, b = .05, t = .50)
  p + coord_sf(xlim = c(bb$xmin - l * XR, bb$xmax + r * XR),
               ylim = c(bb$ymin - b * YR, bb$ymax + t * YR),
               expand = FALSE, datum = NA, crs = crs) +
    theme_void() + theme_transparent()

# hexSticker returns a ggplot that picks up the ambient theme's gridlines under
# current ggplot2; strip them and save with ggsave().
hex <- function(p, file, s_y = 1.0, s_w = 1.74, p_y = 1.47, p_size = 21,
                p_color = gold, h_color = famred, h_fill = charcoal,
                p_fontface = "plain", p_family = "AllerHex") {
  s <- sticker(p, package = "cangeocode", s_x = 1, s_y = s_y,
               s_width = s_w, s_height = s_w, p_y = p_y, p_size = p_size,
               p_color = p_color, p_fontface = p_fontface, p_family = p_family,
               h_color = h_color, h_fill = h_fill, h_size = 3,
               # sticker() always writes its `filename`; ggsave() below is the
               # real save, so keep the side effect out of the repo root
               filename = tempfile(fileext = ".png")) +
    theme_transparent() +
    theme(panel.grid = element_blank(), panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(), panel.background = element_blank(),
          plot.background = element_blank(), panel.border = element_blank())
  ggsave(p_(file), s, width = 3.5, height = 3.5, bg = "transparent", dpi = 300)
}

## == A — pins on the map =================================================
ctA <- pick("Vancouver", "Calgary", "Winnipeg", "Toronto", "Montreal", "Halifax",
            "StJohns", "Yellowknife", "Iqaluit", "Whitehorse")
pA <- ggplot() +
  geom_sf(data = ca, fill = "#565656", colour = NA) +
  geom_polygon(data = pins_df(ctA, R), aes(px, py, group = id),
               fill = gold, colour = charcoal, linewidth = 0.25) +
  geom_polygon(data = pins_df(ctA, R, pin_hole), aes(px, py, group = id),
               fill = charcoal, colour = NA)
hex(fr(pA), "hex-A-pins.png")

## == B — the country drawn out of real NAR address points ================
ctB <- pick("Vancouver", "Winnipeg", "Montreal", "Halifax", "Yellowknife")
pB <- ggplot() +
  geom_sf(data = ca, fill = "#3a3a3a", colour = "#6e6e6e", linewidth = 0.2) +
  geom_point(data = dots, aes(X, Y), colour = gold, size = 0.16, alpha = 0.85,
             stroke = 0) +
  geom_polygon(data = pins_df(ctB, R * 0.95), aes(px, py, group = id),
               fill = famred, colour = gold, linewidth = 0.3) +
  geom_polygon(data = pins_df(ctB, R * 0.95, pin_hole), aes(px, py, group = id),
               fill = "#1c1c1c", colour = NA)
hex(fr(pB), "hex-B-nar-points.png")

## == C — one big pin with accuracy rings =================================
# The design the package settled on, so it is drawn in several palettes.
#
# The rings nod at uncertainty_m.  The big pin sits in the middle of the
# country rather than on Toronto so the outermost ring stays inside the
# hexagon.  The country is drawn in Canada red and scaled up until it very
# nearly fills the hexagon, which leaves no clear sky for the wordmark — so
# the wordmark deliberately overprints the Arctic in white, and the frame's
# top pad is small on purpose.  Push s_w past ~1.82 and Vancouver Island and
# the Whitehorse pin start getting clipped by the hex edge.

canred <- "#D52B1E"   # flag red, a touch warmer than the family border red
white  <- "#FFFFFF"

ring <- function(x, y, rad, n = 240) {
  th <- seq(0, 2*pi, length.out = n)
  data.frame(rx = x + rad * cos(th), ry = y + rad * sin(th))
}
tgt   <- pick("Thompson")
BIG   <- R * 2.6
ctC <- pick("Vancouver", "Toronto", "Halifax", "Whitehorse", "Iqaluit")

# The rings are sized as multiples of the big pin, not of R, so growing the pin
# keeps them clear of its head instead of swallowing the innermost one.
ring_k <- c(1.154, 1.846, 2.615)

# The land is drawn with colour = NA — an outline on the lakes and on the Arctic
# archipelago turns the interior to noise.  What actually brightens the dense
# regions is the NAR sample below: it is one dot per address, so the St Lawrence
# and the Windsor–Quebec corridor pile up into near-solid light grey.  dotalpha
# is the knob for that, not the polygon boundary.
motif_C <- function(land = canred, dotcol = white, dotalpha = 0.22,
                    ringcol = gold, pincol = white, bigfill = white,
                    holecol = canred, pinline = "#8f1c14", big = BIG,
                    dotsize = 0.075) {
  rg <- bind_rows(lapply(ring_k, function(k)
    cbind(ring(tgt$X, tgt$Y, k * big), id = k)))
  ggplot() +
    geom_sf(data = ca, fill = land, colour = NA) +
    geom_point(data = dots, aes(X, Y), colour = dotcol, size = dotsize,
               alpha = dotalpha, stroke = 0) +
    geom_path(data = rg, aes(rx, ry, group = id), colour = ringcol,
              linewidth = 0.42, alpha = 0.95) +
    geom_polygon(data = pins_df(ctC, R * 0.85), aes(px, py, group = id),
                 fill = pincol, colour = pinline, linewidth = 0.25) +
    geom_polygon(data = pins_df(ctC, R * 0.85, pin_hole), aes(px, py, group = id),
                 fill = holecol, colour = NA) +
    geom_polygon(data = pin_poly(tgt$X, tgt$Y, big), aes(px, py),
                 fill = bigfill, colour = pinline, linewidth = 0.4) +
    geom_polygon(data = pin_hole(tgt$X, tgt$Y, big), aes(px, py),
                 fill = holecol, colour = NA)
}

hex_C <- function(p, file, h_color = famred, p_color = white, ...)
  hex(fr(p, l = .03, r = .03, b = .08, t = .16), file, s_y = 0.95, s_w = 1.80,
      p_y = 1.46, p_size = 26, p_color = p_color, h_color = h_color, ...)

# C1 — the pick: red country, white pins, gold rings, white wordmark
hex_C(motif_C(), "hex-C1-red.png")
# C2 — all-white marks; quietest, closest to the flag
hex_C(motif_C(ringcol = white), "hex-C2-red-white.png")
# C3 — gold hex border instead of the family red
hex_C(motif_C(), "hex-C3-gold-border.png", h_color = gold)
# C4 — the original charcoal-and-grey palette, at the new scale
hex_C(motif_C(land = "#4d4d4d", dotcol = "#b0b0b0", dotalpha = 0.5,
              pincol = gold, bigfill = famred, holecol = charcoal,
              pinline = charcoal), "hex-C4-grey.png")

# C5 — C1 with every marker in the rings' gold, so gold reads as "the package's
# marks" and red as "the country".  The pin outline has to be dark: gold on
# Canada red is a warm-on-warm pair and loses its edge without one.
hex_C(motif_C(pincol = gold, bigfill = gold), "hex-C5-gold-marks.png")
# C5b — same, with the outline in a deep gold instead of the dark red
hex_C(motif_C(pincol = gold, bigfill = gold, pinline = "#8a6a00"),
      "hex-C5b-gold-marks-warm.png")
# C5c — same, with the pin holes gold-dark rather than showing the country
hex_C(motif_C(pincol = gold, bigfill = gold, holecol = "#7a1610"),
      "hex-C5c-gold-marks-dark-hole.png")

# C6 / C6b — C4 with the land lifted off the charcoal fill.  #4d4d4d is only
# 1.4:1 against #333333, which is why the grey country disappeared at favicon
# size; these push it to a real separation.
hex_C(motif_C(land = "#787878", dotcol = "#d8d8d8", dotalpha = 0.5,
              pincol = gold, bigfill = famred, holecol = "#787878",
              pinline = charcoal), "hex-C6-grey-lighter.png")
hex_C(motif_C(land = "#9a9a9a", dotcol = "#efefef", dotalpha = 0.45,
              pincol = gold, bigfill = famred, holecol = "#9a9a9a",
              pinline = charcoal), "hex-C6b-grey-lightest.png")

# The C6 grey, reused below; the wordmark sits on charcoal, so which red it is
# matters: #D52B1E is 3.0:1 against #333333 and #E60013 is 3.6:1, and neither
# is white's 12.6:1 — this is the one place the palette costs legibility.
grey6 <- function(...) do.call(motif_C, utils::modifyList(
  list(land = "#787878", dotcol = "#d8d8d8", dotalpha = 0.5,
       holecol = "#787878", pinline = charcoal), list(...)))

# C7 — C6 with the wordmark in Canada red instead of white
hex_C(grey6(pincol = gold, bigfill = famred), "hex-C7-red-word.png",
      p_color = canred)
# C7b — same, wordmark in the brighter border red
hex_C(grey6(pincol = gold, bigfill = famred), "hex-C7b-red-word-bright.png",
      p_color = famred)

# C8 — the gold retired: rings and the small pins go red too, so the sticker is
# red-on-grey throughout and gold is gone from the design entirely
hex_C(grey6(pincol = famred, bigfill = famred, ringcol = famred),
      "hex-C8-all-red.png", p_color = canred)
# C8b — same, but the big pin stays white so it still reads as the answer
hex_C(grey6(pincol = famred, bigfill = white, ringcol = famred),
      "hex-C8b-all-red-white-pin.png", p_color = canred)
# C8c — all red with the wordmark left white, for the contrast comparison
hex_C(grey6(pincol = famred, bigfill = famred, ringcol = famred),
      "hex-C8c-all-red-white-word.png")

## --- C9: the chosen C6, with a bold wordmark and a bigger main pin -------
# p_fontface is a hexSticker argument, so the bold goes in through sticker()
# rather than by patching the returned ggplot's text layer.  The pin grows and
# the rings grow with it (see ring_k); past about 3.6 R the outer ring starts
# reaching for the hex edge on the Ontario side.

hex_C9 <- function(..., file, big) hex_C(grey6(pincol = gold, bigfill = famred,
                                               big = big, ...),
                                         file, p_fontface = "bold")

hex_C9(file = "hex-C9-bold.png",        big = R * 2.6)   # bold only
hex_C9(file = "hex-C9b-bold-pin31.png", big = R * 3.1)   # bold + pin +19%
hex_C9(file = "hex-C9c-bold-pin36.png", big = R * 3.6)   # bold + pin +38%
# C9d — pin +19% with the NAR cloud pulled back, so the dense corridors stop
# blowing out to near-white
hex_C9(file = "hex-C9d-quiet-dots.png", big = R * 3.1, dotalpha = 0.3,
       dotcol = "#c4c4c4")
# C9e — the same without the point cloud at all, for comparison
hex_C9(file = "hex-C9e-no-dots.png",    big = R * 3.1, dotalpha = 0)

## == D — the country cut into the shape of one map pin ===================
# The head has to be big enough to hold the whole country: a circle contains a
# w x h rectangle only if r >= sqrt(w^2 + h^2)/2, which for Canada's bbox is
# 0.66 * XR.  A squatter pin (hf = 1.95) is what keeps that head from pushing
# the tip out of the hexagon.
HF <- 1.95
cx <- (bb$xmin + bb$xmax) / 2 + 0.02 * XR
pr <- XR * 0.66
cy <- (bb$ymin + bb$ymax) / 2 - HF * pr
pm <- as.matrix(pin_poly(cx, cy, pr, n = 600, hf = HF))
pm <- rbind(pm, pm[1, ])                       # st_polygon needs a closed ring
shape <- st_sfc(st_polygon(list(pm)), crs = crs)
land  <- suppressWarnings(st_intersection(st_make_valid(st_union(ca)), shape))
bx    <- st_bbox(shape)
pD <- ggplot() +
  geom_sf(data = shape, fill = famred, colour = "#8f0a10", linewidth = 0.6) +
  geom_sf(data = land,  fill = gold, colour = NA) +
  coord_sf(xlim = c(bx$xmin - .03 * XR, bx$xmax + .03 * XR),
           ylim = c(bx$ymin - .03 * YR, bx$ymax + .03 * YR),
           expand = FALSE, datum = NA, crs = crs) +
  theme_void() + theme_transparent()
hex(pD, "hex-D-country-in-pin.png", s_y = 0.84, s_w = 1.40, p_y = 1.57, p_size = 19)

## == E — inverted: gold landmass, red pins ===============================
ctE <- pick("Vancouver", "Calgary", "Winnipeg", "Toronto", "Montreal", "Halifax",
            "StJohns", "Yellowknife", "Whitehorse")
pE <- ggplot() +
  geom_sf(data = ca, fill = gold, colour = NA) +
  geom_polygon(data = pins_df(ctE, R), aes(px, py, group = id),
               fill = famred, colour = "#8a1212", linewidth = 0.25) +
  geom_polygon(data = pins_df(ctE, R, pin_hole), aes(px, py, group = id),
               fill = "#fce9a3", colour = NA)
hex(fr(pE), "hex-E-inverted.png")

## --- contact sheets ------------------------------------------------------
if (requireNamespace("magick", quietly = TRUE)) {
  files <- c("hex-C1-red.png", "hex-C2-red-white.png", "hex-C3-gold-border.png",
             "hex-C4-grey.png", "hex-A-pins.png", "hex-B-nar-points.png",
             "hex-D-country-in-pin.png", "hex-E-inverted.png")
  labs  <- c("C1  red + gold rings", "C2  red, all white", "C3  gold border",
             "C4  original grey", "A  pins on the map", "B  real NAR points",
             "D  country in a pin", "E  inverted")
  sheet <- magick::image_append(magick::image_join(Map(function(f, l) {
    im <- magick::image_background(magick::image_scale(magick::image_read(p_(f)), "300x"), "white")
    magick::image_annotate(magick::image_extent(im, "300x340", gravity = "north", color = "white"),
                           l, gravity = "south", size = 17, color = "#333333", location = "+0+8")
  }, files, labs)))
  magick::image_write(sheet, p_("contact-sheet.png"))
  # legibility check at favicon size
  small <- magick::image_append(magick::image_join(lapply(files, function(f)
    magick::image_extent(magick::image_background(
      magick::image_scale(magick::image_read(p_(f)), "96x"), "white"),
      "130x120", gravity = "center", color = "white"))))
  magick::image_write(small, p_("contact-sheet-small.png"))
}

## --- C-family variant sheet ---------------------------------------------
if (requireNamespace("magick", quietly = TRUE)) {
  cf <- c("hex-C6-grey-lighter.png", "hex-C9-bold.png", "hex-C9b-bold-pin31.png",
          "hex-C9c-bold-pin36.png", "hex-C9d-quiet-dots.png",
          "hex-C9e-no-dots.png")
  cl <- c("C6  as picked", "C9  bold word", "C9b  bold + pin +19%",
          "C9c  bold + pin +38%", "C9d  pin +19%, quiet dots",
          "C9e  pin +19%, no dots")
  sheet <- magick::image_append(magick::image_join(Map(function(f, l) {
    im <- magick::image_background(magick::image_scale(magick::image_read(p_(f)), "300x"), "white")
    magick::image_annotate(magick::image_extent(im, "300x340", gravity = "north", color = "white"),
                           l, gravity = "south", size = 17, color = "#333333", location = "+0+8")
  }, cf, cl)))
  magick::image_write(sheet, p_("C-variants.png"))
  small <- magick::image_append(magick::image_join(lapply(cf, function(f)
    magick::image_extent(magick::image_background(
      magick::image_scale(magick::image_read(p_(f)), "96x"), "white"),
      "130x120", gravity = "center", color = "white"))))
  magick::image_write(small, p_("C-variants-small.png"))
}
