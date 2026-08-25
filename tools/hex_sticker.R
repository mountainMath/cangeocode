# The cangeocode hex sticker.
#
# Motif: Canada, drawn out of a sample of NAR's own address points, with one
# large map pin and three concentric rings around it -- the package's answer
# and the `uncertainty_m` it comes with -- and five smaller pins for the
# addresses it did not have to guess at.  Palette follows the canpumf / canivt
# family: gold marks on a charcoal hexagon with a red border.
#
# Run this file to write `man/figures/logo.png`, on a transparent background:
#
#     Rscript tools/hex_sticker.R
#
# Needs cancensus (with an API key) for the country geometry and an imported
# NAR database via NAR_CACHE_PATH for the point cloud.  Both are cached to
# tools/hex-prototypes/ on first run, so re-runs need neither.  The rejected
# designs and the palette variations this one was chosen from are kept in
# tools/hex-prototypes/prototypes.R.

library(ggplot2)
library(sf)
library(hexSticker)
library(dplyr)

root  <- if (requireNamespace("here", quietly = TRUE)) here::here() else "."
cache <- file.path(root, "tools", "hex-prototypes")
out   <- file.path(root, "man", "figures", "logo.png")
dir.create(cache, showWarnings = FALSE, recursive = TRUE)
dir.create(dirname(out), showWarnings = FALSE, recursive = TRUE)

charcoal <- "#333333"   # hexagon fill
famred   <- "#E60013"   # hexagon border, and the one pin that is the answer
gold     <- "#F2C200"   # the rings and the ordinary pins
land     <- "#787878"   # the country; see the note on contrast below
dotcol   <- "#d8d8d8"   # NAR's addresses

# Statistics Canada's usual Lambert conformal conic, as in the cansim sticker.
crs <- paste("+proj=lcc +lat_1=50 +lat_2=70 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0",
             "+ellps=GRS80 +datum=NAD83 +units=m no_defs")

## --- the bold wordmark ---------------------------------------------------
# `p_fontface = "bold"` is a silent no-op against hexSticker's own font: it
# registers only Aller's regular cut, and hexSticker:::load_font() maps the
# family name "Aller" back to "Aller_Rg" and re-registers it from that single
# file.  Registering under a name it does not recognise passes through
# untouched.  It has to be done twice, because ggsave() writes through ragg
# when ragg is installed and ragg resolves fonts with systemfonts, not with
# showtext -- register in only one and the bold cut is dropped without warning.
aller <- function(f) system.file(file.path("fonts/Aller", f), package = "hexSticker")
sysfonts::font_add("AllerHex", regular = aller("Aller_Rg.ttf"),
                   bold = aller("Aller_Bd.ttf"))
showtext::showtext_auto()
if (requireNamespace("systemfonts", quietly = TRUE))
  systemfonts::register_font("AllerHex", plain = aller("Aller_Rg.ttf"),
                             bold = aller("Aller_Bd.ttf"),
                             italic = aller("Aller_It.ttf"),
                             bolditalic = aller("Aller_BdIt.ttf"))

## --- data ----------------------------------------------------------------

ca_rds <- file.path(cache, "ca.rds")
if (file.exists(ca_rds)) {
  ca <- readRDS(ca_rds)
} else {
  ca <- cancensus::get_census("CA21", regions = list(C = "01"),
                              geo_format = "sf", quiet = TRUE) |>
    st_transform(crs)
  saveRDS(ca, ca_rds)
}
bb <- st_bbox(ca)
XR <- as.numeric(bb$xmax - bb$xmin)
YR <- as.numeric(bb$ymax - bb$ymin)

# A ~0.35% system sample of NAR's own location points -- the actual addresses
# the package geocodes.  The blockface representative point is used because it
# is populated for every placed row; at this size the metre-scale difference
# from the building point is invisible.
dots_rds <- file.path(cache, "nar_dots.rds")
dots <- tryCatch({
  if (file.exists(dots_rds)) readRDS(dots_rds) else {
    con <- cangeocode::nar_connection()
    d <- DBI::dbGetQuery(con, paste(
      "SELECT BF_REPPOINT_LONGITUDE lon, BF_REPPOINT_LATITUDE lat FROM Locations",
      "WHERE BF_REPPOINT_LONGITUDE IS NOT NULL USING SAMPLE 0.35% (system)"))
    xy <- st_as_sf(d, coords = c("lon", "lat"), crs = 4269) |>
      st_transform(crs) |> st_coordinates() |> as.data.frame()
    xy <- xy[is.finite(xy$X) & is.finite(xy$Y), ]
    saveRDS(xy, dots_rds)
    xy
  }
}, error = function(e) {
  message("no NAR database -- the logo will be drawn without the point cloud")
  data.frame(X = numeric(), Y = numeric())
})

## --- a map pin, as a polygon ---------------------------------------------
# Tip at (x, y); the head is a circle of radius r centred hf*r above the tip,
# and the sides are the two tangents from the tip to that circle.

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

ring <- function(x, y, rad, n = 240) {
  th <- seq(0, 2*pi, length.out = n)
  data.frame(rx = x + rad * cos(th), ry = y + rad * sin(th))
}

## --- where the pins go ----------------------------------------------------
# The big pin sits in central Manitoba rather than on Toronto: put it on the
# populated south and the outermost ring runs off the hexagon.

city_ll <- tribble(
  ~name,          ~lon,    ~lat,
  "Vancouver",  -123.12,  49.28,
  "Toronto",     -79.38,  43.65,
  "Halifax",     -63.57,  44.65,
  "Whitehorse", -135.05,  60.72,
  "Iqaluit",     -68.51,  63.75,
  "Thompson",    -97.86,  55.74
)
cities <- st_as_sf(city_ll, coords = c("lon", "lat"), crs = 4269) |>
  st_transform(crs) |> st_coordinates() |> as.data.frame() |>
  bind_cols(name = city_ll$name)
pick <- function(...) cities[match(c(...), cities$name), ]

R   <- XR * 0.024        # small-pin radius, in map units
BIG <- R * 3.1           # the answer.  Past ~3.6 R the outer ring reaches the
                         # hex edge on the Ontario side and the head crowds the
                         # wordmark
tgt <- pick("Thompson")
small <- pick("Vancouver", "Toronto", "Halifax", "Whitehorse", "Iqaluit")

# Rings as multiples of the pin, not of R, so the innermost one clears the head
# whatever size the pin is drawn at.
rings <- bind_rows(lapply(c(1.154, 1.846, 2.615), function(k)
  cbind(ring(tgt$X, tgt$Y, k * BIG), id = k)))

## --- the motif ------------------------------------------------------------
# The land carries no outline: outlining the lakes and the Arctic archipelago
# turns the interior to noise.  What brightens the dense regions is the point
# cloud, one dot per sampled address, which piles up along the St Lawrence and
# the Windsor-Quebec corridor -- the alpha below is the knob for that, and it
# is deliberately left high enough to keep the settlement pattern legible.
#
# #787878 on #333333 is a real separation; the #4d4d4d this started at is only
# 1.4:1 against the fill, which is why the country vanished at favicon size.

p <- ggplot() +
  geom_sf(data = ca, fill = land, colour = NA) +
  geom_point(data = dots, aes(X, Y), colour = dotcol, size = 0.075,
             alpha = 0.5, stroke = 0) +
  geom_path(data = rings, aes(rx, ry, group = id), colour = gold,
            linewidth = 0.42, alpha = 0.95) +
  geom_polygon(data = pins_df(small, R * 0.85), aes(px, py, group = id),
               fill = gold, colour = charcoal, linewidth = 0.25) +
  geom_polygon(data = pins_df(small, R * 0.85, pin_hole), aes(px, py, group = id),
               fill = land, colour = NA) +
  geom_polygon(data = pin_poly(tgt$X, tgt$Y, BIG), aes(px, py),
               fill = famred, colour = charcoal, linewidth = 0.4) +
  geom_polygon(data = pin_hole(tgt$X, tgt$Y, BIG), aes(px, py),
               fill = land, colour = NA) +
  # Padding in fractions of the country's own bbox.  The country is scaled up
  # until it nearly fills the hexagon, which leaves no clear sky for the
  # wordmark -- so the wordmark overprints the Arctic in white and the top pad
  # is small on purpose.
  coord_sf(xlim = c(bb$xmin - .03 * XR, bb$xmax + .03 * XR),
           ylim = c(bb$ymin - .08 * YR, bb$ymax + .16 * YR),
           expand = FALSE, datum = NA, crs = crs) +
  theme_void() + theme_transparent()

## --- the sticker ----------------------------------------------------------
# Push s_width past ~1.82 and Vancouver Island and the Whitehorse pin start
# getting clipped by the hex edge.
#
# hexSticker returns a ggplot that picks up the ambient theme's gridlines under
# current ggplot2, which draws them straight through the hexagon; strip them and
# save with ggsave(bg = "transparent") rather than hexSticker::save_sticker().

s <- sticker(
  p, package = "cangeocode",
  s_x = 1, s_y = 0.95, s_width = 1.80, s_height = 1.80,
  p_y = 1.46, p_size = 26, p_color = "#FFFFFF",
  p_family = "AllerHex", p_fontface = "bold",
  h_color = famred, h_fill = charcoal, h_size = 3,
  # sticker() always writes its `filename` as a side effect, defaulting to
  # "cangeocode.png" in the working directory; the real save is the ggsave()
  # below, so send that one to the bin.
  filename = tempfile(fileext = ".png")
) +
  theme_transparent() +
  theme(panel.grid = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        plot.background = element_blank(),
        panel.border = element_blank())

ggsave(out, s, width = 3.5, height = 3.5, bg = "transparent", dpi = 300)
message("wrote ", out)
