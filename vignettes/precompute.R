# Pre-compute the vignettes.
#
# Both vignettes query the real NAR database, which is several GB and has to be
# downloaded from StatCan first -- neither is something `R CMD build` can do. So
# the sources live in `*.Rmd.orig`, are knitted here against a local database,
# and the resulting `*.Rmd` (with output already inlined) is what ships and what
# gets committed.
#
# Re-run this after editing a `.Rmd.orig`, or after importing a NAR release whose
# numbers would change the text:
#
#   Rscript vignettes/precompute.R
#
# It needs NAR_CACHE_PATH set and a database already imported.

if (!nzchar(Sys.getenv("NAR_CACHE_PATH"))) {
  stop("NAR_CACHE_PATH must be set to pre-compute the vignettes.")
}

old <- setwd("vignettes")
on.exit(setwd(old), add = TRUE)

for (vignette in c("cangeocode", "querying-nar",
                   "address-normalization", "geocoding")) {
  message("Knitting ", vignette)
  knitr::knit(paste0(vignette, ".Rmd.orig"), paste0(vignette, ".Rmd"))
}
