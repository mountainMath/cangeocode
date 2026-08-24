# Pre-compute the vignettes.
#
# Every vignette queries the real NAR database, which is several GB and has to be
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
# It needs NAR_CACHE_PATH set and a database already imported -- and, since the
# geocoding and source vignettes show the `"rqa"` and `"rnf"` tiers answering,
# one that `rqa_import()` and `rnf_import()` have both been run against.
#
# The `source-*` vignettes for the online geocoders knit live requests to BC,
# NRCan and Quebec, so those three services have to be reachable. `source-osm`
# has no live chunks -- the accuracy probe behind it has not been run.

if (!nzchar(Sys.getenv("NAR_CACHE_PATH"))) {
  stop("NAR_CACHE_PATH must be set to pre-compute the vignettes.")
}

old <- setwd("vignettes")
on.exit(setwd(old), add = TRUE)

for (vignette in c("cangeocode", "querying-nar",
                   "address-normalization", "geocoding",
                   "source-nar", "source-rqa", "source-rnf",
                   "source-bc", "source-nrcan", "source-qc",
                   "source-osm")) {
  message("Knitting ", vignette)
  knitr::knit(paste0(vignette, ".Rmd.orig"), paste0(vignette, ".Rmd"))
}
