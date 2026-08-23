# A miniature RQA release, with the register's own column names.

rqa_fixture_columns <- function() {
  c("identifiant_unique_adresse", "date_diffusion_version", "numero_municipal",
    "numero_municipal_suffixe", "numero_unite", "code_postal", "seqodo",
    "generique_odonyme", "particule_odonyme", "specifique_odonyme",
    "point_cardinal_odonyme", "odonyme_recompose_normal", "adresse_formatee",
    "qualite_positionnement_geometrique", "etat", "code_region_administrative",
    "nom_region_administrative", "code_municipalite", "nom_municipalite",
    "nom_arrondissement", "longitude", "latitude")
}

# Five rows, each carrying one thing the import has to get right:
#
#   rqa1  1255 Rue Peel, Montreal -- the one address the NAR fixture also has,
#         so IN_NAR must come back TRUE and everything else FALSE.
#   rqa2  431 Rue Courtemanche, Montreal-Est -- absent from NAR, and a building
#         placement, so it is what the tier should answer with.
#   rqa3  100 Boulevard du Cure-Labelle, Saint-Jerome -- a leading particule,
#         which NAR keeps inside the street name and RQA in a column of its
#         own. STREET_NAME has to end up "du Cure-Labelle", not "Cure-Labelle".
#   rqa4  5510 Rue Saint-Jacques Ouest, Montreal (Sud-Ouest borough) -- a point
#         cardinal to canonicalize to O, and a borough the municipality name
#         does not name.
#   rqa5  a retired row, which `etat` must exclude.
rqa_fixture_rows <- function() {
  row <- function(id, civic, generique, particule, specifique, cardinal,
                  full, mun, borough, postal, quality, lon, lat,
                  etat = "Certifiée", seqodo = "1") {
    c(id, "20260801", civic, "", "", postal, seqodo, generique, particule,
      specifique, cardinal, full,
      paste0(civic, " ", full, ", ", mun, " ", postal), quality, etat,
      "06", "Montréal", "66023", mun, borough, lon, lat)
  }
  list(
    row("rqa1", "1255", "Rue", "", "Peel", "", "Rue Peel",
        "Montréal", "Ville-Marie", "H3B2T9", "Bâtiment", "-73.5730", "45.4995"),
    row("rqa2", "431", "Rue", "", "Courtemanche", "", "Rue Courtemanche",
        "Montréal-Est", "", "H1B5K2", "Bâtiment", "-73.5100", "45.6300",
        seqodo = "2"),
    row("rqa3", "100", "Boulevard", "du", "Curé-Labelle", "",
        "Boulevard du Curé-Labelle", "Saint-Jérôme", "", "J7Z5T3",
        "Incertaine", "-74.0030", "45.7800", seqodo = "3"),
    row("rqa4", "5510", "Rue", "", "Saint-Jacques", "Ouest",
        "Rue Saint-Jacques Ouest", "Montréal", "Le Sud-Ouest", "H4A2E3",
        "Géocodée", "-73.6100", "45.4700", seqodo = "4"),
    row("rqa5", "999", "Rue", "", "Retiree", "", "Rue Retiree",
        "Montréal", "", "H3B2T9", "Bâtiment", "-73.5700", "45.5000",
        etat = "Non certifiée", seqodo = "5"))
}

#' Write the miniature RQA release and return the path to its CSV
local_rqa_fixture <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  csv <- file.path(dir, "RQA.csv")
  rows <- do.call(rbind, lapply(rqa_fixture_rows(), function(r) {
    stats::setNames(as.data.frame(as.list(r), stringsAsFactors = FALSE),
                    rqa_fixture_columns())
  }))
  # Quoted, because adresse_formatee carries a comma of its own -- as it does
  # in the release.
  utils::write.csv(rows, csv, row.names = FALSE, quote = TRUE,
                   fileEncoding = "UTF-8")
  csv
}

#' Import both fixtures and hand back an open read-only connection
#'
#' The NAR import has to finish and release the file before RQA can be written
#' into it: DuckDB takes an exclusive lock for a writer, and nar_connection()
#' holds a reader.
local_rqa_connection <- function(env = parent.frame()) {
  local_nar_env(local_nar_fixture(blockface = TRUE, qc = TRUE, env = env),
                env = env)
  con <- suppressMessages(nar_connection(version = "test-01"))
  DBI::dbDisconnect(con)

  suppressMessages(rqa_import(version = "test-01",
                              csv = local_rqa_fixture(env = env)))

  con <- suppressMessages(nar_connection(version = "test-01"))
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}
