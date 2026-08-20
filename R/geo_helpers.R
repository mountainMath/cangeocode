# Spatial layer shared by the NAR import and query paths.
#
# Geometry is stored as *untagged* DuckDB GEOMETRY in EPSG:3347 (NAD83 /
# Statistics Canada Lambert, metres). The CRS is deliberately kept out of the
# column type even though DuckDB's spatial extension now supports typed
# GEOMETRY('EPSG:3347') columns: RTREE indexes can only be built over plain
# GEOMETRY columns. The CRS is instead recorded in the `nar_metadata` table and
# re-attached at query time with ST_SetCRS(), which restores DuckDB's
# CRS-mismatch checking without giving up the index.
#
# Every piece of spatial SQL in the package is defined here as a TEMP macro
# registered on each connection, so there is exactly one definition of each
# operation and databases built by earlier package versions keep working
# without a rebuild.

#' CRS in which NAR geometry is stored
#' @return CRS identifier string
#' @keywords internal
nar_storage_crs <- function() "EPSG:3347"

#' CRS of NAR longitude/latitude columns
#'
#' @description NAR's `BG_LATITUDE`/`BG_LONGITUDE` are NAD83 geographic
#' coordinates (EPSG:4269), the same datum as the `BG_X`/`BG_Y` projected
#' coordinates in EPSG:3347. Naming the datum correctly matters: EPSG:4269 to
#' EPSG:3347 is a pure change of projection that PROJ reports at accuracy 0,
#' whereas going through a WGS84 identifier invites a ballpark datum
#' transformation of a metre or so.
#'
#' This was checked against the data rather than assumed. For NAR locations
#' that have exactly one address, the location's own lon/lat and the address's
#' own `BG_X`/`BG_Y` describe the same point, so re-deriving the projected
#' coordinate from the lon/lat and measuring the residual identifies the datum.
#' Over 300,000 such records the median residual is 0.057 m when the lon/lat
#' are read as EPSG:4269 -- that is just NAR's own rounding of lon/lat to six
#' decimals -- against 1.08 m when read as OGC:CRS84.
#' @return CRS identifier string
#' @keywords internal
nar_lonlat_crs <- function() "EPSG:4269"

#' Load the DuckDB spatial extension on a connection
#'
#' @description Loads the extension, installing it first if it is not present.
#' This deliberately uses DuckDB's own `LOAD spatial` rather than
#' `duckspatial::ddbs_load()`: the latter creates *persistent* helper macros,
#' which fails outright on the read-only connections this package hands out.
#' All spatial SQL the package needs is native to the extension, plus the
#' TEMP macros registered by [nar_register_spatial()].
#' @param con A DuckDB connection
#' @return The connection, invisibly
#' @keywords internal
nar_load_spatial <- function(con) {
  tryCatch({
    DBI::dbExecute(con, "LOAD spatial;")
  }, error = function(e) {
    DBI::dbExecute(con, "INSTALL spatial;")
    DBI::dbExecute(con, "LOAD spatial;")
  })
  invisible(con)
}

#' Record NAR database metadata
#'
#' @param con A writable DuckDB connection
#' @param version Normalized NAR version string
#' @return The connection, invisibly
#' @keywords internal
nar_write_metadata <- function(con, version) {
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE nar_metadata (key VARCHAR, value VARCHAR);")
  meta <- data.frame(
    key = c("version", "crs", "lonlat_crs", "schema_version",
            "package_version", "imported_at"),
    value = c(version, nar_storage_crs(), nar_lonlat_crs(),
              as.character(nar_schema_version()),
              as.character(utils::packageVersion("cangeocode")),
              format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  )
  DBI::dbAppendTable(con, "nar_metadata", meta)
  invisible(con)
}

#' Render a CRS as a string DuckDB's spatial extension accepts
#'
#' @description DuckDB wants an authority string such as `"EPSG:4326"`; the
#' bare number `4326` that `sf` and the rest of this package take happily is a
#' binder error there. Everything user-supplied is funnelled through here so a
#' numeric CRS works the same way in [collect_nar()] as it does in
#' [reverse_geocode()].
#' @param crs An EPSG code, an authority string, or an `sf` crs object
#' @return A length-1 character CRS identifier
#' @keywords internal
nar_crs_string <- function(crs) {
  if (is.numeric(crs) && length(crs) == 1 && !is.na(crs)) {
    return(paste0("EPSG:", as.integer(crs)))
  }
  parsed <- sf::st_crs(crs)
  if (is.na(parsed)) {
    stop("Could not interpret crs; supply an EPSG code or an authority string.")
  }
  if (!is.na(parsed$epsg)) {
    return(paste0("EPSG:", parsed$epsg))
  }
  # No authority code, so hand DuckDB the full definition instead.
  parsed$wkt
}

#' Layout version of the NAR database
#'
#' @description Bumped when the import produces a materially different
#' database. Version 2 added the `x`/`y` coordinate columns that make radius
#' queries fast, and corrected the datum used to read NAR's lon/lat columns.
#' Version 3 fell back to the blockface centroid for addresses with no building
#' point and added `geom_source` to tell the two apart.
#' Databases built by earlier versions still work; see [nar_within_radius()].
#' @return Integer schema version
#' @keywords internal
nar_schema_version <- function() 3L

#' Read NAR database metadata
#'
#' @param con A DuckDB connection
#' @return A named character vector, empty if the database predates metadata
#' @keywords internal
nar_metadata <- function(con) {
  if (!DBI::dbExistsTable(con, "nar_metadata")) return(stats::setNames(character(0), character(0)))
  meta <- DBI::dbGetQuery(con, "SELECT key, value FROM nar_metadata;")
  stats::setNames(meta$value, meta$key)
}

#' Read one metadata value with a fallback
#'
#' @param con A DuckDB connection
#' @param key Metadata key
#' @param default Value to use when the key is absent
#' @return A character scalar
#' @keywords internal
nar_meta_value <- function(con, key, default) {
  value <- unname(nar_metadata(con)[key])
  if (length(value) != 1 || is.na(value) || !nzchar(value)) default else value
}

#' CRS of the geometry in a NAR database
#'
#' @description Reads the CRS recorded at import time, falling back to the
#' package default for databases built before metadata was recorded.
#' @param con A DuckDB connection
#' @return CRS identifier string
#' @keywords internal
nar_crs <- function(con) nar_meta_value(con, "crs", nar_storage_crs())

#' Register the NAR spatial macros on a connection
#'
#' @description Creates the `nar_*` TEMP macros that every spatial operation in
#' the package goes through. They are temporary, so they are recreated on each
#' connection and work against read-only databases and databases built by
#' earlier versions of the package.
#'
#' Every transform between the storage CRS and lon/lat passes `always_xy`.
#' EPSG:4269, like most authority-defined geographic CRSs, declares its axes in
#' latitude/longitude order, while this package -- and \pkg{sf} -- always speak
#' longitude/latitude. Without the flag DuckDB reads a longitude of -123 as a
#' latitude and quietly returns `POINT (inf inf)` rather than an error.
#'
#' The macros are:
#' \describe{
#'   \item{`nar_point(lon, lat)`}{a longitude/latitude pair as a point in the storage CRS}
#'   \item{`nar_xy(x, y)`}{a coordinate pair that is already in the storage CRS}
#'   \item{`nar_lon(geom)`, `nar_lat(geom)`}{the inverse: stored geometry back to longitude/latitude}
#'   \item{`nar_geom(geom)`}{tags stored geometry with its CRS, enabling DuckDB's CRS-mismatch check}
#'   \item{`nar_store(geom)`}{the inverse, dropping the tag so the column stays RTREE-indexable}
#'   \item{`nar_wkb(geom)`}{WKB for transfer to \pkg{sf}, mapping NULL geometry to an empty point}
#' }
#' @param con A DuckDB connection
#' @param crs CRS of the stored geometry, read from the database by default
#' @return The connection, invisibly
#' @keywords internal
nar_register_spatial <- function(con, crs = nar_crs(con)) {
  lonlat <- nar_meta_value(con, "lonlat_crs", nar_lonlat_crs())

  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TEMP MACRO nar_point(lon, lat) AS
       st_transform(st_setcrs(st_point(lon, lat), '", lonlat, "'), '", crs, "', TRUE);"))

  DBI::dbExecute(con,
    "CREATE OR REPLACE TEMP MACRO nar_xy(x, y) AS st_point(x, y);")

  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TEMP MACRO nar_geom(geom) AS st_setcrs(geom, '", crs, "');"))

  # Inverse of nar_geom(): drops the CRS tag so the value can be stored in a
  # plain GEOMETRY column. Required for anything that gets an RTREE index,
  # which DuckDB refuses to build over a GEOMETRY('<crs>') column.
  DBI::dbExecute(con,
    "CREATE OR REPLACE TEMP MACRO nar_store(geom) AS geom::GEOMETRY;")

  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TEMP MACRO nar_lon(geom) AS
       st_x(st_transform(st_setcrs(geom, '", crs, "'), '", lonlat, "', TRUE));"))

  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TEMP MACRO nar_lat(geom) AS
       st_y(st_transform(st_setcrs(geom, '", crs, "'), '", lonlat, "', TRUE));"))

  # NAR has ~1.2M address records without coordinates; sf needs an empty point
  # rather than a NULL, and a NaN point is how DuckDB spells one in WKB.
  DBI::dbExecute(con,
    "CREATE OR REPLACE TEMP MACRO nar_wkb(geom) AS
       st_aswkb(coalesce(geom, st_point('nan'::DOUBLE, 'nan'::DOUBLE)));")

  invisible(con)
}

#' Resolve a coordinate input to the NAR storage CRS
#'
#' @description The single place where user-supplied coordinates are parsed, so
#' that an \code{sf} object and a bare lon/lat pair are treated identically and
#' each is reprojected exactly once. Reprojecting in \pkg{sf} rather than in
#' DuckDB keeps the transform under the caller's PROJ configuration, which is
#' what decides whether a WGS84-to-NAD83 datum shift is applied.
#' @param x An \code{sf}/\code{sfc} POINT object, or a length-2 numeric
#' longitude/latitude vector
#' @param crs CRS of `x` when `x` is a bare numeric vector, or when an
#' \code{sf} object carries no CRS. Defaults to EPSG:4326.
#' @param storage_crs CRS to return coordinates in
#' @return A length-2 numeric vector of coordinates in `storage_crs`
#' @keywords internal
nar_project <- function(x, crs = 4326, storage_crs = nar_storage_crs()) {
  if (inherits(x, "sf") || inherits(x, "sfc")) {
    geom <- sf::st_geometry(x)
    if (length(geom) != 1) {
      stop("Input x must contain exactly one point.")
    }
    if (is.na(sf::st_crs(geom))) {
      sf::st_crs(geom) <- sf::st_crs(crs)
    }
  } else if (is.numeric(x) && length(x) == 2 && all(is.finite(x))) {
    geom <- sf::st_sfc(sf::st_point(as.numeric(x)), crs = sf::st_crs(crs))
  } else {
    stop("Input x must be an sf POINT object or a length-2 numeric lon/lat vector.")
  }

  if (sf::st_crs(geom) != sf::st_crs(storage_crs)) {
    geom <- sf::st_transform(geom, sf::st_crs(storage_crs))
  }
  as.numeric(sf::st_coordinates(geom)[1, 1:2])
}

#' Whether a NAR table carries plain coordinate columns
#'
#' @param tbl A lazy table
#' @return `TRUE` if both `x` and `y` columns are present
#' @keywords internal
nar_has_xy <- function(tbl) all(c("x", "y") %in% colnames(tbl))

#' Filter a NAR table to rows within a radius of a point
#'
#' @description Takes coordinates already in the storage CRS -- use
#' [nar_project()] to get there -- and filters with `ST_DWithin`, the
#' extension's native distance predicate, which avoids the square root an
#' explicit `ST_Distance` comparison performs.
#'
#' When the table carries plain `x`/`y` `DOUBLE` columns, a bounding-box
#' prefilter is applied first. This is what makes radius queries fast, and it is
#' not an index: DuckDB keeps min/max zonemaps per row group for numeric
#' columns and skips whole row groups whose range cannot satisfy the
#' comparison. Measured over 17.3M addresses this cuts a query from ~0.21s to
#' ~0.04s, consistently across the country. The box is in the same planar,
#' metric CRS as the `ST_Distance` that follows it, so it cannot exclude a row
#' the distance predicate would have kept.
#'
#' This deliberately does *not* route through the RTREE index. Only
#' `ST_Intersects`-family predicates can drive that index, and while a
#' bounding-box prefilter does produce an index scan, it is slower here at every
#' radius once the row payload is fetched: the index yields row ids that must
#' then be randomly accessed across a ~5 GB file, whereas DuckDB's parallel
#' columnar scan evaluates the predicate over 17M rows in ~0.25s and stays flat
#' as the radius grows (measured: 0.24s vs 0.38s at 100m, 0.25s vs 1.5s at
#' 1000m). Neither an ART index on `x` nor ordering the table along a Hilbert
#' curve changed the timings at all. The RTREE index still pays off for
#' `count`/existence queries that never touch the rows, which is why it is
#' still built at import time.
#' @param tbl A lazy table with a `geom` column
#' @param x Easting of the search centre, in the storage CRS
#' @param y Northing of the search centre, in the storage CRS
#' @param radius Search radius in metres
#' @return A lazy table with an added `dist` column. Ordering is left to the
#' caller: any further verb wraps this in a subquery, and DuckDB drops
#' `ORDER BY` in subqueries without `LIMIT`, so sorting here would be silently
#' discarded rather than honoured.
#' @keywords internal
nar_within_radius <- function(tbl, x, y, radius) {
  if (nar_has_xy(tbl)) {
    tbl <- tbl |>
      filter(.data$x >= !!(x - radius), .data$x <= !!(x + radius),
             .data$y >= !!(y - radius), .data$y <= !!(y + radius))
  }
  tbl |>
    filter(st_dwithin(.data$geom, nar_xy(!!x, !!y), !!radius)) |>
    mutate(dist = st_distance(.data$geom, nar_xy(!!x, !!y)))
}

#' Extract the DBI connection behind a lazy table
#'
#' @param x A lazy table or a DBI connection
#' @return A DBI connection, or NULL
#' @keywords internal
nar_con <- function(x) {
  if (inherits(x, "DBIConnection")) return(x)
  con <- try(x[["src"]][["con"]], silent = TRUE)
  if (inherits(con, "try-error") || !inherits(con, "DBIConnection")) NULL else con
}
