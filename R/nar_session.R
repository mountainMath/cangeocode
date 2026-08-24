# A connection to the NAR database that outlives the call that opened it.
#
# Every entry point takes an explicit `con`, and that stays the way to be
# unambiguous about which database answered. But a connection is expensive
# enough -- opening the file, loading the spatial extension, redefining the
# TEMP macros, about half a second on a national import -- that making
# `geocode()` pay it per call punishes the obvious way to write the code. So a
# call that opens its own connection parks it here instead of closing it, and
# the next one reuses it.
#
# The connection is read-only, so parking it costs a file handle and blocks
# nothing except a writer. Writers are the reason `nar_session_release()`
# exists: an import has to be able to take the write lock, and the session is
# the one holder the package knows how to get out of the way.

#' Session state for the implicitly cached NAR connection
#'
#' @description Holds at most one connection, its resolved version key and the
#' file it points at. Not exported and not an option: a stale connection is
#' detected by asking DBI, never by trusting what was stored here.
#' @keywords internal
.nar_session <- new.env(parent = emptyenv())

#' The parked connection, if there is a usable one
#'
#' @description Validity is re-checked on every read rather than assumed. The
#' connection can die without this package hearing about it -- a caller can
#' disconnect the object [open_nar()] returned, and the duckdb driver can be
#' finalized -- and a dead handle must look like no handle at all.
#' @return The session state list, or `NULL`
#' @keywords internal
nar_session_state <- function() {
  st <- .nar_session$state
  if (is.null(st)) return(NULL)
  ok <- tryCatch(DBI::dbIsValid(st$con), error = function(e) FALSE)
  if (!ok) {
    .nar_session$state <- NULL
    return(NULL)
  }
  st
}

#' Park a connection for the rest of the session
#'
#' @description The version key is read back out of the database rather than
#' taken from the request, so `"latest"` is stored as the release it resolved
#' to and a later call naming that release explicitly matches it.
#' @param con An open NAR connection
#' @return The connection, invisibly
#' @keywords internal
nar_session_store <- function(con) {
  version <- nar_meta_value(con, "version", NA_character_)
  path <- if (is.na(version)) NA_character_ else
    file.path(Sys.getenv("NAR_CACHE_PATH"), paste0(version, ".duckdb"))
  .nar_session$state <- list(con = con, version = version, path = path)
  invisible(con)
}

#' Does the parked connection answer this version request?
#'
#' @description `"latest"` matches whatever is parked. That is deliberate: the
#' point of parking is to stop asking StatCan what "latest" means, and a
#' release published mid-session is not a reason to switch databases underneath
#' a running script. Name the release, or [close_nar()], to move.
#' @param st Session state
#' @param version Requested version
#' @return `TRUE` if the parked connection may be reused
#' @keywords internal
nar_session_matches <- function(st, version) {
  if (identical(version, "latest")) return(TRUE)
  if (identical(version, st$version)) return(TRUE)
  cache <- Sys.getenv("NAR_CACHE_PATH")
  if (!nzchar(cache)) return(FALSE)
  resolved <- try(nar_resolve_version(version, cache), silent = TRUE)
  !inherits(resolved, "try-error") && identical(resolved, st$version)
}

#' Resolve the connection an entry point should use
#'
#' @description The implicit half of the `con` argument: reuse what is parked,
#' otherwise open one and park it. Callers do not close what this returns --
#' that is the whole difference from calling [nar_connection()] directly.
#' @param version Version of the NAR database, as passed to the entry point
#' @return An open NAR connection owned by the session
#' @keywords internal
nar_session_use <- function(version = "latest") {
  st <- nar_session_state()
  if (!is.null(st) && nar_session_matches(st, version)) return(st$con)
  con <- nar_connection(version = version)
  nar_session_store(con)
  con
}

#' Let go of the database so it can be written
#'
#' @description A read-only handle blocks an import. Called by every path that
#' is about to open the file for writing, and silent unless it actually had to
#' close something, since the common case is that no session connection exists.
#' @param path Database file about to be written
#' @return `TRUE` if a connection was closed
#' @keywords internal
nar_session_release <- function(path) {
  st <- nar_session_state()
  if (is.null(st) || !identical(st$path, path)) return(invisible(FALSE))
  message("Closing the session NAR connection so ", basename(path),
          " can be written. Later calls will reopen it.")
  close_nar()
}

#' Open a NAR connection for the session to reuse
#'
#' @description `geocode()` and `reverse_geocode()` open a connection when none
#' is passed, and keep it open for the next call. `open_nar()` does that
#' up front, which is worth doing for two reasons: to name a release other than
#' the latest, or a province subset, without repeating it at every call site;
#' and to pay the connection cost at a moment of your choosing rather than
#' inside the first thing you time.
#'
#' It is never required. Calling nothing at all gives the same connection, just
#' opened lazily.
#'
#' @details Once a connection is parked, a call that asks for `"latest"` gets
#' it without asking StatCan what the latest release is. This is the intended
#' behaviour and not an optimization detail: a release published while a script
#' is running should not change which database that script is reading. To move
#' to another release, name it, or [close_nar()] first.
#'
#' The connection is read-only. An import that needs the write lock --
#' [nar_connection()] with `refresh = TRUE`, [rqa_import()], [rnf_import()] --
#' closes it first and says so; later calls reopen it.
#'
#' @param version Version of the NAR database to open. Default is `"latest"`.
#' @param provinces Provinces to make available, as for [nar_connection()].
#' @return The connection, invisibly. Passing it explicitly as `con` is
#'   equivalent to leaving `con` unset.
#' @seealso [close_nar()], [nar_connection()] for a connection you own and
#'   close yourself.
#' @export
#' @examples
#' \dontrun{
#' open_nar()
#' geocode("100 Queen St W, Toronto, ON")
#' reverse_geocode(c(-79.383, 43.653))
#' close_nar()
#'
#' # A specific release, for the rest of the session.
#' open_nar(version = "2025-12")
#' }
open_nar <- function(version = "latest", provinces = NULL) {
  st <- nar_session_state()
  if (!is.null(st) && is.null(provinces) && nar_session_matches(st, version)) {
    return(invisible(st$con))
  }
  # A province request may import, and importing needs the write lock this
  # session connection is holding.
  if (!is.null(st)) close_nar()
  con <- nar_connection(version = version, provinces = provinces)
  invisible(nar_session_store(con))
}

#' Close the session's NAR connection
#'
#' @description Releases the connection [open_nar()] opened, or the one a bare
#' [geocode()] or [reverse_geocode()] call opened for itself. Safe to call when
#' there is nothing open.
#'
#' Worth calling before an import, and worth calling in a long-running process
#' that is done with NAR. Nothing else needs it: the connection is read-only,
#' and R releases it at the end of the session anyway.
#'
#' @return `TRUE` if a connection was closed, `FALSE` if there was none, invisibly.
#' @seealso [open_nar()]
#' @export
#' @examples
#' \dontrun{
#' open_nar()
#' close_nar()
#' }
close_nar <- function() {
  st <- .nar_session$state
  .nar_session$state <- NULL
  if (is.null(st)) return(invisible(FALSE))
  try(DBI::dbDisconnect(st$con, shutdown = TRUE), silent = TRUE)
  invisible(TRUE)
}
