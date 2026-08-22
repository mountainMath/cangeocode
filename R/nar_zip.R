# Reading selected members out of a remote zip archive.
#
# StatCan publishes NAR as one ~1.7 GB zip whose members are split by province
# -- Addresses/Address_59_part_1.csv is British Columbia. The www150 server
# honours HTTP range requests, so a caller who wants one province can read the
# archive's own index for a few kilobytes and then fetch only the members that
# province needs: 192 MB for BC against 1,665 MB for the country.
#
# Everything here goes through a *reader*, a function(from, len) returning raw
# bytes, rather than reaching for the network directly. That is the seam the
# tests use: nar_file_reader() serves a local file through the identical
# interface, so the zip parsing and reassembly are exercised without a server
# -- the same reason nar_version_table() takes an already-parsed document.

#' Read a little-endian unsigned integer out of a raw vector
#'
#' @description `readBin()` reads four bytes as a *signed* integer, so any
#' value past 2^31 comes back negative -- offsets into a 1.7 GB archive are
#' already close to that, and a growing NAR will cross it. Accumulating into a
#' double keeps every field exact well past 4 GB.
#' @param bytes A raw vector
#' @param at Zero-based offset of the field
#' @param n Field width in bytes
#' @return A numeric scalar
#' @keywords internal
nar_le <- function(bytes, at, n) {
  sum(as.numeric(bytes[(at + 1):(at + n)]) * 256^(seq_len(n) - 1))
}

#' Encode a number as little-endian bytes
#'
#' @param x A non-negative number
#' @param n Field width in bytes
#' @return A raw vector of length `n`
#' @keywords internal
nar_le_raw <- function(x, n) {
  as.raw(floor(x / 256^(seq_len(n) - 1)) %% 256)
}

#' A reader over a local file
#'
#' @description The test-side implementation of the reader interface: same
#' signature as [nar_range_reader()], no network.
#' @param path Path to a file
#' @return A function of `(from, len)` returning raw bytes, carrying the file
#'   size as its `size` attribute
#' @keywords internal
nar_file_reader <- function(path) {
  size <- file.size(path)
  structure(function(from, len) {
    conn <- file(path, "rb")
    on.exit(close(conn))
    seek(conn, where = from)
    readBin(conn, "raw", n = len)
  }, size = size)
}

#' A reader over an HTTP resource, using range requests
#'
#' @description Establishes the resource size with a HEAD request, then serves
#' each read as a `Range` request. A server that ignores the range and answers
#' `200` with the whole body is rejected rather than accepted, since silently
#' downloading 1.7 GB to satisfy a 7 KB read is exactly what this exists to
#' avoid.
#' @param url URL of the archive
#' @return A function of `(from, len)` returning raw bytes, carrying the
#'   resource size as its `size` attribute
#' @keywords internal
nar_range_reader <- function(url) {
  head_handle <- curl::new_handle(nobody = TRUE, followlocation = TRUE)
  probe <- curl::curl_fetch_memory(url, handle = head_handle)
  if (probe$status_code >= 400) {
    stop("Could not read ", url, " (HTTP ", probe$status_code, ").")
  }
  headers <- curl::parse_headers_list(probe$headers)
  size <- suppressWarnings(as.numeric(headers[["content-length"]]))
  if (!length(size) || is.na(size) || size <= 0) {
    stop("The server did not report a size for ", url,
         ", so the archive index cannot be located without downloading it.")
  }

  structure(function(from, len) {
    handle <- curl::new_handle(followlocation = TRUE)
    curl::handle_setheaders(handle,
      Range = sprintf("bytes=%.0f-%.0f", from, from + len - 1))
    resp <- curl::curl_fetch_memory(url, handle = handle)
    if (resp$status_code != 206) {
      stop("The server at ", url, " does not support range requests ",
           "(HTTP ", resp$status_code, " for a ranged read). ",
           "Download the whole release instead with provinces = \"all\".")
    }
    resp$content
  }, size = size)
}

#' Locate and parse a zip archive's central directory
#'
#' @description Reads the end-of-central-directory record from the tail of the
#' archive, then the directory itself, and returns one row per member. ZIP64 is
#' handled in both places -- the locator at the tail and the per-entry extra
#' field -- because the archive is already 1.7 GB and the 32-bit fields it
#' still fits in are not a safe assumption for future releases.
#' @param reader A reader function, from [nar_range_reader()] or
#'   [nar_file_reader()]
#' @return A data frame with one row per member: `name`, `method`, `flags`,
#'   `time`, `date`, `crc`, `csize`, `usize`, `offset`
#' @keywords internal
nar_zip_directory <- function(reader) {
  size <- attr(reader, "size")

  # The EOCD is the last record in the file and is at most 22 bytes plus a
  # comment of up to 65535, so it is always inside the final 64 KB or so.
  tail_len <- min(size, 65557 + 22)
  tail_bytes <- reader(size - tail_len, tail_len)

  sig <- as.raw(c(0x50, 0x4b, 0x05, 0x06))
  starts <- which(tail_bytes[1:(length(tail_bytes) - 3)] == sig[1])
  hits <- Filter(function(i) identical(tail_bytes[i:(i + 3)], sig), starts)
  if (!length(hits)) {
    stop("No zip end-of-central-directory record found; the archive is not a ",
         "zip file, or the server truncated the response.")
  }
  eocd <- max(unlist(hits)) - 1L

  n_entries <- nar_le(tail_bytes, eocd + 10, 2)
  cd_size <- nar_le(tail_bytes, eocd + 12, 4)
  cd_offset <- nar_le(tail_bytes, eocd + 16, 4)

  # A 0xFFFF/0xFFFFFFFF sentinel in the classic EOCD means the real values live
  # in the ZIP64 record, which the locator immediately before it points at.
  if (n_entries == 0xFFFF || cd_size == 0xFFFFFFFF || cd_offset == 0xFFFFFFFF) {
    loc_sig <- as.raw(c(0x50, 0x4b, 0x06, 0x07))
    loc <- Filter(function(i) identical(tail_bytes[i:(i + 3)], loc_sig),
                  which(tail_bytes[1:(length(tail_bytes) - 3)] == loc_sig[1]))
    if (!length(loc)) stop("ZIP64 archive with no ZIP64 locator record.")
    z64_offset <- nar_le(tail_bytes, max(unlist(loc)) - 1L + 8, 8)
    z64 <- reader(z64_offset, 56)
    n_entries <- nar_le(z64, 32, 8)
    cd_size <- nar_le(z64, 40, 8)
    cd_offset <- nar_le(z64, 48, 8)
  }

  cd <- reader(cd_offset, cd_size)

  entries <- vector("list", n_entries)
  at <- 0
  for (i in seq_len(n_entries)) {
    if (!identical(cd[(at + 1):(at + 4)], as.raw(c(0x50, 0x4b, 0x01, 0x02)))) {
      stop("Malformed zip central directory at entry ", i, ".")
    }
    nlen <- nar_le(cd, at + 28, 2)
    elen <- nar_le(cd, at + 30, 2)
    clen <- nar_le(cd, at + 32, 2)
    name <- rawToChar(cd[(at + 46 + 1):(at + 46 + nlen)])
    Encoding(name) <- "UTF-8"

    entry <- list(
      name = name,
      flags = nar_le(cd, at + 8, 2),
      method = nar_le(cd, at + 10, 2),
      time = nar_le(cd, at + 12, 2),
      date = nar_le(cd, at + 14, 2),
      crc = nar_le(cd, at + 16, 4),
      csize = nar_le(cd, at + 20, 4),
      usize = nar_le(cd, at + 24, 4),
      offset = nar_le(cd, at + 42, 4)
    )

    # ZIP64 extra field: the oversized fields appear in the order
    # uncompressed, compressed, offset, and only those that were sentinelled.
    if (elen > 0) {
      extra <- cd[(at + 46 + nlen + 1):(at + 46 + nlen + elen)]
      ex <- 0
      while (ex + 4 <= elen) {
        id <- nar_le(extra, ex, 2)
        len <- nar_le(extra, ex + 2, 2)
        if (id == 0x0001) {
          pos <- ex + 4
          for (field in c("usize", "csize", "offset")) {
            sentinel <- entry[[field]] == 0xFFFFFFFF
            if (sentinel && pos + 8 <= ex + 4 + len) {
              entry[[field]] <- nar_le(extra, pos, 8)
              pos <- pos + 8
            }
          }
          break
        }
        ex <- ex + 4 + len
      }
    }

    entries[[i]] <- entry
    at <- at + 46 + nlen + elen + clen
  }

  out <- do.call(rbind.data.frame, c(entries, list(stringsAsFactors = FALSE)))
  out[order(out$offset), , drop = FALSE]
}

#' Copy selected members of a remote zip into a small local zip
#'
#' @description Fetches each selected member's compressed bytes and writes a
#' fresh, self-contained archive containing only those members. The compressed
#' data is copied verbatim -- nothing is inflated here -- so the result is a
#' normal zip that [utils::unzip()] extracts, which keeps the import path
#' identical whether the caller downloaded one province or the country.
#'
#' Local headers are rebuilt from the central directory rather than copied,
#' because the central directory is the authoritative record of the sizes: when
#' an archive sets the streaming flag, the local header carries zeros and the
#' real sizes trail the data. Rebuilding also lets that flag be cleared, so no
#' data descriptors are needed in the output.
#' @param reader A reader function over the source archive
#' @param entries Rows of [nar_zip_directory()] to copy
#' @param dest Path to write the new archive to
#' @return `dest`, invisibly
#' @keywords internal
nar_zip_copy_members <- function(reader, entries, dest) {
  if (!nrow(entries)) stop("No zip members selected to copy.")
  if (sum(entries$csize) + 4096 * nrow(entries) >= 2^32) {
    stop("The selected members exceed the 4 GB a plain zip can address. ",
         "Download the whole release instead with provinces = \"all\".")
  }

  conn <- file(dest, "wb")
  on.exit(close(conn), add = TRUE)

  written <- 0
  headers <- vector("list", nrow(entries))

  for (i in seq_len(nrow(entries))) {
    e <- entries[i, ]
    name <- charToRaw(enc2utf8(e$name))

    # The local header's own name and extra lengths need not match the central
    # directory's, so read the 30 fixed bytes to find where the data starts
    # rather than assuming.
    local <- reader(e$offset, 30)
    if (!identical(local[1:4], as.raw(c(0x50, 0x4b, 0x03, 0x04)))) {
      stop("Expected a local file header for ", e$name, " at offset ", e$offset, ".")
    }
    data_at <- e$offset + 30 + nar_le(local, 26, 2) + nar_le(local, 28, 2)
    data <- reader(data_at, e$csize)
    if (length(data) != e$csize) {
      stop("Short read for ", e$name, ": expected ", e$csize, " bytes, got ",
           length(data), ".")
    }

    flags <- bitwAnd(as.integer(e$flags), bitwNot(0x0008L))
    header <- c(as.raw(c(0x50, 0x4b, 0x03, 0x04)),
                nar_le_raw(20, 2), nar_le_raw(flags, 2),
                nar_le_raw(e$method, 2), nar_le_raw(e$time, 2),
                nar_le_raw(e$date, 2), nar_le_raw(e$crc, 4),
                nar_le_raw(e$csize, 4), nar_le_raw(e$usize, 4),
                nar_le_raw(length(name), 2), nar_le_raw(0, 2))

    writeBin(c(header, name), conn)
    writeBin(data, conn)

    headers[[i]] <- list(entry = e, name = name, flags = flags, offset = written)
    written <- written + length(header) + length(name) + e$csize
  }

  cd_start <- written
  for (h in headers) {
    e <- h$entry
    central <- c(as.raw(c(0x50, 0x4b, 0x01, 0x02)),
                 nar_le_raw(20, 2), nar_le_raw(20, 2),
                 nar_le_raw(h$flags, 2), nar_le_raw(e$method, 2),
                 nar_le_raw(e$time, 2), nar_le_raw(e$date, 2),
                 nar_le_raw(e$crc, 4), nar_le_raw(e$csize, 4),
                 nar_le_raw(e$usize, 4), nar_le_raw(length(h$name), 2),
                 nar_le_raw(0, 2), nar_le_raw(0, 2), nar_le_raw(0, 2),
                 nar_le_raw(0, 2), nar_le_raw(0, 4), nar_le_raw(h$offset, 4))
    writeBin(c(central, h$name), conn)
    written <- written + length(central) + length(h$name)
  }

  eocd <- c(as.raw(c(0x50, 0x4b, 0x05, 0x06)),
            nar_le_raw(0, 2), nar_le_raw(0, 2),
            nar_le_raw(nrow(entries), 2), nar_le_raw(nrow(entries), 2),
            nar_le_raw(written - cd_start, 4), nar_le_raw(cd_start, 4),
            nar_le_raw(0, 2))
  writeBin(eocd, conn)

  invisible(dest)
}

#' Which zip members belong to which provinces
#'
#' @description NAR names its member files by SGC province code, optionally
#' split into parts: `Addresses/Address_35_part_3.csv` is Ontario, and
#' `Addresses/Address_11.csv` is Prince Edward Island in one piece. Anything
#' that is not a per-province CSV -- the user guides, the readme, the directory
#' entries -- gets `NA` and is carried along regardless, since it is negligible
#' in size and the guides are worth having.
#' @param names Member names from [nar_zip_directory()]
#' @return A character vector of province abbreviations, `NA` where the member
#'   is not province-specific
#' @keywords internal
nar_zip_member_province <- function(names) {
  code <- sub("^.*/(?:Address|Location)_([0-9]{2})(?:_part_[0-9]+)?\\.csv$",
              "\\1", names)
  code[code == names] <- NA_character_
  tbl <- nar_province_table()
  tbl$abvn[match(code, tbl$code)]
}

# Reading the index of the real 1.7 GB release costs about half a minute --
# the central directory is a single ~7 KB range read, but the server is slow to
# first byte. The interactive prompt and the download that follows it would each
# pay that, so the parsed directory is memoized for the session, keyed by URL.
# A release at a given URL is immutable, so there is nothing to invalidate.
nar_zip_dir_cache <- new.env(parent = emptyenv())

#' The parsed index of a remote NAR release, read at most once per session
#'
#' @param url URL of the StatCan release zip
#' @return The data frame [nar_zip_directory()] returns, with a `prov` column
#' @keywords internal
nar_release_directory <- function(url) {
  hit <- nar_zip_dir_cache[[url]]
  if (!is.null(hit)) return(hit)
  dir <- nar_zip_directory(nar_range_reader(url))
  dir$prov <- nar_zip_member_province(dir$name)
  nar_zip_dir_cache[[url]] <- dir
  dir
}

#' Download only the members of a NAR release a set of provinces needs
#'
#' @description Reads the release's zip index over range requests, selects the
#' members for the requested provinces, and writes them to a local archive. The
#' bytes actually transferred are reported, because the whole point of this path
#' is that they are a fraction of the release.
#' @param url URL of the StatCan release zip
#' @param provinces Canonical province abbreviations
#' @param dest Path to write the reduced archive to
#' @return `dest`, invisibly
#' @keywords internal
nar_download_provinces <- function(url, provinces, dest) {
  dir <- nar_release_directory(url)

  # Directory entries carry no data and would add empty members to the output.
  dir <- dir[dir$usize > 0 | !is.na(dir$prov), , drop = FALSE]

  wanted <- dir[is.na(dir$prov) | dir$prov %in% provinces, , drop = FALSE]
  missing <- setdiff(provinces, stats::na.omit(dir$prov))
  if (length(missing)) {
    stop("The release at ", url, " contains no data for ",
         paste(missing, collapse = ", "),
         ". Its members are named for ",
         paste(sort(unique(stats::na.omit(dir$prov))), collapse = ", "), ".")
  }

  message("Downloading ", nar_coverage_label(provinces), " (",
          format(round(sum(wanted$csize) / 1e6), big.mark = ","), " MB of ",
          format(round(sum(dir$csize) / 1e6), big.mark = ","), " MB) from StatCan.")

  nar_zip_copy_members(nar_range_reader(url), wanted, dest)
}

#' Download size per province for a NAR release
#'
#' @description Reads the release's zip index -- a few kilobytes over range
#' requests, no data transfer -- and totals the compressed size of each
#' province's members. Used to put real numbers in front of the interactive
#' prompt rather than estimates.
#' @param url URL of the StatCan release zip
#' @return A data frame of `abvn`, `name` and `mb`, plus an `ALL` row, ordered
#'   as [nar_province_table()] is
#' @keywords internal
nar_release_sizes <- function(url) {
  dir <- nar_release_directory(url)
  shared <- sum(dir$csize[is.na(dir$prov)])

  # Only the provinces the release actually carries: a province with no members
  # would otherwise be offered at the shared members' size and then fail.
  tbl <- nar_province_table()
  tbl <- tbl[tbl$abvn %in% dir$prov, , drop = FALSE]
  tbl$mb <- vapply(tbl$abvn, function(p) {
    (sum(dir$csize[!is.na(dir$prov) & dir$prov == p]) + shared) / 1e6
  }, numeric(1))
  rbind(tbl[, c("abvn", "name", "mb")],
        data.frame(abvn = nar_all_provinces(), name = "All of Canada",
                   mb = sum(dir$csize) / 1e6, stringsAsFactors = FALSE))
}
