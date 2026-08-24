# Do these response headers describe an actual zip?

Split out of \[rnf_latest_release()\] so the soft-404 rule is testable
without a network call. A real release is \`application/x-zip-\*\` and
hundreds of megabytes; StatCan's missing-file page is \`text/html\` and
4 KB, served with \`200 OK\` after a redirect. The size floor is
deliberately far below the smallest real release (296 MB for 2024) – it
is there to catch an error page, not to police the file.

## Usage

``` r
rnf_headers_are_a_zip(headers)
```

## Arguments

- headers:

  Raw or character response headers from \[curl::curl_fetch_memory()\]

## Value

\`TRUE\` when the headers describe a zip large enough to be a release
