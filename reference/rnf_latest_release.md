# The newest RNF release the server actually has

Probed rather than hard-coded, so the package does not quietly stay on
one release forever, and probed with \`HEAD\` rather than scraped: the
file names are entirely regular, so the only question is which of them
exist, and that is one request each to answer. The walk starts at the
current year and goes back, because a release appears partway through
its year.

\*\*A status code is not an answer here.\*\* StatCan serves a missing
release as a 302 to a 4 KB HTML error page returned with \`200 OK\`, so
a probe that tests \`status_code \< 400\` accepts a release that does
not exist and the failure surfaces much later, as an unzip error. The
content type is what distinguishes them: a real release answers
\`application/x-zip-compressed\` and hundreds of megabytes, the error
page answers \`text/html\`. Both are checked, because a server that lies
about one may lie about the other.

The catalogue page for 92-500-X is the human-readable counterpart and
lists the issues as \`92-500-X\<year\>001\`; it is not scraped here
because it does not say which distribution formats were published, which
is the part that varies.

## Usage

``` r
rnf_latest_release(from = as.integer(format(Sys.Date(), "%y")), back = 8L)
```

## Arguments

- from:

  Two-digit year to start the walk at

- back:

  How many years back to look before giving up

## Value

A two-digit release string
