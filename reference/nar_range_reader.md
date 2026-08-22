# A reader over an HTTP resource, using range requests

Establishes the resource size with a HEAD request, then serves each read
as a \`Range\` request. A server that ignores the range and answers
\`200\` with the whole body is rejected rather than accepted, since
silently downloading 1.7 GB to satisfy a 7 KB read is exactly what this
exists to avoid.

## Usage

``` r
nar_range_reader(url)
```

## Arguments

- url:

  URL of the archive

## Value

A function of \`(from, len)\` returning raw bytes, carrying the resource
size as its \`size\` attribute
