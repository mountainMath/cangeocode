# A reader over a local file

The test-side implementation of the reader interface: same signature as
\[nar_range_reader()\], no network.

## Usage

``` r
nar_file_reader(path)
```

## Arguments

- path:

  Path to a file

## Value

A function of \`(from, len)\` returning raw bytes, carrying the file
size as its \`size\` attribute
