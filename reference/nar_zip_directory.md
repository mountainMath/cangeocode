# Locate and parse a zip archive's central directory

Reads the end-of-central-directory record from the tail of the archive,
then the directory itself, and returns one row per member. ZIP64 is
handled in both places – the locator at the tail and the per-entry extra
field – because the archive is already 1.7 GB and the 32-bit fields it
still fits in are not a safe assumption for future releases.

## Usage

``` r
nar_zip_directory(reader)
```

## Arguments

- reader:

  A reader function, from \[nar_range_reader()\] or
  \[nar_file_reader()\]

## Value

A data frame with one row per member: \`name\`, \`method\`, \`flags\`,
\`time\`, \`date\`, \`crc\`, \`csize\`, \`usize\`, \`offset\`
