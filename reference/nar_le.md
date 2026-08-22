# Read a little-endian unsigned integer out of a raw vector

\`readBin()\` reads four bytes as a \*signed\* integer, so any value
past 2^31 comes back negative – offsets into a 1.7 GB archive are
already close to that, and a growing NAR will cross it. Accumulating
into a double keeps every field exact well past 4 GB.

## Usage

``` r
nar_le(bytes, at, n)
```

## Arguments

- bytes:

  A raw vector

- at:

  Zero-based offset of the field

- n:

  Field width in bytes

## Value

A numeric scalar
