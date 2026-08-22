# Map the empty string back to NA

\[nar_paste_parts()\] treats \`NA\` as absent but an empty string as a
part, so anything assembled before it is handed over has to say which
one it means.

## Usage

``` r
nar_blank_na(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector with \`""\` replaced by \`NA\`
