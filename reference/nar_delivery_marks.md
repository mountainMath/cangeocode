# Flag the address forms NAR has no civic address for

Post office boxes and rural routes are delivery instructions, not
locations, and NAR contains neither. Recognizing them is worth more than
parsing them: it separates "this address is wrong" from "this address
was never going to be in the gazetteer", which are very different
problems for whoever is looking at the output.

## Usage

``` r
nar_delivery_marks(txt)
```

## Arguments

- txt:

  A character vector of normalized address strings

## Value

A character vector of \`"po_box"\`, \`"rural_route"\` or \`NA\`
