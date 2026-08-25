# How the NAR tier ranks the addresses that matched

A building point always outranks a blockface one for the same address, a
record with no point at all comes last, and \`ADDR_GUID\` breaks any
remaining tie so the answer is stable across runs rather than depending
on scan order.

## Usage

``` r
nar_geocode_nar_rank()
```

## Value

A SQL \`ORDER BY\` expression
