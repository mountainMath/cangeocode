
# cangeocode

<!-- badges: start -->
<!-- badges: end -->

The goal of cangeocode is to provide functionality for geocoding and reverse geocoding addresses in Canada.

## Installation

You can install the development version of cangeocode via:

``` r
remotes::install_github("mountainMath/cangeocode")
```

## Example

This is a basic example to get all residential addresses within 100m of a given location:

``` r
library(cangeocode)
reverse_geocode(c(-123.2,49.25))
```

