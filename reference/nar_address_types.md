# Non-string column types in the NAR address file

Every other column is read as a string, matching the original StatCan
text. Only the columns listed here are given a numeric type.

## Usage

``` r
nar_address_types()
```

## Value

A named list of \`arrow\` types
