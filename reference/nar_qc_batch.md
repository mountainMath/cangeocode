# Send one batch of addresses to the Quebec geocoder

One HTTP request for up to \`MaxBatchSize\` addresses. A failed request
is data, not an exception: the batch comes back as unanswered rows so
one unreachable request does not abandon the rest of the vector.

## Usage

``` r
nar_qc_batch(q, rate = 5, crs = 4326)
```

## Arguments

- q:

  Address strings, at most 1000

- rate:

  Requests per second

- crs:

  Output SRS to ask the service for

## Value

A data frame as \[nar_qc_locations()\] returns
