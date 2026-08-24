# Session state for the implicitly cached NAR connection

Holds at most one connection, its resolved version key and the file it
points at. Not exported and not an option: a stale connection is
detected by asking DBI, never by trusting what was stored here.

## Usage

``` r
.nar_session
```
