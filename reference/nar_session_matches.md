# Does the parked connection answer this version request?

\`"latest"\` matches whatever is parked. That is deliberate: the point
of parking is to stop asking StatCan what "latest" means, and a release
published mid-session is not a reason to switch databases underneath a
running script. Name the release, or \[close_nar()\], to move.

## Usage

``` r
nar_session_matches(st, version)
```

## Arguments

- st:

  Session state

- version:

  Requested version

## Value

\`TRUE\` if the parked connection may be reused
