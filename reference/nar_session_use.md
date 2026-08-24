# Resolve the connection an entry point should use

The implicit half of the \`con\` argument: reuse what is parked,
otherwise open one and park it. Callers do not close what this returns –
that is the whole difference from calling \[nar_connection()\] directly.

## Usage

``` r
nar_session_use(version = "latest")
```

## Arguments

- version:

  Version of the NAR database, as passed to the entry point

## Value

An open NAR connection owned by the session
