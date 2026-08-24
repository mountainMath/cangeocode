# Let go of the database so it can be written

A read-only handle blocks an import. Called by every path that is about
to open the file for writing, and silent unless it actually had to close
something, since the common case is that no session connection exists.

## Usage

``` r
nar_session_release(path)
```

## Arguments

- path:

  Database file about to be written

## Value

\`TRUE\` if a connection was closed
