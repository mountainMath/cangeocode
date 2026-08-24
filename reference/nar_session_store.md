# Park a connection for the rest of the session

The version key is read back out of the database rather than taken from
the request, so \`"latest"\` is stored as the release it resolved to and
a later call naming that release explicitly matches it.

## Usage

``` r
nar_session_store(con)
```

## Arguments

- con:

  An open NAR connection

## Value

The connection, invisibly
