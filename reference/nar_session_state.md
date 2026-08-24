# The parked connection, if there is a usable one

Validity is re-checked on every read rather than assumed. The connection
can die without this package hearing about it – a caller can disconnect
the object \[open_nar()\] returned, and the duckdb driver can be
finalized – and a dead handle must look like no handle at all.

## Usage

``` r
nar_session_state()
```

## Value

The session state list, or \`NULL\`
