# Close the session's NAR connection

Releases the connection \[open_nar()\] opened, or the one a bare
\[geocode()\] or \[reverse_geocode()\] call opened for itself. Safe to
call when there is nothing open.

Worth calling before an import, and worth calling in a long-running
process that is done with NAR. Nothing else needs it: the connection is
read-only, and R releases it at the end of the session anyway.

## Usage

``` r
close_nar()
```

## Value

\`TRUE\` if a connection was closed, \`FALSE\` if there was none,
invisibly.

## See also

\[open_nar()\]

## Examples

``` r
if (FALSE) { # \dontrun{
open_nar()
close_nar()
} # }
```
