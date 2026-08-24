# Open a NAR connection for the session to reuse

\`geocode()\` and \`reverse_geocode()\` open a connection when none is
passed, and keep it open for the next call. \`open_nar()\` does that up
front, which is worth doing for two reasons: to name a release other
than the latest, or a province subset, without repeating it at every
call site; and to pay the connection cost at a moment of your choosing
rather than inside the first thing you time.

It is never required. Calling nothing at all gives the same connection,
just opened lazily.

## Usage

``` r
open_nar(version = "latest", provinces = NULL)
```

## Arguments

- version:

  Version of the NAR database to open. Default is \`"latest"\`.

- provinces:

  Provinces to make available, as for \[nar_connection()\].

## Value

The connection, invisibly. Passing it explicitly as \`con\` is
equivalent to leaving \`con\` unset.

## Details

Once a connection is parked, a call that asks for \`"latest"\` gets it
without asking StatCan what the latest release is. This is the intended
behaviour and not an optimization detail: a release published while a
script is running should not change which database that script is
reading. To move to another release, name it, or \[close_nar()\] first.

The connection is read-only. An import that needs the write lock –
\[nar_connection()\] with \`refresh = TRUE\`, \[rqa_import()\],
\[rnf_import()\] – closes it first and says so; later calls reopen it.

## See also

\[close_nar()\], \[nar_connection()\] for a connection you own and close
yourself.

## Examples

``` r
if (FALSE) { # \dontrun{
open_nar()
geocode("100 Queen St W, Toronto, ON")
reverse_geocode(c(-79.383, 43.653))
close_nar()

# A specific release, for the rest of the session.
open_nar(version = "2025-12")
} # }
```
