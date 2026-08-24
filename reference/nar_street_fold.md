# Fold every gazetteer name once per connection

\[nar_match_fold_sql()\] is six string operations, and the fuzzy branch
evaluates it against every candidate street of every probe row. That is
the same 511,848 names folded over and over: measured on the Part A
sample it cost 45 217. Folding the whole gazetteer once instead takes 68
ms.

So it is done once per connection and kept, the same way the spatial
macros are – a TEMP table, invisible to other sessions, dropped when the
connection closes. \`Streets\` is written once at import and never
updated, so \`rowid\` is a stable key to join it back on.

The alternative was a stored column and a schema bump, which would make
every database built before it slower rather than merely different. This
costs nothing at import and needs no re-import.

## Usage

``` r
nar_street_fold(con)
```

## Arguments

- con:

  An open NAR connection

## Value

Invisibly \`TRUE\` when the table is present
