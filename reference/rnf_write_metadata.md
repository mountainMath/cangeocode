# Record what was imported, in the same metadata table NAR uses

Keyed with an \`rnf\_\` prefix, so \[nar_metadata()\] reports the whole
state of the database in one read and a database with no RNF simply has
no such keys. The counts of what was \*left out\* are recorded here
because the tables themselves no longer carry it: only named, ranged
segments are stored, and how much of the file that was is the tier's
coverage ceiling.

## Usage

``` r
rnf_write_metadata(con, release, shp)
```

## Arguments

- con:

  A writable DuckDB connection

- release:

  The release that was read

- shp:

  Path the file was read from

## Value

The connection, invisibly
