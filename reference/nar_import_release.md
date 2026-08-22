# Import a NAR release, or add provinces to an existing database

Wraps the whole create-or-append decision so \[nar_connection()\] reads
as a sequence of steps rather than as two interleaved import paths.

A fresh import builds into a side path and is published by renaming, so
a run that fails partway leaves no database behind that later calls
would mistake for a finished one. An append has no such luxury – it
writes into the live file – so it is the coverage metadata that is
updated last, after the data and the derived tables are in place. A
crash mid-append therefore leaves a database that under-reports what it
holds, which costs a redundant download rather than producing wrong
answers.

## Usage

``` r
nar_import_release(nar_path, exdir, version, plan)
```

## Arguments

- nar_path:

  Path to the \`\<version\>.duckdb\` file

- exdir:

  Directory holding the extracted NAR CSVs

- version:

  Normalized version string

- plan:

  The list returned by \[nar_import_plan()\]

## Value

\`nar_path\`, invisibly
