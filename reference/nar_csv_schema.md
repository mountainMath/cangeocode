# Build an Arrow schema from a NAR CSV header

The schema is derived from the file's own header, with types attached
\*\*by name\*\*, rather than declared as a fixed positional list.

This matters because NAR's layout changes between releases and \`arrow\`
maps a declared schema onto CSV columns by position. The June 2026
release inserted \`BF_REPPOINT_X\`/\`BF_REPPOINT_Y\` in the \*middle\*
of the address record, after \`BG_X\`/\`BG_Y\`, shifting
\`BU_N_CIVIC_ADD\` and \`BU_USE\` along by two. A fixed list that was
merely extended at the end would have read blockface centroid
coordinates into \`BU_N_CIVIC_ADD\` without complaint; only the column
count differing made the mismatch an error rather than silent
corruption.

Reading the header keeps the import working across releases and surfaces
a genuinely breaking change – a column that disappears – through
\`required\`.

## Usage

``` r
nar_csv_schema(path, types, required = character(0))
```

## Arguments

- path:

  Path to a NAR CSV

- types:

  Named list of \`arrow\` types for the columns that are not strings

- required:

  Column names that must be present

## Value

An \`arrow\` schema
