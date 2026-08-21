# Load the DuckDB spatial extension on a connection

Loads the extension, installing it first if it is not present. This
deliberately uses DuckDB's own \`LOAD spatial\` rather than
\`duckspatial::ddbs_load()\`: the latter creates \*persistent\* helper
macros, which fails outright on the read-only connections this package
hands out. All spatial SQL the package needs is native to the extension,
plus the TEMP macros registered by \[nar_register_spatial()\].

## Usage

``` r
nar_load_spatial(con)
```

## Arguments

- con:

  A DuckDB connection

## Value

The connection, invisibly
