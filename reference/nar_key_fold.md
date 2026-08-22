# Fold a component to a match key

\[nar_fold()\] handles case and accents; this drops what is left.
Periods and apostrophes vanish outright, because NAR keeps them in
municipality names and the parser does not (\`ST. JOHN'S\` against \`ST
JOHNS\`); every other separator becomes a space, so a hyphenated
\`NOTRE-DAME\` keys the same as the spaced spelling rather than as
\`NOTREDAME\`.

## Usage

``` r
nar_key_fold(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector
