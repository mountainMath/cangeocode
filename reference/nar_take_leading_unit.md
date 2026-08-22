# Take a leading unit designator off the front of a street

Handles the four leading forms Canadian addresses use: the hyphenated
\`302-1055\`, an explicit designator (\`APT 302\`, \`BUREAU 12\`), a
\`#302\`, and a bare label such as \`BSMT\`. This is the step that has
to be right – a unit left attached is read as the civic number, and the
real civic number is then lost entirely.

## Usage

``` r
nar_take_leading_unit(toks, lang = "en")
```

## Arguments

- toks:

  A character vector of tokens

- lang:

  \`"en"\` or \`"fr"\`

## Value

A list with \`unit\`, \`civic\` (set only by the hyphenated form) and
\`rest\`
