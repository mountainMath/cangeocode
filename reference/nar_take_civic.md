# Take the civic number and its suffix off the front of a street

\`CIVIC_NO_SUFFIX\` holds a single letter or \`1/2\` and nothing else,
so only those two forms are recognised. The letter is taken only when it
is attached to the digits (\`990A\`): a spaced \`990 W\` is a direction
far more often than a suffix, by roughly three orders of magnitude.

## Usage

``` r
nar_take_civic(toks)
```

## Arguments

- toks:

  A character vector of tokens

## Value

A list with \`civic\`, \`suffix\` and \`rest\`
