# Pick the gazetteer's tuning arguments out of \`geocode()\`'s dots

\[geocode()\] normalizes the string before it geocodes it, so the
gazetteer's own arguments have to reach \[normalize_address()\] or they
are accepted and silently dropped – which is what happened until this
existed, and it made \`mun_swap_penalty\` look inert from \`geocode()\`
while working perfectly when the same penalty was applied by calling
\[normalize_address()\] first and passing the frame. A measurement taken
the first way and a measurement taken the second way then disagree for
no visible reason.

Only forwarded when \`x\` is a character vector. A data frame has
already been parsed by whoever made it, and re-applying a parse argument
to it would be claiming an influence over a decision that was taken
elsewhere.

Derived from the formals of \[nar_resolve_gazetteer()\] rather than
listed, so the two cannot drift apart. \`res\` and \`con\` are supplied
here.

## Usage

``` r
nar_gazetteer_dots(dots)
```

## Arguments

- dots:

  \`list(...)\` as \[geocode()\] captured it

## Value

The subset of \`dots\` to forward to \[normalize_address()\]
