# What the caller already knows about an address

Address data does not always arrive as one string. An assessment roll
carries the community in a column of its own, a filing carries the
province, a cleaned list may carry everything but the street. \`known\`
is how that structure is handed to the parser and to the search instead
of being thrown away and re-derived from a string it was concatenated
into.

Every key is \*\*authoritative\*\*: it overrides whatever the string
parsed to, it lands on the returned row, and it constrains the search.
\`NA\` for a row means nothing is known about that row, so the parse
stands.

What lands is the caller's own value, normalized the way a parse is –
upper case, accents kept, punctuation that only decorates abbreviations
dropped. It is \*not\* replaced by NAR's spelling of whatever matched,
because nothing was matched: the component was asserted rather than
resolved. So an asserted \`CSD_NAME = "Toronto"\` comes back \`TORONTO\`
where a resolved one comes back NAR's \`Toronto\`.

## Usage

``` r
nar_known(known, n)
```

## Arguments

- known:

  A named list, or \`NULL\`

- n:

  The number of addresses being resolved

## Value

A data frame with \`n\` rows and one column per supplied key, or
\`NULL\`

## The two kinds of municipality

\`MUN_NAME\` and \`CSD_NAME\` are different questions and this is the
argument that separates them.

\* \`MUN_NAME\` is the \*\*mailing city\*\* – NAR's \`MAIL_MUN_NAME\`,
the name on the envelope. It restricts to streets NAR files under that
exact name. \* \`CSD_NAME\` is the \*\*census subdivision\*\*, the
administrative unit. It is resolved through NAR's alias set, so
\`Toronto\` reaches everything filed under \`SCARBOROUGH\`, \`NORTH
YORK\` and \`ETOBICOKE\`, and a name denoting several jurisdictions
means all of them.

The two do not nest – one mailing city can span several jurisdictions
and one jurisdiction carries many mailing cities – so asking for the
wrong one is not a near miss. \`MUN_NAME = "Toronto"\` will \*not\* find
an address NAR files under \`SCARBOROUGH\`; \`CSD_NAME = "Toronto"\`
will. Supply both and both constrain, which is how a caller narrows to
one community inside a large amalgamated city.

A municipality that resolves to nothing leaves the row unresolved rather
than being ignored, which is what a parsed municipality already does.

## Keys

The NAR-shaped column names, so the list form and the data-frame form of
\`x\` share one vocabulary: \`APT_NO_LABEL\`, \`CIVIC_NO\`,
\`CIVIC_NO_SUFFIX\`, \`STREET_NAME\`, \`STREET_TYPE\`, \`STREET_DIR\`,
\`MUN_NAME\`, \`CSD_NAME\`, \`PROV_ABVN\`, \`POSTAL_CODE\`. Anything
else is an error rather than a silently dropped constraint: a constraint
that does not bind produces a confident wrong answer, which is the
failure this argument exists to prevent.
