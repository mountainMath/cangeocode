# Fold a street name to the form the fuzzy branch compares on

\[nar_fold()\] settles case and accents, which is enough for an equality
join. The fuzzy branch needs more, because two of Quebec's spelling
conventions put a correct parse and NAR's own spelling of the same
street on opposite sides of the name gate:

\* \*\*The hyphen is not a distinguishing character.\*\* NAR writes \`du
Square-Victoria\`, \`du Curé-Labelle\`, \`Alexis-Nihon\`; people write
the words with spaces, and usually without the leading particule.
Whole-word containment is exactly the rule that should catch
\`VICTORIA\` inside \`du Square-Victoria\` – and it does not, because
with the hyphen in place \`SQUARE-VICTORIA\` is one word. Folding it to
a space is what lets the rule fire. This is not a Quebec-only change:
\`du Bord-du-Lac–Lakeshore\` and \`Grande Côte\` are the same problem,
and English Canada's hyphenated names gain the same way. \* \*\*\`ST\`
and \`STE\` are abbreviations of \`SAINT\` and \`SAINTE\`\*\*, and NAR
spells them out. \`ST-JACQUES\` against \`Saint-Jacques\` is six edits
on a thirteen-character string – nowhere near the gate, and nowhere near
the top of a similarity ranking either. Expanding both sides is the only
thing that makes them meet, and applying it to both sides is what keeps
it safe: a name that really does contain a bare \`ST\` still matches
itself.

The apostrophe goes the same way as the hyphen, for the same reason:
\`de l'Orme\` and \`DE L ORME\` are one street.

Applied to the \*probe\* it produces \`match_fold\`, which is
deliberately a second column rather than a replacement for \`name_fold\`
– the exact branch joins on \`name_fold\` through an index, and this
expression would defeat it.

## Usage

``` r
nar_match_fold(x)
```

## Arguments

- x:

  A character vector

## Value

A character vector folded for comparison
