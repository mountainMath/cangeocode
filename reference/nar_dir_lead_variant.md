# The reading in which a leading compass word is part of the street name

\`East Beaver Creek Rd\` and \`West Beaver Creek Rd\` are two streets in
Richmond Hill; \`North Edgely Ave\` and \`South Edgely Ave\` are two
streets in Scarborough. The left-to-right parse reads the opening word
as a direction and hands the gazetteer \`BEAVER CREEK\`, which
whole-word containment scores 0.90 against \*both\* halves of the pair.
Direction agreement is worth 0.06 and the stripped reading has no
direction left in the name to agree with, so the mirror image wins about
as often as the street does – and it wins \*confidently\*, with nothing
in the output to say so. Some 92,000 NAR addresses are on a street NAR
itself spells with the word in the name and no direction on either name
family.

So the unstripped reading is offered as a parallel candidate rather than
as a fallback fired when the stripped one finds nothing: a fallback
repairs only the addresses that end up unplaced, which measured as 68 of
453 losses, and leaves the other 385 confidently on the wrong street. As
a candidate the restored probe \`EAST BEAVER CREEK\` matches one of the
pair exactly at 1.0 and the other not at all, so it beats 0.868
outright.

Two things keep it from displacing a correct reading. Both candidates
carry the same municipality, so the comparison is like-for-like and the
restricted-beats-unrestricted asymmetry that governs
\[nar_baseline_is_defective()\] never arises – which is why this one
needs no gate. And a street genuinely called \`Park\` still wins,
because the baseline probe \`PARK\` matches it exactly at 1.0 while the
restored probe \`NORTH PARK\` matches nothing; when neither exists the
restored probe falls under the name threshold and is refused, leaving
today's answer untouched.

The word is restored verbatim, abbreviations included. \`W GEORGIA\` is
not a street name NAR carries, so that candidate simply loses; the
~2,000 addresses whose NAR name really does open with an abbreviated
compass word are the ones it is there for.

## Usage

``` r
nar_dir_lead_variant(base, word)
```

## Arguments

- base:

  The baseline reading, one row, with \`strategy\` already set

- word:

  The leading direction token as it arrived, or \`NA\`/\`NULL\`

## Value

A list holding one candidate row, or an empty list
