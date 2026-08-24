# Strip a leading prose prefix from a normalized address string

Free-text address fields open with a great deal that is not the address
– \`"located at 41 Cultus Rd"\`, \`"attn: J Smith, 119 Markham St"\`,
\`"Toronto General Hospital, 200 Elizabeth St"\`. Every civic-number
rule in this parser anchors on a number at the \*front\* of the string,
so a prefix does not degrade the parse, it collapses it: the prefix and
the civic number together are read as one street name and the pattern
falls to \`street_only\`. On the generated dirty corpus the affected
classes go from 0–19 over 90

Cutting to the first digit-initial token is easy to get wrong, because a
lot of legitimate address openings put words in front of the first
number. Four guards, and each one is holding back a real address form:

\* At most \*\*one comma\*\* may be crossed. The rule can reach past a
care-of line or a building name, never past a municipality. \* A number
that \*\*closes its comma segment\*\* is the tail of a street name, not
the head of an address: \`Highway 7\`, \`Line 5\`, \`Rang 9\`, and the
leading \`Suite 200,\` of \`Suite 200, 119 Markham St\`. \* A \*\*unit
designator\*\* anywhere in the dropped run means the number is a unit:
\`Apt 4B-1234 Bloor St W\`, \`Unit 5 100 Main St\`, \`# 5 100 Main St\`.
So does a \*\*digit inside a dropped token\*\*, which is how an
undesignated unit shows up: \`PH12, 2160 Terry-Fox Av\`, \`E10, 20
Palace St\`. Prose does not carry digits. \* A \*\*street type or
numbered-road word\*\* directly in front of the number – after peeling
the French particules that sit between them – means the number belongs
to the name: \`Range Road 272\`, \`County Road 21 North\`, \`Chemin du
4e Rang\`, \`Avenue du 8 Mai\`. Only the run \*after\* the last comma is
examined, because a type separated from the number by a comma cannot be
governing it (\`Sunnybrook Health Sciences Centre, 2075 Bayview\`).

Strings carrying a delivery mark are exempt, and the caller enforces
that: a PO box or rural route line is an instruction rather than an
address, and the number in it is not a civic number.

## Usage

``` r
nar_strip_lead_prose(s)
```

## Arguments

- s:

  A single normalized address string (post-\[nar_norm_text()\], so
  uppercase with commas standing as their own tokens)

## Value

\`s\`, or \`s\` with the prefix removed
