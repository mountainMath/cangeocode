# Does this token announce itself as a unit with no designator in front of it?

Narrower than \[nar_is_unit_value()\], and deliberately so: that test
asks whether a value \*offered\* as a unit looks like one, with a
designator already vouching for it. This one has no such warrant, so it
has to carry the claim itself.

A bare number does not. \`Cascumpec - Rte 12\` and \`Chicoltin-Bella
Coola Highway 20\` are street names that end in one, and reading the
number as a unit takes it off a name that needs it – both were measured,
in the Part A sample. A letter-and-digit token (\`TH25\`, \`4B\`,
\`PH2\`) is not a street name's last word in any of the 374k NAR
carries.

## Usage

``` r
nar_is_undesignated_unit(x)
```

## Arguments

- x:

  A single token, unfolded

## Value

\`TRUE\` when the token can stand as a unit unaided
