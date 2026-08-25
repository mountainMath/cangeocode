# The municipality co-occurrence tables the swap penalty is scored against

Two TEMP tables, built once per connection like \[nar_street_fold()\]
and for the same reason – they are derived from \`Addresses\` and would
otherwise be recomputed on every call.

\* \*\*\`MunMail\`\*\* – every name NAR files postal-coded mail under,
folded. It is what separates "this name was swapped for another" from
"this name is not a mailing municipality at all", which is the ordinary
case of a jurisdiction name being resolved to the mailing city NAR files
under. Only the first is penalised; the second is what \`MunAlias\`
exists to do. Read off the same rows as \`MunCoPostal\` on purpose: a
name that could never have appeared in the pair table must not be scored
as a name that could have and did not. \* \*\*\`MunCoPostal\`\*\* –
pairs of mailing municipalities that share at least one \*\*full\*\*
postal code. This is the alias evidence, and it is read off Canada
Post's own delivery geography rather than guessed: two names that
receive mail at the same postal code are two names for the same place,
whatever the census thinks. \`HOWIE CENTER\` and \`SYDNEY\` share three,
which is exactly the rural-community-to-mailing-city remap the gazetteer
exists to perform. \`MILFORD\` and \`MIDDLE SACKVILLE\` share none, and
that is the swap that puts an address 60 km away.

The full postal code and not the FSA. An FSA is a forward sortation area
and in rural Nova Scotia one of them covers most of a county, so
\`PostalMun\` – which is FSA-keyed and already present – would attest
nearly every pair in the province and the penalty would never fire.

32,216 directed pairs nationally, 0.2 s to build. 95 shared postal code,
which is why the default \`min_pc\` is 1: raising it to 2 discards
30,694 of the 32,216 and turns a permissive check into a strict one.

## Usage

``` r
nar_mun_copostal(con, min_pc = 1)
```

## Arguments

- con:

  A NAR connection

- min_pc:

  Shared full postal codes required to attest a pair

## Value

Invisibly \`TRUE\` when the tables are present
