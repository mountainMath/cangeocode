# Could this token run be a street name at all?

The residue test for \[nar_mun_anchor_variants()\], and the reason a
place name that is also a street name does not have to be listed
anywhere. Anchoring a municipality off the end of \`135 de Nantes\`
leaves \`DE\`, off \`22 avenue de la Durantaye\` leaves \`DE LA\`, off
\`80 rue Albanel\` leaves \`RUE\` – three different failures that are
all the same failure, and all of them visible in what is left rather
than in what was taken.

Particules do not name a street on their own, and a street type standing
alone does not either. Both tests are on the residue after the
particules come off, so \`RUE DE LA\` fails as surely as \`RUE\` does.

## Usage

``` r
nar_is_street_name(name, lang = "en")
```

## Arguments

- name:

  A street name as parsed, or \`NA\`

- lang:

  \`"en"\` or \`"fr"\`

## Value

\`FALSE\` when the name is nothing a street could be called
