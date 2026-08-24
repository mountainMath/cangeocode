# The data sources

This package geocodes from seven sources. One is the base; three more
can be imported beside it or queried over the network; three are online
services it falls through to. Each has a vignette of its own covering
what it adds, the licence it comes with, and what goes wrong with it.
This one is the map.

``` r

library(cangeocode)
library(dplyr)

con <- nar_connection()
```

## The seven

| source | vignette | where it lives | tier | licence |
|----|----|----|----|----|
| National Address Repository | [`vignette("source-nar")`](https://mountainmath.github.io/cangeocode/articles/source-nar.md) | local, imported | `"nar"`, `"nar_interpolate"` | OGL – Canada |
| Répertoire québécois des adresses | [`vignette("source-rqa")`](https://mountainmath.github.io/cangeocode/articles/source-rqa.md) | local, [`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md) | `"rqa"` | CC-BY 4.0 |
| Road Network File | [`vignette("source-rnf")`](https://mountainmath.github.io/cangeocode/articles/source-rnf.md) | local, [`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md) | `"rnf"` | StatCan Open Licence |
| BC Address Geocoder | [`vignette("source-bc")`](https://mountainmath.github.io/cangeocode/articles/source-bc.md) | online | `"bc"` | OGL – British Columbia |
| NRCan national geolocator | [`vignette("source-nrcan")`](https://mountainmath.github.io/cangeocode/articles/source-nrcan.md) | online | `"nrcan"` | OGL – Canada |
| Quebec MRNF geocoder | [`vignette("source-qc")`](https://mountainmath.github.io/cangeocode/articles/source-qc.md) | online | `"qc"` | CC-BY 4.0 |
| GoC Nominatim (OpenStreetMap) | [`vignette("source-osm")`](https://mountainmath.github.io/cangeocode/articles/source-osm.md) | online | **none** | ODbL |

Three things about that table are worth reading twice, and the rest of
this vignette is those three: **NAR is not one source among seven**, the
licence column is what decides the tier column, and the last row has no
tier because of its licence rather than its quality.

## NAR is the base, not a tier among others

Everything here is organized around the National Address Repository. It
is what
[`nar_connection()`](https://mountainmath.github.io/cangeocode/reference/nar_connection.md)
opens, what
[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
reads, what the gazetteer behind
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
is built from, and what
[`collect_nar()`](https://mountainmath.github.io/cangeocode/reference/collect_nar.md)
hands back. Without an imported NAR release the only working parts of
this package are the four online geocoders and the rule-based half of
the parser.

It is also the **yardstick**. Every other source here — the two local
imports and the four services — has been measured *against* NAR, because
there was nothing else national to measure against. That gives NAR’s
limits a double weight: a wrong record is both a wrong answer and a
wrong ruler. Read
[`vignette("source-nar")`](https://mountainmath.github.io/cangeocode/articles/source-nar.md)
before any of the others, and read the accuracy figures in the rest of
them as *disagreement with NAR* rather than as error.

## The tiers, in order

`method` names the tiers to try **and the order to try them in**. Each
tier is offered only the rows the ones before it left without a
position, so the order is the priority.
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
defaults to `c("nar", "nar_interpolate")` — local, free, and
conservative.

Here is a chain that names all seven, against seven addresses picked so
that each finds a different way through:

``` r

addr <- c("7 Saint Andrews Gdns, Toronto, ON",
          "9999 Jasper Ave, Edmonton, AB",
          "1650 Chabanel, Montreal, QC",
          "185 Deerfield Rd, Newmarket, ON",
          "8850 University High St, Burnaby, BC",
          "1545 Maley Dr, Sudbury, ON",
          "3510 Rue Somerled, Montreal, QC")

offline <- geocode(addr, method = c("nar", "rqa", "nar_interpolate", "rnf"),
                   con = con)
online  <- geocode(addr, method = c("nar", "rqa", "nar_interpolate", "rnf",
                                    "bc", "nrcan", "qc"), con = con)

data.frame(input = addr,
           offline = offline$match_method,
           everything = online$match_method)
#>                                  input          offline       everything
#> 1    7 Saint Andrews Gdns, Toronto, ON     nar_building     nar_building
#> 2        9999 Jasper Ave, Edmonton, AB nar_interpolated nar_interpolated
#> 3          1650 Chabanel, Montreal, QC     rqa_building     rqa_building
#> 4      185 Deerfield Rd, Newmarket, ON rnf_interpolated rnf_interpolated
#> 5 8850 University High St, Burnaby, BC             none         bc_civic
#> 6           1545 Maley Dr, Sudbury, ON    rnf_ambiguous            nrcan
#> 7      3510 Rue Somerled, Montreal, QC             none             none
```

Reading down the `offline` column: NAR has the first address; NAR does
not have the second but has its neighbours, so it is interpolated;
Quebec’s register has the third; the road network file has the street of
the fourth and no address source does. The fifth and sixth need the
network, and the last one nothing here can place — which is the honest
answer and not a failure of the chain.

The sixth row is the interesting one. Three separate segments in Sudbury
named Maley claim a range containing 1545, so the road network tier
**refuses** rather than guessing — `rnf_ambiguous` is a decision, not a
failure — and the row is passed on to the online tiers, where the
geolocator places it.

## What each layer is worth

Measured on the same 5,000 Corporations Canada registered-office
addresses the package measures everything on:

| chain                               |  coverage |
|-------------------------------------|----------:|
| `"nar"` alone                       |     87.9% |
| `+ "nar_interpolate"` — the default |     92.4% |
| `+ "rnf"`                           | **94.3%** |

The road network file is the largest single addition, and the reason is
structural: it is the only source here that reaches **streets NAR does
not carry at all**, which is the biggest component of what NAR fails on.

The other three are measured on their own populations, because they are
regional:

| tier | population | what it adds |
|----|----|----|
| `"rqa"` | 4,000 Quebec filings | 88.5% → 90.1% placed; and 82.7% → 89.1% placed on a *register* point rather than an interpolated one |
| `"bc"` | 600 BC filings | roughly **half** the NAR failures in BC are addresses BC’s registry holds |
| `"nrcan"` | the national residual | 8.1% of what the offline tiers leave unplaced |
| `"qc"` | 600 Quebec filings | 81.0% → 83.3% — but see below |

The `"rqa"` row is the one whose headline is in the second column. Most
of what it does is not new coverage but *replacing an interpolated guess
with the register’s own coordinate*, a median 26 m away.

And `"qc"` overlaps `"rqa"` almost entirely: they serve the same
register, one online and one imported. If you have run
[`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md),
the online Quebec tier has very little left to do — which is why it
never fires in the demo above. It is what you reach for when you have
not.

## Offline or online

This is the first practical division, and it is not the same as good or
bad.

**The three local sources are snapshots.** They answer instantly, in
bulk, with no rate limit and no network, and they are as current as the
release you imported. An address newer than that release is in none of
them.

**The four services are live and are rate-limited.** They answer one
request at a time — five per second is what this package throttles to —
so a chain that ends in an online tier takes as long as the number of
rows that reach it. That is usually fine, because so few do: online
tiers only ever see what every offline tier failed on.

Two consequences worth planning around. `"nrcan"` is the only tier that
works **before NAR has been downloaded**, and the only one that covers a
province a partial import does not hold. And your addresses leave the
machine when an online tier runs, which is a decision to make
deliberately if the address list is sensitive.

## Licences decide which sources can be default tiers

Every source here is openly licensed, and that is not enough on its own
— what matters is whether the licences **compose**, because
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
returns one table with rows from several of them.

Open Government Licence (Canada, and British Columbia), the Statistics
Canada Open Licence and CC-BY 4.0 are all attribution licences. They
compose: mixing them obliges you to attribute each, and nothing more.
Six of the seven sources here are in that group, which is why all six
are tiers.

**OpenStreetMap data is ODbL**, which is attribution *plus share-alike*,
and the share-alike obligation attaches to a derived **database**.
Folding a handful of ODbL rows into a result table changes what the
caller may do with the whole of it. So
[`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
is exported and is deliberately **not** a tier: reaching for it is an
explicit call, and the service’s own licence string rides along on every
row as `osm_licence`. That decision is about the licence and not about
the accuracy, which has never been measured — see
[`vignette("source-osm")`](https://mountainmath.github.io/cangeocode/articles/source-osm.md).

The two CC-BY sources need their attribution discharged, and there is a
function for it:

``` r

rqa_attribution()
#> [1] "Contains information licensed under CC-BY 4.0 from the Repertoire quebecois des adresses, Ministere des Ressources naturelles et des Forets, Gouvernement du Quebec."
```

## Only one source can check NAR, and only in one province

It is tempting to read agreement between two sources as confirmation.
Mostly it is not, because these sources are not independent of each
other.

Quebec’s register **is** where NAR’s Quebec rows come from: over 2.5
million paired addresses the two agree to a median of **21 cm**. That is
lineage, not accuracy, and it means neither `"rqa"` nor `"qc"` can tell
you whether a NAR Quebec point is right. There is no second opinion
available for Quebec inside this package.

The BC Address Geocoder is the closest thing to an exception. It is a
parcel-level provincial authority rather than a compilation, and where
it and NAR disagree **BC’s answer is the more reliable of the two** —
which is what
[`bc_validate()`](https://mountainmath.github.io/cangeocode/reference/bc_validate.md)
is for. Even there the independence is partial: on a quarter of
addresses the two agree to within 1.6 m, which is not two readings
converging.

``` r

g <- geocode(c("20460 Douglas Cres, Langley, BC", "1188 Bidwell St, Vancouver, BC"),
             method = c("nar", "nar_interpolate"), con = con)

bc_validate(g) |>
  select(input, match_method, bc_match_method, bc_precision, bc_dist_m)
#>                             input match_method bc_match_method bc_precision  bc_dist_m
#> 1 20460 Douglas Cres, Langley, BC nar_building        bc_civic CIVIC_NUMBER 16.4159747
#> 2  1188 Bidwell St, Vancouver, BC nar_building        bc_civic CIVIC_NUMBER  0.2946784
```

Validate rows an **offline** tier placed. Re-asking BC about a row the
`"bc"` tier itself answered returns the same point by construction, and
a `bc_dist_m` of zero that means nothing.

[`qc_validate()`](https://mountainmath.github.io/cangeocode/reference/qc_validate.md)
is the Quebec counterpart and works the same way — with the caveat above
about what its small distances mean.

## Reverse geocoding is local, with one exception

[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
reads NAR and nothing else, which makes it fast and offline. Of the four
services here only Quebec’s does reverse at all:
[`qc_reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_reverse_geocode.md)
is the one online reverse geocoder in the package. BC’s, NRCan’s and
Nominatim’s bindings are forward only — that was probed rather than
assumed.

## `uncertainty_m` is not one thing

Read the column with the source in mind. `uncertainty_m` is defined as
the error **this package** adds relative to NAR’s own building point, so
a `0` means “nothing was added”, not “this point is exact” — and it says
nothing about NAR’s own error.

Beyond that, the numbers have three different provenances:

- **Measured.** The NAR and road network tiers. 176 m for a blockface
  point is the p90 building-to-blockface separation over 1.85 million
  addresses that carry both; the interpolation figure comes from an
  error/span ratio that turns out to be scale-invariant; the road
  network tier’s `max(95, 0.35 × len_m)` covers the true distance 91.8%
  of the time.
- **Assigned.** The `bc_*`, `nrcan` and `qc_*` figures. Those services
  publish a precision *vocabulary* and no distances, so the metres are
  deliberately pessimistic order-of-magnitude translations. They are a
  ranking safe to filter on, not an error bar comparable to the NAR
  tiers’.
- **Absent.** `NA` for the weaker RQA classes and for every `osm` row,
  because nothing has measured what they are worth and a plausible
  invented constant would be indistinguishable in the output from the
  two kinds above.

## Where to go next

Start with
[`vignette("source-nar")`](https://mountainmath.github.io/cangeocode/articles/source-nar.md),
since everything else is measured against it. Then read the vignette for
whichever source you are about to turn on.

Each of them ends with a pointer to the note it is drawn from. The notes
ship with the package and carry the full measurements, the failure modes
that have not been fixed, and what to do next:

``` r

list.files(system.file("notes", package = "cangeocode"))
```

``` r

DBI::dbDisconnect(con)
```
