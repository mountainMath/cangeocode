# Source: the NRCan-hosted Nominatim instance

*One of this package’s seven data sources — the one source bound but not
wired as a tier.
[`vignette("data-sources")`](https://mountainmath.github.io/cangeocode/articles/data-sources.md)
is the overview and puts it in context.*

[`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
queries **`https://maps.canada.ca/nominatim/search`** — the Nominatim
instance **Natural Resources Canada** hosts over OpenStreetMap data on
the Federal Geospatial Platform, and the one NRCan’s own aggregator
queries internally under its `nominatim` service key.

It is **not** `nominatim.openstreetmap.org`, and that matters: the
volunteer instance’s usage policy forbids bulk geocoding outright, so
pointing this package at it would make every user of the package a
violation. The NRCan instance is keyless, national, and under no such
restriction.

[`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
is exported. It is **deliberately not a
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
tier**, and the whole of this vignette is about that decision.

``` r

library(cangeocode)
```

Unlike the other source vignettes here, the chunks below are **not
evaluated**. There is no measured result to show, for the reason in the
next section but one.

## What this adds to the package

**A geocoder that refuses**, and a second data lineage.

The other three online services always answer. BC returns a locality
centroid for an address that does not exist; NRCan’s geolocator returns
a confident wrong street; Quebec’s returns a street centroid scoring
higher than the correct civic point would. Every one of them needs a
floor built on top of it to turn “the service responded” into “the
address was found”.

Nominatim returns an empty array. When it has the street but not the
number it returns the road itself at `place_rank` 26, which is a legible
*found the street, not the address* rather than a point pretending to be
a house. Both of the confident wrong answers this package uses as
cautionary examples — `1 Rue Notre-Dame Ouest, Montreal` placed 500 km
away by the geolocator, `28 Silver St, Corner Brook` placed on a
different street — come back correct or refused here.

It is also the only source here that is not, directly or indirectly, a
government address register. NAR, RQA and the BC and Quebec services all
draw on official registers; where two of them agree, that can be shared
lineage rather than confirmation. OSM is compiled differently. That
would make it a genuine second opinion — if the licence allowed it to be
used as one by default.

## Licence: why this is not a tier

**OpenStreetMap data is ODbL** — attribution plus share-alike, with the
obligation attaching to a derived **database**. NAR and the road network
file are the Statistics Canada Open Licence, the BC geocoder and NRCan’s
geolocator the Open Government Licence, RQA and Quebec’s geocoder CC-BY.
Those compose freely. ODbL does not compose with them the same way.

A default tier would fold a handful of ODbL rows into a result table and
change what the caller may do with **the whole of it**, silently, for
the sake of a few rows the other tiers missed. That is not a decision a
geocoding package should make on a user’s behalf.

So: the service’s own licence string rides along as `osm_licence` on
every row, and reaching for this source is an explicit call.

``` r

osm_geocode("990 Bute St, Vancouver, BC")$osm_licence
#> "Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright"
```

If the accuracy probe eventually shows it recovers a useful part of
NAR’s tail, the decision to make is still about the licence, and it is
the user’s to make per project rather than this package’s to make by
default.

## `uncertainty_m` is `NA`, and that is deliberate

``` r

osm_geocode(c("990 Bute St, Vancouver, BC", "99999 Nowhere Rd, Nowhereville, SK"))
```

[`nar_osm_uncertainty_m()`](https://mountainmath.github.io/cangeocode/reference/nar_osm_uncertainty_m.md)
returns `NA_real_`. Every other source here has a number attached to
each precision class; this one does not, because **it has not been
measured against NAR**. `data-raw/probe_osm.R` exists and runs over the
same sample as `data-raw/probe_geolocator.R`, so the two services will
be directly comparable — it has not been run at scale. Until it has
there is no coverage figure, no p90, and no basis for placing this
service anywhere in a `method` chain.

Inventing a plausible constant to make the row tidy would assert
something unmeasured, and would be indistinguishable in the output from
the figures that *were* measured. So the column stays `NA`.

## What the first live runs showed

On a handful of addresses rather than a sample, so read these as
observations and not as measurements:

- **Coverage is uneven by construction.** OSM’s Canadian addresses are
  concentrated in cities that had a municipal open-data import. Downtown
  Vancouver, Montreal and Toronto all answer at building level; nothing
  rural has been tried. The number to watch when the probe runs is the
  *answer rate* — most of the loss here will be coverage, not rejection.
- **French word order matters**, and is handled. `1 NOTRE-DAME RUE O`
  returns nothing where `1 Rue Notre-Dame Ouest` returns the address.
  This is the second place in the package where a query is spelled for a
  particular service rather than in NAR’s canonical form; Quebec’s
  locator is the other.

## Two things about the response

**The road is parsed and the municipality is not.** Nominatim already
separates `house_number`, `road`, `city` and `ISO3166-2-lvl4`, which is
a real advantage over the geolocator’s single string. But `road` is a
full street line (`Bute Street`, `Rue Notre-Dame Ouest`) and still has
to go through
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
to be compared component by component; the municipality fields are
already components and go straight through. Parsing `display_name`
instead would mean getting past a building name and two sub-municipal
localities —
`The Berkeley, 990, Bute Street, Davie Village, West End, Vancouver, …`
— to reach what the service handed over separately.

The municipality is coalesced from `city`, `town`, `village`,
`municipality` and `hamlet`, and **deliberately stops there**. `suburb`,
`neighbourhood`, `quarter` and `city_district` sit below the
municipality and would match a query’s municipality against something
smaller than one.

**`n_matches` counts distinct addresses, not results.**
`1155 Robson St, Vancouver` comes back as two OSM objects — the
building, and an office inside it 8 m away — with identical house
number, road and city. Counting results would report an ambiguity that
does not exist, so survivors are deduplicated on
[`address_key()`](https://mountainmath.github.io/cangeocode/reference/address_key.md)
of the parsed answer.

## The floor still exists

Refusing honestly does not remove the need for a check. Two conditions:

1.  `place_rank >= 30` **and** a `house_number` of its own. Rank 30
    alone is not enough — `24 Sussex Dr, Ottawa` comes back at rank 30
    with no house number at all, so both halves are load-bearing.
2.  The separated fields must agree with the address that was sent,
    through the same
    [`nar_address_agreement()`](https://mountainmath.github.io/cangeocode/reference/nar_address_agreement.md)
    the geolocator’s floor uses.

That comparison is shared code rather than per-service, because its
rules — an empty string is absent and cannot contradict; the street name
and civic number are what was asked, so a missing one is a failure; the
municipality matches by whole-word containment in either direction — are
properties of comparing two Canadian addresses, not of any one service.
And the re-parse gets no `con`, so no gazetteer: a gazetteer would
rewrite the answer back toward the question and launder the error the
floor exists to catch.

## Where the measurements live

``` r

file.show(system.file("notes", "nrcan-geolocator.md", package = "cangeocode"))
```

The geolocator note covers this service too — it is the geolocator’s own
Nominatim sibling — and the OpenStreetMap section of
`geocoding-status.md` records what remains unmeasured.
