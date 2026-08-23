# Quebec addresses: NAR against the source register

Quebec is the one province where the address data in this package has a
published upstream that can be read directly. The **Répertoire québécois des
adresses** (RQA) is maintained by the Ministère des Ressources naturelles et
des Forêts, published under CC-BY 4.0, and is the register NAR's Quebec rows
are derived from. It is also what the `qc` geocoding tier queries — the
locator's own reference names are `RQA_*` — so this note is what establishes
that the tier, the register and NAR are three views of one dataset rather than
three sources.

Everything below comes from `data-raw/compare_rqa.R`, run against NAR 2026-06
and the RQA release dated 2026-08-01. Re-run it after importing a new NAR
release; the numbers move.

```
RQA_PART=all Rscript data-raw/compare_rqa.R
```

The bulk download is `https://diffusion.mern.gouv.qc.ca/Diffusion/RQA/RQA_CSV.zip`
(778 MB, extracting to a 3.08 GB `RQA.csv` of 5,322,997 rows plus a 24 MB
`Odonymes_renvois.csv`). RQA rows carry an `etat`; all counts here are
restricted to `Certifiée`, which drops about 7,500 retired rows.

## How much address there is on each side

| | NAR Quebec | RQA |
| --- | ---: | ---: |
| rows | 4,568,811 | 5,315,435 |
| with a unit designator | 1,460,540 | 1,665,467 |
| distinct civic addresses | 3,236,571 | 3,652,473 |
| rows flagged as a building point | 4,559,562 (99.8%) | 1,429,139 (26.9%) |

RQA holds **415,902 more distinct civic addresses than NAR**, about 12.9% on
top of what NAR carries. Keyed on postal code plus civic number — the coarsest
key that survives the two registers spelling street names differently — NAR has
2,744,951 keys, RQA 2,957,686, and 2,732,411 are shared. That is 99.5% of NAR
inside RQA, and **225,275 RQA keys NAR does not have**. They are ordinary
addresses, not noise:

```
210 B Rue Heriot, Drummondville J2C1J8
32 Montée des Chevaliers, Val-des-Monts J8N4C5
3231 Chemin de la Claire-Fontaine, Saint-Placide J0V2B0
1736 Grand Rang, Saint-Tite G0X3H0
1846 4e Rang, Saint-Côme–Linière G0M1J0
77 Boulevard du Souvenir, Laval H7N4G1
```

Rural and semi-rural, weighted towards the address shapes the parser already
finds hardest — numbered rangs, `Montée`, long hyphenated specifics. RQA's own
quality flag on the missing keys: Géocodée 92,638, Incertaine 67,981, Bâtiment
45,693, Centre lot 13,484, Front lot 5,465.

## The positional-quality field NAR does not carry

RQA classifies every row by how its point was placed. NAR has no analogue.

| `qualite_positionnement_geometrique` | rows | % |
| --- | ---: | ---: |
| Géocodée | 2,086,835 | 39.3 |
| Incertaine | 1,674,182 | 31.5 |
| Bâtiment | 1,429,797 | 26.9 |
| Centre lot | 73,303 | 1.4 |
| Front lot | 58,856 | 1.1 |
| Site / Accès propriété | 24 | 0.0 |

This matters for how NAR's Quebec coverage reads. NAR reports `geom_source =
'building'` on **99.8%** of its Quebec rows, and elsewhere in this package that
flag is treated as the good case — the alternative is a blockface point, which
is a different kind of measurement and not comparable (see
[`nar-database.md`](../../.claude/nar-database.md)). In Quebec it is not a
quality statement at all. It says which NAR file the row arrived in, and for
Quebec that file is carrying RQA points of which RQA itself calls **31.5%
`Incertaine`** and only 26.9% building-placed. Do not read Quebec's 99.8%
building coverage as 99.8% building-accurate geometry.

## Point-to-point disagreement

2,512,836 addresses that both registers carry, on keys unique on both sides,
NAR's building point against RQA's point:

| n | p50 | p90 | p99 | < 1 m | > 100 m |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2,512,836 | 0.21 m | 8.0 m | 55.9 m | 72.3% | 0.5% |

A median of 21 cm over two and a half million addresses is not agreement
between two sources. It is the same coordinate, round-tripped through a
projection. Split by RQA's flag, what is actually going on becomes visible:

| RQA quality | n | % | p50 | p90 | < 1 m |
| --- | ---: | ---: | ---: | ---: | ---: |
| Géocodée | 1,343,276 | 53.5 | 0.16 m | 0.4 m | 97.1% |
| Bâtiment | 754,416 | 30.0 | 4.25 m | 16.0 m | 18.0% |
| Incertaine | 367,289 | 14.6 | 0.14 m | 0.3 m | 98.2% |
| Front lot | 28,476 | 1.1 | 14.86 m | 29.4 m | 2.2% |
| Centre lot | 19,379 | 0.8 | 0.21 m | 31.5 m | 75.6% |

Where RQA interpolated or was unsure, NAR has RQA's coordinate to within
centimetres — 97% and 98% inside a metre. Where RQA has a *building*-placed
point, NAR has something else, a few metres away. The offset is scatter and not
a shift: mean `dx`/`dy` is within 0.05 m of zero in every class, with a standard
deviation of 27 m in the Bâtiment class against 13 m in the Géocodée one, so it
is not a datum or transform error on either side. The reading — an inference,
not something either register states — is that NAR takes a building centroid
from its own building layer where one is available and falls through to RQA's
coordinate otherwise, which would put the two points at opposite ends of the
same structure.

The practical consequence is the one that matters here:

> **NAR's Quebec geometry is not independent of RQA, and neither is the `qc`
> geocoding tier.** Checking a NAR Quebec point against `qc_geocode()`, against
> `qc_validate()`, or against RQA directly measures how well the address parsed,
> not whether the coordinate is right. There is no second opinion available for
> Quebec inside this package.

`qc_validate()` says this in its own documentation, and
[`geocoding.md`](../../.claude/geocoding.md) records it for the tier. The
250,000-address BC comparison in
[`geocoding-status.md`](geocoding-status.md) is a genuinely partial second
opinion because the BC Geocoder maintains its own civic register; Quebec has no
equivalent.

## What RQA is actually worth here

Not as a geometry source. Two things:

**The 225,275 addresses NAR lacks.** They are concentrated in exactly the rural
forms the parser struggles with, so they would extend coverage where coverage is
thinnest. Adding them means either a second local table or a merged import, and
either way a licence question — RQA is CC-BY where NAR is OGL, both
attribution-compatible, unlike the ODbL problem that keeps `osm_geocode()` out
of the tier list.

**The odonyme decomposition, which is the larger prize.** RQA does not store a
street name as a string. It stores it decomposed, with a stable identifier:

| générique | particule | spécifique | cardinal | recomposé (normal) | recomposé (court) |
| --- | --- | --- | --- | --- | --- |
| Boulevard | de la | Côte-Vertu | | Boulevard de la Côte-Vertu | boul. de la Côte-Vertu |
| Avenue | | Élie-Beauregard | | Avenue Élie-Beauregard | av. Élie-Beauregard |
| Rang | | Saint-Ange | | Rang Saint-Ange | rang Saint-Ange |
| Rue | des | Violettes | | Rue des Violettes | rue des Violettes |

115,352 distinct odonymes over 43 génériques and 16 particules, with a particule
present on **27.8%** of rows, four recomposed surface forms per odonyme, and
551,160 rows carrying a `renvoi_seqodo` cross-reference to another odonyme —
the alternative and former names, expanded in `Odonymes_renvois.csv`.

That is a labelled decomposition of every street name in Quebec, in the exact
shape `normalize_address()` is trying to produce, together with the register's
own alternative spellings.

**It is worth less than it was when this was written, and the reason is worth
recording.** The diagnosis that ranked it — a Québec Part B join rate of 68.2%
against building points on 99.8% of NAR's Quebec rows — was right that the
geometry is there and the parse is what failed to reach it, and wrong about why.
Cross-referencing 912 Québec failures against RQA split them: 33.7% a parse
failure on an address both registers hold, **26.4% an address whose parse RQA
confirms and NAR simply does not carry**, 24.8% in neither register, 9.0% a
spelling NAR disagrees with. Half the shortfall was never the parser's. Of the
half that was, the dominant classes were `ST-`/`STE-` left unexpanded, a dropped
leading particule, and hyphen-versus-space — all three of which the gazetteer's
match fold now handles for free, taking Québec to **75.5%**. What the
decomposition can still buy is the part folding cannot reach: former and
alternative names via the renvois, and génériques that belong to the name rather
than the type. Re-measure the split before loading it. See
[`address-normalization-status.md`](address-normalization-status.md).
