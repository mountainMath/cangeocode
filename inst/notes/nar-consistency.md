# Finding NAR's misplaced addresses using nothing but NAR

Some addresses in NAR are in the wrong place. This note is about finding them without a
second source, and about what survives when the method is made honest about two things it
originally got wrong.

Reproduce everything here with `Rscript data-raw/probe_consistency.R`. It needs
`NAR_CACHE_PATH`, an imported road network file (`rnf_import()`), and **cancensus**, **sf**
and **igraph**; it runs in about six minutes and touches nothing.

## Why this can be done at all

Everything else in this package that measures NAR measures it against something else, and
the standing caveat applies to all of it — see [`geocoding-status.md`](geocoding-status.md):
a disagreement with the BC Geocoder is a disagreement, not an error. A row whose own postal
code and own coordinate contradict each other is different in kind. The contradiction is
*internal*, no reference dataset is needed to see it, and none can be blamed for it. That
is the whole reason this test is worth running.

## Two corrections, before any of it works

### There are two witnesses, not three

The first version of this note claimed a NAR row makes *three* independent statements about
where it is: a postal code, a municipality (`CSD_ENG_NAME`), and a coordinate. Stage 1 of
the probe tests that claim directly, by intersecting every point with Statistics Canada's
**2021 CSD boundaries, digital rather than cartographic** — the cartographic ones are
clipped to the coastline and generalized, so a waterfront address falls outside them for
reasons that have nothing to do with NAR. They come from
`cancensus::get_statcan_geographies(2021, "CSD", type = "digital")` and arrive in EPSG:3347,
which is the storage CRS here and the road network file's, so nothing is reprojected
anywhere in this work.

| 17,297,393 points against 5,161 polygons | rows |
| --- | ---: |
| province agrees with the polygon's `PRUID` | 17,297,314 |
| CSD name agrees with the polygon's name (either official language) | 17,092,703 — **98.8%** |
| point falls outside every CSD | 7 |

**The claim does not survive.** A label that agrees with the polygon its own coordinate
falls in 98.8% of the time is not an independent statement about location — it is derived
from the coordinate. There are two sides here, the **mail side** (postal code, mailing
municipality) and the **geographic side** (coordinate, CSD), and a two-way contradiction
cannot be arbitrated by majority. Only the neighbourhood can arbitrate it, which is what
the rest of the probe does.

The residuals are worth reading, because they are what confirms the reading rather than
merely being small.

**Every row whose point lands in the wrong province, or in none — 79 in all:**

| NAR province | polygon's province | polygon | mailing municipality | rows |
| --- | --- | --- | --- | ---: |
| 46 MB | 47 SK | Flin Flon (Part) | Flin Flon | 58 |
| 46 MB | 47 SK | Creighton | Flin Flon | 6 |
| 48 AB | 47 SK | Lloydminster (Part) | Lloydminster | 6 |
| 59 BC | — | outside every CSD | Surrey (3), Aldergrove (1) | 4 |
| 13 NB | — | outside every CSD | Upper Knoxford | 2 |
| 35 ON | — | outside every CSD | Stoney Creek | 1 |
| 12 NS | 13 NB | Westmorland | Tidnish Bridge | 1 |
| **24 QC** | **35 ON** | **Kitchener** | **Amos** | **1** |

Flin Flon, Creighton and Lloydminster are single towns split by a provincial line, and their
own CSDs are split with them; Tidnish Bridge sits on the Nova Scotia–New Brunswick border.
The seven points outside every polygon are on water or on a boundary. One row is a genuine
misplacement, and stage 5 comes back to it.

**The name disagreements are vintage and homonymy, not misplacement.** 204,672 rows carry a
CSD name that differs from the polygon they fall in. 7,069 of them carry no CSD name at all,
and 147,429 name a municipality that **no 2021 CSD carries** — New Brunswick's 2023 municipal
reform supplies most of those. That is a gap in the boundary vintage, not an error in NAR.

The other 50,174 name a municipality that **does** still exist in 2021, and vintage alone
does not excuse those. So ask the vintage-robust question instead: how far is the point from
the nearest 2021 CSD *anywhere in its province* carrying the name NAR gave it?

| distance from the point to the nearest CSD of that name | rows |
| --- | ---: |
| under 1 km | 26,531 |
| 1–10 km | 19,939 |
| over 10 km | 3,704 |

A point sitting beside a polygon that still carries its name is a boundary that moved or an
amalgamation that redrew it. The 3,704 over 10 km are the only residue this test could
convict, and they turn out to be a tail of **labels**, not of addresses — twelve of them
account for 3,679:

| label | province | rows | median distance | worst |
| --- | --- | ---: | ---: | ---: |
| Kent | 13 NB | 2,082 | 152.2 km | 167 km |
| Harvey | 13 NB | 990 | 13.4 km | 26 km |
| Campbellton | 13 NB | 214 | 10.8 km | 13 km |
| Hartland | 13 NB | 128 | 13.5 km | 17 km |
| Woodstock | 13 NB | 91 | 10.9 km | 13 km |
| St. Stephen | 13 NB | 69 | 11.7 km | 15 km |
| Kedgwick | 13 NB | 39 | 11.7 km | 28 km |
| La Pocatière | 24 QC | 31 | 13.6 km | 15 km |
| Doaktown | 13 NB | 20 | 11.8 km | 13 km |
| Cariboo I | 59 BC | 6 | 13.5 km | 14 km |
| Sudbury, Unorganized, North Part | 35 ON | 5 | 14.3 km | 16 km |
| Bonnyville No. 87 | 48 AB | 4 | 62.0 km | 62 km |

Eight of the twelve are New Brunswick, where the 2023 reform gave new municipalities names
that already belonged to something else — the 2,082 rows labelled `Kent` sit a median 152 km
from the 2021 CSD of that name because they are not the same Kent. None of this is an
address in the wrong place; all of it is a label read against the wrong vintage.

Which is the finding of the whole stage: **point-in-polygon against the CSD boundaries
convicts nothing.** It was brought in to arbitrate between the three witnesses, and what it
settled instead is that there are not three.

### A postal code is a delivery route, not a place

The second correction is the statistic. The first version measured every address against
its postal code's coordinate-wise median and flagged the ones far from it. That assumes the
group is one cluster. A postal code is a **delivery route**: it may legitimately be
disconnected, and a rural one routinely is, so the median can sit where the postal code has
no addresses at all. The v1 answer to that was to throw away every rural postal code
outright — 4,670 groups, and with them any hope of finding a misplaced rural address.

What replaces it is a pair of nearest-neighbour distances, which assume nothing about shape:

- **`d_own`** — the distance to the nearest address sharing the **full postal code**;
- **`d_other`** — the distance to the nearest address carrying a **different** one.

A multi-cluster postal code has a small `d_own` inside every one of its clusters, so being
disconnected costs it nothing. What no postal code should produce is a member far from every
one of its own addresses *and* sitting on top of somebody else's. The flag is
`d_own > 1 km AND d_other < d_own / 10`, and because it is a **ratio** it self-normalizes for
density: a rural row is judged against rural distances. Rural postal codes stay in.

## A municipality for the rows that have no coordinate

65,083 addresses carry no coordinate at all, so no polygon can label them. 57,152 of them
already carry a `CSD_ENG_NAME`; the remaining 7,931 can only be labelled from the mail side,
by looking their **mailing municipality** up in `MunAlias`, the postal-city → CSD table the
normalizer already maintains.

| the 7,931 rows with neither a coordinate nor a CSD label | rows |
| --- | ---: |
| the postal city matches no alias at all | 15 |
| the alias set holds exactly one CSD | 1 |
| several CSDs, but one holds ≥ 90% of the alias set's addresses | 4,862 |
| several CSDs and none dominant | 3,053 |

How good the default is can be measured on the 55,833 rows that have *both* a postal city
with an alias and a CSD label already: **the alias set contains the right CSD 55,833 times
out of 55,833**, and its largest member alone is the right one 42,173 times — **75.5%**. So
the backfill is honest as a candidate set with a default, and dishonest as an assignment.
One postal city can name dozens of municipalities — Prince Rupert reaches 47 of them — and
that is a property of Canada Post's delivery geography, not a defect in the table.

## The funnel

| | rows |
| --- | ---: |
| addresses with a 6-character postal code and a coordinate | 17,243,149 |
| — anchored by a **cellmate**: another address of the same postal code in the same 250 m cell | 16,550,860 |
| — loose, so `d_own` is computed exactly against the whole group | 692,289 |
| of the loose: `d_own` is undefined, the postal code has exactly one address | 84,282 |
| of the loose: `d_own` > 1 km | 46,679 (29,806 rural) |
| **flagged** — `d_own` > 1 km and `d_other` < `d_own`/10 | **17,224** (7,893 rural) |
| **flagged and reachable**, after `d_other` is re-measured along the road | **13,460** |

That is 0.10% of the file flagged and 0.078% surviving the road — and 7,893 of the flags are
rural postal codes (second character `0`), which the previous method could not have produced
at all, having excluded every one of them.

The cellmate step is what makes the exact search affordable: anything sharing a 250 m cell
with a sibling has `d_own` under 354 m and cannot be an outlier, so one `GROUP BY` settles
96% of the file in a fraction of a second and the exact within-group join runs on the 4%
that is left. All the neighbourhood tests use a grid bucket and an equi-join rather than a
spatial index — a correlated `ST_DWithin` subquery over 17M points takes two minutes for 400
probes; the grid does 17 million in seconds.

The 84,282 only-members are the method's **blind spot**, and they are named here because
nothing in this file can test them: a postal code NAR carries exactly one address for has no
sibling to be far from. They are carried into the flag set as candidates and every one of
them fails it, since `d_own` is undefined.

## The road: "close to a different postal code" has to mean reachable

`d_other` as a straight line is the wrong measurement, and the reason is exactly the one a
reader of a map would give: the line crosses the water. So `d_other` is measured again over
the **road network file** — 1,405,014 segments, built into an undirected graph over their
endpoints and tiled at 3 km with a ±2-tile window, so each candidate is solved in a local
graph of a few thousand edges rather than in a national one. Both the address and its five nearest strangers are snapped
**perpendicular onto the segment**, not onto its nearest endpoint: snapping to the endpoint
charges every rural address the distance to the next intersection, on both legs, and
inflated the median ratio to 9×. With the perpendicular snap the whole stage takes about
half a minute for 17,224 candidates.

| | |
| --- | ---: |
| straight-line `d_other`, median | 32 m |
| along the road, median | 110 m |
| flags that survive (`d_other_net` still < `d_own`/10) | 13,460 |
| dropped: the stranger is too far by road | 3,573 |
| dropped: no road path at all | 191 |
| of the dropped, more than 250 m from any road, where this test has little to say | 937 |

The 3,573 the road drops are not noise — they are the objection made concrete, and they
have two shapes. By count, they are Alberta's range-and-township grid, where the crow cuts
across a quarter-section the road has to go around:

| mailing municipality | province | rows | median crow `d_other` | median road `d_other` |
| --- | --- | ---: | ---: | ---: |
| Bonnyville | 48 AB | 128 | 172 m | 373 m |
| *(no municipality named)* | 48 AB | 90 | 178 m | 447 m |
| *(no municipality named)* | 59 BC | 63 | 59 m | 527 m |
| Rocky Mountain House | 48 AB | 57 | 212 m | 446 m |
| Medicine Hat | 48 AB | 44 | 77 m | 304 m |
| Brazeau County | 48 AB | 42 | 144 m | 360 m |
| Wainwright | 48 AB | 42 | 179 m | 414 m |
| Red Deer County | 48 AB | 36 | 124 m | 338 m |

Sorted instead by how much further the car goes than the crow, they are Ontario's lake
country, and nothing else:

| mailing municipality | rows | median crow `d_other` | median road `d_other` |
| --- | ---: | ---: | ---: |
| The Archipelago | 6 | 0 m | 9,927 m |
| Sioux Lookout | 7 | 0 m | 1,425 m |
| Dryden | 8 | 0 m | 858 m |
| Bracebridge | 6 | 0 m | 826 m |
| Sharbot Lake | 6 | 0 m | 777 m |
| Perth | 5 | 0 m | 765 m |
| Vermilion Bay | 6 | 0 m | 518 m |
| Bancroft | 18 | 0 m | 462 m |

Both columns need reading with care, and doing so is what shows the mechanism. The zero on
the left is real: **a blockface representative point is routinely shared between two
delivery routes** — 388 of The Archipelago's 1,303 addresses sit on a point that also
carries a MacTier, Nobel or Pointe-au-Baril address — so the straight line finds a stranger
at distance zero and the flag it produces means nothing at all. The kilometres on the right
are mostly *snap offset* rather than driving: 396 of those same 1,303 addresses are more
than 250 m from any mapped road (p90 4.3 km, worst 10.0 km) because they are water-access,
and the graph charges the walk to the road at both ends. So these rows are dropped for the
right reason by an argument that is only half about roads, and they are the 937 counted
above: where the address is not on the network, this test can say little beyond
*not reachable*.

## The verdict: which of the two sides to disbelieve

A flag says the row is inconsistent. It does not say whether the postal code or the
coordinate is the wrong part, and with only two sides there is no majority to appeal to.
One field on the row was produced by neither of them: the **street name**. So ask where the
street NAR names actually exists — in the 400 m around the point, or in the 400 m around
the postal code's own addresses?

| of the 13,460 survivors | rows | reading |
| --- | ---: | --- |
| street at the point, **not** at the postal code | **8,875** | the coordinate is corroborated; the **postal code** is the odd field |
| street at the postal code, **not** at the point | **653** | the **coordinate** is the part to disbelieve |
| street at both | 2,911 | no verdict — a common name, or a long street |
| street at neither | 1,021 | no verdict |

The 653 are the actionable set, and the only rows in this file where the coordinate is what
to disbelieve. Ordered by how built-up the wrong location is:

| postal code | address | municipality | `d_own` | road `d_other` | addresses within 400 m of the point |
| --- | --- | --- | ---: | ---: | ---: |
| M4W2C9 | 7 Saint Andrews | Toronto | 2.0 km | 37 m | 15,535 |
| K1R6T6 | 73 Anderson | Ottawa | 1.7 km | 40 m | 4,155 |
| H3J1M1 | 1630 Notre-Dame | Montréal | 4.9 km | 53 m | 3,323 |
| S7W1C9 | 119 Thakur | Saskatoon | 9.0 km | 11 m | 3,133 |
| L8B1V4 | 59 Forest Ridge | Waterdown | 10.9 km | 30 m | 3,012 |
| T6W2S6 | 721 178a | Edmonton | 12.8 km | 26 m | 2,892 |

All six carry a **building** point, not a blockface one, so this is not an artefact of the
weaker geometry source. All 23 addresses in M4W2C9 are on a Saint Andrews and 22 of them sit
within 80 m of each other in Rosedale; the twenty-third is 2.0 km away in downtown Toronto,
among 15,535 addresses, none of them on a Saint Andrews anything.

### The street test can be fooled, and here is the row that fools it

The one genuine misplacement stage 1 found — `105 Appalachian, AMOS QC`, whose point lands
in Kitchener, Ontario, 609 km from every other address in postal code J9T3A3 — comes out of
stage 5 as **coordinate *supported***. It is not. There is an Appalachian Cres in Kitchener,
and the point sits 15 m from it, among 79 NAR addresses that carry the name. The coordinate
was geocoded against the right street name in the wrong city, and a street test cannot tell
that from a correct placement. Two lessons: `coordinate_supported` is corroboration and not
proof, and a shared street name is exactly the failure mode that produces the misplacement
in the first place. The province test in stage 1 catches this row; the street test does not.

## Methods considered and not used

The literature name for this problem is the **spatial outlier**: a location whose attribute
disagrees with its spatial neighbours (Shekhar's SLOM, Lu's *z*-value approach). Here the
attribute is categorical — postal code, municipality — so those degenerate into the
neighbourhood counts used above, which is also the **local join count** statistic
(Anselin & Li) for a categorical mark. Having arrived at the same place, the count is
preferable: it needs no permutation inference and its output is a number of addresses,
which is auditable by hand.

- **Distance from the group's median point**, which is what v1 used. It assumes one cluster
  per postal code, and a delivery route is not obliged to be one. Replaced by `d_own`.
- **LOF / DBSCAN per group.** The textbook density answer. Its threshold is not
  interpretable in metres, and a median postal code here has nine members — far too few for
  a local density estimate to mean anything. The `d_own`/`d_other` ratio gets the
  density-invariance for free.
- **Robust Mahalanobis distance (MCD).** Fits an ellipse to each group. A postal code is a
  blockface, i.e. a line segment, so the ellipse is nearly degenerate and the covariance
  estimate is unstable at n = 9.
- **The municipality as the test.** Scored as a neighbourhood purity — the share of a point's
  neighbours carrying its own CSD — this exonerates nearly everything, because a
  municipality like Vaughan or Edmonton is large enough to hold a 15 km error inside itself.
  The postal code is the right grain, the street name is a good second, and the CSD is not a
  test at this scale. What the CSD *is* good for is stage 1, where it turned out to answer a
  different question than the one it was brought in for.

## What this does not settle

- **Nothing has been repaired.** No row is changed, no tier reads any of this, and the
  output is a list of candidates plus a directed verdict for 653 of them.
- **The blind spot is named and unfixable from inside NAR.** 84,282 addresses are the only
  member of their postal code. They cannot be far from a sibling they do not have.
- **`street at both` and `street at neither` — 3,932 rows — are neither convicted nor
  exonerated.** Either the coordinate is right and the postal code is unusual, or the
  postal code's cluster is itself the misplaced thing.
- **`coordinate_supported` is not a clean bill of health for the coordinate.** It is 8,875
  rows where the coordinate has corroboration the postal code lacks; the Amos row above
  shows what that corroboration is worth when a street name is shared between two towns.
- **This is a lower bound, and a biased one.** It can only see an error that leaves its
  postal code's neighbourhood entirely. An address put on the wrong side of the street, or
  on the wrong block of the right street, is invisible to it, and those are certainly more
  common than anything counted here. What this finds is the tail far enough out to be
  undeniable.
