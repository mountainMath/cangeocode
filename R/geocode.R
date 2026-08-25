#' The positional error a blockface point carries
#'
#' @description NAR's blockface representative point is the centroid of one side
#' of a street between two intersections, so an address that has no building
#' point is placed at a segment centroid shared with every other address on that
#' segment. Measured over 1.85M addresses that carry *both* points, the
#' building-to-blockface separation has a median of 50 m and a 90th percentile
#' of **176 m**, which is the constant used here.
#'
#' It is a lower bound for the addresses that actually need it. Only the 1.14M
#' blockface-only addresses ever fall back to this point, and they skew rural,
#' where blockfaces are longer than the national mix this was measured over.
#' @return A single number, metres
#' @keywords internal
nar_blockface_uncertainty_m <- function() 176

#' The positional error a remapped municipality carries, by what attests it
#'
#' @description When [normalize_address()] hands back a municipality that is not
#' the one the string named, the place that was searched was chosen by the
#' gazetteer rather than asserted by the input. `mun_remapped` says that
#' happened; `mun_evidence` says on what grounds, and the two populations it
#' separates are far enough apart that one floor cannot serve both.
#'
#' Measured against Nova Scotia's PVSC assessment points -- the one reference
#' established to be independent of NAR -- over the 32,512 exact unambiguous
#' building matches in a 40,000-address sample:
#'
#' | `mun_evidence` | n | p50 | p90 | >5 km | floor |
#' | --- | ---: | ---: | ---: | ---: | ---: |
#' | `kept` | 30,045 | 10.2 | 56.9 | 0.05% | 0 |
#' | `copostal` | 1,632 | 7.5 | 50.6 | 0.80% | 0 |
#' | `csd` | 91 | 14.5 | 87.2 | 0.00% | 0 |
#' | `unattested` | 560 | 12.9 | 83.3 | 1.79% | 118 |
#' | `untestable` | 184 | 32.0 | 327.5 | 1.63% | 118 |
#'
#' **The attested classes get no floor, because there is no spread to report.**
#' Pooled, `copostal` and `csd` sit at p90 52.6 m over 1,723 rows -- *below* the
#' 56.9 m of the rows whose municipality survived the parse. A remap the register
#' itself vouches for, whether by two names sharing a postal code or by one name
#' being the other's census subdivision, is positionally indistinguishable from
#' no remap at all. Reporting metres against it would be inventing them.
#'
#' **The unattested and untestable classes pool at p90 118.2 m over 744 rows**,
#' and that is the constant. `untestable` is the wider of the two and is *not*
#' fined by [nar_gazetteer_sql()] -- refusing it costs 119 exact matches for 2
#' errors past 5 km -- so the floor is the whole of what is done about it.
#'
#' `inferred` -- the string named no municipality and the gazetteer supplied one
#' -- takes the same floor **unmeasured**. PVSC always carries a city, so the
#' Nova Scotia corpus contains none of these rows and cannot price them. Grouping
#' it with the unattested is the conservative reading, not a measured one.
#'
#' Two things about all of these matter more than their size.
#'
#' They are *disagreements*, not error budgets: each contains NAR's own distance
#' from PVSC, which the `kept` row shows to be almost all of it.
#'
#' And none of them describes the tail, which is where the remap risk actually
#' lives. An unattested remap lands more than 5 km out 1.79% of the time and an
#' untestable one 1.63%, against 0.05% for a kept row -- thirty times the rate,
#' at a distance no 90th percentile of any of these populations reports. A caller
#' who cannot tolerate a kilometre-scale error should filter on `mun_evidence`
#' itself. These floors exist so that `uncertainty_m` stops reporting 0 m on the
#' populations where 0 is least true, not so that they can be read as bounds.
#'
#' See `inst/notes/nova-scotia-pvsc.md`.
#' @return A named numeric vector of metres, indexed by `mun_evidence`
#' @keywords internal
nar_remap_uncertainty_m <- function() {
  c(kept = 0, copostal = 0, csd = 0,
    unattested = 118, untestable = 118, inferred = 118)
}

#' Geocode Canadian addresses to coordinates
#'
#' @description Parses each address with [normalize_address()] and resolves the
#' result, returning one row per input in input order. `method` names the tiers
#' to try and the order to try them in; the column `match_method` records which
#' one answered:
#'
#' * **`nar_building`** -- the civic number is in NAR and carries its own
#'   building representative point. This is the exact match.
#' * **`nar_blockface`** -- the civic number is in NAR but has only a blockface
#'   point, the centroid of one side of a street between two intersections.
#' * **`nar_no_geometry`** -- the civic number is in NAR and `ADDR_GUID` names
#'   the record, but NAR holds no coordinates for it and none could be
#'   interpolated. 65k addresses are in this state. It is a different answer
#'   from `none`, which means the address was not found at all.
#' * **`nar_interpolated`** -- the civic number is *not* in NAR, so the position
#'   is interpolated between the nearest known civic numbers of the same parity
#'   on either side of it. See the section below.
#' * **`rqa_building`**, **`rqa_geocoded`**, **`rqa_uncertain`**, **`rqa_lot`**,
#'   **`rqa_other`** -- answered by the `rqa` tier, which carries Quebec's own
#'   positional-quality class rather than one label. Only `rqa_building` is a
#'   building placement, and it is the only one that reports an
#'   `uncertainty_m`. See [rqa_import()].
#' * **`rnf_interpolated`** -- answered by the `rnf` tier: the civic number
#'   fell inside the address range Statistics Canada's road network file gives
#'   for one side of one street segment, and the address is placed along that
#'   segment. See [rnf_import()].
#' * **`rnf_ambiguous`** -- the `rnf` tier found the civic number in the ranges
#'   of **several** segments and refused to choose between them. No coordinates,
#'   and `n_matches` says how many were in contention. It is reported rather
#'   than silently dropped because the whole of that tier's gross-error tail is
#'   these rows; see the tier description below.
#' * **`bc_site`**, **`bc_civic`**, **`bc_block`**, **`bc_street`**,
#'   **`bc_locality`** -- answered by the `bc` tier. See [bc_geocode()].
#' * **`nrcan`** -- answered by the `nrcan` tier. One value rather than several,
#'   because only one class of geolocator answer survives its floors at all.
#'   See [nrcan_geocode()].
#' * **`qc_address`** -- answered by the `qc` tier. Also one value: the
#'   service's other locator resolves a street rather than an address, and the
#'   tier does not place a row from it. See [qc_geocode()].
#' * **`not_covered`** -- the address parsed to a province this database does
#'   not hold, so no tier could have matched it. Only a partial import (see the
#'   `provinces` argument of [nar_connection()]) ever produces this, and it is
#'   deliberately distinct from `none`: the address may be perfectly good.
#' * **`none`** -- nothing resolved.
#'
#' @section Choosing the tiers: `method` is a vector of tier names in priority
#' order. Each tier is offered only the rows its predecessors left without a
#' position, so the order is what decides which answer wins:
#'
#' * **`"nar"`** -- look the civic number up in NAR directly. Answers
#'   `nar_building`, `nar_blockface` or `nar_no_geometry`.
#' * **`"rqa"`** -- look the civic number up in the **Repertoire quebecois des
#'   adresses**, Quebec's own register, which has to be imported once with
#'   [rqa_import()] and lives beside `Addresses` rather than in it. Quebec only,
#'   offline, and it holds roughly 308,000 civic addresses NAR does not. It
#'   belongs **after `"nar"` and before `"nar_interpolate"`**: a register point
#'   beats an interpolated one, and NAR's own building point beats both. Placed
#'   there it is worth about a third of Quebec's unplaceable tail outright and
#'   replaces an interpolated guess -- median 23 m from RQA's own coordinate,
#'   with 7% of them more than 500 m out -- for most of the rest.
#' * **`"nar_interpolate"`** -- place a civic number NAR does not carry between
#'   its known neighbours. Answers `nar_interpolated`.
#' * **`"rnf"`** -- interpolate along Statistics Canada's **Road Network
#'   File**, which has to be imported once with [rnf_import()] and lives beside
#'   `Addresses` rather than in it. Offline, national, and it answers for
#'   streets NAR does not carry at all -- which is what separates it from
#'   `"nar_interpolate"`, whose flanking civics have to come from NAR itself. It
#'   belongs **after `"nar_interpolate"`**: where both can answer, NAR's own
#'   neighbours are about six times more accurate. On a 5,000-address sample of
#'   business filings it placed a quarter of what the offline pair left
#'   unplaced, the largest recovery any tier here has offered, and **it refuses
#'   whenever more than one segment matches**, which is where its accuracy comes
#'   from rather than a nicety.
#' * **`"bc"`** -- ask the Province of BC's [Address Geocoder][bc_geocode()].
#'   British Columbia only, and **this makes one network request per unplaced BC
#'   row**; nothing contacts it unless the tier is named. The constraints are
#'   honoured: what is sent is rebuilt from the components after any
#'   `prov`/`mun` override, and a point outside `within` is discarded rather
#'   than returned.
#' * **`"qc"`** -- ask the Quebec government's [geocoder][qc_geocode()].
#'   Answers `qc_address`. Quebec only, and the one online tier that does
#'   **not** cost a request per row: it batches 1000 addresses per request, so
#'   naming it is cheap even on a large unplaced tail. It is also the only one
#'   that refuses -- it returns no point rather than a locality centroid when
#'   it cannot match -- so its answers need less rejecting than the others'.
#' * **`"nrcan"`** -- ask NRCan's national [geolocator][nrcan_geocode()].
#'   Answers `nrcan`. One network request per unplaced row, and it covers
#'   the whole country, so unlike `"bc"` and `"qc"` there is no province that
#'   excludes a row from being sent. **It belongs last.** Its surviving answers
#'   are roughly interpolation-grade at the median with a much longer tail, and
#'   it has no score of its own -- everything that separates a hit from a
#'   confident wrong answer is done by re-parsing the returned title, which is
#'   strict but not free of false positives.
#'
#' The default `c("nar", "nar_interpolate")` is offline and prefers a real NAR
#' record over an interpolated one. It does **not** include `"rqa"`, which would
#' otherwise appear and disappear depending on whether [rqa_import()] had been
#' run; in Quebec, `c("nar", "rqa", "nar_interpolate")` is the recommended
#' offline set once it has. `method = "nar"` keeps only the addresses
#' NAR actually carries. `c("nar", "nar_interpolate", "bc")` adds the BC
#' service as a last resort, and `c("bc", "nar")` prefers it over NAR wherever
#' it answers. `"qc"` is the same shape as `"bc"` for the other province that
#' publishes its own geocoder. `"nrcan"` is the national counterpart to both
#' and is the only tier that answers with no local database at all, which is
#' the case it exists for; it should be named after every other tier, never
#' before one.
#'
#' A row NAR holds without coordinates (`nar_no_geometry`) is passed on to the
#' next tier: knowing the address exists is worth reporting, but it is not worth
#' withholding a position a later tier can supply, and the `ADDR_GUID` found
#' survives whichever tier ends up placing the row. Note that the reverse costs
#' something -- a tier that never runs for a row reports nothing about it, so
#' putting `"nar"` last means interpolated rows carry no `ADDR_GUID`.
#'
#' @section Interpolation: Only civics of the **same parity** are used, because
#' odd and even numbers sit on opposite sides of the street and pooling them is markedly worse: measured
#' by leave-one-out over all 10.6M distinct NAR civic points, same-side
#' interpolation has a median error of 4.2 m against 35.2 m for both sides
#' pooled, and beats simply taking the nearest known civic (16.9 m).
#'
#' **Extrapolation is refused.** A civic number past the last known one on its
#' side has no second point to interpolate against, and guessing from the run's
#' spacing is close to worthless -- median error 15.1 m but a 90th percentile of
#' 237 m, barely better than the nearest neighbour it would displace. Those rows
#' fall through to the next tier rather than carrying a number that looks like
#' the others. 7.3% of NAR civics sit at the end of a run.
#'
#' @section Constraining the search: `prov`, `mun` and `within` are assertions
#' about where the address is, not hints. Each overrides whatever the string
#' itself claimed -- a row geocoded with `prov = "BC"` comes back with
#' `PROV_ABVN` reading `BC` no matter what was written -- and they compose, so
#' `prov` plus `mun` is the province-and-postal-city case and either can be
#' combined with a polygon.
#'
#' They earn their keep twice over. They resolve the ambiguity that `n_matches`
#' otherwise only reports, since a bare `100 Main St` means something definite
#' once the municipality is fixed. And `within` is close to free: the bounding
#' box is compared against the stored `x`/`y` columns, which DuckDB prunes with
#' per-row-group zonemaps rather than scanning -- the same mechanism that makes
#' [reverse_geocode()] fast.
#'
#' @section Uncertainty: `uncertainty_m` estimates the **90th-percentile
#' positional error this package's method introduces, relative to NAR's own
#' building point.** It is 0 for `nar_building`, 176 for `nar_blockface`, and
#' half the distance between the two flanking civics for `nar_interpolated`.
#'
#' For `rnf_interpolated` it is `max(95, 0.35 * len_m)` in the segment's length,
#' which is two-part because the error is: a short block is dominated by the
#' setback and the side offset, which do not shrink with it, and a long one by
#' how far along the block the range put the house, which does scale.
#'
#' That last figure is measured, and it holds across scales: the ratio of error
#' to flanking span has a 90th percentile of 0.50 in every span bucket from
#' under 50 m to over 2 km (0.496--0.522). So a 40 m gap between neighbours
#' gives 20 m and a 3 km gap gives 1.5 km, and filtering on `uncertainty_m` is
#' the way to drop the interpolations that are too coarse to use.
#'
#' **NAR's own error is not included and is not estimated.** The User Guide
#' warns that a building point "may not correspond exactly to the physical
#' center of the building structure itself" -- it can be the road access point
#' or the driveway -- and that offset is neither published nor consistent, so
#' `uncertainty_m = 0` means "this package added nothing", not "this point is
#' exact".
#'
#' `n_matches` counts the distinct NAR points that satisfied the query. Anything
#' above 1 means the address was ambiguous -- most often a street name the input
#' did not pin to a municipality -- and `uncertainty_m` is then widened to the
#' distance from the point returned to the furthest rejected candidate.
#'
#' **`n_matches == 1` is not the safety guarantee it looks like**, and this is
#' where the remaining widening comes from. One candidate means one was found,
#' not that the right one was among those searched -- and when the gazetteer
#' substituted the municipality, the uniqueness was manufactured by the same
#' step that chose the place, because the street was searched for only in the
#' municipality the gazetteer had already decided on. In Nova Scotia, measured
#' against PVSC's independent points, one exact unambiguous match in 180 was
#' more than a kilometre wrong and 85% of everything past 5 km was a remap.
#'
#' Two things answer that, and neither of them is `n_matches`. The gazetteer
#' fines a municipality swap nothing attests -- the attestations being co-postal
#' partners read out of NAR itself and the census subdivision the street already
#' sits in -- which more than halves the errors past 5 km in that same 40,000-row
#' Nova Scotia sample, 98 to 42, and takes the kilometre rate from one exact
#' unambiguous match in 192 to one in 286, at a cost of 373 exact matches; see
#' [nar_gazetteer_sql()]. And what survives is *reported*: `uncertainty_m` is
#' floored per [nar_remap_uncertainty_m()] according to `mun_evidence`, which
#' records *how* the substitution was attested, so an unattested remap no longer
#' claims the 0 m an exact civic match would otherwise imply -- while a remap a
#' postal code or a census subdivision vouches for is left alone, because
#' measured against PVSC it lands no further out than a municipality the input
#' got right. Both flags are [normalize_address()]'s and are returned alongside
#' the answer; read `mun_remapped` directly when what you need is *whether* the
#' place was chosen for you rather than how far the error might be, because the
#' risk it carries lives in a tail no distance at the 90th percentile describes.
#'
#' @section How many matched: `n_matches` and `n_records` count two different
#' things and the gap between them is the point of having both.
#'
#' `n_matches` counts distinct **points**. It is the ambiguity measure: it is
#' what widens `uncertainty_m`, and it is what tells you the answer may be in
#' the wrong place. `n_records` counts distinct **NAR addresses**, which is
#' usually the larger number, and it tells you the answer may be in the right
#' place but stand for more than one thing.
#'
#' They come apart because NAR files every unit of a multi-unit building as its
#' own address, all at the building's one coordinate. `49321 Range Road 72` in
#' Brazeau County, Alberta returns `n_matches = 1` and `n_records = 19`: there
#' is exactly one place to put it and nineteen addresses there, units 1 through
#' 29, and the input named none of them. This is not a corner case -- **47% of
#' the addresses NAR places share their coordinate with at least one other
#' address.**
#'
#' Naming the unit is what closes the gap. Where the input carries an
#' `APT_NO_LABEL` and NAR holds that unit at that civic number, the candidates
#' are narrowed to it: `49321 Range Road 72, Unit 9` is one record rather than
#' nineteen. The narrowing **narrows or it does nothing** -- a unit NAR has no
#' row for is dropped rather than enforced, and the address is placed as though
#' it had been written without one. That fallback is not defensive tidiness.
#' Over 5,000 Corporations Canada filings, 27.6% supply a unit; of those the
#' tier can match, 72.5% find that unit in NAR and **every one of them narrows
#' to exactly one record**, while the remaining 27.5% name a unit NAR does not
#' carry there -- enforcing it would take those 327 addresses from placed to
#' unplaced. Over the whole corpus the narrowing cuts 118,937 matched records
#' to 25,955, and the inputs reporting more than one record from 1,422 to 578.
#'
#' A record count above 1 is therefore not a warning by itself. It is a warning
#' when the collapsed records disagree about something you care about, and the
#' one such disagreement reported today is the postal code: `match_postal_code`
#' goes `NA` rather than pick one. The Brazeau County address is `NA` for that
#' reason -- its nineteen units carry four postal codes between them, and
#' naming one of the nineteen fills it in.
#'
#' `n_records` is 0 wherever no record was matched: every interpolated row that
#' did not first hit the `nar` or `rqa` tier, and every online tier.
#'
#' @section Two postal codes: the result carries two postal-code columns and
#' they answer different questions. `POSTAL_CODE` comes from
#' [normalize_address()] and is **what the input string said** -- `NA` when it
#' said nothing, which is the usual case for an address typed without one.
#' `match_postal_code` is **what the matched record carries**, and it is filled
#' in from the source rather than from the input.
#'
#' Only the tiers that match a record can fill it: the `nar` tier
#' (`nar_building`, `nar_blockface` and `nar_no_geometry` alike -- an address
#' NAR holds without coordinates still has a postal code) and the `rqa` tier.
#' It then **survives whichever tier ends up placing the row**, exactly as
#' `ADDR_GUID` does, so a `nar_interpolated` row carries a postal code when the
#' exact tier found the record first and NAR simply had no coordinates for it.
#' A row interpolated without such a hit, an `rnf_interpolated` row and every
#' online answer leave it `NA`: none of them resolve to a record with a postal
#' code of its own, an interpolated point sits between two addresses that may
#' not share one, and guessing which flank to copy would produce a value
#' indistinguishable from a looked-up one.
#'
#' It is also `NA` when the candidates disagree. NAR holds one row per address,
#' so a civic number with units contributes many rows, and 1.4% of civic numbers
#' -- 4.2% of addresses, since the buildings this happens to are large -- span
#' more than one postal code. Where the input names no unit -- the usual case --
#' nothing in the query says which of those rows was meant, and reporting one of
#' them would be a coin flip. `100 Queen St W, Toronto` is one: NAR carries it
#' as `M5H2N1` and `M5H2N2` both. A postal code in the *input* does not break
#' the tie either, since it is what the address claims rather than something the
#' query established. Naming the unit does break it, because it narrows the
#' candidates rather than choosing among them: 55 of the 5,000 corpus filings
#' gain a `match_postal_code` for that reason alone.
#'
#' @param x A character vector of address strings, or a data frame of already
#' parsed components as returned by [normalize_address()]. Passing the data
#' frame lets you parse once and geocode repeatedly, or edit a parse before
#' resolving it.
#' @param known Components the caller already has, as a named list of vectors
#' each length 1 or `length(x)`. **Authoritative**: each overrides whatever the
#' address string said, lands on the returned row, and constrains the search.
#' `PROV_ABVN` also reaches [normalize_address()], where knowing the province
#' disambiguates the parse.
#'
#' The two municipality keys are two different searches. `MUN_NAME` is the
#' **mailing city**, compared straight at NAR's `MAIL_MUN_NAME`. `CSD_NAME` is
#' the **census subdivision**, resolved through NAR's alias set -- so
#' `CSD_NAME = "Toronto"` reaches the addresses NAR files under `SCARBOROUGH`
#' and `MUN_NAME = "Toronto"` does not. Supply both to narrow to one community
#' inside an amalgamated city. See [nar_known()] for the full key list.
#'
#' `CSD_NAME` also comes back as an output column, reporting the census
#' subdivision the match turned out to be in -- which is a weaker claim than
#' the constraint, since the search was not restricted to it. A parse handed
#' back to `geocode()` therefore answers exactly as the string did; only a
#' `CSD_NAME` you assert here, or one on a frame you built yourself, restricts
#' anything. [nar_known_csd()] has the address that proves the difference.
#' @param within A spatial restriction: an `sf`/`sfc` object, an `st_bbox`, or a
#' length-4 numeric `c(xmin, ymin, xmax, ymax)`, interpreted in `crs` unless it
#' carries its own. **Authoritative**, and applied to every tier.
#' @param method Tiers to try, in priority order: any of `"nar"`, `"rqa"`,
#' `"nar_interpolate"`, `"rnf"`, `"bc"`, `"nrcan"` and `"qc"`. Default
#' `c("nar", "nar_interpolate")`, which is the offline pair. See the section
#' below.
#' @param geometry Whether to return an `sf` object with POINT geometry.
#' Unmatched rows get an empty point. Default `FALSE`, which returns `lon` and
#' `lat` columns instead.
#' @param crs CRS for the returned coordinates, default EPSG:4326.
#' @param version NAR version to query, passed to [nar_connection()]. Ignored
#' when `con` is supplied.
#' @param con An open NAR connection to reuse. The caller keeps ownership: a
#' connection passed in here is left open, while one opened internally is closed
#' again before returning.
#' @param ... Passed to the online tiers named in `method`, and to the parse.
#' Gazetteer arguments -- [nar_resolve_gazetteer()]'s, `mun_swap_penalty` among
#' them -- are forwarded to [normalize_address()] when `x` is a character
#' vector, and ignored when it is a data frame someone else has already parsed.
#' `keep_refused = TRUE` is the one worth knowing about: it places the matches
#' the gazetteer's threshold would have turned away and flags them in
#' `refused_for`, which turns an invisible false negative into an answer
#' [geocode_accept()] can drop again.
#' `rate` is
#' understood by all of them; `api_key` is [bc_geocode()]'s, as is anything
#' else it does not recognize, which it forwards to its own service as a query
#' parameter. [nrcan_geocode()] and [qc_geocode()] are each given only the
#' arguments they declare, so a BC-only argument passed alongside `"nrcan"`
#' reaches the BC tier alone rather than erroring. Note that `min_score` is
#' understood by [bc_geocode()] and [qc_geocode()] both, and means different
#' things to them -- see [qc_geocode()] on why its score is not a ranking.
#' Unused when `method` names no online tier.
#' @return A data frame with one row per input, carrying every column
#' [normalize_address()] returns -- `mun_remapped` and `mun_evidence` among them
#' -- plus
#' `ADDR_GUID`, `match_method`,
#' `uncertainty_m`, `n_matches`, `n_records`, `match_postal_code`, and either
#' `lon`/`lat` or an `sf` geometry column. `POSTAL_CODE` is the *parsed input* -- what the
#' address string itself said, or `NA` when it said nothing --  while
#' `match_postal_code` is what the matched record carries; see the section
#' below.
#' @seealso [geocode_accept()], for applying your own bar to the result without
#' re-running the query.
#' @export
#' @examples
#' \dontrun{
#' geocode("1055 W Georgia St, Vancouver BC")
#'
#' # Only addresses NAR actually carries -- nothing interpolated.
#' geocode(addresses, method = "nar")
#'
#' # Add the BC service as a last resort. Makes network requests.
#' geocode(addresses, method = c("nar", "nar_interpolate", "bc"))
#'
#' # Quebec's own register, offline, after one rqa_import().
#' geocode(addresses, method = c("nar", "rqa", "nar_interpolate"))
#'
#' # The road network file reaches streets NAR does not carry, after one
#' # rnf_import().
#' geocode(addresses, method = c("nar", "nar_interpolate", "rnf"))
#'
#' # NRCan's geolocator is national, so it can back up the whole country.
#' geocode(addresses, method = c("nar", "nar_interpolate", "nrcan"))
#'
#' # Parse once, resolve many times, and keep only the precise matches.
#' parsed <- normalize_address(addresses)
#' g <- geocode(parsed, geometry = TRUE)
#' g[g$uncertainty_m <= 25, ]
#' }
geocode <- function(x, known = NULL, within = NULL,
                    method = c("nar", "nar_interpolate"), geometry = FALSE,
                    crs = 4326, version = "latest", con = NULL, ...) {
  method <- nar_geocode_methods(method)
  q <- nar_geocode_setup(x, known, within, method, crs, version, con,
                         dots = list(...))

  hits <- nar_geocode_match(q$res, q$con, method = method,
                            bounds = nar_geocode_bounds_sql(q$bounds),
                            bounds_geom = q$bounds, ...)
  out <- cbind(q$res, hits[, c("ADDR_GUID", "match_method", "uncertainty_m",
                               "n_matches", "n_records",
                               "match_postal_code")])

  nar_geocode_geometry(out, hits$x, hits$y, q$con, crs = crs, geometry = geometry)
}

#' Every NAR record behind a geocoding answer
#'
#' @description [geocode()] returns one row per address and reports how many
#' NAR records stood behind it in `n_records`. This returns those records --
#' one row each, in the order the tier ranks them, so `match_rank == 1` is by
#' construction the record `geocode()` answered with.
#'
#' It exists because `n_records` is routinely greater than 1 and that is
#' usually **not** an error. NAR files every unit of a multi-unit building as
#' its own address at the building's single coordinate, and 47% of the
#' addresses it places share a coordinate with at least one other, so the
#' collapse is the normal case rather than the exceptional one. What varies is
#' whether the collapsed records differ in a way that matters to you, and the
#' only way to find out is to look at them.
#'
#' A unit in the input narrows this set exactly as it narrows [geocode()]'s
#' answer, because it is the same candidate set:
#' `geocode_matches("49321 Range Road 72, Unit 9")` returns that one record,
#' and a unit NAR does not carry there returns all nineteen.
#'
#' @section What it does not do: This is the **exact NAR tier only** -- the same
#' candidate set [geocode()]'s `"nar"` tier collapses, built by the same code.
#' There is deliberately no `method` argument, because no other tier has a
#' candidate set to enumerate: interpolation stands between two civic numbers
#' and resolves to no record at all, `"rnf"` interpolates along a street
#' segment, and the online services return an answer rather than a set. An
#' address only those tiers can place therefore has no matches here, which is
#' the correct answer and not a gap. Quebec's `"rqa"` tier does resolve to
#' records, but they are RQA rows with RQA columns and would not stack with
#' NAR's.
#'
#' Past `match_rank == 1` the order carries no meaning. It is the tier's
#' tie-break -- building points before blockface before none, then `ADDR_GUID`
#' -- which exists to make the *first* row reproducible, not to rank the rest.
#' Sort on whatever you are actually asking about.
#'
#' An address that matched nothing contributes **no rows**, so the result is not
#' aligned with the input the way [geocode()]'s is; `input_id` indexes back into
#' it. Use [geocode()] when you need one row per address.
#'
#' @inheritParams geocode
#' @return A data frame with one row per matched NAR record: `input_id` (the
#' index of the address in `x`), `input`, `match_rank`, the record columns
#' listed by `nar_geocode_match_cols()`, and either `lon`/`lat` or an `sf`
#' geometry column. Zero rows if nothing matched anything.
#' @seealso [geocode()], which collapses this to one row per address.
#' @export
#' @examples
#' \dontrun{
#' # One point, nineteen addresses: the units of one property, and the four
#' # postal codes between them are why geocode() reports no match_postal_code.
#' geocode_matches("49321 Range Road 72")
#'
#' # The usual workflow -- resolve first, then look only where it collapsed.
#' g <- geocode(addresses)
#' geocode_matches(addresses[g$n_records > 1])
#' }
geocode_matches <- function(x, known = NULL, within = NULL,
                            geometry = FALSE, crs = 4326,
                            version = "latest", con = NULL) {
  q <- nar_geocode_setup(x, known, within, "nar", crs, version, con)
  res <- q$res

  probe <- nar_geocode_probe(res)
  hits <- nar_geocode_run_tier(probe, probe$row_id, q$con,
                               nar_geocode_matches_sql,
                               nar_geocode_bounds_sql(q$bounds))

  i <- hits$row_id
  # Absent when the caller parsed the addresses themselves and handed over a
  # data frame, which is a supported way in. Tested against `names()` rather
  # than with `res$input`, which warns on a tibble instead of answering NULL.
  input <- if ("input" %in% names(res)) res$input
           else rep(NA_character_, nrow(res))
  out <- data.frame(input_id   = i,
                    input      = input[i],
                    match_rank = as.integer(hits$match_rank),
                    stringsAsFactors = FALSE)
  out <- cbind(out, hits[, nar_geocode_match_cols(), drop = FALSE])

  nar_geocode_geometry(out, hits$x, hits$y, q$con, crs = crs,
                       geometry = geometry)
}

#' Everything a geocoding call has to settle before it can query
#'
#' @description Shared by [geocode()] and [geocode_matches()], which ask the
#' same question of the same database and differ only in whether they report
#' the record chosen or all of them. Resolving the connection, checking that
#' the tiers named have something to run against, parsing, applying the
#' authoritative overrides and building the spatial restriction are the same
#' work in both, and drifting apart on any of them -- most of all on the
#' overrides -- would make the enumeration describe a different search than the
#' answer it is meant to explain.
#'
#' The tier availability checks run **before any parsing** rather than when a
#' tier is first reached: whether a tier runs at all depends on what its
#' predecessors left unplaced, so a missing import would otherwise surface on
#' one batch and stay silent on the next.
#' @param x Address strings, or a parsed data frame
#' @param known,within Constraints, as in [geocode()]
#' @param method The tiers that will be run, already validated
#' @param crs The CRS `within` is expressed in
#' @param version,con Which database to use
#' @return A list of `con`, the parsed `res`, and `bounds` as an `sfc` or `NULL`
#' @keywords internal
nar_geocode_setup <- function(x, known, within, method, crs, version, con,
                              dots = list()) {
  # Not closed on the way out: an unsupplied `con` resolves to the session's
  # connection, which the next call reuses. close_nar() is what ends it.
  if (is.null(con)) con <- nar_session_use(version)
  if ("rqa" %in% method && !nar_has_rqa(con)) {
    stop("The \"rqa\" tier needs the Repertoire quebecois des adresses, which ",
         "this database does not carry. Run rqa_import() first.", call. = FALSE)
  }
  if ("rnf" %in% method && !nar_has_rnf(con)) {
    stop("The \"rnf\" tier needs Statistics Canada's road network file, which ",
         "this database does not carry. Run rnf_import() first.", call. = FALSE)
  }
  k <- nar_known(known, if (is.data.frame(x)) nrow(x) else length(x))
  if (!is.null(k$CSD_NAME) && !nar_has_streets(con)) {
    stop("`known$CSD_NAME` resolves through the MunAlias table, which arrived ",
         "in schema version 5. Rebuild with nar_connection(refresh = TRUE), ",
         "or constrain with `known$MUN_NAME` or `within` instead.",
         call. = FALSE)
  }

  res <- if (is.data.frame(x)) {
    # Only the two that decide whether a row can be searched at all. Everything
    # else constrains when it is there and is silent when it is not, which is
    # what lets a caller hand over the breakdown they have rather than the whole
    # of one -- the columns below are materialized so the tiers can read them.
    missing <- setdiff(c("CIVIC_NO", "STREET_NAME"), names(x))
    if (length(missing)) {
      stop("`x` is a data frame but has no ", paste(missing, collapse = ", "),
           " column. Pass address strings, or the output of normalize_address().",
           call. = FALSE)
    }
    for (nm in c("MUN_NAME", "CSD_NAME", "PROV_ABVN")) {
      if (is.null(x[[nm]])) x[[nm]] <- NA_character_
    }
    # Only here: a frame never went through the parser, so an asserted
    # jurisdiction has not yet had the mailing city it contradicts cleared.
    # The string path had that done inside normalize_address(), before the
    # gazetteer, and re-doing it now would throw away the mailing city the
    # gazetteer resolved.
    nar_known_clear_mun(x, k)
  } else {
    do.call(normalize_address,
            c(list(x, known = known, con = con), nar_gazetteer_dots(dots)))
  }

  # Authoritative, so the override lands on `res` rather than only on the probe:
  # the caller asserted these, and a result that reported the string's own
  # province next to a point constrained to a different one would be a lie about
  # what was searched. normalize_address() has already done this for the strings
  # it parsed -- and threaded `known` inward, where the province additionally
  # disambiguates the parse (ROUTE is New Brunswick's typeless numbered road and
  # Quebec's street type, and only the province separates them). Repeated here
  # for the data-frame path, which never went through the parser.
  res <- nar_known_apply(res, k)
  asserted <- nar_known_has_mun(k, nrow(res))
  if (any(asserted) && "mun_remapped" %in% names(res)) {
    res$mun_remapped[asserted] <- FALSE
    res$mun_evidence[asserted] <- "kept"
  }

  # Carried as an attribute rather than a column because it is not part of the
  # answer: `res` is cbind()ed into the result, and the jurisdiction that
  # *restricted* the search is a different claim from the one the match turned
  # out to be in. See nar_known_csd() for why the second may not become the
  # first.
  attr(res, "nar_csd_constraint") <-
    nar_known_csd(res, k, is.data.frame(x) && !("parse_source" %in% names(x)))

  list(con = con, res = res, bounds = nar_geocode_bounds_geom(within, crs, con))
}

#' Attach coordinates to a geocoding result
#'
#' @description Storage-CRS coordinates in, either `lon`/`lat` columns or an
#' `sf` object out. The reprojection is done in `sf` rather than in DuckDB
#' because these are freshly computed coordinates rather than a stored geometry
#' column, and because it leaves the axis-order handling to `sf`, which always
#' means lon/lat -- the `always_xy` trap that [collect_nar()] has to work around
#' does not arise. The storage CRS is still read from the database rather than
#' assumed.
#' @param out The result data frame
#' @param x,y Coordinates in the storage CRS, `NA` where nothing matched
#' @param con A NAR connection
#' @param crs Target CRS
#' @param geometry Whether to return an `sf` object
#' @return `out` with coordinates attached
#' @keywords internal
nar_geocode_geometry <- function(out, x, y, con, crs = 4326, geometry = FALSE) {
  storage <- nar_crs(con)
  ok <- !is.na(x) & !is.na(y)

  pts <- sf::st_sfc(rep(list(sf::st_point()), length(x)), crs = storage)
  if (any(ok)) {
    pts[ok] <- sf::st_sfc(lapply(which(ok), function(i) sf::st_point(c(x[i], y[i]))),
                          crs = storage)
  }
  if (!is.null(crs)) pts <- sf::st_transform(pts, crs)

  if (geometry) return(sf::st_sf(out, geometry = pts))

  # Coordinates come off the matched subset, not the whole column: an empty
  # point contributes no row to st_coordinates() rather than a row of NAs, so
  # binding the full matrix on would silently shift every value up.
  # `rep(NA_real_, nrow(out))` and not `NA_real_`: assigning a length-one value
  # into a zero-row data frame is an error rather than a no-op.
  out$lon <- rep(NA_real_, nrow(out))
  out$lat <- rep(NA_real_, nrow(out))
  if (any(ok)) {
    co <- sf::st_coordinates(pts[ok])
    out$lon[ok] <- co[, 1]
    out$lat[ok] <- co[, 2]
  }
  out
}

#' Resolve parsed components by running the requested tiers in priority order
#'
#' @description Each tier is offered only the rows its predecessors left
#' unplaced. Running them in sequence rather than running them all and picking
#' the best answer is worth the extra temp table: a NAR tier is a full scan of
#' the 17.4M-row `Addresses` table, and the all-exact case is the common one, so
#' the later tiers usually see almost nothing.
#'
#' That scan is also why neither NAR query goes through `Streets` or wants an
#' index.
#' Measured on the 2026-06 release, the folded street-key join costs 0.05s for a
#' 5-row probe and **0.08s for a 200-row probe** -- the scan is the whole cost
#' and every probe row shares it, exactly as with the radius query. Batch your
#' addresses into one call rather than looping.
#' @param res Parsed components, as [normalize_address()] returns
#' @param con A NAR connection
#' @param method Tiers to try, in priority order
#' @param bounds A spatial restriction from [nar_geocode_bounds_sql()], or `""`
#' @param bounds_geom The same restriction as an `sfc`, for the tiers that run
#' outside the database
#' @param ... Passed to the online tiers; see [geocode()] on how they are split
#' @return A data frame with one row per row of `res`, carrying `ADDR_GUID`,
#' `match_method`, `uncertainty_m`, `n_matches`, `n_records`,
#' `match_postal_code`, `x` and `y`
#' @keywords internal
nar_geocode_match <- function(res, con, method = c("nar", "nar_interpolate"),
                              bounds = "", bounds_geom = NULL, ...) {
  n <- nrow(res)
  out <- data.frame(ADDR_GUID     = rep(NA_character_, n),
                    match_method  = rep("none", n),
                    uncertainty_m = rep(NA_real_, n),
                    n_matches     = rep(0L, n),
                    n_records     = rep(0L, n),
                    match_postal_code = rep(NA_character_, n),
                    x             = rep(NA_real_, n),
                    y             = rep(NA_real_, n),
                    stringsAsFactors = FALSE)
  if (!n) return(out)

  probe <- nar_geocode_probe(res)
  dots <- list(...)

  # Priority is expressed as running order: each tier sees only the rows its
  # predecessors left unplaced, and a row is unplaced exactly when it has no
  # coordinates. That definition is what sends `nar_no_geometry` on to the next
  # tier -- knowing the address exists is worth reporting, but not worth
  # withholding a position its neighbours can supply, and the ADDR_GUID found
  # by the exact tier survives whichever tier ends up placing it.
  for (m in method) {
    todo <- which(is.na(out$x))
    if (!length(todo)) break
    out <- switch(m,
      nar             = nar_geocode_tier_nar(out, probe, todo, con, bounds),
      rqa             = nar_geocode_tier_rqa(res, out, probe, todo, con, bounds),
      nar_interpolate = nar_geocode_tier_interp(out, probe, todo, con, bounds),
      rnf             = nar_geocode_tier_rnf(out, probe, todo, con,
                                             bounds = bounds_geom),
      bc              = nar_geocode_tier_bc(res, out, todo, con,
                                            bounds = bounds_geom, ...),
      # The online tiers have different vocabularies, so `...` cannot go to
      # all of them: `min_score` means nothing to the geolocator, and the BC
      # tier forwards anything it does not recognize to its own service as a
      # query parameter, so it is the one that has to keep receiving all of
      # `...`. The other two take closed argument lists and are filtered
      # against them.
      nrcan           = do.call(nar_geocode_tier_nrcan,
                                c(list(res, out, todo, con,
                                       bounds = bounds_geom),
                                  nar_nrcan_dots(dots))),
      qc              = do.call(nar_geocode_tier_qc,
                                c(list(res, out, todo, con,
                                       bounds = bounds_geom),
                                  nar_qc_dots(dots))))
  }
  out <- nar_geocode_remap_floor(out, res)
  nar_geocode_mark_uncovered(out, res, con)
}

#' Widen uncertainty where the municipality searched was not the one written
#'
#' @description Applied once, after every tier, rather than inside the tiers:
#' each of them resolves against the same parse, and it is the parse's
#' municipality that is in question. A tier that never consulted it is not
#' thereby safe -- the online ones are handed the formatted address, remapped
#' municipality included.
#'
#' It is a floor and not a replacement, so a blockface point, an ambiguous
#' candidate set or a long interpolation span keeps its own larger number.
#' Rows that were not placed keep `NA`: there is no position for an uncertainty
#' to be about.
#'
#' A `res` from an older parse, or a data frame the caller assembled, carries no
#' `mun_remapped` column. That is read as "not remapped" rather than as an
#' error, which keeps every existing caller working -- and is why the flag is
#' set by [normalize_address()] rather than recomputed here, where the string
#' that was written is no longer available to compare against.
#' @param out The result so far
#' @param res Parsed components
#' @return `out`, with `uncertainty_m` floored on the remapped rows
#' @keywords internal
nar_geocode_remap_floor <- function(out, res) {
  remapped <- res$mun_remapped
  if (is.null(remapped)) return(out)
  floors <- nar_remap_uncertainty_m()
  # An older parse carries mun_remapped but not mun_evidence. It is known to be
  # remapped and not known to be attested, so it takes the unattested floor --
  # the reading that does not credit a row with evidence it never produced.
  f <- if (is.null(res$mun_evidence)) rep(unname(floors[["unattested"]]),
                                          length(remapped))
       else unname(floors[match(res$mun_evidence, names(floors))])
  # An evidence value this version does not know contributes no floor rather
  # than the largest one: the same forward-compatibility the NULL branch has.
  f[is.na(f)] <- 0
  i <- which(!is.na(remapped) & remapped & !is.na(out$uncertainty_m) & f > 0)
  if (!length(i)) return(out)
  out$uncertainty_m[i] <- pmax(out$uncertainty_m[i], f[i])
  out
}

#' Separate "not in the gazetteer" from "not in this database"
#'
#' @description A partial NAR import holds only the provinces it downloaded, so
#' an address in a province it does not hold cannot match however good the
#' parse is. Reporting that as `none` would say the address is wrong; it says
#' instead that this database was never asked to know.
#'
#' Only rows whose province is both **parsed** and demonstrably outside the
#' coverage are marked. An unparsed province stays `none`, because nothing has
#' been established about it, and a national database marks nothing at all.
#' @param out The result so far
#' @param res Parsed components
#' @param con A NAR connection
#' @return `out`, with `match_method` set to `"not_covered"` where it applies
#' @keywords internal
nar_geocode_mark_uncovered <- function(out, res, con) {
  have <- nar_coverage(con)
  if (identical(have, nar_all_provinces())) return(out)
  prov <- res$PROV_ABVN
  uncovered <- !is.na(prov) & nzchar(prov) & !(toupper(prov) %in% have) &
    out$match_method == "none"
  out$match_method[uncovered] <- "not_covered"
  out
}

#' Write a probe subset to a temporary table and run one tier's query
#'
#' @description The two NAR tiers differ only in their SQL, so the temp-table
#' round trip is shared. The table is dropped on exit rather than left for the
#' connection to clean up, because a caller-supplied connection outlives the
#' call and geocoding in a loop would otherwise accumulate them.
#'
#' The empty probe is written and queried like any other rather than
#' short-circuited, so a caller always gets a result with the query's own
#' columns and types. Skipping it would return a shapeless `data.frame()`, and
#' every caller would need its own idea of what the columns should have been.
#' @param probe The full probe table
#' @param todo Row indices still needing a position
#' @param con A NAR connection
#' @param sql_fn A function of `(table_name, bounds)` returning SQL
#' @param bounds A spatial restriction, or `""`
#' @return The query result, possibly zero rows
#' @keywords internal
nar_geocode_run_tier <- function(probe, todo, con, sql_fn, bounds) {
  probe <- probe[probe$row_id %in% todo, , drop = FALSE]

  tmp <- paste0("nar_geo_", as.integer(stats::runif(1) * 1e9))
  DBI::dbWriteTable(con, tmp, probe, temporary = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, tmp), silent = TRUE), add = TRUE)
  DBI::dbGetQuery(con, sql_fn(tmp, bounds))
}

#' The exact NAR tier
#'
#' @description Looks the civic number up directly. Answers `nar_building` or
#' `nar_blockface` depending on which point NAR carries, or `nar_no_geometry`
#' when it carries the record but no coordinates.
#' @param out The result so far
#' @param probe The probe table
#' @param todo Row indices still needing a position
#' @param con A NAR connection
#' @param bounds A spatial restriction, or `""`
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_nar <- function(out, probe, todo, con, bounds = "") {
  exact <- nar_geocode_run_tier(probe, todo, con, nar_geocode_exact_sql, bounds)
  if (!nrow(exact)) return(out)

  i <- exact$row_id
  located <- !is.na(exact$x)
  out$ADDR_GUID[i]    <- exact$ADDR_GUID
  out$match_method[i] <- ifelse(located, paste0("nar_", exact$geom_source),
                                "nar_no_geometry")
  out$n_matches[i]    <- as.integer(exact$n_points)
  out$n_records[i]    <- as.integer(exact$n_records)
  out$match_postal_code[i] <- exact$match_postal_code
  out$x[i]            <- exact$x
  out$y[i]            <- exact$y
  # The ambiguity widening: pmax, so a blockface match that is also ambiguous
  # keeps whichever of the two errors is larger rather than the later one.
  base <- ifelse(!located, NA_real_,
                 ifelse(exact$geom_source == "blockface",
                        nar_blockface_uncertainty_m(), 0))
  out$uncertainty_m[i] <- pmax(base, exact$spread_m)
  out
}

#' The NAR interpolation tier
#'
#' @description Places a civic number NAR does not carry between the nearest
#' known civics of the same parity on either side of it.
#' @inheritParams nar_geocode_tier_nar
#' @return `out`, with this tier's answers filled in
#' @keywords internal
nar_geocode_tier_interp <- function(out, probe, todo, con, bounds = "") {
  interp <- nar_geocode_run_tier(probe, todo, con, nar_geocode_interp_sql, bounds)
  if (!nrow(interp)) return(out)

  i <- interp$row_id
  out$match_method[i]  <- "nar_interpolated"
  out$uncertainty_m[i] <- 0.5 * interp$span_m
  out$n_matches[i]     <- 2L
  out$x[i]             <- interp$x
  out$y[i]             <- interp$y
  out
}

#' The probe table a geocoding query joins against
#'
#' @description Drops the rows nothing could be done with -- no street name, or
#' no civic number to place along it -- and blanks the `NA`s, because the SQL
#' treats an absent component as "do not constrain on this" and `NULL` would
#' instead make every comparison against it unknown.
#' @param res Parsed components, as [normalize_address()] returns
#' @return A data frame with a `row_id` back-reference into `res`
#' @keywords internal
nar_geocode_probe <- function(res) {
  # A hand-built data frame may carry only the columns it needed to; an absent
  # column and an all-NA one mean the same thing here, namely do not constrain.
  blank <- function(name) {
    v <- if (is.null(res[[name]])) NA else res[[name]]
    ifelse(is.na(v), "", as.character(v))[keep]
  }
  blank_csd <- function() {
    v <- attr(res, "nar_csd_constraint")
    if (is.null(v)) return(rep("", sum(keep)))
    ifelse(is.na(v), "", as.character(v))[keep]
  }
  keep <- !is.na(res$STREET_NAME) & !is.na(res$CIVIC_NO)
  # `rep("", sum(keep))` and not `""`: when nothing is keepable every other
  # column is length zero, and a length-one literal does not recycle down to
  # zero rows -- data.frame() errors instead. A batch in which no row parsed to
  # both a street and a civic number is a normal input, not a mistake, and has
  # to reach the tiers as an empty probe so they can decline it.
  unconstrained <- rep("", sum(keep))
  data.frame(
    row_id    = which(keep),
    name_fold = nar_fold(res$STREET_NAME[keep]),
    # Only the RQA tier joins on this. The NAR tiers keep the plain fold,
    # which is indexed; see rqa_geocode_sql() on why RQA cannot.
    match_fold = nar_match_fold(res$STREET_NAME[keep]),
    # The two municipality grains, and both constrain when both are present.
    # MUN_NAME is compared straight at MAIL_MUN_NAME because it is a mailing
    # city -- either NAR's own string, the gazetteer having canonicalized it,
    # or one the caller asserted as a mailing city on purpose. The jurisdiction
    # cannot be compared to a mailing name at all, so it goes through MunAlias:
    # constraining to TORONTO by mailing city would drop everything NAR files
    # under SCARBOROUGH. It is read off the attribute and *not* off
    # `res$CSD_NAME`, because only some of the values in that column are a
    # constraint -- nar_known_csd() has the case that proves it.
    mun_fold  = gsub(".", "", nar_fold(blank("MUN_NAME")), fixed = TRUE),
    mun_auth  = gsub(".", "", nar_fold(blank_csd()), fixed = TRUE),
    prov      = blank("PROV_ABVN"),
    type      = blank("STREET_TYPE"),
    dir       = blank("STREET_DIR"),
    suffix    = nar_fold(blank("CIVIC_NO_SUFFIX")),
    apt       = nar_unit_fold(blank("APT_NO_LABEL")),
    civic     = as.integer(res$CIVIC_NO[keep]),
    stringsAsFactors = FALSE
  )
}

#' The street key both geocoding tiers join on
#'
#' @description Shared so the two tiers cannot drift apart -- an interpolation
#' that selected its flanking civics from a different street than the exact tier
#' searched would be a silent, invisible error.
#'
#' Both NAR name families are matched, as the gazetteer does, because neither is
#' complete on its own. Every other component only ever constrains when the
#' input supplied it: a string that never named a municipality is resolved
#' against the whole province rather than being refused, and the ambiguity that
#' invites is reported through `n_matches` instead.
#'
#' Periods come off both sides of the municipality comparison. NAR files ST.
#' JOHN'S, SAULT STE. MARIE and ST. ALBERT with them while `nar_norm_text()`
#' strips them from input, so without this those cities never match.
#' @return A SQL fragment, with the probe aliased `p` and `Addresses` aliased `a`
#' @keywords internal
nar_geocode_street_key <- function(name_col, bounds = "") {
  sprintf(
   "p.name_fold = strip_accents(upper(a.%1$s))
   AND (p.prov = '' OR a.MAIL_PROV_ABVN = p.prov)
   AND (p.mun_fold = ''
        OR replace(strip_accents(upper(a.MAIL_MUN_NAME)), '.', '') = p.mun_fold)
   AND (p.mun_auth = ''
        OR (a.PROV_CODE || ':' || a.CSD_TYPE_ENG_CODE || ':' || a.CSD_ENG_NAME) IN (
             SELECT m.MUN_KEY FROM MunAlias m
              WHERE replace(m.NAME_FOLD, '.', '') = p.mun_auth
                AND (p.prov = '' OR m.PROV_ABVN = p.prov)))
   AND (p.type = '' OR p.type IN (upper(a.OFFICIAL_STREET_TYPE),
                                  upper(a.MAIL_STREET_TYPE)))
   AND (p.dir  = '' OR p.dir  IN (upper(a.OFFICIAL_STREET_DIR),
                                  upper(a.MAIL_STREET_DIR)))%2$s",
   name_col, bounds)
}

#' Every NAR address on the street a probe row names
#'
#' @description One branch per NAR street-name family, unioned, rather than one
#' join with `OFFICIAL = x OR MAIL = x`. **This is a 99x difference and it is
#' not a micro-optimization.** An `OR` across two columns has no equijoin key,
#' so DuckDB falls back to a nested loop over the whole 17.4M-row table: the
#' interpolation tier, which has no civic-number equality to rescue it, took
#' **15.87s** written that way and **0.16s** as a union, for byte-identical
#' results. The exact tier hid the problem, because `CIVIC_NO = p.civic` gave
#' the planner a hash key of its own.
#'
#' `UNION` and not `UNION ALL`: the two families agree for most addresses, and
#' the select list carries `ADDR_GUID`, so the set union drops exactly the rows
#' both branches matched and nothing else.
#' @param probe Name of the temp table holding the parsed components
#' @param select The select list, with the probe aliased `p` and `Addresses` `a`
#' @param extra Tier-specific predicates, appended to the join condition
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A SQL fragment producing the candidate set
#' @keywords internal
nar_geocode_candidates <- function(probe, select, extra = "", bounds = "") {
  branch <- function(col) {
    sprintf("SELECT %s
        FROM %s p
        JOIN Addresses a
          ON %s%s",
            select, probe, nar_geocode_street_key(col, bounds), extra)
  }
  paste(branch("OFFICIAL_STREET_NAME"), "
      UNION
      ",
        branch("MAIL_STREET_NAME"))
}

#' The rank that decides which candidate a tier answers with
#'
#' @description One window expression, defined once, because both readings of a
#' candidate set are supposed to agree: [nar_geocode_best_sql()] keeps the row
#' it puts first and [nar_geocode_ranked_sql()] returns them all in that same
#' order, so `match_rank == 1` in [geocode_matches()] is by construction the
#' record [geocode()] answered with. Written twice they would drift, and the
#' drift would be invisible -- an enumeration that quietly disagreed with the
#' answer it exists to explain.
#' @param rank The tier's `ORDER BY` expression
#' @return A SQL fragment
#' @keywords internal
nar_geocode_rank_sql <- function(rank) {
  sprintf("row_number() OVER (PARTITION BY row_id ORDER BY %s)", rank)
}

#' How the NAR tier ranks the addresses that matched
#'
#' @description A building point always outranks a blockface one for the same
#' address, a record with no point at all comes last, and `ADDR_GUID` breaks
#' any remaining tie so the answer is stable across runs rather than depending
#' on scan order.
#' @return A SQL `ORDER BY` expression
#' @keywords internal
nar_geocode_nar_rank <- function() {
  "CASE WHEN x IS NULL THEN 2 WHEN geom_source = 'building' THEN 0 ELSE 1 END,
                 ADDR_GUID"
}

#' Put a parsed unit into the vocabulary the address files use
#'
#' @description NAR stores a unit as Canada Post spells one -- `BSMT`, `UPPR`,
#' `LWR` -- and a person types `Basement`, `Sous-sol`, `Upper`. Those are the
#' bare labels [normalize_address()] already goes out of its way to recognize,
#' so failing to match them afterwards would be recognizing a word in order to
#' throw it away.
#'
#' **The translation runs one way only, and that is the point.** It is applied
#' to the *input* and never to the stored column, because the stored column
#' does not need it: of NAR's 5.96M units, `BASEMENT` appears zero times,
#' `UPPER` once and `GROUND` once, against 137,413 `BSMT` and 22,757 `UPPR`.
#' So this is a translation into NAR's vocabulary, not a fold both sides share
#' -- which means it carries none of the keep-the-two-halves-identical hazard
#' that [nar_match_fold()] does.
#'
#' Zero padding is **not** normalized, having been measured and declined: 11,966
#' of 5.96M units carry an interior leading zero, essentially all of them
#' `PH01`-style penthouse labels, and a rule that turned `PH01` into `PH1` would
#' be reaching for 0.2% of units while acquiring an opinion about every unit
#' whose label is meaningfully padded.
#' @param x A parsed `APT_NO_LABEL`, or `""` for none
#' @return The unit as NAR would spell it
#' @keywords internal
nar_unit_fold <- function(x) {
  u <- gsub("[. ]", "", nar_fold(x))
  from <- c("BASEMENT", "SOUS-SOL", "SOUSSOL", "UPPER", "LOWER")
  to   <- c("BSMT",     "BSMT",     "BSMT",    "UPPR",  "LWR")
  i <- match(u, from)
  ifelse(is.na(i), u, to[i])
}

#' The stored side of the unit comparison
#'
#' @description Case, spaces and periods only -- 592,561 of NAR's units are
#' not upper case and 16 are untrimmed. The vocabulary translation happens on
#' the input, in [nar_unit_fold()].
#' @param col The stored unit column, qualified
#' @return A SQL fragment
#' @keywords internal
nar_unit_sql <- function(col) {
  sprintf("replace(replace(strip_accents(upper(%s)), '.', ''), ' ', '')", col)
}

#' Narrow a candidate set to the unit that was asked for, when there is one
#'
#' @description A supplied apartment number is what tells the difference between
#' the 19 addresses at `49321 Range Road 72`, and without it `geocode()` reports
#' all 19 as the record count of an answer the caller had already disambiguated.
#'
#' **It narrows or it does nothing; it never refuses.** The filter keeps the
#' matching records when there are any and the whole set when there are none,
#' which is not defensive coding but the measured majority case: over 5,000
#' Corporations Canada filings, 1,189 supplied a unit and matched NAR records,
#' and **27.5% of those units are not in NAR at that civic number**. Filtering
#' unconditionally would take 327 addresses in 5,000 from placed to unplaced --
#' trading a wrong record count, which is visible, for a lost coordinate, which
#' is worse. What it does buy where the unit is there is total: all 862 hits
#' narrow to exactly one record, from 93,844 candidates between them.
#'
#' The consequence worth knowing is that `n_records` is the report on this. A
#' unit that was found leaves `n_records = 1`; a unit that was not leaves it at
#' the full count, unchanged from what it would have been with no unit at all.
#' Nothing else says which happened.
#' @param cand SQL producing a candidate set with `row_id` and `unit_hit`
#' @return A SQL fragment producing the narrowed set, without `unit_hit`
#' @keywords internal
nar_geocode_unit_filter <- function(cand) {
  sprintf("SELECT * EXCLUDE (unit_hit)
        FROM (%s)
      QUALIFY unit_hit OR NOT bool_or(unit_hit) OVER (PARTITION BY row_id)",
          cand)
}

#' The candidate column that says whether a record is the unit asked for
#'
#' @param col The stored unit column, qualified
#' @return A SQL fragment ending in `AS unit_hit`
#' @keywords internal
nar_geocode_unit_hit <- function(col) {
  sprintf("(p.apt <> '' AND %s = p.apt) AS unit_hit", nar_unit_sql(col))
}

#' The civic-number half of an exact match
#'
#' @description Shared by the query that answers and the query that enumerates,
#' for the same reason the rank is: two spellings of "which addresses count as
#' this one" would be two different searches.
#' @return A SQL fragment, appended to the street key
#' @keywords internal
nar_geocode_civic_key <- function() {
  "\n         AND a.CIVIC_NO = p.civic
         -- A suffix that was written has to be honoured -- 990A and 990 are
         -- different addresses -- but one that was not is left unconstrained,
         -- since the great majority of NAR rows carry no suffix at all.
         AND (p.suffix = '' OR upper(coalesce(a.CIVIC_NO_SUFFIX, '')) = p.suffix)"
}

#' Pick one candidate per row and measure the set it came from
#'
#' @description The shape both record-resolving tiers share -- NAR's and
#' Quebec's -- with only the candidate set, the rank and the column names
#' differing. They were written out twice before, which meant the ambiguity
#' measurements were maintained twice.
#'
#' The second aggregation exists only to measure that ambiguity: it rejoins the
#' chosen point to every candidate that satisfied the query and reports how
#' many distinct points there were, how many distinct records, and how far the
#' furthest of the points sits from the one returned. Points and records are
#' counted separately because they routinely differ: every unit of a
#' multi-unit building is its own address at the building's one coordinate, so
#' `n_records` can be 19 where `n_points` is 1 -- see [geocode()].
#' @param cand SQL producing the candidate set, with a `row_id` and `x`/`y`
#' @param rank The tier's `ORDER BY` expression
#' @param cols The select list of chosen-row columns, aliased `b`
#' @param id The candidate table's record identifier, counted for `n_records`
#' @param postal The candidate table's postal-code column
#' @return A single SQL string
#' @keywords internal
nar_geocode_best_sql <- function(cand, rank, cols, id, postal) {
  sprintf("
    WITH cand AS (
      %1$s
    ),
    best AS (
      SELECT * FROM cand
      QUALIFY %2$s = 1
    )
    SELECT %3$s,
           count(DISTINCT c.x::VARCHAR || ',' || c.y::VARCHAR) AS n_points,
           count(DISTINCT c.%4$s) AS n_records,
           max(sqrt((c.x - b.x)^2 + (c.y - b.y)^2)) AS spread_m,
           %5$s
      FROM best b
      JOIN cand c USING (row_id)
     GROUP BY ALL",
          cand, nar_geocode_rank_sql(rank), cols, id,
          nar_geocode_postal_sql(paste0("c.", postal)))
}

#' Return every candidate instead of one, in the order the tier ranks them
#'
#' @description The other reading of the same candidate set
#' [nar_geocode_best_sql()] collapses. No aggregation and no `QUALIFY`: the
#' rank becomes a column rather than a filter, so row 1 of each `row_id` is the
#' row the collapsing query would have kept.
#' @param cand SQL producing the candidate set, with a `row_id`
#' @param rank The tier's `ORDER BY` expression
#' @return A single SQL string
#' @keywords internal
nar_geocode_ranked_sql <- function(cand, rank) {
  sprintf("
    WITH cand AS (
      %1$s
    )
    SELECT *, %2$s AS match_rank
      FROM cand
     ORDER BY row_id, match_rank", cand, nar_geocode_rank_sql(rank))
}

#' The exact-match geocoding query
#'
#' @description Kept as its own function, like [nar_gazetteer_sql()], so the SQL
#' can be read and tested without a database. The candidate set it collapses is
#' the same one [nar_geocode_matches_sql()] enumerates.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A single SQL string
#' @keywords internal
nar_geocode_exact_sql <- function(probe, bounds = "") {
  nar_geocode_best_sql(
    nar_geocode_unit_filter(nar_geocode_candidates(
      probe,
      paste("p.row_id, a.ADDR_GUID, a.geom_source, a.x, a.y, a.MAIL_POSTAL_CODE,",
            nar_geocode_unit_hit("a.APT_NO_LABEL")),
      nar_geocode_civic_key(), bounds)),
    nar_geocode_nar_rank(),
    "b.row_id, b.ADDR_GUID, b.geom_source, b.x, b.y",
    "ADDR_GUID", "MAIL_POSTAL_CODE")
}

#' The columns [geocode_matches()] reports for each NAR record
#'
#' @description Chosen to answer the question the function exists for -- why
#' are these separate records, and does the difference matter. `APT_NO_LABEL`,
#' `MAIL_POSTAL_CODE`, `MAIL_MUN_NAME` and `BU_USE` are what actually
#' distinguish the units of one building; `LOC_GUID` is what shows they *are*
#' one building; both street-name families are carried because either may be
#' the one that matched.
#' @return A character vector of `Addresses` column names
#' @keywords internal
nar_geocode_match_cols <- function() {
  c("ADDR_GUID", "LOC_GUID", "APT_NO_LABEL", "CIVIC_NO", "CIVIC_NO_SUFFIX",
    "OFFICIAL_STREET_NAME", "MAIL_STREET_NAME", "MAIL_MUN_NAME",
    "CSD_ENG_NAME", "MAIL_PROV_ABVN", "MAIL_POSTAL_CODE", "BU_USE",
    "geom_source")
}

#' The query behind [geocode_matches()]
#'
#' @description Every NAR record that satisfied the query, ranked. The
#' candidate set is built by the same [nar_geocode_candidates()] and
#' [nar_geocode_civic_key()] the exact tier uses, so the two cannot disagree
#' about what matched -- only about how much of it to report.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A single SQL string
#' @keywords internal
nar_geocode_matches_sql <- function(probe, bounds = "") {
  cols <- paste0("a.", nar_geocode_match_cols(), collapse = ", ")
  nar_geocode_ranked_sql(
    nar_geocode_unit_filter(nar_geocode_candidates(
      probe,
      paste0("p.row_id, ", cols, ", a.x, a.y, ",
             nar_geocode_unit_hit("a.APT_NO_LABEL")),
      nar_geocode_civic_key(), bounds)),
    nar_geocode_nar_rank())
}

#' The postal code of the record that was matched
#'
#' @description An aggregate over the *candidate* set rather than a column read
#' off the row that was returned, and that is the whole point of it. NAR carries
#' one row per address, so a civic number with units contributes many rows to
#' `cand`; the tier picks one of them for its coordinates, and picking one of
#' them for a postal code as well would be a coin flip wherever the units of a
#' building do not share one. They usually do -- 98.6% of civic numbers in NAR
#' carry a single postal code -- but the 1.4% that do not are 4.2% of addresses,
#' since a building large enough to split across postal codes is large.
#'
#' So the value is reported only when every candidate agrees, and is `NULL`
#' otherwise. The empty-string fold makes a missing postal code participate in
#' that agreement rather than being skipped by `count(DISTINCT)`: a set that is
#' half `NULL` reports nothing, not the half that had a value.
#'
#' It is an aggregate over whatever `cand` holds by the time it runs, so
#' [nar_geocode_unit_filter()] having narrowed the set to one unit is what turns
#' a declined postal code into a reported one -- 55 of 5,000 corpus filings.
#' @param col The postal-code column, qualified with the candidate alias
#' @return A SQL fragment, aliased `match_postal_code`
#' @keywords internal
nar_geocode_postal_sql <- function(col) {
  sprintf("CASE WHEN count(DISTINCT coalesce(%1$s, '')) = 1
                THEN nullif(min(coalesce(%1$s, '')), '') END AS match_postal_code",
          col)
}

#' The interpolation query
#'
#' @description Finds the nearest known civic number below and above the one
#' asked for, **on the same side of the street**, and places the address on the
#' straight line between them in proportion to where its number falls in the
#' numbering.
#'
#' Three things in here are load-bearing:
#'
#' * `(a.CIVIC_NO %% 2) = (p.civic %% 2)` is the same-side restriction, and it
#'   is what makes this accurate: 4.2 m median error against 35.2 m when both
#'   sides are pooled.
#' * Candidates are restricted to `geom_source = 'building'`. Interpolating
#'   between two blockface centroids would compound a 176 m error at each end.
#' * The final `WHERE lo_n IS NOT NULL AND hi_n IS NOT NULL` is the refusal to
#'   extrapolate. Both flanks are required; a number past the end of the run
#'   returns nothing at all.
#'
#' Duplicate civic numbers are averaged first. NAR carries one row per address
#' rather than per civic number, so a building with units contributes many rows
#' at one point, and `arg_max` over the raw rows would pick an arbitrary one.
#' @param probe Name of the temp table holding the parsed components
#' @param bounds A spatial restriction from [nar_geocode_bounds()], or `""`
#' @return A single SQL string
#' @keywords internal
nar_geocode_interp_sql <- function(probe, bounds = "") {
  sprintf("
    WITH cand AS (
      %s
    ),
    pt AS (
      SELECT row_id, civic, cn, avg(x) AS x, avg(y) AS y
        FROM cand GROUP BY row_id, civic, cn
    ),
    flank AS (
      SELECT row_id, civic,
             max(cn)         FILTER (WHERE cn < civic) AS lo_n,
             arg_max(x, cn)  FILTER (WHERE cn < civic) AS lo_x,
             arg_max(y, cn)  FILTER (WHERE cn < civic) AS lo_y,
             min(cn)         FILTER (WHERE cn > civic) AS hi_n,
             arg_min(x, cn)  FILTER (WHERE cn > civic) AS hi_x,
             arg_min(y, cn)  FILTER (WHERE cn > civic) AS hi_y
        FROM pt
       GROUP BY row_id, civic
    )
    SELECT row_id, lo_n, hi_n,
           lo_x + (hi_x - lo_x) * ((civic - lo_n) / (hi_n - lo_n)::DOUBLE) AS x,
           lo_y + (hi_y - lo_y) * ((civic - lo_n) / (hi_n - lo_n)::DOUBLE) AS y,
           sqrt((hi_x - lo_x)^2 + (hi_y - lo_y)^2) AS span_m
      FROM flank
     WHERE lo_n IS NOT NULL AND hi_n IS NOT NULL",
    nar_geocode_candidates(
      probe,
      "p.row_id, p.civic, a.ADDR_GUID, a.CIVIC_NO AS cn, a.x, a.y",
      "\n         AND a.geom_source = 'building'
         AND a.CIVIC_NO IS NOT NULL
         AND (a.CIVIC_NO % 2) = (p.civic % 2)",
      bounds))
}

#' Pick the gazetteer's tuning arguments out of `geocode()`'s dots
#'
#' @description [geocode()] normalizes the string before it geocodes it, so the
#' gazetteer's own arguments have to reach [normalize_address()] or they are
#' accepted and silently dropped -- which is what happened until this existed,
#' and it made `mun_swap_penalty` look inert from `geocode()` while working
#' perfectly when the same penalty was applied by calling [normalize_address()]
#' first and passing the frame. A measurement taken the first way and a
#' measurement taken the second way then disagree for no visible reason.
#'
#' Only forwarded when `x` is a character vector. A data frame has already been
#' parsed by whoever made it, and re-applying a parse argument to it would be
#' claiming an influence over a decision that was taken elsewhere.
#'
#' Derived from the formals of [nar_resolve_gazetteer()] rather than listed, so
#' the two cannot drift apart. `res` and `con` are supplied here.
#' @param dots `list(...)` as [geocode()] captured it
#' @return The subset of `dots` to forward to [normalize_address()]
#' @keywords internal
nar_gazetteer_dots <- function(dots) {
  if (!length(dots) || is.null(names(dots))) return(list())
  dots[intersect(names(dots),
                 setdiff(names(formals(nar_resolve_gazetteer)), c("res", "con")))]
}

#' Pick the `...` arguments the geolocator tier understands
#'
#' @description `geocode(...)` has to serve two online tiers whose arguments do
#' not overlap. The BC tier keeps receiving all of `...`, because
#' [bc_geocode()] forwards what it does not recognize to its own service as a
#' query parameter and a filter would break that. [nrcan_geocode()] has no such
#' passthrough -- the geolocator takes one query parameter -- so its formals are
#' a closed set and unknown names can be dropped rather than raising an error
#' about an argument meant for the other tier.
#'
#' Derived from the formals rather than listed, so an argument added to
#' [nrcan_geocode()] does not have to be remembered here.
#' @param dots `list(...)` as [nar_geocode_match()] captured it
#' @return The subset of `dots` to forward
#' @keywords internal
nar_nrcan_dots <- function(dots) {
  if (!length(dots) || is.null(names(dots))) return(list())
  # The tier supplies these itself, so a caller-supplied one would be silently
  # overridden or would conflict with an argument geocode() already owns.
  supplied <- c("x", "prov", "geometry", "crs", "con")
  dots[intersect(names(dots), setdiff(names(formals(nrcan_geocode)), supplied))]
}

#' Validate and normalize the `method` argument
#'
#' @description Partial matching, deduplication, and an error naming the tier
#' that was not recognized rather than the whole vocabulary. **Order is
#' preserved**, since order is the priority.
#' @param method What the caller passed
#' @return A character vector of tier names
#' @keywords internal
nar_geocode_methods <- function(method) {
  known <- c("nar", "rqa", "nar_interpolate", "rnf", "bc", "nrcan", "qc")
  if (!length(method) || !is.character(method)) {
    stop("`method` must be one or more of ", paste0('"', known, '"', collapse = ", "),
         ", in the order they should be tried.", call. = FALSE)
  }
  # Exact matches win over prefixes in pmatch, so "nar" is unambiguous even
  # though it prefixes "nar_interpolate".
  i <- pmatch(method, known, nomatch = 0L, duplicates.ok = TRUE)
  if (any(i == 0L)) {
    stop("Unknown geocoding method ",
         paste0('"', method[i == 0L], '"', collapse = ", "), ". `method` takes ",
         paste0('"', known, '"', collapse = ", "), ".", call. = FALSE)
  }
  # A tier offered a second time would see only the rows it already declined,
  # so a duplicate is dropped rather than run twice.
  unique(known[i])
}

#' Recycle an authoritative constraint to one value per input
#'
#' @description Length 1 or length `n` and nothing in between: a partial vector
#' would recycle silently and constrain the wrong rows.
#' @param v The supplied value
#' @param n Number of inputs
#' @param what Argument name, for the error message
#' @return A character vector of length `n`
#' @keywords internal
nar_recycle <- function(v, n, what) {
  if (length(v) == 1) return(rep(as.character(v), n))
  if (length(v) == n) return(as.character(v))
  stop("`", what, "` must be length 1 or length ", n, ", not ", length(v), ".",
       call. = FALSE)
}

#' Turn a spatial restriction into a SQL fragment
#'
#' @description Two clauses, and both are wanted. The bounding box is compared
#' against the `x`/`y` columns, which is the cheap half: those are plain
#' `DOUBLE` columns with per-row-group zonemaps, so DuckDB skips whole row
#' groups whose range cannot satisfy it instead of reading them. `ST_Within`
#' then makes the restriction exact for a genuine polygon. For a rectangle the
#' second clause is nearly redundant, but not quite -- a rectangle in the
#' caller\'s CRS is not a rectangle in the storage CRS -- and it keeps one code
#' path rather than two.
#'
#' The outline is densified before reprojection. Transforming only the corners
#' of a longitude/latitude rectangle into a projected CRS and taking the box of
#' the result clips the bulge along each edge, which would silently drop
#' addresses inside the region the caller asked for.
#' @param within An `sf`/`sfc` object, an `st_bbox`, a length-4 numeric
#' `c(xmin, ymin, xmax, ymax)`, or `NULL`
#' @param crs CRS to interpret `within` in when it carries none
#' @param con A NAR connection, for the storage CRS
#' @return A SQL fragment to append to the join condition, or `""`
#' @keywords internal
nar_geocode_bounds <- function(within, crs, con) {
  nar_geocode_bounds_sql(nar_geocode_bounds_geom(within, crs, con))
}

#' Resolve `within` to a geometry in the storage CRS
#'
#' @description Split out from the SQL so the same restriction can be enforced
#' twice: pushed into the NAR query as a predicate, and applied in R to points
#' that came from somewhere else -- the BC fallback, which is a separate service
#' and cannot be given this package's SQL.
#' @param within An `sf`/`sfc`/`sfg`, an `st_bbox`, or a length-4 numeric
#' @param crs CRS to interpret a bare numeric or an untagged geometry in
#' @param con A NAR connection, for the storage CRS
#' @return An `sfc` in the storage CRS, or `NULL`
#' @keywords internal
nar_geocode_bounds_geom <- function(within, crs, con) {
  if (is.null(within)) return(NULL)
  # crs = NULL asks for output in the storage CRS, and a bare bbox given
  # alongside it is naturally in the same coordinates.
  if (is.null(crs)) crs <- nar_crs(con)

  g <- if (inherits(within, "bbox")) {
    sf::st_as_sfc(within)
  } else if (inherits(within, c("sf", "sfc", "sfg"))) {
    sf::st_geometry(sf::st_as_sf(within))
  } else if (is.numeric(within) && length(within) == 4) {
    sf::st_as_sfc(sf::st_bbox(c(xmin = within[1], ymin = within[2],
                                xmax = within[3], ymax = within[4]),
                              crs = sf::st_crs(crs)))
  } else {
    stop("`within` must be an sf/sfc object, an st_bbox, or a length-4 ",
         "numeric c(xmin, ymin, xmax, ymax).", call. = FALSE)
  }
  if (is.na(sf::st_crs(g))) g <- sf::st_set_crs(g, sf::st_crs(crs))

  # Densified with the CRS temporarily off. st_segmentize() on a geographic
  # geometry measures along the great circle and hands the job to lwgeom, which
  # is not a dependency of this package; with no CRS it interpolates in the
  # plane, which is exactly what is wanted here -- extra vertices along the
  # straight edges as drawn, before those edges are bent by the reprojection.
  bb <- sf::st_bbox(g)
  step <- max(bb[["xmax"]] - bb[["xmin"]], bb[["ymax"]] - bb[["ymin"]]) / 100
  crs_in <- sf::st_crs(g)
  g <- sf::st_set_crs(sf::st_segmentize(sf::st_set_crs(g, NA), step), crs_in)
  g <- sf::st_transform(g, nar_crs(con))
  sf::st_union(g)
}

#' The SQL predicate for a resolved `within` geometry
#'
#' @description The bounding box goes first so the zonemap prefilter on `x`/`y`
#' can skip row groups before the polygon test is evaluated -- the same
#' mechanism [nar_within_radius()] relies on.
#' @param g An `sfc` in the storage CRS, or `NULL`
#' @return A SQL fragment, or `""`
#' @keywords internal
nar_geocode_bounds_sql <- function(g) {
  if (is.null(g)) return("")
  b <- sf::st_bbox(g)
  sprintf("
   AND a.x BETWEEN %.3f AND %.3f AND a.y BETWEEN %.3f AND %.3f
   AND st_within(nar_xy(a.x, a.y), st_geomfromtext('%s'))",
          b[["xmin"]], b[["xmax"]], b[["ymin"]], b[["ymax"]],
          sf::st_as_text(g))
}

#' Does an online service's answer agree with the address that was asked for?
#'
#' @description **The online tiers always answer, and their wrong answers are
#' confident.** NRCan's geolocator returns `1 Rue Notre-Dame Ouest, Montreal, QC`
#' as an `INTERPOLATED_POSITION` on a real Rue Notre-Dame Ouest in Lorrainville,
#' 500 km away, and `330 Spadina Rd, Toronto` as `330 Spadina Avenue`, a
#' different street 3 km off. Neither is imprecision -- both are answers to a
#' different question, and no confidence field distinguishes them.
#'
#' What separates them is that the answer is itself an address, so it can be put
#' back into components and compared to the ones that were sent. That is the
#' whole floor, and it is shared by every online tier: **the answer has to come
#' back as the address that was asked for.** Where the components come from
#' differs -- [nar_nrcan_floors()] re-parses a returned title, [nar_osm_floors()]
#' reads fields the service already separated -- but what is done with them once
#' they exist does not, and a second copy of these rules would drift from this
#' one.
#'
#' The comparison is per component rather than a single [address_key()]
#' equality, because the components are not equally strict:
#'
#' * `CIVIC_NO`, `STREET_NAME` -- must be **present on both sides and equal**.
#'   These are what the query was about; a missing one means nothing was
#'   verified.
#' * `STREET_TYPE`, `STREET_DIR`, `PROV_ABVN` -- rejected only when both sides
#'   are present and **contradict**. An absent one cannot contradict anything.
#' * `MUN_NAME` -- whole-word containment either way, not equality. Both services
#'   return incorporated names, so `TORONTO` comes back as `City Of Toronto`,
#'   `NORTH VANCOUVER` as `District Of North Vancouver` and `CHARLOTTETOWN` as
#'   `City of Charlottetown`. Equality would reject all three.
#'
#' Whole-word matters: comparing the municipality against the *whole* returned
#' string with a plain substring test -- the first thing tried -- passes
#' `28 Silver ST, CORNER BROOK` against `28 Brook Street, Corner Brook`, because
#' `Brook` appears in the street name. Field-wise comparison is what catches it.
#' @param q Parsed components of the address that was sent
#' @param t Parsed components of the answer that came back
#' @return A character vector, `NA` where the answer agrees and otherwise a
#' short reason naming the component that disagreed
#' @keywords internal
nar_address_agreement <- function(q, t) {
  col <- function(d, name) {
    v <- d[[name]]
    v <- if (is.null(v)) rep(NA_character_, nrow(d)) else as.character(v)
    v <- nar_key_fold(v)
    ifelse(is.na(v), "", v)
  }
  # `""` is absent, so a comparison against it is never a contradiction. Every
  # rule below is written in terms of "both present" for that reason.
  contradicts <- function(a, b) nzchar(a) & nzchar(b) & a != b
  missing_or_differs <- function(a, b) !nzchar(a) | !nzchar(b) | a != b

  # Only A-Z, 0-9 and spaces survive nar_key_fold(), so the folded value is
  # already safe to paste into a pattern -- there is no metacharacter left.
  contained <- function(a, b) {
    mapply(function(x, y) {
      grepl(paste0("\\b", x, "\\b"), y) || grepl(paste0("\\b", y, "\\b"), x)
    }, a, b, USE.NAMES = FALSE)
  }

  reason <- rep(NA_character_, nrow(q))
  note <- function(bad, what, a, b) {
    bad <- bad & is.na(reason)
    if (!any(bad)) return(reason)
    reason[bad] <<- sprintf("%s %s != %s", what,
                            ifelse(nzchar(a[bad]), a[bad], "?"),
                            ifelse(nzchar(b[bad]), b[bad], "?"))
    reason
  }

  qn <- col(q, "STREET_NAME");     tn <- col(t, "STREET_NAME")
  qc <- col(q, "CIVIC_NO");        tc <- col(t, "CIVIC_NO")
  qt <- col(q, "STREET_TYPE");     tt <- col(t, "STREET_TYPE")
  qd <- col(q, "STREET_DIR");      td <- col(t, "STREET_DIR")
  qm <- col(q, "MUN_NAME");        tm <- col(t, "MUN_NAME")
  qp <- col(q, "PROV_ABVN");       tp <- col(t, "PROV_ABVN")

  reason <- note(missing_or_differs(qn, tn), "street name", qn, tn)
  reason <- note(missing_or_differs(qc, tc), "civic number", qc, tc)
  reason <- note(contradicts(qt, tt), "street type", qt, tt)
  reason <- note(contradicts(qd, td), "street direction", qd, td)
  reason <- note(nzchar(qm) & nzchar(tm) & !contained(qm, tm),
                 "municipality", qm, tm)
  reason <- note(contradicts(qp, tp), "province", qp, tp)
  reason
}
