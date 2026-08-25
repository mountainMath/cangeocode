# Changelog

## cangeocode 0.3.0

0.2.0 geocoded from one source. This release adds two more offline ones
beside it — Quebec’s own address register and Statistics Canada’s road
network file — and three online geocoders to fall through to. Together
they take the 5,000 Corporations Canada filings the package measures
itself on from 89.1% placed to **94.3%**, and the road network file is
where most of that came from.

**No rebuild is needed.** Both new imports write their own tables and
neither bumps
[`nar_schema_version()`](https://mountainmath.github.io/cangeocode/reference/nar_schema_version.md),
so an existing 0.2.0 database keeps working and gains a tier when you
run the import.

### The road network file

- New
  [`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md)
  loads Statistics Canada’s Road Network File (product 92-500-X) into
  the same DuckDB database, in tables of its own, and adds an offline
  **`"rnf"`** tier. It places a civic number along the street segment
  whose address range contains it, at the position the range implies,
  offset 13 m to the side of the centreline that range belongs to.

- **It is the only tier that reaches streets NAR does not carry at
  all**, which is the largest single component of what
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  fails. On the 5,000-filing draw it takes coverage from **92.4% to
  94.3%** — 93 of the 379 addresses the offline pair leaves unplaced,
  24.5% of the residual, against the `"nrcan"` tier’s 8.1%.

- **The file carries no provenance flag on its address ranges**, so an
  observed range and an imputed one are the same bytes and nothing in it
  says which is which. Every threshold the tier uses therefore rests on
  a measurement against NAR rather than on the file’s own word: 89.7% of
  NAR civic numbers fall inside the range their own side claims, and the
  geometric side agrees with the range’s parity 94.2% of the time
  against 7% for the other side.

- **It refuses when more than one segment matches**, reporting
  `match_method = "rnf_ambiguous"` and the count in `n_matches` rather
  than a coordinate. That is not caution about an unmeasured risk: where
  NAR can check the answer, rows with two same-named segments both
  containing the number run p90 1,678 m with one in eight over a
  kilometre, against p90 108 m and one in a thousand for the rest.
  Refusing costs 7 rows in 5,000. It is not *sufficient*, though — on
  the rows only this tier can place, the three worst are all
  unambiguous, and one of them is a bad parse the tier placed
  faithfully.

- `uncertainty_m` is `max(95, 0.35 × len_m)`, which covers 91.8% of the
  measured error where NAR can check it, and 92.6% on segments over 600
  m where a flat 110 m covers 67.2%. Where both answer, the tier sits a
  median 26.0 m from NAR’s own building point (p90 108 m, 0.2% beyond a
  kilometre).

- **Accuracy on the addresses NAR also has does not transfer to the ones
  it does not.** Checked against the filing’s own postal code — which
  the pathway never reads — the recovered rows sit p50 149 m from their
  urban FSA centroid, against 60 m for this tier on rows NAR also placed
  and 43 m for NAR’s own answer on those same rows. The residual is
  harder by more than twice what the tier’s own error is worth. Treat
  `rnf_interpolated` as the coarse tier it is, and filter on
  `uncertainty_m`.

- Only the **shapefile** is published for every release, so that is what
  [`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md)
  downloads: the GeoPackage resolves for 2025 alone, and it also carries
  13 CircularStrings that DuckDB’s spatial extension refuses in a way
  that fails the whole read.
  [`rnf_latest_release()`](https://mountainmath.github.io/cangeocode/reference/rnf_latest_release.md)
  finds the newest release by probing constructed URLs rather than
  scraping a page that can be redesigned.

- `inst/notes/road-network-file.md` records all of it and
  `data-raw/probe_rnf.R` reproduces it, stage 5 against the shipped
  tier.

### Quebec’s address register

- New
  [`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md)
  loads the *Répertoire québécois des adresses* — the source NAR’s own
  Quebec rows are derived from, published in full and about 750,000
  certified addresses larger than NAR’s Quebec extract — and adds an
  offline **`"rqa"`** tier. It is Quebec-only and deliberately not in
  the default `method`: the tables exist only if the import was run, and
  a tier that appears or disappears depending on that would be worse
  than an explicit one.

- **It is kept beside NAR rather than merged into it.** Merging would
  spend the only instrument Quebec’s coverage is measurable with. The
  tier joins on the *match* fold rather than the plain one, because the
  addresses it exists for are exactly the ones the gazetteer could not
  resolve.

- On 4,000 Quebec filings, `c("nar", "rqa", "nar_interpolate")` places
  90.1% against 88.5% for the offline pair — but **the placement figure
  is not the result**. The share landing on a *register* point goes from
  82.7% to 89.1%, because 196 of the 258 rows the tier answers were
  already being interpolated between two neighbours and it replaces that
  guess with the register’s own coordinate, a median 26 m away. It
  belongs below `"nar"` and above `"nar_interpolate"` for that reason,
  and it costs nothing measurable.

- `match_method` carries the register’s own positional class —
  `rqa_building`, `rqa_geocoded`, `rqa_uncertain`, `rqa_lot`,
  `rqa_other` — and `uncertainty_m` is filled in only for
  `rqa_building`, where `0` means what it means for NAR. Nothing here
  has measured what *Géocodée* or *Incertaine* are worth on the ground,
  and an invented figure would be indistinguishable from the two that
  were measured.

- [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  runs a **second gazetteer pass** over the register where NAR left a
  Quebec row unresolved. It is worth 4 rows in 942, not the six points
  that were projected, and the reason that projection was wrong is now
  the standing warning in `inst/notes/quebec-addresses.md`: a coverage
  share measured over NAR’s residual is not a coverage share of the
  parser’s residual.

- New
  [`rqa_attribution()`](https://mountainmath.github.io/cangeocode/reference/rqa_attribution.md)
  returns the attribution the register’s CC-BY 4.0 licence requires,
  which is a different licence from everything else here.

### Online geocoders

- New
  [`nrcan_geocode()`](https://mountainmath.github.io/cangeocode/reference/nrcan_geocode.md)
  and the **`"nrcan"`** tier bind NRCan’s national geolocator. Keyless,
  national, and needing no local database, which is the whole reason to
  want it: it is the only tier that can answer before NAR has been
  downloaded, and the only one that covers provinces a partial import
  does not hold. It does **not** reverse geocode — the alternatives were
  probed rather than assumed.

  **It always answers, and it answers plausibly**, which is harder to
  defend against than an error. `1 Rue Notre-Dame Ouest, Montreal, QC`
  comes back as a real interpolated position on a real Rue Notre-Dame
  Ouest 500 km away, with nothing marking it as a substitution. Two
  floors decide: the result type, and agreement on the parsed
  components. Component agreement is a strict improvement over comparing
  the returned title as a string — it removes 27 answers a substring
  floor keeps, median 1,615 m off, and recovers 7 it rejects. Rejected
  rows keep a `nrcan_reject` column saying why.

  The whole result list is put through those floors rather than just the
  top result, the civic-number suffix is dropped from the query (the
  service’s own house-number regex cannot match `990A`, so a suffixed
  civic never reached its interpolator), and the roughly one request in
  twelve the service drops as a clean HTTP 500 is retried — worth about
  8 points of coverage that earlier measurements silently charged to the
  geolocator.

  As a fallback for NAR’s tail it is worth little — 8.1% of the unplaced
  — because the addresses NAR cannot place are largely the ones no
  national compilation has.

- New
  [`qc_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_geocode.md),
  [`qc_reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_reverse_geocode.md),
  [`qc_validate()`](https://mountainmath.github.io/cangeocode/reference/qc_validate.md)
  and the **`"qc"`** tier bind the MRNF’s geocoder over the same Quebec
  register. **How the query is spelled decides whether the service works
  at all**, which is the single largest effect measured anywhere in this
  package: rendering NAR’s own `NOTRE-DAME RUE O` matches 31.5% of the
  time, and spelling it French-canonical as `Rue Notre-Dame Ouest`
  matches 95.5%. The failure is silent — the wrong spelling returns a
  street centroid scoring *higher* than the correct civic point.

  Its `Score` carries no positional information (Spearman 0.018 against
  distance from NAR’s point), so `min_score` defaults to 0 and should be
  left there.
  [`qc_reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_reverse_geocode.md)
  is the one online reverse geocoder in the package.

  It agrees with NAR to within a metre, and **that is shared lineage
  rather than accuracy** — it serves the register NAR’s Quebec rows are
  built from. It cannot settle NAR’s Quebec accuracy, and
  [`qc_validate()`](https://mountainmath.github.io/cangeocode/reference/qc_validate.md)
  says so in its own documentation.

- New
  [`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
  binds the Nominatim instance the Government of Canada hosts, not the
  volunteer-funded one whose usage policy forbids bulk geocoding. **It
  is exported and deliberately not a tier, and the reason is the licence
  rather than the accuracy**: OSM data is ODbL where NAR, the BC
  geocoder and the geolocator are all Open Government Licence, and a
  default tier would fold a handful of ODbL rows into a result table and
  change what the caller may do with the whole of it, silently. The
  licence string rides along on every row as `osm_licence`. Its coverage
  has not been measured at scale, so `uncertainty_m` is `NA` rather than
  a plausible constant.

- [`bc_geocode()`](https://mountainmath.github.io/cangeocode/reference/bc_geocode.md)
  now reports **`bc_descriptor`** and **`bc_accuracy`** — which
  reference point BC actually returned, and its own categorical accuracy
  class. Asking is not getting: of the six `locationDescriptor` values,
  only `accessPoint` and `routingPoint` are distinct requests.
  `frontDoorPoint`, `rooftopPoint` and `parcelPoint` each returned a
  point identical to the default on **100%** of 400 sampled addresses,
  because the service answers with whatever main location it holds
  rather than looking for the kind of point named.

  Measured against NAR’s building points, **the existing default is
  already the closest match** — p50 20.2 m, against 28.9 m for
  `accessPoint` and 31.6 m for `routingPoint` — so nothing about what
  the package requests changed. That refutes the standing hypothesis
  that NAR’s “may be the road access point” hedge meant `accessPoint`
  was the right thing to ask for. Per address, though, the default wins
  only 58% of the time: NAR’s BC points are a mixture of definitions,
  not one of them. `data-raw/probe_bc.R` reproduces it.

### Choosing which errors to make

The two mistakes a geocoder makes are not symmetric and not
interchangeable. A false negative is *visible* – the row comes back with
`is.na(lon)`, and you can count it. A false positive is invisible by
construction: a point that looks exactly like every other point, and is
118 km from the address. So the defaults here stay conservative where
the answer is decided and generous where it is reported, and these two
additions are both about the reporting.

- **New
  [`geocode_accept()`](https://mountainmath.github.io/cangeocode/reference/geocode_accept.md)**
  applies your own bar to a result that already exists: `method`,
  `refused`, `attested_only`, `unambiguous`, `postal_code`,
  `max_uncertainty` and `min_confidence`, each off by default. A row
  that fails one loses its coordinates and keeps everything else, with
  `rejected_for` naming the test – so the count of what a bar cost, and
  the evidence for each rejection, both survive. It re-runs nothing,
  which is the point: forty thousand addresses take minutes to resolve
  and finding the right bar takes several tries.

- `attested_only` reads its classification off
  [`nar_remap_uncertainty_m()`](https://mountainmath.github.io/cangeocode/reference/nar_remap_uncertainty_m.md)
  rather than naming the evidence classes again, so the bar and the
  uncertainty floor cannot drift apart.

- **New `keep_refused = TRUE`**, passed through
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  or
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  to the gazetteer. A match scoring below the combined threshold is
  normally dropped, and the row comes back unresolved – which from the
  outside is indistinguishable from the street not existing. With this
  the match is adopted anyway, `confidence` carries the sub-threshold
  score, `mun_evidence` the class, and a new `refused_for` column names
  the gate: `"mun_swap"` where the score cleared the threshold before
  the municipality-swap multiplier and not after, `"score"` otherwise.
  Pair it with `geocode_accept(refused = FALSE)` to take one pass with
  and one without.

- `"mun_swap"` is the one refusal a caller can act on with evidence the
  package does not have: it says the street matched and the municipality
  did not. Rows that failed the *name* gate cannot be reported this way
  – that gate is applied inside the query, so a name too far from every
  candidate never comes back at all.

### Structured input, and two kinds of city

- **Breaking: `prov` and `mun` are gone**, absorbed into a single
  `known` argument on
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md),
  [`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md),
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md),
  [`address_pattern()`](https://mountainmath.github.io/cangeocode/reference/address_pattern.md),
  [`address_key()`](https://mountainmath.github.io/cangeocode/reference/address_key.md),
  [`format_address()`](https://mountainmath.github.io/cangeocode/reference/format_address.md)
  and the online geocoders. `known` is a named list of components the
  caller already has – `geocode(x, known = list(PROV_ABVN = "BC"))`
  where 0.2.0 said `geocode(x, prov = "BC")`. Each component is still
  authoritative: it overrides what the string parsed to, constrains the
  search, and lands on the returned row.

- **It takes the whole address, not just the two constraints.**
  `APT_NO_LABEL`, `CIVIC_NO`, `CIVIC_NO_SUFFIX`, `STREET_NAME`,
  `STREET_TYPE`, `STREET_DIR`, `MUN_NAME`, `CSD_NAME`, `PROV_ABVN` and
  `POSTAL_CODE`, keyed by the names the *output* uses, so an assessment
  roll that carries the community and the postal code in columns of
  their own reaches the parser with its structure intact instead of
  being concatenated into a string for the parser to take apart again. A
  key that is not one of the ten is an **error**, not a silently dropped
  constraint.

- **The two municipality grains are now separable.** `MUN_NAME` is the
  mailing city, compared straight at NAR’s `MAIL_MUN_NAME`; `CSD_NAME`
  is the census subdivision, resolved through NAR’s alias set.
  `CSD_NAME = "Toronto"` reaches an address NAR files under
  `SCARBOROUGH` and `MUN_NAME = "Toronto"` does not. They do not nest,
  so supplying both narrows to one community inside an amalgamated city.
  Before this the choice was made by provenance – a supplied
  municipality went through the alias set and a parsed one did not –
  which is the right default for each but gave the caller no way to ask
  the other question.

- **`CSD_NAME` is also an output column now**, on
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  and
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  alike, so a result says which census subdivision it landed in rather
  than reporting only a mailing city that an administrative search
  produced. As an output it is a *report* and not a constraint – the
  search was never restricted to it – which is why handing a parse back
  to
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  answers exactly as the original string did.

- A municipality supplied through `known` reports
  `mun_evidence = "kept"` and `mun_remapped = FALSE`: the caller settled
  what the swap penalty exists to arbitrate, so those rows take no
  uncertainty floor for a substitution that never happened.

### Forward geocoding

- **New
  [`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)**
  returns the NAR records behind a
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  answer, one row each, in the order the tier ranks them – so
  `match_rank == 1` is the record
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  answered with, and the rest are what it collapsed. It reports
  `APT_NO_LABEL`, `LOC_GUID`, both street-name families, the mailing
  municipality, the CSD, the postal code and `BU_USE`, which is what
  makes it possible to see *why* the records are separate and whether
  the difference matters.

- [`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
  is the exact NAR tier only, and takes no `method` argument, because no
  other tier has a candidate set to enumerate: interpolation and `"rnf"`
  resolve to no record, and the online services return an answer rather
  than a set. An address only those tiers could place therefore has no
  matches, which is the correct answer rather than a gap. Its result is
  not aligned with the input – an address that matched nothing
  contributes no rows – and `input_id` indexes back.

- Internally the two queries are now one query read two ways. The
  candidate set, the civic-number key and the ranking expression are
  each defined once and used by both, so
  [`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
  cannot enumerate a different search than
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  answered. The same shared shape absorbed Quebec’s `"rqa"` tier, which
  had its own copy of the pick-one-then-measure-the-set SQL.

- **[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  reports how many records matched**, in a new `n_records` column beside
  `n_matches`. They count different things: `n_matches` counts distinct
  *points* and is the ambiguity measure that widens `uncertainty_m`;
  `n_records` counts distinct *NAR addresses*. A building with units is
  one point and many addresses — `49321 Range Road 72` in Brazeau County
  is `n_matches = 1` and `n_records = 19` — and **47% of the addresses
  NAR places share their coordinate with at least one other**, so the
  old column was silently reporting a large class of collapsed answers
  as unambiguous. It is spatially unambiguous, which is what `n_matches`
  said and all it said. It is 0 for every tier that placed a row without
  resolving it to a record.

- **[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  returns the matched record’s postal code**, in a new
  `match_postal_code` column. The existing `POSTAL_CODE` is unchanged
  and still means what it always meant — the postal code the *input
  string* carried, which is `NA` for an address typed without one. The
  two sit beside each other because they answer different questions:
  what was said, and what was found.

- Only the tiers that resolve to a record fill it — `nar` (including
  `nar_no_geometry`, since an address NAR holds without coordinates
  still has a postal code) and `rqa`. It then survives whichever tier
  ends up placing the row, as `ADDR_GUID` already did, so an
  interpolated row carries one when the exact tier found the record
  first. Everything else leaves it `NA` rather than copying a
  neighbour’s.

- It is also `NA` when the candidates disagree. NAR holds one row per
  address, so a civic number with units contributes many, and 1.4% of
  civic numbers — 4.2% of addresses, since the buildings this happens to
  are large — span more than one postal code. Where the input names no
  unit, nothing in the query says which of them was meant.
  `100 Queen St W, Toronto` is one of them, and a postal code in the
  input does not break the tie: that is what the address claims, not
  something the lookup established. Naming the unit does break it, by
  narrowing the candidates rather than choosing among them — worth 55 of
  the 5,000 filings.

- **A unit in the address now narrows the records it matches.**
  `49321 Range Road 72` resolves to 19 NAR records;
  `49321 Range Road 72, Unit 9` resolves to one. Where the parse
  produced an `APT_NO_LABEL` and NAR carries that unit at that civic
  number, the candidate set is cut to it, which is what `n_records` and
  `match_postal_code` are then computed over. Across the 5,000-filing
  draw the matched records fall from 118,937 to 25,955, and the
  addresses reporting more than one record from 1,422 to 578.

- **The narrowing narrows or it does nothing.** A unit NAR has no row
  for at that civic number is dropped rather than enforced, so no
  address is made unplaceable by a unit label. This is not caution:
  **27.5% of the units these filings supply are not in NAR at the civic
  number they were written against**, and an unconditional filter would
  take 327 of 5,000 addresses from placed to unplaced. Where the unit is
  there the narrowing is total — all 862 such inputs collapse to exactly
  one record.

- The comparison is folded asymmetrically, on purpose. `Basement`,
  `Sous-sol`, `Upper` and `Lower` are translated into the
  `BSMT`/`UPPR`/`LWR` that NAR actually stores, on the *input* side only
  — of NAR’s 5.96M units, `BASEMENT` appears zero times against 137,413
  `BSMT`. Zero padding was measured and left alone: 0.20% of units carry
  an interior leading zero, almost all `PH01`-style penthouses, and
  normalizing it would mean holding an opinion about every deliberately
  padded label. Quebec’s `"rqa"` tier narrows through the same code,
  over the 1.67M of its 5.32M rows that carry a unit.

- **[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  reports `mun_remapped` and `mun_evidence`, and `uncertainty_m` is
  floored at 118 m on a remap nothing attests.** A remapped row is one
  the gazetteer answered under a different community of the same census
  subdivision, and
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  then searches only the community it chose – so `n_matches == 1` there
  is manufactured by the same step that chose the place, and
  `uncertainty_m` was reporting 0 m. `mun_evidence` records *how* the
  swap was attested – `kept`, `copostal` (the two names share a full
  postal code in NAR), `csd` (an amalgamation or legacy name, read off
  `CSD_ENG_NAME`), `inferred`, `untestable`, `unattested` – and the
  floor is keyed on it. **An attested swap is not floored at all**,
  because measured against Nova Scotia’s independent PVSC points the
  attested classes land at p90 52 m over 1,723 rows, *below* the 57 m of
  rows whose municipality was never touched: a remap a postal code or a
  census subdivision vouches for is as good as no remap. The unattested
  classes pool to 118 m, which is the floor. It describes the bulk and
  not the tail: they run 1.6–1.8% past 5 km against 0.05% for untouched
  rows, which no single metre value can express, so `mun_remapped` is
  still the column to filter on when a kilometre-scale error is
  unacceptable.

- **[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  now forwards gazetteer arguments to the parse it runs itself.**
  `geocode(x, mun_swap_penalty = 1)` on a character vector silently
  parsed at the default and dropped the argument, while the same
  argument given to
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  first was honoured – a wrong answer with no warning.

### Fixes

- **[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  no longer errors on a batch it cannot look anything up in.**
  `geocode("49321, BRAZEAU COUNTY, AB")` — a civic number and a
  municipality with no street between them — raised
  `arguments imply differing number of rows` instead of reporting
  `none`. The probe drops rows with no street name, and its
  unconstrained columns were length-one literals, which do not recycle
  down to zero rows. Any input where *no* row parsed to both a street
  and a civic number hit it.

- **[`nar_match_fold()`](https://mountainmath.github.io/cangeocode/reference/nar_match_fold.md)
  answers nothing to nothing.** It pads with `paste0(" ", x, " ")`, and
  [`paste0()`](https://rdrr.io/r/base/paste.html) given a zero-length
  argument returns one element rather than none, so an empty query
  folded to a one-row vector. That was the other half of the error
  above.

- **An empty input is answered with an empty result.**
  `geocode(character(0))` and `normalize_address(character(0))` both
  errored; they now return zero rows with the usual columns. Geocoding a
  vector that a filter emptied is a normal thing to do.

### The NAR connection

- **New
  [`open_nar()`](https://mountainmath.github.io/cangeocode/reference/open_nar.md)
  and
  [`close_nar()`](https://mountainmath.github.io/cangeocode/reference/close_nar.md)**,
  and
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  and
  [`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
  now keep the connection they open. Passing `con` is no longer needed,
  and no longer costs anything to omit: the first call opens the
  database and every call after it reuses the same handle. On the real
  ~5 GB database that is 1.49 s for the first call and 0.38 s for the
  rest, against ~0.9 s every time before.
  [`close_nar()`](https://mountainmath.github.io/cangeocode/reference/close_nar.md)
  ends it; an import closes it for you and says so, since a writer
  cannot open a database a reader holds.

### Address normalization

- **The gazetteer compares on a match fold**, which spells `ST` out to
  `SAINT` and turns the hyphen into a word boundary. Quebec was failing
  at the door without it: the Part B join rate for Québec went from
  68.2% to 75.5%. The R and SQL halves of that fold must stay identical
  or matching silently degrades.

- **The parser produces candidate readings and evidence chooses between
  them**, rather than committing to the first rule that fires. An
  alternative reading is generated only when the baseline is
  *demonstrably* broken, because the gazetteer scores a
  municipality-restricted match higher by construction and so cannot
  arbitrate a bad candidate away.

- **A prose prefix is cut off the front before anything else reads the
  string.** Every civic-number rule anchors on a number at the front, so
  `Located at 123 Main St` defeated all of them; each of the four guards
  on the strip is holding back a real address form.

- **A comma-free string is segmented on the municipality inventory** — a
  trailing run longer than the municipality claimed that also names one.
  This and the prefix strip came out of benchmarking the strongest
  off-the-shelf neural address tagger against this parser, and each
  reversed one of the two results the tagger still led on.

- Part B — 5,000 registered offices nobody cleaned — now extracts a
  civic number and street name from 98.9% and joins a real NAR address
  for 88.8%.

- **[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  gains `mun_remapped`**, and the gazetteer now fines a municipality
  swap it cannot corroborate. `MunAlias` restricts candidates at *census
  subdivision* grain, so `MILFORD, NS` was admitting all 166 communities
  of Halifax Regional Municipality over 127 km, and a near-miss on the
  street name could beat the right answer inside a set that wide. The
  new penalty is keyed on signals read out of NAR rather than an assumed
  alias list: two mailing municipality names appearing on the same
  *full* six-character postal code, and the census subdivision the
  street already sits in, which is what carries amalgamations and legacy
  names (`Bathurst St, Toronto` reaches a street NAR still mails to
  `NORTH YORK`, and no postal code will ever attest that pair). Measured
  against Nova Scotia’s independent PVSC points, that separates attested
  from unattested swaps by two orders of magnitude (p95 121 m vs 12,028
  m), and the penalty cuts exact-match errors past 5 km from 98 to 42
  for 373 lost matches in 32,886. Swaps are fined, not forbidden –
  refusing the class outright costs 928 matches, 85% of them within 100
  m of the right point.
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  also returns `mun_evidence`, which names the arm that decided.

### Documentation

Five new notes ship with the package, each recording what was measured
rather than what was designed:

- `road-network-file.md` — the file measured against NAR, which is how
  the missing provenance flag got replaced by a number.
- `quebec-addresses.md` — NAR’s Quebec rows measured against the
  register they come from, over 2.5 million paired addresses.
- `nrcan-geolocator.md` — what the geolocator does on the other end of
  the wire, read from its own source.
- `deepparse.md` — the neural tagger measured against this parser on
  four corpora, two of which the parser was never tuned on. Neither a
  fine-tune nor a from-scratch model is warranted on the evidence.
- `nar-consistency.md` — finding NAR’s misplaced addresses using nothing
  but NAR, and the 653 rows where the coordinate rather than the postal
  code is the part to disbelieve. Nothing is repaired and no tier reads
  it.

Read them with `system.file("notes", "<name>", package = "cangeocode")`.

**A new vignette per data source** is where those measurements reach a
user. Each one covers what that source adds to the package, the licence
it comes with, and what to watch out for when using it.

- **[`vignette("data-sources")`](https://mountainmath.github.io/cangeocode/articles/data-sources.md)**
  is the parent, and the place to start. How the seven sources relate,
  what each layer of the tier chain is worth, offline against online,
  why the licence column decides the tier column, and which source can
  be trusted to check another — only BC, and only in BC, because
  Quebec’s register is where NAR’s Quebec rows come from.
- **[`vignette("source-nar")`](https://mountainmath.github.io/cangeocode/articles/source-nar.md)**
  — what reads NAR, and its limits one at a time, each with a live
  example and the package’s answer: the two kinds of positional point
  and why their distances are not comparable, the addresses whose
  coordinate contradicts their own postal code, the ones NAR does not
  carry, ambiguity that is a property of the question rather than the
  file, and a closing remark on the complex relationship between a
  municipality and a postal city.
- **[`vignette("source-rqa")`](https://mountainmath.github.io/cangeocode/articles/source-rqa.md)**
  — Quebec’s register, why it is kept beside NAR rather than merged into
  it, and the standing warning against reading a parser gain off NAR’s
  residual.
- **[`vignette("source-rnf")`](https://mountainmath.github.io/cangeocode/articles/source-rnf.md)**
  — the road network file, the refusal that carries the tier’s quality,
  and why accuracy measured where a source overlaps NAR is an upper
  bound on how it behaves where NAR is silent.
- **[`vignette("source-bc")`](https://mountainmath.github.io/cangeocode/articles/source-bc.md)**
  — the BC Address Geocoder, the always-answers trap, and which of the
  six `locationDescriptor` reference points matches NAR best.
- **[`vignette("source-nrcan")`](https://mountainmath.github.io/cangeocode/articles/source-nrcan.md)**
  — the national geolocator, and what its `INTERPOLATED_POSITION` does
  and does not certify.
- **[`vignette("source-qc")`](https://mountainmath.github.io/cangeocode/articles/source-qc.md)**
  — Quebec’s online locator, why the query has to be spelled
  French-canonical, and why its `Score` is not a precision ranking.
- **[`vignette("source-osm")`](https://mountainmath.github.io/cangeocode/articles/source-osm.md)**
  — the one source bound but not wired as a tier, and the ODbL licence
  question that decides it.

## cangeocode 0.2.0

The package went one direction only in 0.1.0: coordinates to addresses.
This release adds the other direction, and with it a second thing the
package now does in its own right — **normalizing** free-text addresses
into structured components, which is what most address work actually
needs. There is also one external geocoder to check the results against.

**Rebuild your database.** The import schema is now version 6. Existing
databases keep working for reverse geocoding, but address normalization
and
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)’s
`mun` argument both need the new gazetteer tables:

``` r

nar_connection(refresh = TRUE)
```

### Downloading only the provinces you need

- [`nar_connection()`](https://mountainmath.github.io/cangeocode/reference/nar_connection.md)
  gained a **`provinces`** argument. The StatCan release is one 1.7 GB
  zip whose members are split by province, and the server honours HTTP
  range requests, so the package can read the archive’s own index for a
  few kilobytes and then fetch only the members a province needs.
  `nar_connection(provinces = "PE")` is 10 MB and about 40 seconds for a
  working Prince Edward Island geocoder; British Columbia is 192 MB,
  Ontario 552 MB, the country 1,666 MB.

- The addresses are the same NAR rows either way, so a partial database
  geocodes its own provinces **exactly as well** as a national one does
  — same `ADDR_GUID`, same coordinates. It simply holds nothing outside
  them.

- Coverage is recorded in the database and checked before anything is
  downloaded. A national database satisfies every request; asking for a
  province a partial database lacks **adds** just that province rather
  than rebuilding; and `refresh = TRUE` rebuilds the coverage a database
  already has rather than silently widening or narrowing it. New
  [`nar_provinces()`](https://mountainmath.github.io/cangeocode/reference/nar_provinces.md)
  reports what a connection holds.

- In an interactive session, a first call that names no provinces now
  asks, showing what each choice actually costs in megabytes.
  Non-interactively it downloads the whole country, as before.

- [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  answers **`match_method = "not_covered"`** for an address that parsed
  to a province the database does not hold. That is deliberately
  distinct from `none`: the address may be perfectly good, and only a
  partial import ever produces it.

### Forward geocoding

- New
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  turns free-text Canadian addresses into coordinates, one row per input
  in input order, carrying the parsed components alongside the result.
  On 5,000 Corporations Canada addresses it places 89.1%, taking 0.9s
  for the whole batch — so batch rather than loop, since the street-name
  join costs about as much for 5 addresses as for 200.

- Every result says **how** it was found, in `match_method`, and what
  that method costs, in `uncertainty_m`. The latter is the
  90th-percentile error the package adds relative to NAR’s own point:
  `0` for a building match, 176 m for a blockface one, half the flanking
  span for an interpolated one. Both non-zero figures are measured
  rather than assumed.

  `uncertainty_m` says nothing about NAR’s *own* error, which is a
  separate quantity — `0` means “this package added nothing”, not “this
  point is exact”.

- Addresses NAR does not carry are placed by interpolation between their
  neighbours, using **only civics of the same parity** (median error 4.2
  m against 35.2 m for both sides pooled, by leave-one-out over all
  10.6M NAR civic points). Interpolation **refuses to extrapolate** past
  the last known civic on a side rather than returning a number that
  looks like the others.

- `prov`, `mun` and `within` constrain the search and are
  **authoritative**: they override whatever the address string said, and
  the override lands on the returned row too. `mun` resolves through
  NAR’s alias set, so `"Toronto"` reaches the addresses NAR files under
  `SCARBOROUGH`.

- `method` names the tiers to try and the order to try them in — any of
  `"nar"`, `"nar_interpolate"` and `"bc"`. Each tier is offered only the
  rows its predecessors left without a position, so the order is the
  priority. The default `c("nar", "nar_interpolate")` is the offline
  pair.

### Address normalization

This is a step inside
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
and also an end in its own right: matching two address lists to each
other needs the parse and never needs a coordinate.

- New
  [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  parses address strings into the components NAR is keyed on, by rules
  first and then against a NAR gazetteer. New `Streets`, `MunAlias` and
  `PostalMun` tables are built at import time to support it. Measured on
  5,000 real filings nobody cleaned, it extracts a civic number and
  street name from 98.8% and resolves 86.5% to an address NAR actually
  holds.

- Supplying `con` resolves the parse against NAR’s streets, which
  corrects misspellings no rule could reach —
  `29 HPCKING AVE, SAULT STE. MARIE` comes back as `Hocking` — and
  restores NAR’s own spelling, accents and periods included.
  `parse_source` reports which rows cleared the gazetteer and which are
  the parser’s unconfirmed reading.

- The gazetteer **answers with a municipality when NAR determines one**
  — that is, when exactly one municipality in the country carries a
  street of that name. Where two or more do, `MUN_NAME` stays `NA`
  rather than naming the largest, because that would be a guess and a
  wrong municipality joins two different buildings. Together with a name
  match that now recognises a single-keystroke typo and a word the
  parser swallowed (`772` for `Route 772`), this recovers 215 more
  fields per 5,000 rendered NAR addresses and loses none.

- Canonicalization is **conditioned on the province**, because there is
  no single right abbreviation in Canada: NAR writes `AVE` in Ontario
  and `AV` in Quebec, `W` against `O`. `prov` therefore chooses a
  vocabulary rather than offering a hint.

- New
  [`address_pattern()`](https://mountainmath.github.io/cangeocode/reference/address_pattern.md)
  sorts a parse into one of twelve shapes. Two of them, `po_box` and
  `rural_route`, exist to say *this will never resolve*: NAR contains
  neither, so they separate “this address is wrong” from “this address
  was never going to be in the gazetteer”.

- New
  [`address_key()`](https://mountainmath.github.io/cangeocode/reference/address_key.md)
  collapses a parse into a single string that two spellings of the same
  address share, which is what a join or a deduplication needs.
  Components are folded past case, accents and the punctuation NAR and
  the parser disagree on, so `St. John's` and `SAINT JOHNS` key alike.
  The unit is left out by default, keying a building rather than a
  tenant; `unit = TRUE` keys the tenant. A row with no street name keys
  to `NA` rather than to an empty string, so unparseable rows cannot all
  join to each other.

- New
  [`format_address()`](https://mountainmath.github.io/cangeocode/reference/format_address.md)
  writes the components back out as one readable line, with the unit
  hyphenated onto the civic number and the postal code spaced. The
  street type is placed by language rather than by province, so a `Rue`
  in Ottawa still reads correctly. Output parses back to the same
  [`address_key()`](https://mountainmath.github.io/cangeocode/reference/address_key.md),
  so a cleaned column still joins to the column it was cleaned from.

### British Columbia

- New
  [`bc_geocode()`](https://mountainmath.github.io/cangeocode/reference/bc_geocode.md)
  binds the Province of BC’s public Address Geocoder. No API key
  required, and BC only.

- New
  [`bc_validate()`](https://mountainmath.github.io/cangeocode/reference/bc_validate.md)
  compares an existing
  [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  result against BC’s answer in metres. This is the only independent
  positional source currently wired up, and it gives the first read on
  NAR’s own error: a median of 19.8 m between a `nar_building` point and
  BC’s parcel point over 224 addresses.

- `geocode(method = c("nar", "nar_interpolate", "bc"))` adds the service
  as a last-resort tier. On 600 BC addresses the NAR pathway gave up on
  76 and the BC tier resolved 75 of them, 31 at address level.

- **A response from this service is not a match.** It always answers —
  garbage input returns the centre of the locality with a low score
  rather than an error — so `match_method` is derived from its precision
  vocabulary and `min_score` rejects what it scored badly. Rejected rows
  keep their `bc_score` and `bc_faults`.

  The `bc_*` `uncertainty_m` figures are the one set of numbers in this
  package that were chosen rather than measured; BC publishes only a
  categorical accuracy. Treat them as a ranking safe to filter on.

- `httr2` is in `Suggests`, and nothing contacts the network unless one
  of these is called by name.

### Documentation

- Two longer notes ship with the package and record what does *not* work
  yet, with the measurements behind each claim:
  `system.file("notes", "geocoding-status.md", package = "cangeocode")`
  and
  `system.file("notes", "address-normalization-status.md", package = "cangeocode")`.

- New
  [`vignette("geocoding")`](https://mountainmath.github.io/cangeocode/articles/geocoding.md)
  and
  [`vignette("address-normalization")`](https://mountainmath.github.io/cangeocode/articles/address-normalization.md),
  one for each of the two things the package does.

## cangeocode 0.1.0

Initial development version:
[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md),
the NAR import into DuckDB, and
[`collect_nar()`](https://mountainmath.github.io/cangeocode/reference/collect_nar.md)
for getting query results back as `sf`.
