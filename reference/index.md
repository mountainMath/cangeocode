# Package index

## Geocoding

Resolve free-text Canadian addresses to coordinates, interpolating the
civic numbers NAR does not carry.
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
answers one row per address;
[`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
opens up the NAR records behind an answer, which is where the units of a
multi-unit building are.
[`geocode_accept()`](https://mountainmath.github.io/cangeocode/reference/geocode_accept.md)
applies your own bar to a result that already exists, withdrawing the
coordinates on the rows that do not clear it.

- [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  : Geocode Canadian addresses to coordinates
- [`geocode_matches()`](https://mountainmath.github.io/cangeocode/reference/geocode_matches.md)
  : Every NAR record behind a geocoding answer
- [`geocode_accept()`](https://mountainmath.github.io/cangeocode/reference/geocode_accept.md)
  : Withdraw the coordinates a result does not clear your bar

## Address normalization

Parse free-text Canadian addresses into the components NAR is keyed on,
sort them by the shape they parsed as, and put them back together as a
match key or a readable line. Useful on its own for matching and
deduplicating address lists, not only as a step towards a coordinate.

- [`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
  : Normalize Canadian address strings into NAR components
- [`address_pattern()`](https://mountainmath.github.io/cangeocode/reference/address_pattern.md)
  : Sort Canadian address strings into structural buckets
- [`address_key()`](https://mountainmath.github.io/cangeocode/reference/address_key.md)
  : Build a match key from parsed address components
- [`format_address()`](https://mountainmath.github.io/cangeocode/reference/format_address.md)
  : Render parsed address components back into one line

## External geocoders

Online services that place an address without a local database: NRCan’s
national geolocator, the Province of British Columbia’s Address
Geocoder, Quebec’s geocoder over the Repertoire quebecois des adresses,
the two provincial ones also a second source to check a result against,
and the OpenStreetMap geocoder Natural Resources Canada hosts. All make
network requests, and the OpenStreetMap one returns data under a
share-alike licence – read
[`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
before using its results.

- [`nrcan_geocode()`](https://mountainmath.github.io/cangeocode/reference/nrcan_geocode.md)
  : Geocode Canadian addresses with NRCan's geolocator
- [`bc_geocode()`](https://mountainmath.github.io/cangeocode/reference/bc_geocode.md)
  : Geocode British Columbia addresses with the BC Address Geocoder
- [`bc_validate()`](https://mountainmath.github.io/cangeocode/reference/bc_validate.md)
  : Check NAR geocoding results against the BC Address Geocoder
- [`qc_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_geocode.md)
  : Geocode Quebec addresses with the Quebec government geocoder
- [`qc_validate()`](https://mountainmath.github.io/cangeocode/reference/qc_validate.md)
  : Check NAR geocoding results against the Quebec government geocoder
- [`osm_geocode()`](https://mountainmath.github.io/cangeocode/reference/osm_geocode.md)
  : Geocode Canadian addresses with OpenStreetMap data

## Reverse geocoding

Find the addresses nearest a coordinate.
[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
is NAR-backed and local;
[`qc_reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_reverse_geocode.md)
is the one online reverse geocoder here, and it covers Quebec only.

- [`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
  : Reverse Geocode Coordinates to Address
- [`qc_reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/qc_reverse_geocode.md)
  : Reverse geocode Quebec coordinates with the Quebec government
  geocoder

## The NAR database

Download and open a National Address Repository release, and get query
results back as `sf`.
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
and
[`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
open a connection themselves when they are not given one, so these are
for naming a release, querying the database directly, or controlling
when the connection is held.

- [`open_nar()`](https://mountainmath.github.io/cangeocode/reference/open_nar.md)
  : Open a NAR connection for the session to reuse
- [`close_nar()`](https://mountainmath.github.io/cangeocode/reference/close_nar.md)
  : Close the session's NAR connection
- [`nar_connection()`](https://mountainmath.github.io/cangeocode/reference/nar_connection.md)
  : Get NAR data
- [`nar_provinces()`](https://mountainmath.github.io/cangeocode/reference/nar_provinces.md)
  : Provinces a NAR database holds, as a user-facing value
- [`available_nar_versions()`](https://mountainmath.github.io/cangeocode/reference/available_nar_versions.md)
  : Scrape availabe NAR versions from the StatCan website
- [`collect_nar()`](https://mountainmath.github.io/cangeocode/reference/collect_nar.md)
  : Collect a NAR table as an sf object

## The Quebec address register

Import the Repertoire quebecois des adresses into the same database
alongside NAR, in tables of its own. Optional, Quebec only, and what the
`"rqa"` tier of
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
reads. The register is CC-BY 4.0 where the rest of this package’s data
is OGL, so anything published from it has to carry
[`rqa_attribution()`](https://mountainmath.github.io/cangeocode/reference/rqa_attribution.md).

- [`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md)
  : Import Quebec's address register beside NAR
- [`rqa_attribution()`](https://mountainmath.github.io/cangeocode/reference/rqa_attribution.md)
  : Attribution required by the RQA licence

## The road network file

Import Statistics Canada’s Road Network File into the same database
alongside NAR, in tables of its own. Optional, national, and what the
`"rnf"` tier of
[`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
reads: it places a civic number along the street segment whose address
range contains it, which reaches streets NAR does not carry at all.

- [`rnf_import()`](https://mountainmath.github.io/cangeocode/reference/rnf_import.md)
  : Import Statistics Canada's Road Network File beside NAR
