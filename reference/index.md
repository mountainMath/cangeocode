# Package index

## Geocoding

Resolve free-text Canadian addresses to coordinates, interpolating the
civic numbers NAR does not carry.

- [`geocode()`](https://mountainmath.github.io/cangeocode/reference/geocode.md)
  : Geocode Canadian addresses to coordinates

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
national geolocator, and the Province of British Columbia’s Address
Geocoder, the latter also a second source to check a result against.
Both make network requests.

- [`nrcan_geocode()`](https://mountainmath.github.io/cangeocode/reference/nrcan_geocode.md)
  : Geocode Canadian addresses with NRCan's geolocator
- [`bc_geocode()`](https://mountainmath.github.io/cangeocode/reference/bc_geocode.md)
  : Geocode British Columbia addresses with the BC Address Geocoder
- [`bc_validate()`](https://mountainmath.github.io/cangeocode/reference/bc_validate.md)
  : Check NAR geocoding results against the BC Address Geocoder

## Reverse geocoding

Find the addresses nearest a coordinate.

- [`reverse_geocode()`](https://mountainmath.github.io/cangeocode/reference/reverse_geocode.md)
  : Reverse Geocode Coordinates to Address

## The NAR database

Download and open a National Address Repository release, and get query
results back as `sf`.

- [`nar_connection()`](https://mountainmath.github.io/cangeocode/reference/nar_connection.md)
  : Get NAR data
- [`nar_provinces()`](https://mountainmath.github.io/cangeocode/reference/nar_provinces.md)
  : Provinces a NAR database holds, as a user-facing value
- [`available_nar_versions()`](https://mountainmath.github.io/cangeocode/reference/available_nar_versions.md)
  : Scrape availabe NAR versions from the StatCan website
- [`collect_nar()`](https://mountainmath.github.io/cangeocode/reference/collect_nar.md)
  : Collect a NAR table as an sf object
