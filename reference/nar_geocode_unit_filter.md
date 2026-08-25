# Narrow a candidate set to the unit that was asked for, when there is one

A supplied apartment number is what tells the difference between the 19
addresses at \`49321 Range Road 72\`, and without it \`geocode()\`
reports all 19 as the record count of an answer the caller had already
disambiguated.

\*\*It narrows or it does nothing; it never refuses.\*\* The filter
keeps the matching records when there are any and the whole set when
there are none, which is not defensive coding but the measured majority
case: over 5,000 Corporations Canada filings, 1,189 supplied a unit and
matched NAR records, and \*\*27.5 unconditionally would take 327
addresses in 5,000 from placed to unplaced – trading a wrong record
count, which is visible, for a lost coordinate, which is worse. What it
does buy where the unit is there is total: all 862 hits narrow to
exactly one record, from 93,844 candidates between them.

The consequence worth knowing is that \`n_records\` is the report on
this. A unit that was found leaves \`n_records = 1\`; a unit that was
not leaves it at the full count, unchanged from what it would have been
with no unit at all. Nothing else says which happened.

## Usage

``` r
nar_geocode_unit_filter(cand)
```

## Arguments

- cand:

  SQL producing a candidate set with \`row_id\` and \`unit_hit\`

## Value

A SQL fragment producing the narrowed set, without \`unit_hit\`
