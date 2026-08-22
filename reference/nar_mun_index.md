# A hashed index from municipality key to address count

\`match()\` rebuilds its hash on every call, and the parser asks this
question up to six times per candidate parse, so the index is built once
and kept. Keys are stored twice: bare, and qualified by province, so a
province the string named can tighten the lookup without a second table.
Bare keys are assigned in ascending address count, leaving the commonest
place of that name as the value – which is the only reading a tie-break
could sensibly prefer.

## Usage

``` r
nar_mun_index()
```

## Value

An environment mapping key to address count
