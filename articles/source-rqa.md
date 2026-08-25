# Source: the Répertoire québécois des adresses

*One of this package’s seven data sources — one of the two sources that
can be imported beside NAR.
[`vignette("data-sources")`](https://mountainmath.github.io/cangeocode/articles/data-sources.md)
is the overview and puts it in context.*

Quebec’s ministry of natural resources publishes the **Répertoire
québécois des adresses** (RQA), the province’s own address register. It
is where NAR’s Quebec rows come from — a 2.5-million-address point
comparison shows NAR is carrying RQA’s coordinates, not deriving its own
— and it is about **308,000 addresses larger** than what NAR passes on.

[`rqa_import()`](https://mountainmath.github.io/cangeocode/reference/rqa_import.md)
loads it into the same DuckDB file NAR lives in, as its own tables,
which adds the offline `"rqa"` tier for Quebec.

``` r

library(cangeocode)
library(dplyr)

con <- nar_connection()
```

``` r

rqa_import()
```

## What this adds to the package

**Quebec addresses NAR does not carry, and the register’s own coordinate
in place of a guess.**

Both halves matter, and the second is the larger one. On a 4,000-filing
Quebec sample:

| method | placed | placed on a *register* point |
|----|---:|---:|
| `c("nar", "nar_interpolate")` | 88.5% | 82.7% |
| `c("nar", "rqa", "nar_interpolate")` | **90.1%** | **89.1%** |

258 filings get an RQA point. Only 62 of them were unplaced before — the
other 196 were already being *interpolated* between two NAR neighbours,
and the tier replaces that guess with the register’s actual coordinate,
a median 26 m away. The headline is the second column, not the first.

It costs nothing measurable: 10.0 s against 10.1 s on the same batch,
because the tier only ever sees rows NAR left unplaced.

There is a second, smaller contribution on the normalization side.
`RqaStreets` is wired into
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
as a Quebec-only second gazetteer pass over rows the NAR pass could not
settle. On addresses drawn from the *gap population* — what RQA holds
and NAR does not — it answers 8.9% of rows, all correctly. See the
warning below before reading more into that than it says.

## Licence

RQA is **CC-BY 4.0**, where NAR is the Statistics Canada Open Licence.
Both are attribution licences and they compose, which is exactly what
makes RQA usable as a default tier where OpenStreetMap’s ODbL is not.
The obligation is real, and
[`rqa_attribution()`](https://mountainmath.github.io/cangeocode/reference/rqa_attribution.md)
is the string that discharges it:

``` r

rqa_attribution()
#> [1] "Contains information licensed under CC-BY 4.0 from the Repertoire quebecois des adresses, Ministere des Ressources naturelles et des Forets, Gouvernement du Quebec."
```

## Using it

Put `"rqa"` **before** `"nar_interpolate"`. That ordering is the point
of the tier — a register coordinate beats an interpolated one:

``` r

gap <- c("5510 Saint-Jacques, Montreal, QC", "1650 Chabanel, Montreal, QC",
         "431 Courtemanche, Montreal-Est, QC", "45 Gamelin, Gatineau, QC")

geocode(gap, method = c("nar", "nar_interpolate")) |>
  select(input, match_method, uncertainty_m)
#>                                input     match_method uncertainty_m
#> 1   5510 Saint-Jacques, Montreal, QC nar_interpolated      58.76047
#> 2        1650 Chabanel, Montreal, QC             none            NA
#> 3 431 Courtemanche, Montreal-Est, QC             none            NA
#> 4           45 Gamelin, Gatineau, QC nar_interpolated      14.25068

geocode(gap, method = c("nar", "rqa", "nar_interpolate")) |>
  select(match_method, uncertainty_m, lon, lat)
#>   match_method uncertainty_m       lon      lat
#> 1 rqa_geocoded            NA -73.60463 45.46885
#> 2 rqa_building             0 -73.65830 45.52960
#> 3 rqa_building             0 -73.51165 45.63419
#> 4 rqa_geocoded            NA -75.73602 45.44344
```

`match_method` reports the register’s **own positional class** rather
than one label: `rqa_building`, `rqa_geocoded`, `rqa_uncertain`,
`rqa_lot`, `rqa_other`. That is a field RQA carries and NAR does not,
and it is more informative than NAR’s building/blockface pair.

`uncertainty_m` is filled in **only for `rqa_building`**, where `0`
means what it means for NAR: this package added nothing to the
register’s own error. Nothing has measured what `Géocodée` or
`Incertaine` are worth on the ground, and an invented number would be
indistinguishable from the two that were measured.

## What is in the database

``` r

tbl(con, "RqaAddresses") |>
  count(POS_QUALITY, IN_NAR) |>
  arrange(desc(n)) |>
  collect()
#> # A tibble: 13 × 3
#>    POS_QUALITY     IN_NAR       n
#>    <chr>           <lgl>    <dbl>
#>  1 Géocodée        TRUE   1890979
#>  2 Incertaine      TRUE   1522411
#>  3 Bâtiment        TRUE   1332465
#>  4 Géocodée        FALSE   193591
#>  5 Incertaine      FALSE   147425
#>  6 Bâtiment        FALSE    96674
#>  7 Front lot       TRUE     48293
#>  8 Centre lot      TRUE     45985
#>  9 Centre lot      FALSE    27161
#> 10 Front lot       FALSE    10427
#> 11 Site            FALSE       14
#> 12 Accès propriété TRUE         8
#> 13 Accès propriété FALSE        2
```

`POS_QUALITY` is the register’s own positional class, and `IN_NAR` is
whether NAR carries the address too. Reading the `FALSE` rows gives the
gap by quality: it is not concentrated in the worst class — a
substantial share of what NAR is missing carries a `Bâtiment` point.

`RqaStreets` is the companion table, one row per odonyme and
municipality, with `N_NOT_IN_NAR` counting how many of that street’s
addresses NAR has no row for. That is the street-level version of the
same question, and it is what the normalization pass reads.

## What to watch out for

### It sits beside NAR and is not merged into it

Merging the 308,000 missing addresses into `Addresses` would be simpler
to query, and it is deliberately not done. **The comparison against NAR
is the only instrument Quebec’s coverage is measurable with.** Merge the
two and every future question — how much is NAR missing in Quebec, is
that share growing, does a new release close the gap — becomes
unanswerable, because there is no longer a NAR to compare to. The tables
are additive;
[`nar_schema_version()`](https://mountainmath.github.io/cangeocode/reference/nar_schema_version.md)
is deliberately *not* bumped for them, since bumping it would force
everyone to re-download a 5 GB database to gain an optional Quebec
feature.

### `IN_NAR` over-reports by about 14%

`IN_NAR` is **fold equality** on (FSA, civic number, folded street
name), not containment. Containment has no equijoin key and would turn a
scan into a product. The consequence is that the gap it reports is
larger than the real one by roughly 14% — measured independently, not
estimated.

|                    |        rows | distinct address keys |
|--------------------|------------:|----------------------:|
| certified register |   5,315,435 |             3,400,913 |
| **not in NAR**     | **475,294** |           **356,089** |

The whole register is imported, not just the gap, for the same reason:
the gap is a property of the *pair*. A subset built against one NAR
release would be silently wrong against the next, so `IN_NAR` is
computed inside the release’s own file, where it stays correct by
construction.

### Comparing raw street names finds a gap three times too large

NAR keeps the particule inside the street name (`de la Montagne`); RQA
holds it in a column of its own. Compare the raw strings and you get
1.27 million Quebec addresses apparently missing, where the real figure
is 358,000. Every join in `R/rqa.R` goes through a fold that accounts
for this.

The tier joins on the **match fold** — the one that spells `ST` out to
`SAINT` and treats a hyphen as a word boundary — rather than the plain
fold, because the addresses the tier exists for are exactly the ones the
gazetteer could not resolve, and the plain fold is what failed on them.

### Do not read a parser gain off NAR’s residual

This is the standing warning here, and it is recorded because this
repository got it wrong. It was projected that importing RQA would take
Quebec normalization from 81.8% to 88.3%, on the reasoning that 41.3% of
Quebec’s failures are addresses NAR does not carry. Measured after
building it:

|                                         | before | after     |
|-----------------------------------------|--------|-----------|
| Quebec confirmed against NAR            | 77.5%  | 77.5%     |
| Quebec confirmed against NAR **or** RQA | —      | **83.0%** |
| rows the `rqa` gazetteer pass answered  | —      | **4**     |

**The improvement was a confirmation-set effect, not a parser gain.**
The 41.3% was measured over rows that fail to *join* NAR — a question
about the judge, not about the parse. Importing RQA makes the judge
better. It does not make
[`normalize_address()`](https://mountainmath.github.io/cangeocode/reference/normalize_address.md)
read more Quebec strings: it read four more.

What the parser actually leaves behind in Quebec is not uncovered
addresses but unreadable ones —
`20-110 boul. de Mortagne, Bouceherville`, `4150 SteCatherine Ouest`,
`1052 N.P. LAPIERRE`, `1603 - 3410, rue Peel`. A second register cannot
read a misspelling. The eval harness now judges Quebec against both
registers on separate lines so the two effects cannot be confused again.

``` r

DBI::dbDisconnect(con)
```

## Where the measurements live

``` r

file.show(system.file("notes", "quebec-addresses.md", package = "cangeocode"))
```

That note carries the point-to-point comparison showing NAR is
republishing RQA’s coordinates, why Quebec’s 99.8% “building” coverage
is not a quality statement, and the six-way split of what is left of
Quebec’s normalization failures.
