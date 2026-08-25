# The positional error a remapped municipality carries, by what attests it

When \[normalize_address()\] hands back a municipality that is not the
one the string named, the place that was searched was chosen by the
gazetteer rather than asserted by the input. \`mun_remapped\` says that
happened; \`mun_evidence\` says on what grounds, and the two populations
it separates are far enough apart that one floor cannot serve both.

Measured against Nova Scotia's PVSC assessment points – the one
reference established to be independent of NAR – over the 32,512 exact
unambiguous building matches in a 40,000-address sample:

\| \`mun_evidence\` \| n \| p50 \| p90 \| \>5 km \| floor \| \| — \| —:
\| —: \| —: \| —: \| —: \| \| \`kept\` \| 30,045 \| 10.2 \| 56.9 \| 0.05
\| \`copostal\` \| 1,632 \| 7.5 \| 50.6 \| 0.80 \| \`csd\` \| 91 \| 14.5
\| 87.2 \| 0.00 \| \`unattested\` \| 560 \| 12.9 \| 83.3 \| 1.79 \|
\`untestable\` \| 184 \| 32.0 \| 327.5 \| 1.63

\*\*The attested classes get no floor, because there is no spread to
report.\*\* Pooled, \`copostal\` and \`csd\` sit at p90 52.6 m over
1,723 rows – \*below\* the 56.9 m of the rows whose municipality
survived the parse. A remap the register itself vouches for, whether by
two names sharing a postal code or by one name being the other's census
subdivision, is positionally indistinguishable from no remap at all.
Reporting metres against it would be inventing them.

\*\*The unattested and untestable classes pool at p90 118.2 m over 744
rows\*\*, and that is the constant. \`untestable\` is the wider of the
two and is \*not\* fined by \[nar_gazetteer_sql()\] – refusing it costs
119 exact matches for 2 errors past 5 km – so the floor is the whole of
what is done about it.

\`inferred\` – the string named no municipality and the gazetteer
supplied one – takes the same floor \*\*unmeasured\*\*. PVSC always
carries a city, so the Nova Scotia corpus contains none of these rows
and cannot price them. Grouping it with the unattested is the
conservative reading, not a measured one.

Two things about all of these matter more than their size.

They are \*disagreements\*, not error budgets: each contains NAR's own
distance from PVSC, which the \`kept\` row shows to be almost all of it.

And none of them describes the tail, which is where the remap risk
actually lives. An unattested remap lands more than 5 km out 1.79
untestable one 1.63 at a distance no 90th percentile of any of these
populations reports. A caller who cannot tolerate a kilometre-scale
error should filter on \`mun_evidence\` itself. These floors exist so
that \`uncertainty_m\` stops reporting 0 m on the populations where 0 is
least true, not so that they can be read as bounds.

See \`inst/notes/nova-scotia-pvsc.md\`.

## Usage

``` r
nar_remap_uncertainty_m()
```

## Value

A named numeric vector of metres, indexed by \`mun_evidence\`
