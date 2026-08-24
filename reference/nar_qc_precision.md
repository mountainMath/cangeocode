# What each Quebec locator is worth

The service is an Esri \`GeocodeServer\` with two locators behind it,
and \*\*\`Loc_name\` is the field that says which one answered\*\* –
\`RQA_Adresse\` resolved a civic number, \`RQA_Rue\` only found the
street. This table maps that onto the same \`match_method\` and
\`uncertainty_m\` contract \[geocode()\] uses.

\*\*\`Addr_type\` is not the precision field here, and \`Score\` is not
a ranking of it.\*\* \`Addr_type\` comes back as \`Feature\` for both
locators, so it separates nothing. And the score measures how much of
the string that was \*sent\* was consumed, not how much of the address
was resolved. Over the same 400 Quebec addresses \[nar_qc_query()\] was
measured on, the correlation between score and how far the answer landed
from NAR's building point is \*\*Spearman 0.018\*\* – none
(\`data-raw/probe_qc.R\`, \`PROBE_PART=agree\`). Worse, street-only
answers score \*higher\* than civic ones: civic matches ran 75.9 to 86.2
with a median of 83.0, street centroids 75.8 to 95.2 with a median of
87.0. Ranking by score, or gating on it, removes correct addresses and
keeps street centroids, which is why \`min_score\` defaults to off. Read
\`Loc_name\`, then the floor.

The metres are the same order-of-magnitude figures
\[nar_bc_precision()\] uses and are \*\*not measured\*\* – they are a
ranking safe to filter on, not an error bar comparable to the NAR
tiers'.

## Usage

``` r
nar_qc_precision(loc_name)
```

## Arguments

- loc_name:

  The \`Loc_name\` attribute of a result

## Value

A one-row data frame of \`match_method\` and \`uncertainty_m\`
