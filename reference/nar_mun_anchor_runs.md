# Trailing token runs that name a municipality

Shared by the gate and by \[nar_mun_anchor_variants()\] so the two agree
on what "the municipality could reach back this far" means, and so the
gate pays for the inventory lookups only down to the length it cares
about.

The reach stops one token short of the last comma segment. A
municipality never spans a comma the writer put in, and taking the whole
of the last segment would only rediscover the split the comma already
made.

## Usage

``` r
nar_mun_anchor_runs(toks, prov = NA_character_, min_k = 1L)
```

## Arguments

- toks:

  A token vector, comma tokens included

- prov:

  A two-letter province code, or \`NA\`

- min_k:

  The shortest run worth testing

## Value

An integer vector of run lengths, longest first
