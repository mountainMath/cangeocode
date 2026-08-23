# deepparse, measured against this package

[deepparse](https://github.com/GRAAL-Research/deepparse) (GRAAL-Research, Université
Laval, LGPL-3.0) is a neural address **tagger** trained on OpenAddresses data from
twenty countries, Canada among them. It is the strongest off-the-shelf answer to the
question this package answers with rules and a gazetteer, so it sets the bar a
fine-tune or a from-scratch model would have to clear before either is worth building.
That was the open item at the end of
[`address-normalization-status.md`](address-normalization-status.md); this note closes
it.

Everything here is deepparse 0.10.0 on torch 2.13.0, CPU, against NAR 2026-06.

```
uv venv --python 3.12 $NAR_CACHE_PATH/eval/deepparse-venv
uv pip install --python $NAR_CACHE_PATH/eval/deepparse-venv/bin/python deepparse

Rscript data-raw/dirty_corpus.R                 # builds the corpus, once
DP_MODEL=bpemb    Rscript data-raw/eval_deepparse.R
DP_MODEL=fasttext Rscript data-raw/eval_deepparse.R
```

## The comparison is not symmetric, and pretending it is would flatter one side

deepparse assigns each token of the input one of eight tags — StreetNumber, Unit,
StreetName, Orientation, Municipality, Province, PostalCode, GeneralDelivery. Every
token it returns is a token it was given, lowercased by its own preprocessing. It never
expands `st`, never chooses between Street and Saint, never decides that
`NOTRE-DAME RUE O` and `Rue Notre-Dame Ouest` are the same street, and never consults a
register. `StreetName` keeps the street type inside it, and usually the direction too.

So it is measured three ways and all three are reported:

| config | what it is |
| --- | --- |
| `cangeocode` | `normalize_address()` as shipped |
| `deepparse` | the tagger alone |
| `dp -> norm` | deepparse as a *segmenter*: its tags reassembled into a clean comma-delimited string, handed to `normalize_address()` |

The third row is the one that matters. It is what anyone would actually build if the
tagger were good, and it isolates the only thing deepparse can contribute that we do not
already have — **where the fields are**, as opposed to what they mean.

Two measurement decisions keep the comparison honest in deepparse's favour rather than
ours. The street test is containment, not equality — does NAR's street name appear as a
whole word in the answer — because our parser returns the name alone and deepparse
returns it with the type still attached, and an equality test would measure the fact
that it is a tagger rather than how well it tags. On the unlabelled corpora the raw
tagger gets a *containment* join through the gazetteer where the other two rows must
join on equality. Both concessions are stated in the harness output.

The province is resolved through the package's own province lexicon before scoring, so
that `ontario` and `on` are one answer. That is a table lookup, not parsing; refusing it
would score a spelling convention.

## Four corpora, because they fail differently

| corpus | n | labelled | what it is |
| --- | ---: | --- | --- |
| `A` | 4,982 | yes | `data-raw/render_address.R`'s rendered NAR rows — the mess we imagined |
| `llm` | 911 | yes | the generated half of `data-raw/dirty_corpus.R` — the mess a model imagined |
| `odhf` | 3,935 | no | free-text addresses from StatCan's Open Database of Healthcare Facilities |
| `B` | 5,000 | no | Corporations Canada registered offices, as in `eval_normalize.R` Part B |

`A` and `B` were both used to tune this parser, so neither can say what happens on input
nobody here has seen. That is what the other two are for, and it is the reason the
result below splits the way it does.

### The generated half

`data-raw/dirty_corpus.R` hands a local model (`qwen3:8b`) the **fields** of a real NAR
row — never our rendered string, or the corpus would inherit the grammar it exists to
escape — plus one of eight named transformations, and asks for the line a person would
have typed. The NAR row is the label. Every row is checked before it is kept: the civic
number has to survive as a whole token and a distinctive word of the street name has to
survive one edit, or the model changed the address rather than the writing of it and the
label would be a lie. Rejected rows are dropped, never repaired — a repaired row is a row
whose difficulty we chose.

| transformation | asked for | n | kept | lost civic | lost street |
| --- | --- | ---: | ---: | ---: | ---: |
| `abbrev` | abbreviate hard, name as well as type | 144 | 95.8% | 1 | 5 |
| `building` | building or business name first, then floor, then address | 106 | 67.9% | 34 | 0 |
| `careof` | as it would appear with a care-of or attention line | 136 | 86.8% | 18 | 0 |
| `runon` | every comma and period removed | 124 | 99.2% | 1 | 0 |
| `verbose` | as typed into one long web-form box | 140 | 95.7% | 6 | 0 |
| `ocr` | as a bad scan or broken CSV export would leave it | 113 | 99.1% | 1 | 0 |
| `bilingual` | French and English conventions mixed in one line | 117 | 87.2% | 15 | 0 |
| `terse` | fewest characters that still identify the place | 119 | 94.1% | 6 | 1 |

The rejection rates are themselves a result: asked to put a building name first, the
model changes the civic number a third of the time, and the check catches it.

**A caveat that limits what this corpus can prove.** All 134 surviving `verbose` rows
begin with the literal string `located at`, because the prompt named it as an example and
the model took it. `verbose` is therefore one prefix repeated 134 times, not 134 samples
of prose-prefixed input. `careof` is more varied — 99.2% of its rows start with something
other than a digit, `attn:` being the most common — but it is one class too. Read the
`verbose` and `careof` rows below as *this failure exists and is large*, not as an
estimate of how often it occurs in the wild.

### The real half

`odhf` is the free-text `source_format_str_address` column of the Open Database of
Healthcare Facilities v1.1 — what a dozen provincial custodians handed over before anyone
tidied it. It splits into two sub-sources that are hard for opposite reasons:
`odhf_full` (2,241 rows) is a whole address with **no commas in it**
(`8512 164th st surrey bc v4n 1e5`), which Part B never presents because the corporate
form supplies the separators; `odhf_street` (1,694 rows) is a street and nothing else,
overwhelmingly Québec and French with the type in front, and has its municipality and
province appended from the file's own columns or it would be unanswerable rather than
hard. The file is cp1252, not UTF-8.

## Results

`CORE` is the civic number and the street name together. On the unlabelled corpora the
test is Part B's: the parse must join a row NAR actually holds, and the file's own postal
code — which the join never uses — has to agree.

### `bpemb`

| corpus | config | CIVIC | STREET | CORE | MUN | PROV | POSTAL |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | `cangeocode` | **99.9%** | **98.0%** | **98.0%** | **94.4%** | **98.8%** | **55.8%** |
| A | `deepparse` | 97.4% | 83.8% | 82.9% | 59.4% | 67.4% | 51.8% |
| A | `dp -> norm` | 97.9% | 92.7% | 91.3% | 74.2% | 89.1% | 54.6% |
| llm | `cangeocode` | 72.9% | **97.3%** | 70.9% | 73.1% | 76.2% | **100.0%** |
| llm | `deepparse` | **84.2%** | 74.1% | 69.8% | 49.7% | 93.7% | 98.8% |
| llm | `dp -> norm` | 80.8% | 95.1% | **78.5%** | **92.0%** | **95.2%** | **100.0%** |

| corpus | config | joined | postal-confirmed |
| --- | --- | ---: | ---: |
| odhf | `cangeocode` | **72.2%** | **65.4%** |
| odhf | `deepparse` | 58.0% | 53.0% |
| odhf | `dp -> norm` | 70.7% | 64.3% |
| B | `cangeocode` | **88.3%** | **83.3%** |
| B | `deepparse` | 64.1% | 60.2% |
| B | `dp -> norm` | 84.7% | 79.7% |

### `fasttext`

The larger model, and the difference is not marginal.

| corpus | config | CIVIC | STREET | CORE | MUN | PROV | POSTAL |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | `cangeocode` | **99.9%** | **98.0%** | **98.0%** | **94.4%** | **98.8%** | **55.8%** |
| A | `deepparse` | 97.8% | 86.8% | 86.4% | 74.9% | 66.7% | 45.5% |
| A | `dp -> norm` | 98.7% | 94.3% | 93.6% | 81.6% | 90.8% | 53.9% |
| llm | `cangeocode` | 72.9% | **97.3%** | 70.9% | 73.1% | 76.2% | **100.0%** |
| llm | `deepparse` | 81.1% | 94.0% | 78.2% | 92.0% | 95.0% | 94.8% |
| llm | `dp -> norm` | **85.4%** | 96.4% | **83.4%** | **93.2%** | **95.2%** | **100.0%** |

| corpus | config | joined | postal-confirmed | `odhf_full` | `odhf_street` |
| --- | --- | ---: | ---: | ---: | ---: |
| odhf | `cangeocode` | 72.2% | 65.4% | 57.5% | 75.9% |
| odhf | `deepparse` | 64.9% | 58.9% | 54.1% | 65.3% |
| odhf | `dp -> norm` | **75.6%** | **68.8%** | **61.3%** | **78.7%** |

| corpus | config | joined | postal-confirmed | QC | rest of Canada |
| --- | --- | ---: | ---: | ---: | ---: |
| B | `cangeocode` | **88.3%** | **83.3%** | **75.5%** | **85.1%** |
| B | `deepparse` | 71.3% | 66.7% | 59.8% | 68.3% |
| B | `dp -> norm` | 87.1% | 82.2% | 74.6% | 83.9% |

**The split is clean and it is not about accuracy in general.** On the two corpora this
parser was tuned against — `A`, whose noise grammar we wrote, and `B`, which arrives
pre-segmented with a comma between every field — `cangeocode` wins and `dp -> norm` costs
4.4 and 1.1 points. On the two corpora built from mess nobody here anticipated,
`dp -> norm` wins: +12.5 points of CORE on `llm`, +3.4 points postal-confirmed on `odhf`,
and it wins on both `odhf` sub-sources including the comma-free one.

deepparse's own published Canadian figures are 99.76% on clean data and 98.96% on
incomplete data (FastText; BPEmb 99.03% and 96.98%). Those are token-tagging accuracies
against OpenAddresses-derived test data whose surface grammar the model was trained on.
They are not comparable to anything in the tables above and should not be quoted as if
they were.

### Where the difference actually comes from

| transformation | n | `cangeocode` | `deepparse` | `dp -> norm` |
| --- | ---: | ---: | ---: | ---: |
| `abbrev` | 138 | **93.5%** | 75.4% | **93.5%** |
| `bilingual` | 102 | 86.3% | **92.2%** | 89.2% |
| `building` | 72 | 94.4% | **97.2%** | 93.1% |
| `careof` | 118 | 18.6% | **75.4%** | 69.5% |
| `ocr` | 112 | 98.2% | 83.0% | **99.1%** |
| `runon` | 123 | **99.2%** | 79.7% | 98.4% |
| `terse` | 112 | **95.5%** | 90.2% | **95.5%** |
| `verbose` | 134 | **0.0%** | **47.0%** | 38.8% |

CORE by transformation, `fasttext`. Six of the eight classes are a wash or ours. The
entire measured advantage sits in two: `careof` and `verbose`, which are the two classes
where the address does not begin at the start of the string.

`verbose` at **0.0%** is not a rounding artefact. Adding ten characters to the front of
an address whose civic number and street name otherwise parse correctly destroys both:

```r
normalize_address("41 Cultus RD, CLEAR CREEK, ON, N0E1C0")
#>   CIVIC_NO  STREET_NAME  STREET_TYPE  MUN_NAME  PROV_ABVN  pattern
#>         41       CULTUS           RD        ON       <NA>  civic_street

normalize_address("located at 41 Cultus RD, CLEAR CREEK, ON, postal code N0E1C0")
#>   CIVIC_NO           STREET_NAME  STREET_TYPE     MUN_NAME  PROV_ABVN  pattern
#>       <NA>  LOCATED AT 41 CULTUS           RD  POSTAL CODE       <NA>  street_only
```

The civic number is no longer the first token, so `civic_street` never fires, the pattern
falls through to `street_only`, and the prefix is swallowed whole into the street name.
(`MUN_NAME` is wrong in both rows for an unrelated reason — `CLEAR CREEK` is not a
municipality the gazetteer resolves — which is why `CORE` is civic and street only.)
This is precisely the **segmentation** blind spot that
[`address-normalization-status.md`](address-normalization-status.md) flagged as
untested — "a routing job, not a knowledge job, and no measurement here touches it". It
is now measured, and it is the whole of what deepparse buys.

## What it costs

| | `bpemb` | `fasttext` | `cangeocode` |
| --- | ---: | ---: | ---: |
| weights on disk | 160 MB | 6.8 GB | — |
| model load, per process | 4.8 s | 36.8 s | — |
| peak RSS | 0.75 GB | 13.4 GB | — |
| 4,982 rows, end to end | 9 s | 44 s | 10 s |

Load and peak RSS are measured directly, on a one-line input. The end-to-end column is one
process per call, so it carries the load cost once; subtracting it puts both models above a
thousand rows a second on the larger corpora, which is faster than we normalize.

Throughput is therefore not the objection. The 13.4 GB resident set for the model that is
actually worth using is, along with a Python-process dependency in an R package and an
LGPL-3.0 component in an MIT one.

## A six-line rule beats it

If the whole advantage is that the address does not start at the start of the string,
then the cheap version of the fix is to make it start there. In the first comma-delimited
field, drop everything before the first token that begins with a digit — guarded so that a
leading unit designator (`Apt`, `Suite`, `Bureau`, `Unit`, `#`, …) is not eaten, since
those legitimately precede the civic number.

| corpus | measure | as shipped | prefix stripped |
| --- | --- | ---: | ---: |
| A | CORE | 98.0% | 97.9% |
| llm | CORE | 70.9% | **93.3%** |
| odhf | postal-confirmed | 65.4% | 65.4% |
| B | postal-confirmed | 83.3% | 83.2% |

and within the `llm` corpus, unguarded, `careof` goes 18.6% → **93.2%** and `verbose`
0.0% → **90.3%**.

That is +22.4 points where deepparse-as-segmenter bought +12.5, and −0.1 everywhere else,
at no runtime cost, no dependency and no gigabytes. The guard costs 0.7 points against the
unguarded rule on this corpus and is still the right call: promoting a unit number to a
civic number is a wrong answer, where failing to strip is a missing one.

The rule is measured, not shipped. It needs a design pass before it goes in — where in
the pipeline it belongs, what it does to a care-of line carrying a person's name, whether
the guard list is the unit lexicon or a new one, and its own tests. It is item 1 of the
next steps in
[`address-normalization-status.md`](address-normalization-status.md).

## What this settles

**A fine-tune is not warranted, and neither is a from-scratch model.** The plan's
sequencing said this measurement decides it, and the decision is negative on the evidence
rather than on principle:

1. **The knowledge half is already ours by a wide margin.** deepparse cannot expand `st`,
   cannot disambiguate Saint from Street, and cannot reach a register. Against the
   gazetteer it loses on both tuned corpora and on six of eight generated classes. A
   fine-tune would have to acquire the register, which is the thing the register is for.
2. **The segmentation half is real, and it is worth 12.5 points on unanticipated input.**
   That is the honest finding and it should not be softened. But it is also worth 22.4
   points to a rule with no model behind it, which is what happens when a neural component
   is asked to solve a problem that has a shape.
3. **What is left after the rule is the part a model might still earn.** Not measured yet,
   because the rule is not built. Re-run this harness after it ships; if `dp -> norm` still
   leads on `llm` or `odhf` at that point, the case reopens with a smaller and much better
   specified target than "parse addresses".

This does not retract anything in *What a local LLM adds* in
[`address-normalization-status.md`](address-normalization-status.md) — that measured a
foundation model on the gazetteer's residual and found it added nothing. This measures a
purpose-built neural tagger on input the parser had never seen, and finds one thing it is
genuinely better at. The two results are about different halves of the problem and both
stand.

## Reproducing

```
Rscript data-raw/dirty_corpus.R                  # NAR_CACHE_PATH + a local Ollama
DP_MODEL=fasttext Rscript data-raw/eval_deepparse.R
```

Knobs: `DP_MODEL` (`bpemb`, `fasttext`, `bpemb_attention`, `fasttext_attention`),
`DP_CORPORA` (subset of `A,llm,odhf,B`), `DP_PYTHON`, and `EVAL_N` / `EVAL_CACHE` /
`EVAL_VERSION` as in `eval_normalize.R`. The corpus generation is one model call per row
and takes about 35 minutes for 1,000 rows on `qwen3:8b`; the CSV is the artefact and the
script is idempotent unless `DIRTY_REFRESH=1`.

The generated half is seeded (`20260822`) but a local model's output is not reproducible
across Ollama or model versions the way the SQL samples are. `<EVAL_CACHE>/dirty_corpus.csv`
is the corpus of record for the numbers above; regenerating it will move them.
