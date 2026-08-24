# Import Quebec's address register beside NAR

Loads the \*\*Repertoire quebecois des adresses\*\* into the cached NAR
database as its own tables, \`RqaAddresses\` and \`RqaStreets\`. Nothing
in \`Addresses\` is touched. Once imported, \`"rqa"\` becomes available
as a \[geocode()\] tier.

## Usage

``` r
rqa_import(version = "latest", refresh = FALSE, csv = NULL)
```

## Arguments

- version:

  NAR version whose database receives the tables, passed to
  \[nar_connection()\]. Default \`"latest"\`.

- refresh:

  Logical. Re-import even when the tables are already present.

- csv:

  Path to an already-extracted \`RQA.csv\`. Defaults to
  \`getOption("rqa_csv")\`, and downloading the release when that is
  unset.

## Value

The path to the database, invisibly.

## Why a separate table

RQA holds roughly 308,000 civic addresses NAR does not, about 9
\[normalize_address()\] – which has no online fallback – that coverage
is the single largest block of what still fails in Quebec. It is
nonetheless kept separate rather than merged, for three reasons recorded
in \`system.file("notes", "quebec-addresses.md", package =
"cangeocode")\`:

\* Merging destroys the only instrument Quebec has. Everything known
about what NAR is missing in Quebec is known because the two registers
can be read against each other; a merged table can no longer be asked
the question. \* The added rows are positionally \*weaker\* than what
NAR carries – 20.3 building-placed against 26.9 \`Incertaine\` by RQA
itself – so merging would quietly degrade what \`geom_source =
'building'\` means for Quebec, which is already misleading there. \* A
merged table stops being NAR. \[nar_provinces()\], the row counts in the
vignettes and \`nar_schema_version()\` all describe a Statistics Canada
release.

## What is imported

Certified rows only (\`etat = 'Certifiee'\`), which drops about 7,500
retired addresses. The whole register is loaded, not just the rows NAR
lacks: the gap is a property of the pair and has to be recomputable, and
a table subset against one NAR release would be silently wrong against
the next. \`IN_NAR\` records which side of the gap each row fell on
\*\*for the release it was imported into\*\*, since the tables live
inside that release's database file.

Street names are reshaped to NAR's convention on the way in, because
that is what makes the two joinable: NAR keeps the leading particule
inside the street name (\`de la Cote-de-Liesse\`) and RQA keeps it in a
column of its own, so \`STREET_NAME\` here is particule plus specifique,
and the generique becomes \`STREET_TYPE\`, canonicalized through the
same French lexicon \[normalize_address()\] uses. Comparing the raw
columns instead reads 1,265,940 missing addresses where there are
357,723.

## Examples

``` r
if (FALSE) { # \dontrun{
rqa_import()

con <- nar_connection()
geocode("431 Courtemanche, Montreal-Est QC",
        method = c("nar", "rqa", "nar_interpolate"), con = con)
} # }
```
