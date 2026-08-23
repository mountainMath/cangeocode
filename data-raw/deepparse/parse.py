"""Tag addresses with deepparse and write the tags back as TSV.

Called by data-raw/eval_deepparse.R, which owns every decision about what is
measured; this file is only the bridge across the language boundary.

    python parse.py IN.txt OUT.tsv [--model bpemb] [--batch 256]

IN.txt is one address per line, already stripped of tabs and newlines by the
caller. OUT.tsv has one row per input line and one column per deepparse tag,
in input order, with a header. A tag that fired on several tokens is joined
with a single space, in token order; a tag that never fired is an empty field.

Two things about deepparse that the R side depends on and cannot see:

  * It is a *tagger*. Every output token is a token of the input, lowercased by
    deepparse's own preprocessing. It never expands `st` to `street`, never
    canonicalises, and never invents a field the string did not carry. So it
    cannot be scored against NAR's canonical spelling directly -- see the R
    side's `surface_*` labels.
  * `StreetName` is the street name *and* its type *and*, usually, its
    direction. `Orientation` exists in the tag set but the pretrained model
    emits it rarely. Splitting them is the caller's job.

The model files land in ~/.cache/deepparse and are downloaded on first use.
"""

import argparse
import sys

TAGS = ["StreetNumber", "Unit", "StreetName", "Orientation",
        "Municipality", "Province", "PostalCode", "GeneralDelivery"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("infile")
    ap.add_argument("outfile")
    ap.add_argument("--model", default="bpemb")
    ap.add_argument("--batch", type=int, default=256)
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()

    from deepparse.parser import AddressParser

    with open(args.infile, encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh]

    # deepparse raises on an empty or whitespace-only address rather than
    # returning an empty parse, so those are held out and written back blank.
    idx = [i for i, s in enumerate(lines) if s.strip()]
    todo = [lines[i] for i in idx]

    parser = AddressParser(model_type=args.model, device=args.device,
                           verbose=False)
    parsed = parser(todo, batch_size=args.batch) if todo else []
    if todo and not isinstance(parsed, list):
        parsed = [parsed]

    out = [dict.fromkeys(TAGS, "") for _ in lines]
    for i, p in zip(idx, parsed):
        # to_dict() collapses repeats itself, but silently drops the token
        # order between two runs of the same tag; walk the pairs instead.
        acc = {t: [] for t in TAGS}
        for token, tag in p.address_parsed_components:
            if tag in acc and token is not None:
                acc[tag].append(token)
        out[i] = {t: " ".join(v) for t, v in acc.items()}

    with open(args.outfile, "w", encoding="utf-8") as fh:
        fh.write("\t".join(TAGS) + "\n")
        for row in out:
            fh.write("\t".join(row[t] for t in TAGS) + "\n")
    print(f"tagged {len(todo)} of {len(lines)} lines with {args.model}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
