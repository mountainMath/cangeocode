# Parse a StatCan version label into a date

StatCan labels releases inconsistently – a bare year, a month and year,
or a full date – so each form is matched explicitly.

This deliberately avoids \`strptime\`'s \` \`LC_TIME\` and returns
\`NA\` for an English name under, say, a French locale.
\`month.name\`/\`month.abb\` are English constants in base R whatever
the locale, so matching against them keeps version discovery working
everywhere. A silent \`NA\` here is expensive: it propagates into
\`path\`, which is both the database filename and the canonical version
key.

## Usage

``` r
nar_version_date(version)
```

## Arguments

- version:

  Character vector of version labels

## Value

A \`Date\` vector, \`NA\` where the label could not be parsed
