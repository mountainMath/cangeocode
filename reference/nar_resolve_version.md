# Resolve a requested version, preferring the cache over the network

\`nar_connection()\` used to resolve every request against the StatCan
publication page before looking at the cache, which made an already
downloaded multi-gigabyte database unusable offline. A version key that
names a cached database is now answered locally, and \`"latest"\` falls
back to the newest cached database when StatCan cannot be reached.

Resolving still needs the network when there is a genuine question to
answer: which release is currently latest, or which key a label like
\`"May 2024"\` corresponds to.

## Usage

``` r
nar_resolve_version(version, cache_path, refresh = FALSE)
```

## Arguments

- version:

  Requested version, or \`"latest"\`

- cache_path:

  Directory holding the cached databases

- refresh:

  Whether the database is being rebuilt, which always needs the download
  URL and so always needs the network

## Value

A version key
