# Looks for addresses NAR places in the wrong location, using nothing but NAR
# and Statistics Canada's own municipal boundaries.
#
# The premise the first version of this probe ran on was that every row makes
# THREE independent statements about where it is -- a postal code, a
# municipality, and a coordinate -- so a disagreement among them is internal and
# needs no reference dataset. Stage 1 below tests that premise directly by
# intersecting all 17.3 million points with the 2021 CSD boundaries, and it does
# not survive: NAR's CSD label agrees with the polygon its own coordinate falls
# in 98.8% of the time, and every large residual is explained by boundary
# vintage or by two municipalities sharing a name. The CSD label is DERIVED from
# the coordinate. There are two independent statements here, not three -- the
# mail side (postal code, mailing municipality) and the geographic side
# (coordinate, CSD) -- and a two-way contradiction cannot be arbitrated by
# majority. Only the neighbourhood can arbitrate it, which is what stages 2-5 do.
#
# The other correction is the postal code itself. A postal code is a DELIVERY
# ROUTE, not a place: it may legitimately be disconnected, and a rural one
# routinely is. So the group's median is the wrong centre and distance-from-it is
# the wrong statistic -- both assume one cluster. What replaces them is a pair of
# nearest-neighbour distances that assume nothing about shape:
#
#   d_own    the distance to the NEAREST address sharing the full postal code
#   d_other  the distance to the nearest address carrying a DIFFERENT one
#
# A multi-cluster postal code has a small d_own inside every one of its clusters,
# so it costs nothing. What no postal code should produce is a member far from
# every one of its own addresses AND sitting on top of somebody else's. The ratio
# self-normalizes for density, which is why rural rows no longer have to be
# thrown away wholesale the way the first version threw them away.
#
# d_other is then RE-MEASURED ALONG THE ROAD NETWORK, because "close to a
# different postal code" has to mean reachable, not merely nearby: the straight
# line crosses the water in Georgian Bay, the ravine in Whistler and the lake at
# Fraser Lake. That single substitution removes 21.9% of the straight-line flags.
#
# The stages:
#
#   1. BOUNDARIES  -- point-in-polygon against the 2021 CSDs, which is what shows
#      the CSD label is not an independent witness. Also the backfill for the
#      65,083 rows that have no coordinate to intersect.
#   2. THE METRIC  -- d_own for every row, in two steps: a 250 m cellmate test
#      that settles 96% of the file for free, then an exact within-group search
#      for what is left.
#   3. THE FLAG    -- d_other by escalating grid, and the ratio.
#   4. THE ROAD    -- d_other again, along RnfSegments, with a proper
#      perpendicular snap onto the segment rather than onto its endpoint.
#   5. THE VERDICT -- the street name is the only witness independent of both
#      sides, so ask where it exists: at the point, or at the postal code.
#
# The neighbourhood tests use a grid bucket and an equi-join rather than a
# spatial index. A correlated ST_DWithin subquery over 17M points takes two
# minutes for 400 probes; the grid does 17 million in seconds.
#
# Findings are written up in inst/notes/nar-consistency.md. Read that first;
# this file is how to reproduce it.
#
# Run with:  Rscript data-raw/probe_consistency.R    (needs NAR_CACHE_PATH,
# an imported RNF -- see rnf_import() -- and cancensus, sf and igraph installed)

suppressMessages({
  library(DBI)
  library(cangeocode)
})

CELL     <- 250    # metres; the cellmate bucket in stage 2
NEAR     <- 400    # metres; the neighbourhood a point is judged against
MIN_DIST <- 1000   # metres; nothing closer than this is called an outlier
RATIO    <- 10     # d_own must exceed d_other by this factor
NET_CELL <- 3000   # metres; the road-graph tile in stage 4
NET_RING <- 2      # tiles either side, so the local graph spans +/- 7.5 km

work <- file.path(tempdir(), "consistency")
dir.create(work, showWarnings = FALSE, recursive = TRUE)

con <- nar_connection()
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
invisible(DBI::dbExecute(con, "PRAGMA threads=8"))
invisible(DBI::dbExecute(con, sprintf("SET temp_directory='%s'", work)))
invisible(DBI::dbExecute(con, "SET memory_limit='12GB'"))
invisible(DBI::dbExecute(con,
  "CREATE TEMP MACRO nf(s) AS regexp_replace(upper(strip_accents(s)),'[^A-Z0-9]','','g')"))

say  <- function(...) cat(..., "\n", sep = "")
rule <- function(t) cat("\n== ", t, " ", strrep("=", max(0, 60 - nchar(t))), "\n", sep = "")
have <- function(p) requireNamespace(p, quietly = TRUE)

# ---------------------------------------------------------------- stage 1 ----
# The 2021 CSDs, DIGITAL rather than cartographic: the cartographic boundaries
# are clipped to the coastline and generalized, so a waterfront address falls
# outside them for reasons that have nothing to do with NAR. They arrive in
# EPSG:3347, which is also the storage CRS here and the RNF's -- nothing is
# reprojected anywhere in this file.
rule("1. BOUNDARIES: is the CSD label an independent witness?")
if (!have("cancensus") || !have("sf")) {
  say("cancensus and sf are needed for stage 1; skipping it.")
} else {
  gpkg <- file.path(work, "csd2021.gpkg")
  if (!file.exists(gpkg)) {
    csd <- cancensus::get_statcan_geographies(2021, "CSD", type = "digital")
    csd <- sf::st_make_valid(csd)
    sf::st_write(csd, gpkg, quiet = TRUE)
  }
  invisible(DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE TEMP TABLE csd AS
       SELECT CSDUID, CSDNAME, CSDTYPE, PRUID, geom FROM st_read('%s')", gpkg)))
  say("CSD polygons: ", DBI::dbGetQuery(con, "SELECT count(*) n FROM csd")$n)

  # ST_Contains over 17.3M points and 5,161 polygons takes about a minute; the
  # result is materialized because stages 3 and 5 both want it.
  pip <- file.path(work, "pip.parquet")
  if (!file.exists(pip)) {
    invisible(DBI::dbExecute(con, sprintf(
      "COPY (SELECT a.ADDR_GUID, c.CSDUID pip_uid, c.CSDNAME pip_name, c.PRUID pip_pruid
               FROM (SELECT ADDR_GUID, geom FROM Addresses WHERE geom IS NOT NULL) a
               LEFT JOIN csd c ON ST_Contains(c.geom, a.geom))
         TO '%s' (FORMAT PARQUET)", pip)))
  }
  invisible(DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE TEMP TABLE p AS SELECT * FROM '%s'", pip)))

  print(DBI::dbGetQuery(con, "
    SELECT count(*) n,
           count(*) FILTER (WHERE a.PROV_CODE = p.pip_pruid) province_agrees,
           count(*) FILTER (WHERE p.pip_uid IS NULL) outside_every_csd,
           count(*) FILTER (WHERE nf(a.CSD_ENG_NAME) = nf(p.pip_name)
                              OR nf(a.CSD_FRE_NAME) = nf(p.pip_name)) name_agrees
      FROM Addresses a JOIN p USING(ADDR_GUID)"))
  say("A label that agrees with the polygon its own coordinate falls in is not an")
  say("independent statement about location. Treat CSD and coordinate as one side.")

  rule("   every row whose point lands in the wrong province, or in none")
  print(DBI::dbGetQuery(con, "
    SELECT a.PROV_CODE pr, p.pip_pruid pip_pr, p.pip_name, a.MAIL_MUN_NAME mun,
           a.CSD_ENG_NAME csd, count(*) n
      FROM Addresses a JOIN p USING(ADDR_GUID)
     WHERE p.pip_uid IS NULL OR a.PROV_CODE <> p.pip_pruid
     GROUP BY ALL ORDER BY n DESC"), width = 200)
  say("Border towns whose CSD really is split across a provincial line -- Flin Flon,")
  say("Lloydminster, Creighton -- account for nearly all of it. What is left is the")
  say("residue worth reading one row at a time.")

  rule("   the name disagreements: vintage, or misplacement?")
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE nm AS
    SELECT nf(CSDNAME) fold, PRUID, ST_Union_Agg(geom) geom FROM csd GROUP BY 1,2"))
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE dis AS
    SELECT a.ADDR_GUID id, a.PROV_CODE pr, a.CSD_ENG_NAME csde, a.geom
      FROM Addresses a JOIN p USING(ADDR_GUID)
     WHERE p.pip_name IS NOT NULL AND a.PROV_CODE = p.pip_pruid
       AND nf(a.CSD_ENG_NAME) <> nf(p.pip_name)"))
  print(DBI::dbGetQuery(con, "
    SELECT count(*) disagreeing,
           count(*) FILTER (WHERE csde IS NULL OR csde='') no_csd_label,
           count(*) FILTER (WHERE csde<>'' AND NOT EXISTS
             (SELECT 1 FROM nm WHERE nm.fold=nf(dis.csde) AND nm.PRUID=dis.pr)) name_gone_by_2021,
           count(*) FILTER (WHERE csde<>'' AND EXISTS
             (SELECT 1 FROM nm WHERE nm.fold=nf(dis.csde) AND nm.PRUID=dis.pr)) name_still_there
      FROM dis"))
  say("A name NAR uses that no 2021 CSD carries is a municipality created or renamed")
  say("since -- New Brunswick's 2023 reform supplies most of them. It is a vintage")
  say("gap in the boundaries, not an error in NAR.")

  # The rows whose name DOES still exist are the ones a vintage argument cannot
  # excuse, so ask the vintage-robust question instead: how far is the point from
  # the nearest polygon anywhere in the province carrying the name NAR gave it?
  # A homonym -- two Saint-Lamberts, two Kents -- lands the point on top of the
  # other one, and only a real misplacement lands it far from every one of them.
  rule("   name still exists in 2021: how far is the point from it?")
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE strayd AS
    SELECT dis.id, dis.csde, dis.pr, min(ST_Distance(dis.geom, nm.geom)) m
      FROM dis JOIN nm ON nm.fold=nf(dis.csde) AND nm.PRUID=dis.pr
     WHERE dis.csde<>'' GROUP BY 1,2,3"))
  print(DBI::dbGetQuery(con, "
    SELECT count(*) n, count(*) FILTER (WHERE m<=1000) within_1km,
           count(*) FILTER (WHERE m>1000 AND m<=10000) within_10km,
           count(*) FILTER (WHERE m>10000) over_10km FROM strayd"))
  say("A boundary that moved, or a point just over the line, leaves the point beside")
  say("a polygon that still carries the name. Only the far tail is a candidate for")
  say("misplacement, and it is a tail of labels rather than of addresses:")
  print(DBI::dbGetQuery(con, "
    SELECT csde, pr, count(*) n, round(median(m)/1000, 1) med_km, round(max(m)/1000)::INT max_km
      FROM strayd WHERE m>10000 GROUP BY 1,2 ORDER BY n DESC LIMIT 12"), width = 200)

  rule("   BACKFILL: a municipality for the rows with no coordinate")
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE ma AS
    SELECT NAME_FOLD, PROV_ABVN, count(*) n_keys, arg_max(MUN_KEY, N_ADDRESSES) top_key,
           max(N_ADDRESSES)::DOUBLE/sum(N_ADDRESSES) top_shr FROM MunAlias GROUP BY 1,2"))
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE bf AS
    SELECT a.ADDR_GUID id, a.PROV_CODE pr, a.MAIL_PROV_ABVN prov,
           a.CSD_ENG_NAME csde, a.MAIL_MUN_NAME mun,
           m.n_keys, m.top_key, m.top_shr
      FROM Addresses a LEFT JOIN ma m
        ON m.NAME_FOLD = strip_accents(upper(a.MAIL_MUN_NAME))
       AND m.PROV_ABVN = a.MAIL_PROV_ABVN
     WHERE a.x IS NULL"))
  print(DBI::dbGetQuery(con, "
    SELECT count(*) no_coordinate,
           count(*) FILTER (WHERE csde IS NOT NULL AND csde<>'') already_labelled,
           count(*) FILTER (WHERE (csde IS NULL OR csde='') AND top_key IS NULL) no_alias,
           count(*) FILTER (WHERE (csde IS NULL OR csde='') AND n_keys=1) unique_csd,
           count(*) FILTER (WHERE (csde IS NULL OR csde='') AND n_keys>1 AND top_shr>=0.9) dominant_csd,
           count(*) FILTER (WHERE (csde IS NULL OR csde='') AND n_keys>1 AND top_shr<0.9) ambiguous
      FROM bf"))
  print(DBI::dbGetQuery(con, "
    SELECT count(*) n,
           count(*) FILTER (WHERE nf(split_part(top_key,':',3)) = nf(csde)) top_pick_is_right,
           count(*) FILTER (WHERE EXISTS (
             SELECT 1 FROM MunAlias m
              WHERE m.NAME_FOLD = strip_accents(upper(bf.mun))
                AND m.PROV_ABVN = bf.prov
                AND nf(split_part(m.MUN_KEY,':',3)) = nf(bf.csde))) alias_set_contains_it
      FROM bf WHERE csde IS NOT NULL AND csde<>'' AND top_key IS NOT NULL"))
  say("Measured on the rows that already carry a CSD: the alias set the postal city")
  say("names always contains the right municipality, and taking the largest member")
  say("of that set is right about three times in four. The backfill is a candidate")
  say("set with a default, not an assignment.")
}

# ---------------------------------------------------------------- stage 2 ----
rule("2. d_own: the distance to the nearest address sharing the postal code")
invisible(DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE a AS
  SELECT ADDR_GUID id, MAIL_POSTAL_CODE pc, x, y,
         (x//%1$d)::INT gx, (y//%1$d)::INT gy, substr(MAIL_POSTAL_CODE,2,1)='0' rural
    FROM Addresses WHERE length(MAIL_POSTAL_CODE)=6 AND x IS NOT NULL", CELL)))
say("usable (6-char postal code and a coordinate): ",
    format(DBI::dbGetQuery(con, "SELECT count(*) n FROM a")$n, big.mark = ","))

# Anything sharing a 250 m cell with a same-postal-code sibling has d_own under
# 354 m and cannot be an outlier. This one GROUP BY settles 96% of the file in a
# fraction of a second and is what makes the exact search below affordable.
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE anchored AS
  SELECT a.id FROM a JOIN (SELECT pc,gx,gy FROM a GROUP BY 1,2,3 HAVING count(*)>1) c
    USING(pc,gx,gy)"))
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE loose AS
  SELECT a.* FROM a ANTI JOIN anchored USING(id)"))
say("anchored by a cellmate: ",
    format(DBI::dbGetQuery(con, "SELECT count(*) n FROM anchored")$n, big.mark = ","),
    "; loose: ", format(DBI::dbGetQuery(con, "SELECT count(*) n FROM loose")$n, big.mark = ","))

# d_own needs only same-postal-code comparisons, so the exact answer for the
# loose rows is a join restricted to the group. No spatial index, no radius.
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE own AS
  SELECT l.id, l.pc, l.x, l.y, l.rural, min(sqrt((l.x-b.x)^2+(l.y-b.y)^2)) d_own
    FROM loose l LEFT JOIN a b ON b.pc=l.pc AND b.id<>l.id
   GROUP BY l.id,l.pc,l.x,l.y,l.rural"))
print(DBI::dbGetQuery(con, sprintf("
  SELECT count(*) n, count(*) FILTER (WHERE d_own IS NULL) only_member,
         round(quantile_cont(d_own,0.5)) p50, round(quantile_cont(d_own,0.9)) p90,
         count(*) FILTER (WHERE d_own>%1$d) over_1km,
         count(*) FILTER (WHERE d_own>%1$d AND rural) over_1km_rural FROM own", MIN_DIST)))
say("only_member is the method's blind spot: a postal code NAR carries exactly one")
say("address for has no sibling to be far from, and nothing here can test it.")

# ---------------------------------------------------------------- stage 3 ----
rule("3. d_other, and the flag")
invisible(DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE cand AS
  SELECT id,pc,x,y,rural,d_own FROM own WHERE d_own IS NULL OR d_own>%d", MIN_DIST)))

# An escalating grid rather than one big radius: the candidates are isolated by
# construction, so 86% of them find a stranger inside the first 1 km tile.
step <- function(tbl, src, R) invisible(DBI::dbExecute(con, sprintf(
  "CREATE OR REPLACE TEMP TABLE %1$s AS
   WITH b AS (SELECT id,pc,x,y,(x//%3$d)::INT bx,(y//%3$d)::INT byy FROM a),
        q AS (SELECT c.id,c.pc,c.x,c.y,(c.x//%3$d)::INT+dx bx,(c.y//%3$d)::INT+dy byy
                FROM %2$s c,(SELECT unnest([-1,0,1]) dx),(SELECT unnest([-1,0,1]) dy))
   SELECT c.id, min(sqrt((c.x-b.x)^2+(c.y-b.y)^2)) FILTER (WHERE b.pc<>c.pc) d_other
     FROM %2$s c LEFT JOIN q ON q.id=c.id LEFT JOIN b ON b.bx=q.bx AND b.byy=q.byy
    GROUP BY c.id", tbl, src, R)))
carry <- function(out, src, res, R) {
  invisible(DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE %s AS
    SELECT c.*, o.d_other FROM %s c JOIN %s o USING(id)
     WHERE o.d_other IS NOT NULL AND o.d_other<=%d", out, src, res, R)))
}
step("o1", "cand", 1000);  carry("r1", "cand", "o1", 1000)
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE c2 AS SELECT c.* FROM cand c ANTI JOIN r1 USING(id)"))
step("o2", "c2", 8000);    carry("r2", "c2", "o2", 8000)
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE c3 AS SELECT c.* FROM c2 c ANTI JOIN r2 USING(id)"))
step("o3", "c3", 64000)
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE r3 AS
  SELECT c.*, o.d_other FROM c3 c JOIN o3 o USING(id)"))
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE cd AS
  SELECT * FROM r1 UNION ALL SELECT * FROM r2 UNION ALL SELECT * FROM r3"))
print(DBI::dbGetQuery(con, sprintf("
  SELECT count(*) candidates, count(*) FILTER (WHERE d_other IS NULL) no_stranger,
         round(quantile_cont(d_own,0.5)) own_p50, round(quantile_cont(d_other,0.5)) other_p50,
         count(*) FILTER (WHERE d_own>%1$d AND d_other<d_own/%2$d) flagged,
         count(*) FILTER (WHERE d_own>%1$d AND d_other<d_own/%2$d AND rural) flagged_rural
    FROM cd", MIN_DIST, RATIO)))
invisible(DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE fl AS
  SELECT * FROM cd WHERE d_own>%d AND d_other<d_own/%d", MIN_DIST, RATIO)))

# ---------------------------------------------------------------- stage 4 ----
rule("4. THE ROAD: d_other measured along RnfSegments")
if (!have("igraph") ||
    !DBI::dbGetQuery(con, "SELECT count(*) n FROM information_schema.tables
                            WHERE table_name='RnfSegments'")$n) {
  say("RnfSegments (see rnf_import()) and igraph are needed for stage 4; skipping it.")
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE surv AS SELECT *, NULL d_other_net FROM fl"))
} else {
  # The five nearest strangers, not just the first: the closest one may be the
  # one across the water, and the flag has to survive the best of them.
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE near5 AS
    WITH b AS (SELECT id,pc,x,y,(x//2000)::INT bx,(y//2000)::INT byy FROM a),
         q AS (SELECT f.id,f.pc,f.x,f.y,(f.x//2000)::INT+dx bx,(f.y//2000)::INT+dy byy
                 FROM fl f,(SELECT unnest([-1,0,1]) dx),(SELECT unnest([-1,0,1]) dy)),
         j AS (SELECT q.id, b.x bx2, b.y by2,
                      row_number() OVER (PARTITION BY q.id ORDER BY (q.x-b.x)^2+(q.y-b.y)^2) rn
                 FROM q JOIN b ON b.bx=q.bx AND b.byy=q.byy WHERE b.pc<>q.pc)
    SELECT id, bx2, by2, rn FROM j WHERE rn<=5"))
  fl  <- DBI::dbGetQuery(con, "SELECT * FROM fl")
  n5  <- DBI::dbGetQuery(con, "SELECT * FROM near5 ORDER BY id, rn")
  seg <- DBI::dbGetQuery(con, "
    SELECT ST_X(ST_StartPoint(geom)) x1, ST_Y(ST_StartPoint(geom)) y1,
           ST_X(ST_EndPoint(geom))   x2, ST_Y(ST_EndPoint(geom))   y2,
           ST_Length(geom) len FROM RnfSegments")
  say("road segments: ", format(nrow(seg), big.mark = ","))

  key   <- function(x, y) round(x)*1e7 + round(y)
  cellk <- function(x, y) (x %/% NET_CELL)*1e5 + (y %/% NET_CELL)
  k1 <- key(seg$x1, seg$y1); k2 <- key(seg$x2, seg$y2)
  nk <- unique(c(k1, k2)); from <- match(k1, nk); to <- match(k2, nk)
  nx <- nk %/% 1e7; ny <- nk - nx*1e7
  w  <- seg$len
  ok <- from != to & is.finite(w) & w > 0
  from <- from[ok]; to <- to[ok]; w <- w[ok]
  X1 <- seg$x1[ok]; Y1 <- seg$y1[ok]; X2 <- seg$x2[ok]; Y2 <- seg$y2[ok]
  ec <- c(cellk(X1, Y1), cellk(X2, Y2)); ei <- c(seq_along(w), seq_along(w))
  o  <- order(ec); ec <- ec[o]; ei <- ei[o]
  uc <- unique(ec); st <- match(uc, ec); en <- c(st[-1] - 1L, length(ec))

  # Snapping to the nearest NODE would charge every rural address the distance to
  # the next intersection -- hundreds of metres, on both legs. Snap perpendicular
  # onto the segment instead and pay only the offset plus the run along it.
  snap <- function(px, py, i) {
    dx <- X2[i]-X1[i]; dy <- Y2[i]-Y1[i]; L2 <- dx*dx + dy*dy
    t <- ifelse(L2 > 0, ((px-X1[i])*dx + (py-Y1[i])*dy)/L2, 0)
    t <- pmin(1, pmax(0, t))
    dd <- (px - (X1[i]+t*dx))^2 + (py - (Y1[i]+t*dy))^2
    j <- which.min(dd)
    list(e = i[j], off = sqrt(dd[j]), t = t[j], len = w[i[j]])
  }
  fl$cell <- cellk(fl$x, fl$y)
  spl <- split(seq_len(nrow(n5)), n5$id)
  res <- rep(NA_real_, nrow(fl)); soff <- rep(NA_real_, nrow(fl))
  for (rows in split(seq_len(nrow(fl)), fl$cell)) {
    cx <- fl$x[rows[1]] %/% NET_CELL; cy <- fl$y[rows[1]] %/% NET_CELL
    q  <- as.vector(outer((cx-NET_RING):(cx+NET_RING)*1e5, (cy-NET_RING):(cy+NET_RING), "+"))
    m  <- match(q, uc); m <- m[!is.na(m)]
    if (!length(m)) next
    idx <- unique(unlist(lapply(m, function(j) ei[st[j]:en[j]])))
    f <- from[idx]; t2 <- to[idx]; ww <- w[idx]
    vn <- unique(c(f, t2))
    g <- igraph::graph_from_data_frame(
      data.frame(a = match(f, vn), b = match(t2, vn)), directed = FALSE,
      vertices = data.frame(name = seq_along(vn)))
    for (r in rows) {
      sp <- snap(fl$x[r], fl$y[r], idx); soff[r] <- sp$off
      tg <- spl[[as.character(fl$id[r])]]
      if (is.null(tg)) next
      srcw <- c(sp$t*sp$len, (1-sp$t)*sp$len)
      D <- igraph::distances(g, v = c(match(from[sp$e], vn), match(to[sp$e], vn)),
                             to = igraph::V(g), weights = ww)
      best <- Inf
      for (k in tg) {
        tq <- snap(n5$bx2[k], n5$by2[k], idx)
        if (tq$e == sp$e) {                       # same block: no graph needed
          best <- min(best, sp$off + tq$off + abs(sp$t - tq$t)*sp$len); next
        }
        cand <- outer(srcw, c(tq$t*tq$len, (1-tq$t)*tq$len), "+") +
                D[, c(match(from[tq$e], vn), match(to[tq$e], vn)), drop = FALSE]
        best <- min(best, sp$off + tq$off + min(cand))
      }
      res[r] <- best
    }
  }
  fl$d_other_net <- res; fl$snap_off <- soff
  say("straight-line d_other p50: ", round(quantile(fl$d_other, 0.5)),
      " m; along the road: ", round(quantile(res[is.finite(res)], 0.5)), " m")
  keep <- is.finite(res) & res < fl$d_own/RATIO
  say("survive the road test: ", sum(keep), " of ", nrow(fl),
      "  (dropped: ", sum(is.finite(res) & !keep), " too far by road, ",
      sum(!is.finite(res)), " unreachable)")
  say("of the dropped, ", sum(is.finite(res) & !keep & soff > 250, na.rm = TRUE),
      " sit over 250 m from any road at all, where this test has little to say.")

  # The dropped set is the user-visible payoff of stage 4: a stranger the crow
  # reaches in metres and the car reaches in kilometres. Water and terrain, named.
  duckdb::duckdb_register(con, "netdrop",
    fl[is.finite(res) & !keep, c("id", "d_own", "d_other", "d_other_net")])
  print(DBI::dbGetQuery(con, "
    SELECT a.MAIL_MUN_NAME mun, a.PROV_CODE pr, count(*) n,
           round(median(d.d_other))::INT crow_m, round(median(d.d_other_net))::INT road_m
      FROM netdrop d JOIN Addresses a ON a.ADDR_GUID = d.id
     GROUP BY ALL ORDER BY n DESC LIMIT 12"), width = 200)
  say("Sorted instead by how much further the car goes than the crow -- water and")
  say("terrain, named:")
  print(DBI::dbGetQuery(con, "
    SELECT a.MAIL_MUN_NAME mun, a.PROV_CODE pr, count(*) n,
           round(median(d.d_other))::INT crow_m, round(median(d.d_other_net))::INT road_m
      FROM netdrop d JOIN Addresses a ON a.ADDR_GUID = d.id
     GROUP BY ALL HAVING count(*) >= 5
     ORDER BY median(d.d_other_net) / greatest(median(d.d_other), 1) DESC LIMIT 12"), width = 200)
  duckdb::duckdb_register(con, "netfl",
    fl[keep, c("id", "d_own", "d_other", "d_other_net", "rural")])
  invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE surv AS SELECT * FROM netfl"))
}

# ---------------------------------------------------------------- stage 5 ----
# The street name is the only field on the row that neither the mail side nor the
# geographic side produced, which is what makes it the arbiter. Two 400 m
# neighbourhoods: one around the point, one around the postal code's own centre.
rule("5. VERDICT: where does the street NAR names actually exist?")
invisible(DBI::dbExecute(con, sprintf("CREATE OR REPLACE TEMP TABLE an AS
  SELECT ADDR_GUID id, MAIL_POSTAL_CODE pc, x, y, (x//%1$d)::INT gx, (y//%1$d)::INT gy,
         strip_accents(upper(OFFICIAL_STREET_NAME)) snf
    FROM Addresses WHERE length(MAIL_POSTAL_CODE)=6 AND x IS NOT NULL", NEAR)))
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE f AS
  SELECT s.*, an.pc, an.x, an.y, an.snf FROM surv s JOIN an USING(id)"))
invisible(DBI::dbExecute(con, "CREATE OR REPLACE TEMP TABLE ctr AS
  SELECT f.id, f.snf, median(b.x) mx, median(b.y) my
    FROM f JOIN an b ON b.pc=f.pc AND b.id<>f.id GROUP BY f.id, f.snf"))
nbhd <- function(name, src, xc, yc) invisible(DBI::dbExecute(con, sprintf(
  "CREATE OR REPLACE TEMP TABLE %1$s AS
   WITH probe AS (SELECT s.id, s.snf, s.%3$s px, s.%4$s py,
                         (s.%3$s//%5$d)::INT+dx gx, (s.%4$s//%5$d)::INT+dy gy
                    FROM %2$s s,(SELECT unnest([-1,0,1]) dx),(SELECT unnest([-1,0,1]) dy)),
        j AS (SELECT q.id, b.snf bsnf FROM probe q JOIN an b ON b.gx=q.gx AND b.gy=q.gy
               WHERE b.id<>q.id AND (b.x-q.px)^2+(b.y-q.py)^2 <= %5$d*%5$d)
   SELECT s.id, count(j.id) n_near, count(*) FILTER (WHERE j.bsnf=s.snf) same_st
     FROM %2$s s LEFT JOIN j ON j.id=s.id GROUP BY s.id", name, src, xc, yc, NEAR)))
nbhd("at_point", "f",   "x",  "y")
nbhd("at_home",  "ctr", "mx", "my")
print(DBI::dbGetQuery(con, "
  SELECT count(*) survivors,
         count(*) FILTER (WHERE p.same_st>0 AND h.same_st=0) coordinate_supported,
         count(*) FILTER (WHERE p.same_st=0 AND h.same_st>0) coordinate_contradicted,
         count(*) FILTER (WHERE p.same_st>0 AND h.same_st>0) street_in_both,
         count(*) FILTER (WHERE p.same_st=0 AND h.same_st=0) no_verdict
    FROM at_point p JOIN at_home h USING(id)"))
say("coordinate_supported means the street is at the point and NOT at the postal")
say("code's own addresses -- the coordinate is corroborated and the postal code is")
say("the field to disbelieve. coordinate_contradicted is the reverse, and is the")
say("only set in this file where the COORDINATE is the part to disbelieve.")

rule("   coordinate contradicted, densest neighbourhood first")
print(DBI::dbGetQuery(con, "
  SELECT a.MAIL_POSTAL_CODE pc, a.CIVIC_NO::INT civic, a.OFFICIAL_STREET_NAME sn,
         a.MAIL_MUN_NAME mun, a.CSD_ENG_NAME csd, a.PROV_CODE pr, a.geom_source gs,
         round(s.d_own)::INT own_m, round(s.d_other_net)::INT road_m, p.n_near near_pt
    FROM surv s JOIN at_point p USING(id) JOIN at_home h USING(id)
    JOIN Addresses a ON a.ADDR_GUID = s.id
   WHERE p.same_st=0 AND h.same_st>0 ORDER BY p.n_near DESC LIMIT 20"), width = 200)

rule("   coordinate supported: the postal code is the odd field")
print(DBI::dbGetQuery(con, "
  SELECT a.MAIL_POSTAL_CODE pc, a.CIVIC_NO::INT civic, a.OFFICIAL_STREET_NAME sn,
         a.MAIL_MUN_NAME mun, a.CSD_ENG_NAME csd, a.PROV_CODE pr, a.geom_source gs,
         round(s.d_own)::INT own_m, round(s.d_other_net)::INT road_m
    FROM surv s JOIN at_point p USING(id) JOIN at_home h USING(id)
    JOIN Addresses a ON a.ADDR_GUID = s.id
   WHERE p.same_st>0 AND h.same_st=0 ORDER BY s.d_own DESC LIMIT 15"), width = 200)
