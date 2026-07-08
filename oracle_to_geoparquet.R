#!/usr/bin/env Rscript
# =============================================================================
# oracle_to_geoparquet.R
#
# Export Oracle Spatial (SDO_GEOMETRY) tables to GeoParquet for the geocode
# quality gate.
#
# Approach:
#   * Convert SDO_GEOMETRY -> WKB *inside Oracle* with SDO_UTIL.TO_WKBGEOMETRY.
#     WKB (a BLOB) is compact, lossless and reconstructs directly into sf. This
#     keeps the heavy geometry serialisation on the DB server.
#   * Optionally push a bounding-box filter into Oracle via SDO_FILTER so the
#     spatial index does the work and only rows in your area of interest come
#     back over the wire.
#   * Fetch in chunks (streaming) and rebuild an sf object, then write GeoParquet
#     with sfarrow.
#
# Run this once per reference table (state, locality, meshblock, roads). The
# quality gate then reads the local GeoParquet files -- far faster than querying
# Oracle on every batch.
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(odbc)        # DBI backend via the Oracle ODBC driver
  library(sf)
  library(sfarrow)     # st_write_parquet -> GeoParquet
})

# =============================================================================
# CONFIG
# =============================================================================
cfg <- list(

  ## ---- connection (prefer env vars over hard-coding secrets) ---------------
  ## Two ways to connect, pick one:
  ##  (A) a pre-configured ODBC DSN (in odbc.ini / ODBC Data Source Admin):
  dsn     = Sys.getenv("ORA_DSN"),          # e.g. "ORA_PROD"; "" to use (B)
  user    = Sys.getenv("ORA_USER"),
  pass    = Sys.getenv("ORA_PASS"),
  ##  (B) a full DSN-less connection string (overrides A if non-empty). e.g.
  ##  "Driver={Oracle in instantclient_21_9};DBQ=host:1521/service;UID=..;PWD=.."
  connection_string = Sys.getenv("ORA_CONN_STR"),

  ## ---- what to export ------------------------------------------------------
  # one entry per table. attr_cols = non-geometry columns to keep.
  exports = list(
    list(table = "GIS.ASGS_STATE",     geom = "GEOM",
         attr_cols = c("STE_NAME"),  out = "ref/asgs_state.parquet"),
    list(table = "GIS.ASGS_LOCALITY",  geom = "GEOM",
         attr_cols = c("LOC_NAME"),  out = "ref/asgs_locality.parquet"),
    list(table = "GIS.ASGS_MESHBLOCK", geom = "GEOM",
         attr_cols = c("MB_CODE"),   out = "ref/asgs_meshblock.parquet"),
    list(table = "GIS.ROADS",          geom = "GEOM",
         attr_cols = c("ROAD_NAME"), out = "ref/roads_centrelines.parquet")
  ),

  ## ---- SRID handling -------------------------------------------------------
  # Oracle SRIDs usually equal EPSG for modern GDA codes (4283 GDA94, 7844
  # GDA2020, 4326 WGS84) but some legacy Oracle SRIDs (8307, 8311) do NOT.
  # Force the EPSG here if the stored SDO_SRID can't be trusted; NULL = read
  # SDO_SRID from the table and assume it equals EPSG.
  srid_epsg_override = NULL,     # e.g. 7844

  ## ---- optional spatial pushdown (only export rows intersecting this bbox) --
  # coordinates must be in the table's own SRID. NULL = export the whole table.
  bbox = NULL,                  # c(xmin, ymin, xmax, ymax)

  ## ---- circular-arc handling ----------------------------------------------
  # WKB cannot represent Oracle circular arcs. If a layer contains arcs, set a
  # densify tolerance (in SRID units) to stroke them to line segments first.
  arc_densify_tol = NULL,       # e.g. 0.5  (metres, or degrees for geographic)

  ## ---- streaming -----------------------------------------------------------
  fetch_chunk = 50000L
)

# =============================================================================
# Helpers
# =============================================================================

get_srid <- function(con, table, geom) {
  q <- sprintf("SELECT t.%s.SDO_SRID AS SRID FROM %s t WHERE ROWNUM = 1",
               geom, table)
  s <- DBI::dbGetQuery(con, q)$SRID
  if (length(s) == 0 || is.na(s[1])) NA_integer_ else as.integer(s[1])
}

# build the geometry SQL expression, optionally densifying arcs, always -> WKB
geom_expr <- function(geom) {
  g <- sprintf("t.%s", geom)
  if (!is.null(cfg$arc_densify_tol)) {
    g <- sprintf("SDO_GEOM.SDO_ARC_DENSIFY(%s, %s, 'arc_tolerance=%s')",
                 g, "0.005", cfg$arc_densify_tol)
  }
  sprintf("SDO_UTIL.TO_WKBGEOMETRY(%s)", g)
}

# optional SDO_FILTER predicate against a bbox window in the table's SRID
bbox_predicate <- function(geom, srid, bbox) {
  if (is.null(bbox)) return("")
  win <- sprintf(
    paste0("SDO_GEOMETRY(2003,%s,NULL,",
           "SDO_ELEM_INFO_ARRAY(1,1003,3),",
           "SDO_ORDINATE_ARRAY(%s,%s,%s,%s))"),
    ifelse(is.na(srid), "NULL", srid),
    bbox[1], bbox[2], bbox[3], bbox[4]
  )
  sprintf(" WHERE SDO_FILTER(t.%s, %s) = 'TRUE'", geom, win)
}

# turn a fetched chunk (attrs + WKB blob list) into an sf object
chunk_to_sf <- function(df, wkb_col, epsg) {
  # odbc returns a BLOB as a `blob`/list column; coerce to a plain list of raw
  wkb <- as.list(df[[wkb_col]])
  keep <- !vapply(wkb, is.null, logical(1)) &
          vapply(wkb, function(x) length(x) > 0, logical(1))
  df  <- df[keep, , drop = FALSE]
  wkb <- wkb[keep]
  if (nrow(df) == 0) return(NULL)
  geom <- sf::st_as_sfc(structure(wkb, class = "WKB"), EWKB = FALSE)
  attrs <- df[, setdiff(names(df), wkb_col), drop = FALSE]
  sf::st_sf(attrs, geometry = geom, crs = epsg)
}

export_one <- function(con, spec) {
  message(sprintf("\n== %s -> %s", spec$table, spec$out))

  srid <- get_srid(con, spec$table, spec$geom)
  epsg <- if (!is.null(cfg$srid_epsg_override)) cfg$srid_epsg_override else srid
  message(sprintf("   SDO_SRID=%s  -> using EPSG=%s",
                  ifelse(is.na(srid), "NA", srid),
                  ifelse(is.null(epsg) || is.na(epsg), "NA (set manually!)", epsg)))

  cols <- paste(sprintf("t.%s", spec$attr_cols), collapse = ", ")
  sql  <- sprintf("SELECT %s, %s AS WKB FROM %s t%s",
                  cols, geom_expr(spec$geom), spec$table,
                  bbox_predicate(spec$geom, srid, cfg$bbox))

  rs <- DBI::dbSendQuery(con, sql)
  parts <- list(); i <- 0L; nrows <- 0L
  repeat {
    df <- DBI::dbFetch(rs, n = cfg$fetch_chunk)
    if (nrow(df) == 0) break
    i <- i + 1L
    sfx <- chunk_to_sf(df, "WKB", epsg)
    if (!is.null(sfx)) { parts[[length(parts) + 1L]] <- sfx; nrows <- nrows + nrow(sfx) }
    message(sprintf("   fetched chunk %d (%d rows, %d kept so far)",
                    i, nrow(df), nrows))
  }
  DBI::dbClearResult(rs)

  if (length(parts) == 0) { message("   no rows -- skipped"); return(invisible()) }
  out_sf <- do.call(rbind, parts)

  dir.create(dirname(spec$out), recursive = TRUE, showWarnings = FALSE)
  sfarrow::st_write_parquet(out_sf, spec$out)
  message(sprintf("   wrote %d features -> %s", nrow(out_sf),
                  normalizePath(spec$out)))
}

# =============================================================================
# Driver
# =============================================================================
connect <- function() {
  if (nzchar(cfg$connection_string)) {
    # (B) DSN-less full connection string
    DBI::dbConnect(odbc::odbc(), .connection_string = cfg$connection_string)
  } else {
    # (A) named DSN + credentials
    stopifnot(nzchar(cfg$dsn))
    DBI::dbConnect(odbc::odbc(), dsn = cfg$dsn,
                   uid = cfg$user, pwd = cfg$pass)
  }
}

run <- function() {
  con <- connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (spec in cfg$exports) {
    tryCatch(export_one(con, spec),
             error = function(e)
               message(sprintf("   !! FAILED %s: %s", spec$table, conditionMessage(e))))
  }
  message("\nDone.")
}

if (sys.nframe() == 0L) run()
