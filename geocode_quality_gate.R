#!/usr/bin/env Rscript
# =============================================================================
# geocode_quality_gate.R
#
# Quality gate for captured geocodes (lat/lon) against tokenised address fields
# (state, locality, street name).  Australian ABS/G-NAF context.
#
# Strategy: hierarchical, largest -> smallest, spatially refining the search
# space at each step so the expensive string comparison only runs against a
# handful of candidate roads.
#
#   1. STATE     point-in-polygon vs claimed state          (coarse gate)
#   2. LOCALITY  point-in-polygon vs claimed locality       (medium gate)
#   3. MESHBLOCK point-in-polygon -> smallest unit          (search-space cut)
#   4. ROAD      candidate roads within a small radius of   (fine gate)
#                the point -> Jaro-Winkler vs claimed street
#
# Every geometric step uses sf's built-in spatial index (STRtree/s2), and the
# string comparison is vectorised (stringdist).  Input is streamed from parquet
# in record batches so memory stays bounded on large datasets.
#
# Output: one parquet part per input batch, carrying the original columns plus
# diagnostic/score columns and a PASS / REVIEW / FAIL verdict, plus a printed
# summary.
# =============================================================================

suppressPackageStartupMessages({
  library(arrow)      # parquet IO + streaming
  library(sf)         # geometry + spatial joins/index
  library(dplyr)      # data wrangling
  library(stringdist) # vectorised Jaro-Winkler
})

# =============================================================================
# CONFIG  --  edit this block, nothing below should need changing
# =============================================================================
cfg <- list(

  ## ---- input / output ------------------------------------------------------
  input_parquet   = "data/geocodes.parquet",       # file OR directory (dataset)
  output_dir      = "out/quality_gate",            # parquet parts written here
  overwrite_out   = TRUE,

  ## ---- reference layers (paths to shapefiles / gpkg / parquet) -------------
  state_path      = "ref/asgs_state.shp",          # STATE polygons
  locality_path   = "ref/asgs_locality.shp",       # LOCALITY / suburb polygons
  meshblock_path  = "ref/asgs_meshblock.shp",      # MESHBLOCK polygons (smallest)
  roads_path      = "ref/roads_centrelines.shp",   # named road centrelines

  ## name of the attribute holding the *name* in each reference layer
  state_name_col     = "STE_NAME",
  locality_name_col  = "LOC_NAME",
  meshblock_id_col   = "MB_CODE",
  road_name_col      = "ROAD_NAME",

  ## ---- input parquet field names -------------------------------------------
  lat_col         = "latitude",
  lon_col         = "longitude",
  in_state_col    = "state",
  in_locality_col = "locality",
  in_street_col   = "street_name",
  id_col          = NULL,        # optional stable row id; NULL -> generated

  ## ---- CRS -----------------------------------------------------------------
  input_crs   = 4326,   # CRS the lat/lon are stored in (4326 WGS84 / 7844 GDA2020)
  working_crs = 3577,   # projected, metres. GDA2020 Australian Albers = equal area

  ## ---- matching parameters -------------------------------------------------
  road_search_radius_m = 120,   # only roads within this of the point are candidates
  jw_p                 = 0.10,  # Jaro-Winkler prefix weight

  ## ---- decision thresholds -------------------------------------------------
  street_jw_pass   = 0.90,      # street name considered a match at/above this
  street_jw_review = 0.80,      # between review & pass -> REVIEW
  locality_jw_soft = 0.90,      # fuzzy locality-name agreement when not contained

  ## ---- composite weights (used for a 0-1 quality score) --------------------
  w_state    = 0.30,
  w_locality = 0.30,
  w_street   = 0.40,

  ## ---- streaming -----------------------------------------------------------
  batch_size = 100000L          # rows per record batch
)

# =============================================================================
# Address normalisation
# =============================================================================
# Standardise strings before comparison: uppercase, strip punctuation, collapse
# whitespace, and normalise common Australian street-type abbreviations so
# "ST" and "STREET" don't look different to Jaro-Winkler.
AU_STREET_TYPES <- c(
  "ROAD"="RD","STREET"="ST","AVENUE"="AVE","DRIVE"="DR","COURT"="CT",
  "PLACE"="PL","LANE"="LN","CRESCENT"="CRES","HIGHWAY"="HWY","PARADE"="PDE",
  "TERRACE"="TCE","CLOSE"="CL","BOULEVARD"="BLVD","CIRCUIT"="CCT","ESPLANADE"="ESP",
  "GROVE"="GR","SQUARE"="SQ","WAY"="WAY","TRACK"="TRK","PARKWAY"="PWY"
)

normalise_str <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# collapse full street-type words to their abbreviation (token-wise)
normalise_street <- function(x) {
  x <- normalise_str(x)
  # replace whole-word occurrences of the long form with the short form
  for (long in names(AU_STREET_TYPES)) {
    x <- gsub(paste0("\\b", long, "\\b"), AU_STREET_TYPES[[long]], x)
  }
  x
}

jw_sim <- function(a, b, p = cfg$jw_p) {
  # 1 = identical, 0 = disjoint. NA-safe.
  stringdist::stringsim(a, b, method = "jw", p = p)
}

# =============================================================================
# Reference-layer loading (once)
# =============================================================================
read_layer <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext == "parquet") {
    df <- arrow::read_parquet(path)
    sf::st_as_sf(df)          # assumes a geometry/wkb column
  } else {
    sf::st_read(path, quiet = TRUE)
  }
}

load_reference <- function() {
  message("Loading reference layers ...")
  to_wc <- function(x) sf::st_transform(sf::st_make_valid(x), cfg$working_crs)

  st   <- to_wc(read_layer(cfg$state_path))
  loc  <- to_wc(read_layer(cfg$locality_path))
  mb   <- to_wc(read_layer(cfg$meshblock_path))
  rd   <- to_wc(read_layer(cfg$roads_path))

  # keep only the attribute we need + normalised name, drop the rest to save RAM
  st$.state_name    <- normalise_str(st[[cfg$state_name_col]])
  loc$.loc_name     <- normalise_str(loc[[cfg$locality_name_col]])
  mb$.mb_id         <- as.character(mb[[cfg$meshblock_id_col]])
  rd$.road_name     <- normalise_street(rd[[cfg$road_name_col]])

  st  <- st[, ".state_name"]
  loc <- loc[, ".loc_name"]
  mb  <- mb[, ".mb_id"]
  rd  <- rd[rd$.road_name != "" & !is.na(rd$.road_name), ".road_name"]

  list(state = st, locality = loc, meshblock = mb, roads = rd)
}

# =============================================================================
# Core: score one batch of points
# =============================================================================
score_batch <- function(df, ref) {

  n <- nrow(df)

  ## --- build points, drop rows without usable coordinates ------------------
  lat <- suppressWarnings(as.numeric(df[[cfg$lat_col]]))
  lon <- suppressWarnings(as.numeric(df[[cfg$lon_col]]))
  ok  <- is.finite(lat) & is.finite(lon) &
         abs(lat) <= 90 & abs(lon) <= 180

  # normalised claimed fields
  claim_state  <- normalise_str(df[[cfg$in_state_col]])
  claim_loc    <- normalise_str(df[[cfg$in_locality_col]])
  claim_street <- normalise_street(df[[cfg$in_street_col]])

  # result containers (default = NA / FALSE for unusable coords)
  res <- data.frame(
    coords_valid    = ok,
    actual_state    = NA_character_,
    state_match     = NA,
    actual_locality = NA_character_,
    locality_match  = NA,
    locality_jw     = NA_real_,
    meshblock_id    = NA_character_,
    matched_road    = NA_character_,
    road_dist_m     = NA_real_,
    street_jw       = NA_real_,
    stringsAsFactors = FALSE
  )

  if (!any(ok)) return(finalise(df, res, claim_state, claim_loc, claim_street))

  pts <- sf::st_as_sf(
    data.frame(lon = lon[ok], lat = lat[ok]),
    coords = c("lon", "lat"), crs = cfg$input_crs
  )
  pts <- sf::st_transform(pts, cfg$working_crs)
  pts$.pid <- seq_len(nrow(pts))        # stable id so we can dedupe joins
  idx <- which(ok)                      # map pts rows back to df rows

  # st_join is a left join and can emit >1 row per point when polygons overlap;
  # collapse back to one value per point (first match) keyed on .pid.
  first_per_point <- function(j, col) {
    keep <- !duplicated(j$.pid)
    out  <- rep(NA_character_, nrow(pts))
    out[j$.pid[keep]] <- as.character(j[[col]][keep])
    out
  }

  ## --- 1. STATE : point within claimed state -------------------------------
  j_state <- sf::st_join(pts, ref$state, join = sf::st_within)
  actual_state <- first_per_point(j_state, ".state_name")
  res$actual_state[idx] <- actual_state
  res$state_match[idx]  <- !is.na(actual_state) &
                           actual_state == claim_state[idx]

  ## --- 2. LOCALITY : point within claimed locality -------------------------
  j_loc <- sf::st_join(pts, ref$locality, join = sf::st_within)
  actual_loc <- first_per_point(j_loc, ".loc_name")
  res$actual_locality[idx] <- actual_loc
  # hard containment match, plus a fuzzy fallback on the *name* of the polygon
  # the point actually landed in (catches boundary noise / minor spelling diffs)
  res$locality_jw[idx]    <- jw_sim(claim_loc[idx], actual_loc)
  res$locality_match[idx] <- (!is.na(actual_loc) & actual_loc == claim_loc[idx]) |
                             (res$locality_jw[idx] >= cfg$locality_jw_soft)

  ## --- 3. MESHBLOCK : smallest containing unit (search-space provenance) ----
  j_mb <- sf::st_join(pts, ref$meshblock, join = sf::st_within)
  res$meshblock_id[idx] <- first_per_point(j_mb, ".mb_id")

  ## --- 4. ROAD : candidate roads within radius -> best Jaro-Winkler --------
  # spatial index does the heavy lifting: returns, per point, only the road
  # indices lying within road_search_radius_m.
  cand <- sf::st_is_within_distance(
    pts, ref$roads, dist = cfg$road_search_radius_m
  )
  # unnest the sparse (point -> roads) list into a long pair table, score
  # everything vectorised, then take the best road per point.
  lens <- lengths(cand)
  if (any(lens > 0)) {
    p_i  <- rep(seq_along(cand), lens)   # point row (within pts)
    r_i  <- unlist(cand, use.names = FALSE)
    jw   <- jw_sim(claim_street[idx][p_i], ref$roads$.road_name[r_i])
    jw[is.na(jw)] <- 0

    ord  <- order(p_i, -jw)              # best (highest jw) first per point
    best <- !duplicated(p_i[ord])
    bp   <- p_i[ord][best]               # point rows that had >=1 candidate
    br   <- r_i[ord][best]               # winning road row
    bjw  <- jw[ord][best]

    res$matched_road[idx[bp]] <- ref$roads$.road_name[br]
    res$street_jw[idx[bp]]    <- bjw
    # adjacency distance to the winning road (element-wise, vectorised)
    res$road_dist_m[idx[bp]] <- as.numeric(
      sf::st_distance(pts[bp, ], ref$roads[br, ], by_element = TRUE)
    )
  }
  # points with no candidate road inside the radius -> jw stays NA (=no road nearby)

  finalise(df, res, claim_state, claim_loc, claim_street)
}

# =============================================================================
# Composite score + PASS / REVIEW / FAIL verdict
# =============================================================================
finalise <- function(df, res, claim_state, claim_loc, claim_street) {

  # component sub-scores in [0,1]
  s_state <- ifelse(is.na(res$state_match), 0, as.numeric(res$state_match))
  s_loc   <- pmax(
    ifelse(is.na(res$locality_match), 0, as.numeric(res$locality_match)),
    ifelse(is.na(res$locality_jw), 0, res$locality_jw)
  )
  s_str   <- ifelse(is.na(res$street_jw), 0, res$street_jw)

  res$quality_score <- round(
    cfg$w_state * s_state + cfg$w_locality * s_loc + cfg$w_street * s_str, 4
  )

  # verdict logic: state is a hard gate; street JW drives the fine decision
  verdict <- rep("REVIEW", nrow(res))

  fail <- !res$coords_valid |
          (res$state_match %in% FALSE) |
          (is.finite(s_str) & s_str < cfg$street_jw_review &
             (res$locality_match %in% FALSE))

  pass <- res$coords_valid &
          (res$state_match %in% TRUE) &
          (res$locality_match %in% TRUE) &
          (s_str >= cfg$street_jw_pass)

  verdict[fail] <- "FAIL"
  verdict[pass] <- "PASS"
  res$verdict <- verdict

  # attach diagnostics to the original row and echo normalised claims
  out <- cbind(
    df,
    claim_state_norm  = claim_state,
    claim_loc_norm    = claim_loc,
    claim_street_norm = claim_street,
    res
  )
  out
}

# =============================================================================
# Driver: stream parquet -> score -> write parts
# =============================================================================
run <- function() {
  t0 <- Sys.time()

  if (cfg$overwrite_out && dir.exists(cfg$output_dir))
    unlink(cfg$output_dir, recursive = TRUE)
  dir.create(cfg$output_dir, recursive = TRUE, showWarnings = FALSE)

  ref <- load_reference()

  ds     <- arrow::open_dataset(cfg$input_parquet)
  reader <- Scanner$create(ds, batch_size = cfg$batch_size)$ToRecordBatchReader()

  tally <- c(PASS = 0L, REVIEW = 0L, FAIL = 0L)
  part  <- 0L
  repeat {
    batch <- reader$read_next_batch()
    if (is.null(batch)) break
    df <- as.data.frame(batch)
    if (is.null(cfg$id_col))
      df[["._row_id"]] <- part * cfg$batch_size + seq_len(nrow(df))

    scored <- score_batch(df, ref)

    part <- part + 1L
    fn <- file.path(cfg$output_dir, sprintf("part-%05d.parquet", part))
    arrow::write_parquet(scored, fn)

    v <- table(factor(scored$verdict, names(tally)))
    tally <- tally + as.integer(v)
    message(sprintf("  batch %d: %d rows written -> %s",
                    part, nrow(scored), basename(fn)))
  }

  ## --- summary --------------------------------------------------------------
  total <- sum(tally)
  cat("\n================ GEOCODE QUALITY GATE SUMMARY ================\n")
  cat(sprintf("Input      : %s\n", cfg$input_parquet))
  cat(sprintf("Rows scored: %d  (%d batches)\n", total, part))
  cat(sprintf("Elapsed    : %.1f s\n\n", as.numeric(Sys.time() - t0, units = "secs")))
  for (k in names(tally))
    cat(sprintf("  %-7s %8d  (%5.1f%%)\n", k, tally[[k]],
                100 * tally[[k]] / max(total, 1)))
  cat("=============================================================\n")
  cat(sprintf("Output dataset: %s\n", normalizePath(cfg$output_dir)))
  invisible(tally)
}

if (sys.nframe() == 0L) run()
