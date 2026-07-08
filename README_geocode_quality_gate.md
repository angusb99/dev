# Geocode quality gate (R)

Validates captured `latitude`/`longitude` against tokenised `state`, `locality`,
and `street_name` fields, using a hierarchical, spatially-refined procedure so
the expensive string comparison only runs against a few candidate roads.

## Pipeline

| Step | Layer | Test | Output columns |
|---|---|---|---|
| 1 | State polygons | point-in-polygon vs claimed state (hard gate) | `actual_state`, `state_match` |
| 2 | Locality polygons | point-in-polygon vs claimed locality, with JW fallback on the containing polygon's name | `actual_locality`, `locality_match`, `locality_jw` |
| 3 | Meshblock polygons | point-in-polygon → smallest unit (narrows search space) | `meshblock_id` |
| 4 | Road centrelines | candidate roads within `road_search_radius_m` → best Jaro-Winkler vs claimed street | `matched_road`, `road_dist_m`, `street_jw` |

Each record gets a `quality_score` (0–1, weighted) and a `verdict` of
**PASS / REVIEW / FAIL**.

## Why it's efficient

- **Streaming**: parquet is read in record batches (`batch_size`), so memory is
  bounded regardless of dataset size. One output parquet part is written per batch.
- **Spatial index everywhere**: every containment test is `st_join(..., st_within)`
  and road candidacy is `st_is_within_distance` — both use sf's STRtree/s2 index,
  so it's ~O(n log m), not O(n·m).
- **Radius = the search-space cut**: instead of comparing each point to every road,
  only roads within a small radius (default 120 m) become candidates. The meshblock
  assignment gives the "smallest unit" provenance you wanted.
- **Vectorised strings**: candidate (point→road) pairs are unnested into one long
  vector and scored with a single vectorised `stringsim` call, then reduced to the
  best road per point.
- **Street-type normalisation**: `ST`/`STREET`, `RD`/`ROAD`, etc. are standardised
  before Jaro-Winkler so abbreviations don't depress scores.

## Setup

```r
install.packages(c("arrow", "sf", "dplyr", "stringdist"))
```

Edit the `cfg` block at the top of `geocode_quality_gate.R`:

- Input/output paths.
- Reference layer paths + the attribute column holding each layer's name/id.
- Input parquet field names (`lat_col`, `lon_col`, `in_state_col`, ...).
- `input_crs` (4326 WGS84 or 7844 GDA2020) and `working_crs` (3577 GDA2020
  Australian Albers — equal-area metres, good for national distance work).
- Thresholds (`street_jw_pass`, `street_jw_review`, `locality_jw_soft`) and the
  composite weights.

## Run

```bash
Rscript geocode_quality_gate.R
```

Writes `out/quality_gate/part-*.parquet` and prints a PASS/REVIEW/FAIL summary.
Read the results back as a dataset:

```r
arrow::open_dataset("out/quality_gate") |> dplyr::collect()
```

## Scaling further (if the road layer is national + huge)

The road index is rebuilt per batch. If that becomes the bottleneck, either
(a) spatially sort the input parquet so each batch covers a small bbox and add a
bbox pre-crop of roads per batch, or (b) push steps 1–4 into **DuckDB spatial**
(`ST_Within`, `ST_DWithin`) and keep only the Jaro-Winkler reduction in R.

## Notes / assumptions

- State is treated as a hard gate: a state mismatch (or invalid coordinates) is an
  automatic FAIL.
- `locality_match` is TRUE if the point is contained in the claimed locality **or**
  the containing polygon's name is a fuzzy match (`locality_jw_soft`), which absorbs
  boundary noise and minor spelling differences.
- A record with no road inside the radius keeps `street_jw = NA` (nothing nearby to
  match) and will land in REVIEW rather than PASS.
- Reference layers as `.parquet` are assumed to be GeoParquet with a geometry column.
