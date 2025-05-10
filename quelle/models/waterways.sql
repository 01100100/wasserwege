-- This model extracts all waterways with names and their geometries
-- config parameters ensure the model is materialized as a table and writes to a
-- Parquet file, there is a post-hook that creates a geospatial index to speed up
-- spatial queries.
-- ref: https://duckdb.org/docs/stable/extensions/spatial/r-tree_indexes.html
{{
    config(
        materialized="table",
        file_format="parquet",
        location_root="../data/processed/",
        pre_hook="DROP INDEX IF EXISTS waterways_geom_idx;",
        post_hook="CREATE INDEX waterways_geom_idx ON {{ this }} USING RTREE (geom);",
    )
}}
with
    waterway_features as (
        select
            osm_id as id,
            osm_type as feature_type,
            st_geomfromwkb(geometry) as geom,
            tags['name'] as waterway_name,
            tags['waterway'] as waterway_type
        from {{ source("osm", "waterways") }}
        where tags['name'] is not null
    )

select *
from waterway_features
