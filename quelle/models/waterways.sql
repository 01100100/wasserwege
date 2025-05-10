-- This model extracts all waterways with names and their geometries
-- config parameters ensure the model is materialized as a table and writes to a
-- Parquet file
{{
    config(
        materialized="table",
        file_format="parquet",
        location_root="../data/processed/",
    )
}}

with
    waterway_features as (
        select
            osm_id as id,
            osm_type as feature_type,
            geometry as geom,
            tags['name'] as waterway_name,
            tags['waterway'] as waterway_type
        from {{ source("osm", "waterways") }}
        where tags['name'] is not null
    )

select *
from waterway_features
