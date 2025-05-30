{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ ref('stg_hex_case_gsc_export') }}
)

, parsed_data as (
    select
        restaurant_id
        , domain
        , updated_at
        , created_at
        , status
        , parse_json(data):rows as gsc_rows
    from source
)

, unnested_rows as (
    select
        restaurant_id
        , domain
        , updated_at
        , created_at
        , status
        , value:keys[0]::string as search_term
        , value:clicks::integer as clicks
        , value:impressions::integer as impressions
        , value:ctr::float as ctr
        , value:position::float as position
    from parsed_data
        , lateral flatten(input => gsc_rows) as t(value)
)

, final as (
    select
        restaurant_id
        , domain
        , updated_at
        , created_at
        , status
        , search_term
        , clicks
        , impressions
        , round(ctr, 2) as ctr
        , round(position, 2) as avg_position
        -- Calculate additional metrics
        , case 
            when clicks > 0 then true 
            else false 
        end as has_clicks
        , case 
            when position <= 3 then 'Top 3'
            when position <= 10 then 'Top 10'
            when position <= 20 then 'Top 20'
            else 'Below 20'
        end as position_bucket
        -- Branded vs Unbranded classification
        , case
            when jarowinkler_similarity(lower(search_term), lower(regexp_replace(domain, '^www\.|\.com$', ''))) > 70 then 'Branded'
            else 'Unbranded'
        end as search_type
        , round(jarowinkler_similarity(lower(search_term), lower(regexp_replace(domain, '^www\.|\.com$', ''))), 2) as brand_similarity_score
    from unnested_rows
)

select * from final
