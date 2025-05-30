with source as (
    select * from {{ source('pc_fivetran_db', 'hex_brand_cuisine_export') }}
)

, final as (
    select 
        restaurant_id
        , cuisines

    from source
)

select * from final
