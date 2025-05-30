with source as (
    select * from {{ source('pc_fivetran_db', 'hex_case_gsc_export') }}
)

, final as (
    select 
        restaurant_id
        , domain
        , _updated_at as updated_at
        , _created_at as created_at
        , status
        , data 

    from source
)

 select * from final
 