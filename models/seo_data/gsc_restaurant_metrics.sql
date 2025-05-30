{{ config(
    materialized='table'
) }}

{% set metric_types = ['Overall', 'Branded', 'Unbranded'] %}
{% set metrics = [
    'unique_search_terms',
    'total_clicks',
    'total_impressions',
    'avg_ctr',
    'avg_position',
    'median_ctr',
    'median_position',
    'clicks_top_3',
    'clicks_top_10',
    'clicks_top_20',
    'terms_top_3',
    'terms_top_10',
    'terms_top_20'
] %}

{% set metric_aliases = {
    'unique_search_terms': 'unique_terms',
    'total_clicks': 'clicks',
    'total_impressions': 'impressions'
} %}

{% set round_metrics = [
    'avg_ctr',
    'avg_position',
    'median_ctr',
    'median_position'
] %}

with source as (
    select * from {{ ref('int_gsc_search_terms') }}
)

, cuisine_data as (
    select * from {{ ref('stg_hex_brand_cuisine_export') }}
)

, metrics as (
    select
        restaurant_id
        , domain
        , search_type
        , count(distinct search_term) as unique_search_terms
        , sum(clicks) as total_clicks
        , sum(impressions) as total_impressions
        , round(avg(ctr), 2) as avg_ctr
        , round(avg(avg_position), 2) as avg_position
        , round(median(ctr), 2) as median_ctr
        , round(median(avg_position), 2) as median_position
        , sum(case when avg_position <= 3 then clicks else 0 end) as clicks_top_3
        , sum(case when avg_position <= 10 then clicks else 0 end) as clicks_top_10
        , sum(case when avg_position <= 20 then clicks else 0 end) as clicks_top_20
        , count(case when avg_position <= 3 then 1 end) as terms_top_3
        , count(case when avg_position <= 10 then 1 end) as terms_top_10
        , count(case when avg_position <= 20 then 1 end) as terms_top_20
    
    from source
    
    group by 1, 2, 3
)

, metrics_with_overall as (
    select 
        restaurant_id
        , domain
        , search_type
        , unique_search_terms
        , total_clicks
        , total_impressions
        , avg_ctr
        , avg_position
        , median_ctr
        , median_position
        , clicks_top_3
        , clicks_top_10
        , clicks_top_20
        , terms_top_3
        , terms_top_10
        , terms_top_20
    
    from metrics
    
    union all
    
    select 
        restaurant_id
        , domain
        , 'Overall' as search_type
        , sum(unique_search_terms) as unique_search_terms
        , sum(total_clicks) as total_clicks
        , sum(total_impressions) as total_impressions
        , avg(avg_ctr) as avg_ctr
        , avg(avg_position) as avg_position
        , avg(median_ctr) as median_ctr
        , avg(median_position) as median_position
        , sum(clicks_top_3) as clicks_top_3
        , sum(clicks_top_10) as clicks_top_10
        , sum(clicks_top_20) as clicks_top_20
        , sum(terms_top_3) as terms_top_3
        , sum(terms_top_10) as terms_top_10
        , sum(terms_top_20) as terms_top_20
    
    from metrics
    
    group by 1, 2
)

, pivoted as (
    select
        restaurant_id
        , domain
        {% for metric_type in metric_types %}
        -- {{ metric_type }} metrics
        {% for metric in metrics %}
        , {% if metric in ['avg_ctr', 'avg_position', 'median_ctr', 'median_position'] %}
            avg(case when search_type = '{{ metric_type }}' then {{ metric }} end)
        {% else %}
            sum(case when search_type = '{{ metric_type }}' then {{ metric }} end)
        {% endif %} as {{ metric_type.lower() }}_{{ metric_aliases.get(metric, metric) }}
        {% endfor %}
        {% endfor %}
    from metrics_with_overall
    group by 1, 2
)

, final as (
    select
        p.restaurant_id
        , p.domain
        , c.cuisines
        {% for metric_type in metric_types %}
        -- {{ metric_type }} metrics
        {% for metric in metrics %}
        {% if metric in round_metrics %}
        , round({{ metric_type.lower() }}_{{ metric_aliases.get(metric, metric) }}, 2) as {{ metric_type.lower() }}_{{ metric_aliases.get(metric, metric) }}
        {% else %}
        , {{ metric_type.lower() }}_{{ metric_aliases.get(metric, metric) }}
        {% endif %}
        {% endfor %}
        {% endfor %}
        -- Calculated metrics
        , round(branded_clicks::float / nullif(overall_clicks, 0), 2) as branded_clicks_pct
        , round(unbranded_clicks::float / nullif(overall_clicks, 0), 2) as unbranded_clicks_pct
        , round(branded_impressions::float / nullif(overall_impressions, 0), 2) as branded_impressions_pct
        , round(unbranded_impressions::float / nullif(overall_impressions, 0), 2) as unbranded_impressions_pct
        , round(branded_unique_terms::float / nullif(overall_unique_terms, 0), 2) as branded_search_terms_pct
        , round(unbranded_unique_terms::float / nullif(overall_unique_terms, 0), 2) as unbranded_search_terms_pct
    
    from pivoted p
    
    left join cuisine_data c
        on p.restaurant_id = c.restaurant_id
)

select * from final 
