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

with restaurant_metrics as (
    select * from {{ ref('gsc_restaurant_metrics') }}
)

, cuisine_unnest as (
    select
        r.*
        , trim(c.value::string) as cuisine
    from restaurant_metrics r
    , lateral flatten(input => parse_json(r.cuisines)) as c
)

, metrics as (
    select
        cuisine
        , count(distinct restaurant_id) as restaurant_count
        , sum(overall_unique_terms) as unique_search_terms
        , sum(overall_clicks) as total_clicks
        , sum(overall_impressions) as total_impressions
        , avg(overall_avg_ctr) as avg_ctr
        , avg(overall_avg_position) as avg_position
        , avg(overall_median_ctr) as median_ctr
        , avg(overall_median_position) as median_position
        , sum(overall_clicks_top_3) as clicks_top_3
        , sum(overall_clicks_top_10) as clicks_top_10
        , sum(overall_clicks_top_20) as clicks_top_20
        , sum(overall_terms_top_3) as terms_top_3
        , sum(overall_terms_top_10) as terms_top_10
        , sum(overall_terms_top_20) as terms_top_20
    from cuisine_unnest
    group by 1
)

, branded_metrics as (
    select
        cuisine
        , sum(branded_unique_terms) as unique_search_terms
        , sum(branded_clicks) as total_clicks
        , sum(branded_impressions) as total_impressions
        , avg(branded_avg_ctr) as avg_ctr
        , avg(branded_avg_position) as avg_position
        , avg(branded_median_ctr) as median_ctr
        , avg(branded_median_position) as median_position
        , sum(branded_clicks_top_3) as clicks_top_3
        , sum(branded_clicks_top_10) as clicks_top_10
        , sum(branded_clicks_top_20) as clicks_top_20
        , sum(branded_terms_top_3) as terms_top_3
        , sum(branded_terms_top_10) as terms_top_10
        , sum(branded_terms_top_20) as terms_top_20
    from cuisine_unnest
    group by 1
)

, unbranded_metrics as (
    select
        cuisine
        , sum(unbranded_unique_terms) as unique_search_terms
        , sum(unbranded_clicks) as total_clicks
        , sum(unbranded_impressions) as total_impressions
        , avg(unbranded_avg_ctr) as avg_ctr
        , avg(unbranded_avg_position) as avg_position
        , avg(unbranded_median_ctr) as median_ctr
        , avg(unbranded_median_position) as median_position
        , sum(unbranded_clicks_top_3) as clicks_top_3
        , sum(unbranded_clicks_top_10) as clicks_top_10
        , sum(unbranded_clicks_top_20) as clicks_top_20
        , sum(unbranded_terms_top_3) as terms_top_3
        , sum(unbranded_terms_top_10) as terms_top_10
        , sum(unbranded_terms_top_20) as terms_top_20
    from cuisine_unnest
    group by 1
)

, pivoted as (
    select
        m.cuisine
        , m.restaurant_count
        {% for metric_type in metric_types %}
        -- {{ metric_type }} metrics
        {% for metric in metrics %}
        , {% if metric_type == 'Overall' %}
            m.{{ metric }}
        {% elif metric_type == 'Branded' %}
            b.{{ metric }}
        {% else %}
            u.{{ metric }}
        {% endif %} as {{ metric_type.lower() }}_{{ metric_aliases.get(metric, metric) }}
        {% endfor %}
        {% endfor %}
    from metrics m
    left join branded_metrics b
        on m.cuisine = b.cuisine
    left join unbranded_metrics u
        on m.cuisine = u.cuisine
)

, final as (
    select
        cuisine
        , restaurant_count
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
    
    from pivoted
)

select * from final 