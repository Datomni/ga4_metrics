WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu')}}
),

unioned AS (
    SELECT {{ dbt_date.today() }} AS dashboard_date,
        CONCAT({{ dbt_date.n_days_ago(365) }},' - ',{{ dbt_date.today() }}) AS period,
        {% for medium in var('traffic_source_medium_types') %}
            sum({{ medium | replace(' ', '_') }}_traffic_unique) as total_{{ medium | replace(' ', '_') }}_traffic,
        {% endfor %}
        sum(other_traffic_unique) as total_other_traffic
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(365) }}

    UNION ALL

    SELECT {{ dbt_date.n_days_ago(1) }} AS dashboard_date,
        CONCAT({{ dbt_date.n_days_ago(730) }},' - ',{{ dbt_date.n_days_ago(366) }}) AS period,
        {% for medium in var('traffic_source_medium_types') %}
            sum({{ medium | replace(' ', '_') }}_traffic_unique) as total_{{ medium | replace(' ', '_') }}_traffic,
        {% endfor %}
        sum(other_traffic_unique) as total_other_traffic
    FROM src
    WHERE date <= {{ dbt_date.n_days_ago(366) }} AND
        date >= {{ dbt_date.n_days_ago(730) }}
)

SELECT dashboard_date,
        period,
        {% for medium in var('traffic_source_medium_types') %}
            COALESCE(total_{{ medium | replace(' ', '_') }}_traffic, 0) as total_{{ medium | replace(' ', '_') }}_traffic,
        {% endfor %}
        COALESCE(total_other_traffic, 0) as total_other_traffic
FROM unioned
