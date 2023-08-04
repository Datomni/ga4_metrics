WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_conversions')}}
),

unioned AS (
    SELECT {{ dbt_date.today() }} AS dashboard_date,
        CONCAT({{ dbt_date.n_days_ago(30) }},' - ',{{ dbt_date.today() }}) AS period,
        {% for medium in var('traffic_source_medium_types') %}
            sum({{ medium | replace(' ', '') }}_visitor_conversions) as total_{{ medium | replace(' ', '') }}_visitor_conversions
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(30) }}

    UNION ALL

    SELECT {{ dbt_date.n_days_ago(1) }} AS dashboard_date,
        CONCAT({{ dbt_date.n_days_ago(60) }},' - ',{{ dbt_date.n_days_ago(31) }}) AS period,
        {% for medium in var('traffic_source_medium_types') %}
            sum({{ medium | replace(' ', '') }}_visitor_conversions) as total_{{ medium | replace(' ', '') }}_visitor_conversions
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM src
    WHERE date <= {{ dbt_date.n_days_ago(31) }} AND
        date >=  {{ dbt_date.n_days_ago(60) }}
    )

SELECT dashboard_date,
        period,
        {% for medium in var('traffic_source_medium_types') %}
            COALESCE(total_{{ medium | replace(' ', '') }}_visitor_conversions, 0) as total_{{ medium | replace(' ', '') }}_visitor_conversions
        {% if not loop.last %}, {% endif %}
        {% endfor %}
FROM unioned