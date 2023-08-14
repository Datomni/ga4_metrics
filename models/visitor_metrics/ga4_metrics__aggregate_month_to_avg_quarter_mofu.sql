WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_conversions')}}
),

visitor_conversions_last30_days AS (
    SELECT {{ dbt_date.today() }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(30) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_visitor_conversions) as monthly_avg_{{ medium | replace(' ', '_') }}_visitor_conversions,
            {% endfor %}
            sum(other_visitor_conversions) as monthly_avg_other_visitor_conversions
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(30) }}
),

visitor_conversions_last90_days AS (
    SELECT {{ dbt_date.n_days_ago(1) }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(90) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_visitor_conversions)/3 as monthly_avg_{{ medium | replace(' ', '_') }}_visitor_conversions,
            {% endfor %}
            sum(other_visitor_conversions)/3 as monthly_avg_other_visitor_conversions
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(90) }}
),

visitor_conversions_last180_days AS (
    SELECT {{ dbt_date.n_days_ago(2) }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(180) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_visitor_conversions)/6 as monthly_avg_{{ medium | replace(' ', '_') }}_visitor_conversions,
            {% endfor %}
            sum(other_visitor_conversions)/6 as monthly_avg_other_visitor_conversions
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(180) }}
),

unioned AS (
    SELECT *
    FROM visitor_conversions_last30_days

    UNION ALL

    SELECT *
    FROM visitor_conversions_last90_days

    UNION ALL

    SELECT *
    FROM visitor_conversions_last180_days
)

SELECT dashboard_date,
        period,
        {% for medium in var('traffic_source_medium_types') %}
            COALESCE(monthly_avg_{{ medium | replace(' ', '_') }}_visitor_conversions, 0) as monthly_avg_{{ medium | replace(' ', '_') }}_visitor_conversions,
        {% endfor %}
        COALESCE(other_visitor_conversions, 0) as monthly_avg_other_visitor_conversions
FROM unioned
