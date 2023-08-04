WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu')}}
),

traffic_last30_days AS (
    SELECT {{ dbt_date.today() }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(30) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_traffic_unique) as monthly_avg_{{ medium | replace(' ', '_') }}_traffic
            {% if not loop.last %}, {% endif %}
            {% endfor %}
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(30) }}
),

traffic_last90_days AS (
    SELECT {{ dbt_date.n_days_ago(1) }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(90) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_traffic_unique)/3 as monthly_avg_{{ medium | replace(' ', '_') }}_traffic
            {% if not loop.last %}, {% endif %}
            {% endfor %}
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(90) }}
),

traffic_last180_days AS (
    SELECT {{ dbt_date.n_days_ago(2) }} AS dashboard_date,
           CONCAT({{ dbt_date.n_days_ago(180) }},' - ',{{ dbt_date.today() }}) AS period,
           {% for medium in var('traffic_source_medium_types') %}
                sum({{ medium | replace(' ', '_') }}_traffic_unique)/6 as monthly_avg_{{ medium | replace(' ', '_') }}_traffic
            {% if not loop.last %}, {% endif %}
            {% endfor %}
    FROM src
    WHERE date >= {{ dbt_date.n_days_ago(180) }}
),

unioned AS (
    SELECT *
    FROM traffic_last30_days

    UNION ALL

    SELECT *
    FROM traffic_last90_days

    UNION ALL

    SELECT *
    FROM traffic_last180_days
)

SELECT dashboard_date,
        period,
        {% for medium in var('traffic_source_medium_types') %}
            COALESCE(monthly_avg_{{ medium | replace(' ', '_') }}_traffic, 0) as monthly_avg_{{ medium | replace(' ', '_') }}_traffic
        {% if not loop.last %}, {% endif %}
        {% endfor %}
FROM unioned
