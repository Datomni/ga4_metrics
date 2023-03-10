# GA4 Metrics dbt Package
## What does this dbt package do?
* Flattens the GA4 events dataset by extracting the event parameter and user property values into their own fields
* Groups events into sessions and surfaces session properties like session length, first and last host url, refferer host, and visitor source channel
* Creates standardised metrics to provide an insight into visitor traffic broken down by source channel
* Provided an event name marking customer conversions, it creates a mapping indicating whether each session resulted in a conversion


Refer to the table below for a detailed view of final models materialized by default within this package.

**Visitor Sessions**
|   model    | description |
|------------|-------------|
|ga4_metrics__events_flattened|Flattend GA4 dataset with the event parameter and user property values|
|ga4_metrics__page_views|Page views with url host and referrers added|
|ga4_metrics__page_views_sessionized|Page views with page view number assigned|
|ga4_metrics__sessions_stitched|Unique sessions and the session attributes enhanced with referrer mapping|
|ga4_metrics__session_conversions|Session identifiers with conversion indicator|


**Traffic Metrics**
|   model    | description |
|------------|-------------|
|ga4_metrics__aggregate_day_tofu|Unique number of daily visitors coming from organic, paid, social, referrer, earned media and marketplace sources. Each visitor is counted only once in any given month|
|ga4_metrics__aggregate_month_to_avg_quarter_tofu|Monthly, quaterly and half-yearly average unique visitor numbers|
|ga4_metrics__aggregate_month_tofu|Total number of unique visitors in the last two 30-day periods|
|ga4_metrics__aggregate_year_tofu|Total number of unique visitors in the last two 365-day periods|
|ga4_metrics__spot_month_tofu|Spot value of the unique daily visitors on the last day and the 30/90/180 days prior|
|ga4_metrics__spot_year_tofu|Spot value of the unique daily visitors on the last day and the first day of the current year|



## Installation instructions
### Step 1: Installing the Package
Include this package in your packages.yml and run `dbt deps`
```
packages:
  - package: Datomni/ga4_metrics
    version: ">=0.1.0"
```

### (Optional) Step 2: Configure Database and Schema Variables
By default, this package looks for your data in your target database in the `analytics` schema. If this is not where your data is stored, add the following variables to your dbt_project.yml file:
```
# dbt_project.yml
config-version: 2

vars:
    ga4_schema: analytics
    ga4_database: your_database_name
```

### (Optional) Step 3: Configure Table Names, timezone and conversion event
By default, events are read from the `events_*` table. If your input table name differs from this, you can configure the table name by adding them to your own dbt_project.yml file.

In addition, you can configure which timezone should events be converted to for counting daily visitors. By default, daily visitors are counted according to the UTC timezone.

Conversion event marks when a visitor/lead becomes/converted to a customer. The value of 1 indicates conversion, while the value 0 marks no conversion. The conversion event defaults to an empty string, marking all sessions as no conversion.
```
# dbt_project.yml
config-version: 2

vars:
    ga4_events_tbl: "events_*"

    timezone: "US/Pacific"

    conversion_event: "free_trial_initiated"
```

### (Optional) Step 4: Change the Build Schema
By default, this package will build all models in your <target_schema> .

This behavior can be tailored to your preference by making use of custom schemas. If you would like to override the current naming conventions, please add the following configuration to your dbt_project.yml file and rename +schema configs:
```
models:  
  ga4_metrics:
    +schema: ga4_metrics

seeds:
  +schema: ga4_metrics
```

### Database support
This package has been tested on BigQuery.
