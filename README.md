# Profitwell Metrics dbt Package
## What does this dbt package do?
TBA

Refer to the table below for a detailed view of final models materialized by default within this package.

|   model    | description |
|------------|-------------|
|ga4_metrics__events_flattened|TBA|
|ga4_metrics__page_views|TBA|
|ga4_metrics__page_views_sessionized|TBA|
|ga4_metrics__sessions|TBA|
|ga4_metrics__sessions_stitched|TBA|
|ga4_metrics__visitor_traffic_aggregated|TBA|


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
    ga4_schema: profitwell
    ga4_database: your_database_name
```

### (Optional) Step 3: Configure Table Names
By default, events are read from the `events_*` table. If your input table name differs from this, you can configure the table name by adding them to your own dbt_project.yml file:
```
# dbt_project.yml
config-version: 2

vars:
    ga4_events_tbl: "events_*"
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
