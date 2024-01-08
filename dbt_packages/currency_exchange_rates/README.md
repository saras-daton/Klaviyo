# Currency Exchange Rates Modelling
This dbt package models the Exchange Rates data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

This package would be performing the following funtions:

- Consolidation - Different marketplaces & different brands would have similar tables. Helps in consolidating all the tables into one final stage table 
- Deduplication - Based on primary keys , the tables are deduplicated and the latest records are only loaded into the stage models
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly

#### Prerequisite 
Daton Connector for Currency Exchange Data - Exchange Rates

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/currency_exchange_rates
    version: v1.0.2
```

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yml
models:
  currency_exchange_rates:
    +schema: custom_schema_name # leave blank for just the target_schema
```

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
ExchangeRates: False
```

## Models

This package contains the Exchange Rates model coming from the Exchange Rates Daton connector. Please follow this to get more details about [models](https://docs.google.com/spreadsheets/d/1OaJnVpBrPZaBusJXBHrT8dhnD2zctWMmSRN__WLQsl0/edit?usp=sharing). The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Currency Exchange | [ExchangeRates](models/Currency%20Exchange%20Rates/ExchangeRates.sql)  | Date level Currency ExchangeRates |


## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to Bigquery
