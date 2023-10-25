# Klaviyo Data Unification

This dbt package is for the Klaviyo data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Klaviyo
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Klaviyo
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_klaviyo). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  klaviyo:
    +schema: custom_schema_extension
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False. Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table for which you want timezone converison to be taken into account.

Example:
```yaml
vars:
timezone_conversion_flag: True
  raw_table_timezone_offset_hours: {
    "Klaviyo.Raw.BRAND_US_Klaviyo_BQ_bounced_email":-7,
    "Klaviyo.Raw.BRAND_US_Klaviyo_BQ_clicked_sms":-7
  }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
KlaviyoBouncedEmail: False
```

## Models

This package contains models from the Klaviyo API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Bounced Email | [KlaviyoBouncedEmail](models/Klaviyo/KlaviyoBouncedEmail.sql)  | A detailed report which gives infomration about Bounced Emails |
|Campaigns | [KlaviyoCampaigns](models/Klaviyo/KlaviyoCampaigns.sql)  | A detailed report which gives infomration about campaigns |
|Clicked Email | [KlaviyoClickedEmail](models/Klaviyoy/KlaviyoClickedEmail.sql)  | A detailed report which gives infomration about clicked emails |
|Clicked SMS | [KlaviyoClickedSMS](models/Klaviyo/KlaviyoClickedSMS.sql)  | A detailed report which gives infomration about clicked sms |
|Consented to Receive SMS | [KlaviyoConsentedToReceiveSMS](models/Klaviyo/KlaviyoConsentedToReceiveSMS.sql)| A detailed report which gives infomration about people who consented to receive sms |
|Dropped Email | [KlaviyoDroppedEmail](models/Klaviyo/KlaviyoDroppedEmail.sql)| A detailed report which gives infomration about dropped emails |
|Failed to Deliver SMS | [KlaviyoFailedToDeliverSMS](models/Klaviyo/KlaviyoFailedToDeliverSMS.sql)| A detailed report which gives infomration about sms that failed to be delivered |
|Flow Actions | [KlaviyoFlowActions](models/Klaviyo/KlaviyoFlowActions.sql)| A detailed report which gives infomration about flow actions|
|Flow Messages | [KlaviyoFlowMessages](models/Klaviyo/KlaviyoFlowMessages.sql)| A detailed report which gives infomration about flow messages |
|Flows | [KlaviyoFlows](models/Klaviyo/KlaviyoFlows.sql)| A detailed report which gives infomration about flows |
|Lists | [KlaviyoLists](models/Klaviyo/KlaviyoLists.sql)| A detailed report which gives infomration about lists |
|Marked Email as Spam | [KlaviyoMarkedEmailAsSpam](models/Klaviyo/KlaviyoMarkedEmailAsSpam.sql)| A detailed report which gives infomration about email marked as spam |
|Metrics | [KlaviyoMetrics](models/Klaviyo/KlaviyoMetrics.sql)| A detailed report which gives infomration about metrics |
|OpenedEmail | [KlaviyoOpenedEmail](models/Klaviyo/KlaviyoOpenedEmail.sql)  | A detailed report which gives information about opened emails |
|OpenedPush | [KlaviyoOpenedPush](models/Klaviyo/KlaviyoOpenedPush.sql)  | A detailed report which gives information about opened push |
|PlacedOrder | [KlaviyoPlacedOrder](models/Klaviyo/KlaviyoPlacedOrder.sql)  | A detailed report which gives information about placed orders |
|ReceivedEmail | [KlaviyoReceivedEmail](models/Klaviyo/KlaviyoReceivedEmail.sql)  | A detailed report which gives information about received emails |
|ReceivedPush | [KlaviyoReceivedPush](models/Klaviyo/KlaviyoReceivedPush.sql)| A detailed report which gives information about received push |
|ReceivedSMS | [KlaviyoReceivedSMS](models/Klaviyo/KlaviyoReceivedSMS.sql)| A detailed report which gives information about received sms |
|Segments | [KlaviyoSegments](models/Klaviyo/KlaviyoSegments.sql)| A detailed report which gives information about Segments |
|SentSMS | [KlaviyoSentSMS](models/Klaviyo/KlaviyoSentSMS.sql)| A detailed report which gives information about sent SMS |
|SubscribedToList | [KlaviyoSubscribedToList](models/Klaviyo/KlaviyoSubscribedToList.sql)| A detailed report which gives information about customers who subscribed to list |
|Unsubscribed | [KlaviyoUnsubscribed](models/Klaviyo/KlaviyoUnsubscribed.sql)| A detailed report which gives information about customers who unsubscribed |
|UnsubscribedFromList | [KlaviyoUnsubscribedFromList](models/Klaviyo/KlaviyoUnsubscribedFromList.sql)| A detailed report which gives information about customers who unsubscribed from list |



### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: KlaviyoBouncedEmail
    description: A list of bounced emails.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'datetime', 'data_type': 'timestamp'}

  - name: KlaviyoClickedEmail
    description: A list of emails that were clicked.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoClickedSMS
    description: A lsit of SMS that were clicked.
    materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoConsentedToReceiveSMS
    description: A list of receivers that consented to receive SMS.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoDroppedEmail
    description: A list of dropped emails.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']
    

  - name: KlaviyoFailedToDeliverSMS
    description: A list of SMS that were failed to be delivered.
    config:
     materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campagin_Name']

  - name: KlaviyoFlowActions
    description: A detailed report which gives infomration about flow actions.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoFlowMessages
    description: A detailed report which gives infomration about flow messages.
    config:
       materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoFlows
    description: A detailed report which gives infomration about flows.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoLists
    description: A detailed report which gives infomration about lists.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoMarkedEmailAsSpam
    description: A lsit of emails marked as spam.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoMetrics
    description: A detailed report which gives infomration about metrics.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoCampaigns
    description: A detailed report about campaigns.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
  
  - name: KlaviyoOpenedEmail
    description: A detailed report which gives information about opened emails.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoOpenedPush
    description: A detailed report which gives information about opened push.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoPlacedOrder
    description: A detailed report which gives information about placed orders.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id','_attribution__attributed_event_id']
      partition_by: { 'field': 'attributes_datetime', 'data_type': 'timestamp', 'granularity': 'day' }

  - name: KlaviyoReceivedEmail
    description: A detailed report which gives information about received emails.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoReceivedPush
    description: A detailed report which gives information about received push.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      # cluster_by : ['Campaign_Name'] - the column Campaign_Name doesn't exist in the raw table

  - name: KlaviyoReceivedSMS
    description: A detailed report which gives information about received SMS.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoSegments
    description: A detailed report which gives information about different segments.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'updated_date', 'data_type': 'date'}

  - name: KlaviyoSentSMS
    description: A detailed report which gives information about sent SMS.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}

  - name: KlaviyoSubscribedToList
    description: A detailed report which gives information about customers who subscribed to list. 
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}

  - name: KlaviyoUnsubscribed
    description: A detailed report which gives information about customers who unsubscribed.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['event_properties_Campaign_Name']

  - name: KlaviyoUnsubscribedFromList
    description: A detailed report which gives information about customers who unsubscribed from list.
    config:
      materialized : incremental
      incremental_strategy: merge
      unique_key : ['id']
      partition_by : { 'field': 'date', 'data_type': 'date'}
      cluster_by : ['attribution_message']
```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
