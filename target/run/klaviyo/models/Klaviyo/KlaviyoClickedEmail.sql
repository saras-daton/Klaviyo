-- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into `edm-saras`.`edm`.`KlaviyoClickedEmail` as DBT_INTERNAL_DEST
        using (







        select
        'Brand'
 as brand,
        'US'
 as store,
        type,
        coalesce(id) as id,
        

    
    cast(attributes.metric_id as string)
    
    
 as attributes_metric_id,
        

    
    cast(attributes.profile_id as string)
    
    
 as attributes_profile_id,
        timestamp_seconds(

    
    cast(attributes.timestamp as int64)
    
    
) as attributes_timestamp,
        

    
    cast(event_properties.Subject as string)
    
    
 as event_properties_Subject,
        

    
    cast(event_properties.Campaign_Name as string)
    
    
 as event_properties_Campaign_Name,
        

    
    cast(event_properties.flow as string)
    
    
 as event_properties_flow,
        

    
    cast(event_properties.message as string)
    
    
 as event_properties_message,
        

    
    cast(event_properties.Email_Domain as string)
    
    
 as event_properties_Email_Domain,
        

    
    cast(event_properties.cohort_message_send_cohort as string)
    
    
 as event_properties_cohort_message_send_cohort,
        

    
    cast(event_properties.message_interaction as string)
    
    
 as event_properties_message_interaction,
        

    
    cast(event_properties.URL as string)
    
    
 as event_properties_URL,
        

    
    cast(event_properties.Client_Type as string)
    
    
 as event_properties_Client_Type,
        

    
    cast(event_properties.Client_OS_Family as string)
    
    
 as event_properties_Client_OS_Family,
        

    
    cast(event_properties.Client_OS as string)
    
    
 as event_properties_Client_OS,
        

    
    cast(event_properties.Client_Name as string)
    
    
 as event_properties_Client_Name,
        

    
    cast(event_properties._ip as string)
    
    
 as event_properties__ip,
        

    
    cast(event_properties.ESP as numeric)
    
    
 as event_properties_ESP,
        

    
    cast(event_properties.group_ids as string)
    
    
 as event_properties_group_ids,
        

    
    cast(event_properties.event_id as string)
    
    
 as event_properties_event_id,
        

    
    cast(event_properties.variation as string)
    
    
 as event_properties_variation,
        

    
    cast(event_properties.cohort_variation_send_cohort as string)
    
    
 as event_properties_cohort_variation_send_cohort,
        

    
    cast(event_properties.experiment as string)
    
    
 as event_properties_experiment,
        

    
    cast(attribution.attributed_event_id as string)
    
    
 as attribution_attributed_event_id,
        

    
    cast(attribution.send_ts as numeric)
    
    
 as attribution_send_ts,
        

    
    cast(event_properties.`Inbox Provider` as string)
    
    
 as event_properties_Inbox_Provider,
        date(
 
    
        DATETIME(cast(replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00") as timestamp))
    
 
) date,
        
 
    
        DATETIME(cast(replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00") as timestamp))
    
 
 as datetime,
        
 
    
        DATETIME(cast(replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00") as timestamp))
    
 
 as _edm_eff_strt_ts,
        uuid,
        

    
    cast(links.self as string)
    
    
 as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from edm-saras.EDM_Daton.Brand_US_Klaviyo_BQ_clicked_email a
        

    
    left join unnest(attributes) as attributes
    
    

        

    
    left join unnest(attributes.event_properties) event_properties
    
    

        

    
    left join unnest(event_properties.attribution) attribution
    
    

        

    
    left join unnest(links) as links
    
    

    
            
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - 2592000000,0) from `edm-saras`.`edm`.`KlaviyoClickedEmail`)
            
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    

        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
                )

    
    when matched then update set
        `brand` = DBT_INTERNAL_SOURCE.`brand`,`store` = DBT_INTERNAL_SOURCE.`store`,`type` = DBT_INTERNAL_SOURCE.`type`,`id` = DBT_INTERNAL_SOURCE.`id`,`attributes_metric_id` = DBT_INTERNAL_SOURCE.`attributes_metric_id`,`attributes_profile_id` = DBT_INTERNAL_SOURCE.`attributes_profile_id`,`attributes_timestamp` = DBT_INTERNAL_SOURCE.`attributes_timestamp`,`event_properties_Subject` = DBT_INTERNAL_SOURCE.`event_properties_Subject`,`event_properties_Campaign_Name` = DBT_INTERNAL_SOURCE.`event_properties_Campaign_Name`,`event_properties_flow` = DBT_INTERNAL_SOURCE.`event_properties_flow`,`event_properties_message` = DBT_INTERNAL_SOURCE.`event_properties_message`,`event_properties_Email_Domain` = DBT_INTERNAL_SOURCE.`event_properties_Email_Domain`,`event_properties_cohort_message_send_cohort` = DBT_INTERNAL_SOURCE.`event_properties_cohort_message_send_cohort`,`event_properties_message_interaction` = DBT_INTERNAL_SOURCE.`event_properties_message_interaction`,`event_properties_URL` = DBT_INTERNAL_SOURCE.`event_properties_URL`,`event_properties_Client_Type` = DBT_INTERNAL_SOURCE.`event_properties_Client_Type`,`event_properties_Client_OS_Family` = DBT_INTERNAL_SOURCE.`event_properties_Client_OS_Family`,`event_properties_Client_OS` = DBT_INTERNAL_SOURCE.`event_properties_Client_OS`,`event_properties_Client_Name` = DBT_INTERNAL_SOURCE.`event_properties_Client_Name`,`event_properties__ip` = DBT_INTERNAL_SOURCE.`event_properties__ip`,`event_properties_ESP` = DBT_INTERNAL_SOURCE.`event_properties_ESP`,`event_properties_group_ids` = DBT_INTERNAL_SOURCE.`event_properties_group_ids`,`event_properties_event_id` = DBT_INTERNAL_SOURCE.`event_properties_event_id`,`event_properties_variation` = DBT_INTERNAL_SOURCE.`event_properties_variation`,`event_properties_cohort_variation_send_cohort` = DBT_INTERNAL_SOURCE.`event_properties_cohort_variation_send_cohort`,`event_properties_experiment` = DBT_INTERNAL_SOURCE.`event_properties_experiment`,`attribution_attributed_event_id` = DBT_INTERNAL_SOURCE.`attribution_attributed_event_id`,`attribution_send_ts` = DBT_INTERNAL_SOURCE.`attribution_send_ts`,`event_properties_Inbox_Provider` = DBT_INTERNAL_SOURCE.`event_properties_Inbox_Provider`,`date` = DBT_INTERNAL_SOURCE.`date`,`datetime` = DBT_INTERNAL_SOURCE.`datetime`,`_edm_eff_strt_ts` = DBT_INTERNAL_SOURCE.`_edm_eff_strt_ts`,`uuid` = DBT_INTERNAL_SOURCE.`uuid`,`links_self` = DBT_INTERNAL_SOURCE.`links_self`,`_daton_user_id` = DBT_INTERNAL_SOURCE.`_daton_user_id`,`_daton_batch_runtime` = DBT_INTERNAL_SOURCE.`_daton_batch_runtime`,`_daton_batch_id` = DBT_INTERNAL_SOURCE.`_daton_batch_id`,`_edm_eff_end_ts` = DBT_INTERNAL_SOURCE.`_edm_eff_end_ts`,`_edm_runtime` = DBT_INTERNAL_SOURCE.`_edm_runtime`
    

    when not matched then insert
        (`brand`, `store`, `type`, `id`, `attributes_metric_id`, `attributes_profile_id`, `attributes_timestamp`, `event_properties_Subject`, `event_properties_Campaign_Name`, `event_properties_flow`, `event_properties_message`, `event_properties_Email_Domain`, `event_properties_cohort_message_send_cohort`, `event_properties_message_interaction`, `event_properties_URL`, `event_properties_Client_Type`, `event_properties_Client_OS_Family`, `event_properties_Client_OS`, `event_properties_Client_Name`, `event_properties__ip`, `event_properties_ESP`, `event_properties_group_ids`, `event_properties_event_id`, `event_properties_variation`, `event_properties_cohort_variation_send_cohort`, `event_properties_experiment`, `attribution_attributed_event_id`, `attribution_send_ts`, `event_properties_Inbox_Provider`, `date`, `datetime`, `_edm_eff_strt_ts`, `uuid`, `links_self`, `_daton_user_id`, `_daton_batch_runtime`, `_daton_batch_id`, `_edm_eff_end_ts`, `_edm_runtime`)
    values
        (`brand`, `store`, `type`, `id`, `attributes_metric_id`, `attributes_profile_id`, `attributes_timestamp`, `event_properties_Subject`, `event_properties_Campaign_Name`, `event_properties_flow`, `event_properties_message`, `event_properties_Email_Domain`, `event_properties_cohort_message_send_cohort`, `event_properties_message_interaction`, `event_properties_URL`, `event_properties_Client_Type`, `event_properties_Client_OS_Family`, `event_properties_Client_OS`, `event_properties_Client_Name`, `event_properties__ip`, `event_properties_ESP`, `event_properties_group_ids`, `event_properties_event_id`, `event_properties_variation`, `event_properties_cohort_variation_send_cohort`, `event_properties_experiment`, `attribution_attributed_event_id`, `attribution_send_ts`, `event_properties_Inbox_Provider`, `date`, `datetime`, `_edm_eff_strt_ts`, `uuid`, `links_self`, `_daton_user_id`, `_daton_batch_runtime`, `_daton_batch_id`, `_edm_eff_end_ts`, `_edm_runtime`)


    