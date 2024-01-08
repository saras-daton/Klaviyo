
  
    

    create or replace table `edm-saras`.`edm`.`KlaviyoBouncedEmail`
      
    partition by timestamp_trunc(datetime, day)
    

    OPTIONS()
    as (
      








        select
        'Brand'
 as brand,
        'US'
 as store,
        a.type,
        id,
        

    
    cast(attributes.metric_id as string)
    
    
 as attributes_metric_id,
        

    
    cast(attributes.profile_id as string)
    
    
 as attributes_profile_id,
        timestamp_seconds(

    
    cast(attributes.timestamp as BIGINT)
    
    
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
        

    
    cast(event_properties.Bounce_Type as string)
    
    
 as event_properties_bounce_type,
        

    
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
        

    
    cast(event_properties.klaviyo_bounce_category as string)
    
    
 as event_properties_klaviyo_bounce_category,
        

    
    cast(event_properties.`Inbox Provider` as string)
    
    
 as event_properties_Inbox_Provider,
        

    
    cast(bounce_delivery_info.add_exclusion as boolean)
    
    
 as bounce_delivery_info_add_exclusion,
        

    
    cast(bounce_delivery_info.is_autoresponder as boolean)
    
    
 as bounce_delivery_info_is_autoresponder,
        

    
    cast(bounce_delivery_info.reason as string)
    
    
 as bounce_delivery_info_reason,
        

    
    cast(bounce_delivery_info.type as string)
    
    
 as bounce_delivery_info_type,
        

    
    cast(bounce_delivery_info.action_id as string)
    
    
 as bounce_delivery_info_action_id,
        

    
    cast(bounce_delivery_info.ip as string)
    
    
 as bounce_delivery_info_ip,
        

    
    cast(bounce_delivery_info.code as string)
    
    
 as bounce_delivery_info_code,
        

    
    cast(bounce_delivery_info.rigidity as string)
    
    
 as bounce_delivery_info_rigidity,
        

    
    cast(attribution.attributed_event_id as string)
    
    
 as attribution_attributed_event_id,
        

    
    cast(attribution.send_ts as numeric)
    
    
 as attribution_send_ts,
        

    
    cast(attribution.message as string)
    
    
 as attribution_message,
        

    
    cast(attribution.flow as string)
    
    
 as attribution_flow,
        

    
    cast(attribution.variation as string)
    
    
 as attribution_variation,
        

    
    cast(attribution.group_ids as string)
    
    
 as attribution_group_ids,
        

    
    cast(attribution.experiment as string)
    
    
 as attribution_experiment,
        

    
    cast(attribution.attributed_channel as string)
    
    
 as attribution_attributed_channel,
        
 
    
        DATETIME(cast(replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00") as timestamp))
    
 
 as datetime,
        

    
    cast(attributes.uuid as string)
    
    
 as attributes_uuid,
        

    
    cast(links.self as string)
    
    
 as links_self,
        

    
        _daton_user_id
    

 as _daton_user_id,
        

    
        _daton_batch_runtime
    

 as _daton_batch_runtime,
        
    
    
        _daton_batch_id
    

 as _daton_batch_id,
        current_timestamp() as _last_updated,
        'manual' as _run_id
        from edm-saras.EDM_Daton.Brand_US_Klaviyo_BQ_bounced_email a
        

    
    left join unnest(attributes) as attributes
    
    

        

    
    left join unnest(attributes.event_properties) event_properties
    
    

        

    
    left join unnest(event_properties.extra) extra
    
    

        

    
    left join unnest(extra.bounce_delivery_info) bounce_delivery_info
    
    

        

    
    left join unnest(event_properties.attribution) attribution
    
    

        

    
    left join unnest(links) as links
    
    

        
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    

    );
  