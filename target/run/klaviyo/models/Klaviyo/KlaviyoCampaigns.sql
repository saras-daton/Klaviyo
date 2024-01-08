-- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into `edm-saras`.`edm`.`KlaviyoCampaigns` as DBT_INTERNAL_DEST
        using (








        select
        'Brand'
 as brand,
        'US'
 as store,
        '' as Country,
        a.object,	
        a.id as id,
        a.name,	   	   	
        a.subject,	   	   	
        a.from_email,	   	   	
        a.from_name,
        

    
    cast(lists.object as string)
    
    
 as lists_object,
        

    
    cast(lists.id as string)
    
    
 as lists_id,
        

    
    cast(lists.name as string)
    
    
 as lists_name,
        

    
    cast(lists.list_type as string)
    
    
 as lists_list_type,
        

    
    cast(lists.folder as string)
    
    
 as lists_forlder,
        cast(case when regexp_contains(lists.created, r'.*T.{5}$') then lists.created||':00' else lists.created end as timestamp) as lists_created,
        cast(case when regexp_contains(lists.updated, r'.*T.{5}$') then lists.updated||':00' else lists.updated end as timestamp) as lists_updated,
        

    
    cast(lists.person_count as int64)
    
    
 as lists_person_count,
        

    
    cast(lists.person_count_nu as numeric)
    
    
 as lists_person_count_nu,
        

    
    cast(excluded_lists.object as string)
    
    
 as excluded_lists_object,
        

    
    cast(excluded_lists.id as string)
    
    
 as excluded_lists_id,
        

    
    cast(excluded_lists.name as string)
    
    
 as excluded_lists_name,
        

    
    cast(excluded_lists.list_type as string)
    
    
 as excluded_lists_list_type,
        

    
    cast(excluded_lists.folder as string)
    
    
 as excluded_lists_forlder,
        cast(case when regexp_contains(excluded_lists.created, r'.*T.{5}$') then excluded_lists.created||':00' else excluded_lists.created end as timestamp) as excluded_lists_created,
        cast(case when regexp_contains(excluded_lists.updated, r'.*T.{5}$') then excluded_lists.updated||':00' else excluded_lists.updated end as timestamp) as excluded_lists_updated,
        

    
    cast(excluded_lists.person_count as int64)
    
    
 as excluded_lists_person_count,
        

    
    cast(excluded_lists.person_count_nu as numeric)
    
    
 as excluded_lists_person_count_nu,	   	   	
        a.status,	   	   	
        cast(a.status_id as string) as status_id,	   	   	
        a.status_label,	   	   	
        a.sent_at,	   	   	
        cast(case when regexp_contains(a.send_time, r'.*T.{5}$') then a.send_time||':00' else a.send_time end as timestamp) as send_time,
        cast(case when regexp_contains(a.created, r'.*T.{5}$') then a.created||':00' else a.created end as timestamp) as created,
        cast(case when regexp_contains(a.updated, r'.*T.{5}$') then a.updated||':00' else a.updated end as timestamp) as updated, 	   	   	
        a.num_recipients,	   	   	
        a.campaign_type,	   	   	
        a.is_segmented,	   	   	
        a.message_type,	   	   	
        a.template_id,	   	   	
        _daton_user_id,	   	   	
        _daton_batch_runtime,	   	   	
        _daton_batch_id,  	   	
        status_id_nu,	   	   	
        num_recipients_nu,
        unix_micros(current_timestamp()) as _edm_runtime
        from edm-saras.EDM_Daton.Brand_US_Klaviyo_BQ_campaigns a
        

    
    left join unnest(lists) as lists
    
    

        

    
    left join unnest(excluded_lists) as excluded_lists
    
    

        
            
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - 2592000000,0) from `edm-saras`.`edm`.`KlaviyoCampaigns`)
            
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    

        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
                )

    
    when matched then update set
        `brand` = DBT_INTERNAL_SOURCE.`brand`,`store` = DBT_INTERNAL_SOURCE.`store`,`Country` = DBT_INTERNAL_SOURCE.`Country`,`object` = DBT_INTERNAL_SOURCE.`object`,`id` = DBT_INTERNAL_SOURCE.`id`,`name` = DBT_INTERNAL_SOURCE.`name`,`subject` = DBT_INTERNAL_SOURCE.`subject`,`from_email` = DBT_INTERNAL_SOURCE.`from_email`,`from_name` = DBT_INTERNAL_SOURCE.`from_name`,`lists_object` = DBT_INTERNAL_SOURCE.`lists_object`,`lists_id` = DBT_INTERNAL_SOURCE.`lists_id`,`lists_name` = DBT_INTERNAL_SOURCE.`lists_name`,`lists_list_type` = DBT_INTERNAL_SOURCE.`lists_list_type`,`lists_forlder` = DBT_INTERNAL_SOURCE.`lists_forlder`,`lists_created` = DBT_INTERNAL_SOURCE.`lists_created`,`lists_updated` = DBT_INTERNAL_SOURCE.`lists_updated`,`lists_person_count` = DBT_INTERNAL_SOURCE.`lists_person_count`,`lists_person_count_nu` = DBT_INTERNAL_SOURCE.`lists_person_count_nu`,`excluded_lists_object` = DBT_INTERNAL_SOURCE.`excluded_lists_object`,`excluded_lists_id` = DBT_INTERNAL_SOURCE.`excluded_lists_id`,`excluded_lists_name` = DBT_INTERNAL_SOURCE.`excluded_lists_name`,`excluded_lists_list_type` = DBT_INTERNAL_SOURCE.`excluded_lists_list_type`,`excluded_lists_forlder` = DBT_INTERNAL_SOURCE.`excluded_lists_forlder`,`excluded_lists_created` = DBT_INTERNAL_SOURCE.`excluded_lists_created`,`excluded_lists_updated` = DBT_INTERNAL_SOURCE.`excluded_lists_updated`,`excluded_lists_person_count` = DBT_INTERNAL_SOURCE.`excluded_lists_person_count`,`excluded_lists_person_count_nu` = DBT_INTERNAL_SOURCE.`excluded_lists_person_count_nu`,`status` = DBT_INTERNAL_SOURCE.`status`,`status_id` = DBT_INTERNAL_SOURCE.`status_id`,`status_label` = DBT_INTERNAL_SOURCE.`status_label`,`sent_at` = DBT_INTERNAL_SOURCE.`sent_at`,`send_time` = DBT_INTERNAL_SOURCE.`send_time`,`created` = DBT_INTERNAL_SOURCE.`created`,`updated` = DBT_INTERNAL_SOURCE.`updated`,`num_recipients` = DBT_INTERNAL_SOURCE.`num_recipients`,`campaign_type` = DBT_INTERNAL_SOURCE.`campaign_type`,`is_segmented` = DBT_INTERNAL_SOURCE.`is_segmented`,`message_type` = DBT_INTERNAL_SOURCE.`message_type`,`template_id` = DBT_INTERNAL_SOURCE.`template_id`,`_daton_user_id` = DBT_INTERNAL_SOURCE.`_daton_user_id`,`_daton_batch_runtime` = DBT_INTERNAL_SOURCE.`_daton_batch_runtime`,`_daton_batch_id` = DBT_INTERNAL_SOURCE.`_daton_batch_id`,`status_id_nu` = DBT_INTERNAL_SOURCE.`status_id_nu`,`num_recipients_nu` = DBT_INTERNAL_SOURCE.`num_recipients_nu`,`_edm_runtime` = DBT_INTERNAL_SOURCE.`_edm_runtime`
    

    when not matched then insert
        (`brand`, `store`, `Country`, `object`, `id`, `name`, `subject`, `from_email`, `from_name`, `lists_object`, `lists_id`, `lists_name`, `lists_list_type`, `lists_forlder`, `lists_created`, `lists_updated`, `lists_person_count`, `lists_person_count_nu`, `excluded_lists_object`, `excluded_lists_id`, `excluded_lists_name`, `excluded_lists_list_type`, `excluded_lists_forlder`, `excluded_lists_created`, `excluded_lists_updated`, `excluded_lists_person_count`, `excluded_lists_person_count_nu`, `status`, `status_id`, `status_label`, `sent_at`, `send_time`, `created`, `updated`, `num_recipients`, `campaign_type`, `is_segmented`, `message_type`, `template_id`, `_daton_user_id`, `_daton_batch_runtime`, `_daton_batch_id`, `status_id_nu`, `num_recipients_nu`, `_edm_runtime`)
    values
        (`brand`, `store`, `Country`, `object`, `id`, `name`, `subject`, `from_email`, `from_name`, `lists_object`, `lists_id`, `lists_name`, `lists_list_type`, `lists_forlder`, `lists_created`, `lists_updated`, `lists_person_count`, `lists_person_count_nu`, `excluded_lists_object`, `excluded_lists_id`, `excluded_lists_name`, `excluded_lists_list_type`, `excluded_lists_forlder`, `excluded_lists_created`, `excluded_lists_updated`, `excluded_lists_person_count`, `excluded_lists_person_count_nu`, `status`, `status_id`, `status_label`, `sent_at`, `send_time`, `created`, `updated`, `num_recipients`, `campaign_type`, `is_segmented`, `message_type`, `template_id`, `_daton_user_id`, `_daton_batch_runtime`, `_daton_batch_id`, `status_id_nu`, `num_recipients_nu`, `_edm_runtime`)


    