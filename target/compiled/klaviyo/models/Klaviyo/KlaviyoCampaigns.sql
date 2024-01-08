








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
    
