{% if var('KlaviyoCampaigns') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_campaigns_tbl_ptrn','%klaviyo%campaigns','klaviyo_campaigns_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        '{{id}}' as Country,
        a.object,	
        a.id as id,
        a.name,	   	   	
        a.subject,	   	   	
        a.from_email,	   	   	
        a.from_name,
        {{extract_nested_value("lists","object","string")}} as lists_object,
        {{extract_nested_value("lists","id","string")}} as lists_id,
        {{extract_nested_value("lists","name","string")}} as lists_name,
        {{extract_nested_value("lists","list_type","string")}} as lists_list_type,
        {{extract_nested_value("lists","folder","string")}} as lists_forlder,
        cast(case when regexp_contains(lists.created, r'.*T.{5}$') then lists.created||':00' else lists.created end as timestamp) as lists_created,
        cast(case when regexp_contains(lists.updated, r'.*T.{5}$') then lists.updated||':00' else lists.updated end as timestamp) as lists_updated,
        {{extract_nested_value("lists","person_count","int64")}} as lists_person_count,
        {{extract_nested_value("lists","person_count_nu","numeric")}} as lists_person_count_nu,
        {{extract_nested_value("excluded_lists","object","string")}} as excluded_lists_object,
        {{extract_nested_value("excluded_lists","id","string")}} as excluded_lists_id,
        {{extract_nested_value("excluded_lists","name","string")}} as excluded_lists_name,
        {{extract_nested_value("excluded_lists","list_type","string")}} as excluded_lists_list_type,
        {{extract_nested_value("excluded_lists","folder","string")}} as excluded_lists_forlder,
        cast(case when regexp_contains(excluded_lists.created, r'.*T.{5}$') then excluded_lists.created||':00' else excluded_lists.created end as timestamp) as excluded_lists_created,
        cast(case when regexp_contains(excluded_lists.updated, r'.*T.{5}$') then excluded_lists.updated||':00' else excluded_lists.updated end as timestamp) as excluded_lists_updated,
        {{extract_nested_value("excluded_lists","person_count","int64")}} as excluded_lists_person_count,
        {{extract_nested_value("excluded_lists","person_count_nu","numeric")}} as excluded_lists_person_count_nu,	   	   	
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
        from {{i}} a
        {{unnesting("lists")}}
        {{unnesting("excluded_lists")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_campaigns_lookback') }},0) from {{ this }})
            {% endif %}
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

