
{% if var('KlaviyoBouncedEmail') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_bounced_email_tbl_ptrn'),
exclude=var('klaviyo_bounced_email_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}


        select
        '{{brand|replace("`","")}}' as brand,
        '{{store|replace("`","")}}' as store,
        a.type,
        coalesce(id, 'NA') as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_seconds({{extract_nested_value("attributes","timestamp","BIGINT")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Subject","string")}} as event_properties_Subject,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","Email_Domain","string")}} as event_properties_Email_Domain,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","Bounce_Type","string")}} as event_properties_bounce_type,
        {{extract_nested_value("event_properties","ESP","numeric")}} as event_properties_ESP,
        {{extract_nested_value("event_properties","group_ids","string")}} as event_properties_group_ids,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("event_properties","variation","string")}} as event_properties_variation,
        {{extract_nested_value("event_properties","cohort_variation_send_cohort","string")}} as event_properties_cohort_variation_send_cohort,
        {{extract_nested_value("event_properties","experiment","string")}} as event_properties_experiment,
        {{extract_nested_value("event_properties","klaviyo_bounce_category","string")}} as event_properties_klaviyo_bounce_category,
        {{extract_nested_value("event_properties","`Inbox Provider`","string")}} as event_properties_Inbox_Provider,
        {{extract_nested_value("bounce_delivery_info","add_exclusion","boolean")}} as bounce_delivery_info_add_exclusion,
        {{extract_nested_value("bounce_delivery_info","is_autoresponder","boolean")}} as bounce_delivery_info_is_autoresponder,
        {{extract_nested_value("bounce_delivery_info","reason","string")}} as bounce_delivery_info_reason,
        {{extract_nested_value("bounce_delivery_info","type","string")}} as bounce_delivery_info_type,
        {{extract_nested_value("bounce_delivery_info","action_id","string")}} as bounce_delivery_info_action_id,
        {{extract_nested_value("bounce_delivery_info","ip","string")}} as bounce_delivery_info_ip,
        {{extract_nested_value("bounce_delivery_info","code","string")}} as bounce_delivery_info_code,
        {{extract_nested_value("bounce_delivery_info","rigidity","string")}} as bounce_delivery_info_rigidity,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","group_ids","string")}} as attribution_group_ids,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {{extract_nested_value("attribution","attributed_channel","string")}} as attribution_attributed_channel,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp= "cast(datetime as timestamp)") }} as {{ dbt.type_timestamp() }}) as datetime,
        {{extract_nested_value("attributes","uuid","string")}} as attributes_uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","event_properties")}}
        {{multi_unnesting("event_properties","extra")}}
        {{multi_unnesting("extra","bounce_delivery_info")}}
        {{multi_unnesting("event_properties","attribution")}}
        {{unnesting("links")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_bounced_email_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

