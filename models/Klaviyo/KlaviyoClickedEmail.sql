
{% if var('KlaviyoClickedEmail') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_clicked_email_tbl_ptrn'),
exclude=var('klaviyo_clicked_email_tbl_exclude_ptrn'),
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
        type,
        coalesce(id, 'NA') as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_seconds({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Subject","string")}} as event_properties_Subject,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","Email_Domain","string")}} as event_properties_Email_Domain,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","message_interaction","string")}} as event_properties_message_interaction,
        {{extract_nested_value("event_properties","URL","string")}} as event_properties_URL,
        {{extract_nested_value("event_properties","Client_Type","string")}} as event_properties_Client_Type,
        {{extract_nested_value("event_properties","Client_OS_Family","string")}} as event_properties_Client_OS_Family,
        {{extract_nested_value("event_properties","Client_OS","string")}} as event_properties_Client_OS,
        {{extract_nested_value("event_properties","Client_Name","string")}} as event_properties_Client_Name,
        {{extract_nested_value("event_properties","_ip","string")}} as event_properties__ip,
        {{extract_nested_value("event_properties","ESP","numeric")}} as event_properties_ESP,
        {{extract_nested_value("event_properties","group_ids","string")}} as event_properties_group_ids,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("event_properties","variation","string")}} as event_properties_variation,
        {{extract_nested_value("event_properties","cohort_variation_send_cohort","string")}} as event_properties_cohort_variation_send_cohort,
        {{extract_nested_value("event_properties","experiment","string")}} as event_properties_experiment,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("event_properties","`Inbox Provider`","string")}} as event_properties_Inbox_Provider,
        {% if var('timezone_conversion_flag') %}
           datetime(datetime_add(cast(datetime as timestamp), interval {{hr}} hour )) as datetime,
        {% else %}
           datetime(timestamp(datetime)) as datetime,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
           date(datetime_add(cast(datetime as timestamp), interval {{hr}} hour )) as date,
        {% else %}
           date(timestamp(datetime)) as date,
        {% endif %}
        uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           datetime_add(cast(datetime as timestamp), interval {{hr}} hour ) as _edm_eff_strt_ts,
        {% else %}
           cast(datetime as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","event_properties")}}
        {{multi_unnesting("event_properties","attribution")}}
        {{unnesting("links")}}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_clicked_email_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

