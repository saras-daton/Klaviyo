{% if var('KlaviyoSentSMS') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_sent_sms_tbl_ptrn'),
exclude=var('klaviyo_sent_sms_tbl_exclude_ptrn'),
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
        '{{brand}}' as brand,
        '{{id}}' as store,
        a.type,
        coalesce(id, 'NA') as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","From_Number","string")}} as event_properties_From_Number,
        {{extract_nested_value("event_properties","From_Phone_Region","string")}} as event_properties_From_Phone_Region,
        {{extract_nested_value("event_properties","To_Number","string")}} as event_properties_To_Number,
        {{extract_nested_value("event_properties","To_Phone_Region","string")}} as event_properties_To_Phone_Region,
        {{extract_nested_value("event_properties","Message_Body","string")}} as event_properties_Message_Body,
        {{extract_nested_value("event_properties","Message_Type","string")}} as event_properties_Message_Type,
        {{extract_nested_value("event_properties","Intent_1","string")}} as event_properties_Intent_1,
        {{extract_nested_value("event_properties","Intent_1_Confidence","string")}} as event_properties_Intent_1_Confidence,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("extra","Message_Id","string")}} as extra_Message_Id,
        {{extract_nested_value("extra","Inbound_Message_Id","string")}} as extra_Inbound_Message_Id,
        {{extract_nested_value("extra","From_State","string")}} as extra_From_State,
        {{extract_nested_value("extra","From_City","string")}} as extra_From_City,
        {{extract_nested_value("extra","From_Country","string")}} as extra_From_Country,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {% if var('timezone_conversion_flag') %}
            datetime(datetime_add(timestamp(datetime), interval {{hr}} hour )) as datetime,
        {% else %}
            datetime(timestamp(datetime))  as datetime,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
            date(datetime_add(timestamp(datetime), interval {{hr}} hour )) as date,
        {% else %}
            date(timestamp(datetime))  as date,
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
        {{ unnesting("attributes") }}
        {{ multi_unnesting("attributes", "event_properties") }}
        {{ multi_unnesting("event_properties", "extra") }}
        {{ multi_unnesting("event_properties", "attribution") }}
        {{ unnesting("links") }}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_sent_sms_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}