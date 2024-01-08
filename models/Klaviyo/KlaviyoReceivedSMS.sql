{% if var('KlaviyoReceivedSMS') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_received_sms_tbl_ptrn','%klaviyo%received_sms','klaviyo_received_sms_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        a.type,
        coalesce(id) as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","Message_Type","string")}} as event_properties_Message_Type,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","Carrier_Delivery_Status","string")}} as event_properties_Carrier_Delivery_Status,
        {{extract_nested_value("event_properties","Message_Format","string")}} as event_properties_Message_Format,
        {{extract_nested_value("event_properties","Message_Name","string")}} as event_properties_Message_Name,
        {{extract_nested_value("event_properties","From_Number","string")}} as event_properties_From_Number,
        {{extract_nested_value("event_properties","From_Phone_Region","string")}} as event_properties_From_Phone_Region,
        {{extract_nested_value("event_properties","To_Number","string")}} as event_properties_To_Number,
        {{extract_nested_value("event_properties","To_Phone_Region","string")}} as event_properties_To_Phone_Region,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","Segment_Count","numeric")}} as event_properties_Segment_Count,
        {{extract_nested_value("internal","send_timestamp","numeric")}} as internal_send_timestamp,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("extra","Message_Id","string")}} as extra_Message_Id,
        {{extract_nested_value("extra","Message_Body","string")}} as extra_Message_Body,
        {{timezone_conversion('replace(replace(left(datetime,19),"T"," "),"Z",":00")')}} as datetime,
        date({{timezone_conversion('replace(replace(left(datetime,19),"T"," "),"Z",":00")')}}) date,
        uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {{timezone_conversion('replace(replace(left(datetime,19),"T"," "),"Z",":00")')}} as _edm_eff_strt_ts,
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{ unnesting("attributes") }}
        {{ multi_unnesting("attributes", "event_properties") }}
        {{ multi_unnesting("event_properties", "extra") }}
        {{ multi_unnesting("event_properties", "internal") }}
        {{ unnesting("links") }}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_received_sms_lookback') }},0) from {{ this }})
            {% endif %}
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}