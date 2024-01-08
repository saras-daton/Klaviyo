
{% if var('KlaviyoConsentedToReceiveSMS') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_consented_to_receive_sms_tbl_ptrn','%klaviyo%consented_to_receive_sms','klaviyo_consented_to_receive_sms_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(id) as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_seconds({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","Message_Body","string")}} as event_properties_Message_Body,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","From_Number","string")}} as event_properties_From_Number,
        {{extract_nested_value("event_properties","From_Phone_Region","string")}} as event_properties_From_Phone_Region,
        {{extract_nested_value("event_properties","To_Number","string")}} as event_properties_To_Number,
        {{extract_nested_value("event_properties","To_Phone_Region","string")}} as event_properties_To_Phone_Region,
        {{extract_nested_value("event_properties","Method","string")}} as event_properties_Method,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("extra","Message_ID","string")}} as extra_Message_ID,
        {{extract_nested_value("extra","Inbound_Message_ID","string")}} as extra_Inbound_Message_ID,
        {{extract_nested_value("extra","From_State","string")}} as extra_From_State,
        {{extract_nested_value("extra","From_City","string")}} as extra_From_City,
        {{extract_nested_value("extra","From_Country","string")}} as extra_From_Country,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {{extract_nested_value("attribution","group_ids","string")}} as attribution_group_ids,
        date({{timezone_conversion('replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00")')}}) date,
        {{timezone_conversion('replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00")')}} as datetime,
        uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {{timezone_conversion('replace(replace(left(attributes.datetime,19),"T"," "),"Z",":00")')}} as _edm_eff_strt_ts,
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","event_properties")}}
        {{multi_unnesting("event_properties","extra")}}
        {{multi_unnesting("event_properties","attribution")}}
        {{unnesting("links")}}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_consented_to_receive_sms_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

