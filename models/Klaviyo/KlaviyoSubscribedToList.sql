{% if var('KlaviyoSubscribedToList') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_subscribed_to_list_tbl_ptrn','%klaviyo%subscribed_to_list','klaviyo_subscribed_to_list_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
    
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(a.id) as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","Campagin_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","List","string")}} as event_properties_List,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("attribution","group_ids","string")}} as attribution_group_ids,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","attributed_channel","string")}} as attribution_attributed_channel,
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
        {{ multi_unnesting("event_properties", "attribution") }}
        {{ unnesting("links") }}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_subscribed_to_list_lookback') }},0) from {{ this }})
            {% endif %}
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}