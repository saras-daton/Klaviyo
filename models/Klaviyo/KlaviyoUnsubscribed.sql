{% if var('KlaviyoUnsubscribed') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_unsubscribed_tbl_ptrn','%klaviyo%unsubscribed','klaviyo_unsubscribed_tbl_exclude_ptrn','') %}
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
        {{extract_nested_value("event_properties","Subject","string")}} as event_properties_Subject,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","Email_Domain","string")}} as event_properties_Email_Domain,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","group_ids","string")}} as event_properties_group_ids,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("bounce_delivery_info","add_exclusion","boolean")}} as bounce_delivery_info_add_exclusion,
        {{extract_nested_value("bounce_delivery_info","is_autoresponder","boolean")}} as bounce_delivery_info_is_autoresponder,
        {{extract_nested_value("bounce_delivery_info","action_id","string")}} as bounce_delivery_info_action_id,
        {{extract_nested_value("bounce_delivery_info","code","string")}} as bounce_delivery_info_code,
        {{extract_nested_value("bounce_delivery_info","reason","string")}} as bounce_delivery_info_reason,
        {{extract_nested_value("event_properties","variation","string")}} as event_properties_variation,
        {{extract_nested_value("event_properties","experiment","string")}} as event_properties_experiment,
        {{extract_nested_value("event_properties","cohort_variation_send_cohort","string")}} as event_properties_cohort_variation_send_cohort,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","group_ids","string")}} as attribution_group_ids,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
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
        {{ multi_unnesting("extra", "bounce_delivery_info") }}
        {{ multi_unnesting("event_properties", "attribution") }}
        {{ unnesting("links") }}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_unsubscribed_lookback') }},0) from {{ this }})
            {% endif %}
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}