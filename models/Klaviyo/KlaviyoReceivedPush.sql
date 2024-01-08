{% if var('KlaviyoReceivedPush') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_received_push_tbl_ptrn','%klaviyo%received_push','klaviyo_received_push_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(id) as id,
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("event_properties","x","string")}} as event_properties_x,
        {{extract_nested_value("event_properties","c","string")}} as event_properties_c,
        {{extract_nested_value("event_properties","t","numeric")}} as event_properties_t,
        {{extract_nested_value("event_properties","cr","string")}} as event_properties_cr,
        {{extract_nested_value("event_properties","_message","string")}} as event_properties__message,
        {{extract_nested_value("event_properties","__cohort_message_send_cohort","string")}} as event_properties__cohort_message_send_cohort,
        {{extract_nested_value("event_properties","_event_id","string")}} as event_properties__event_id,
        {{extract_nested_value("event_properties","message_type","string")}} as event_properties_message_type,
        {{extract_nested_value("event_properties","_flow","string")}} as event_properties__flow,
        {{extract_nested_value("event_properties","message_name","string")}} as event_properties_message_name,
        {{extract_nested_value("event_properties","__cohort_variation_send_cohort","string")}} as event_properties__cohort_variation_send_cohort,
        {{extract_nested_value("event_properties","message_title","string")}} as event_properties_message_title,
        {{extract_nested_value("event_properties","push_token","string")}} as event_properties_push_token,
        {{extract_nested_value("event_properties","push_platform","string")}} as event_properties_push_platform,
        {{extract_nested_value("event_properties","cohort_variation_send_cohort","string")}} as event_properties_cohort_variation_send_cohort,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("extra","sound","boolean")}} as extra_sound,
        {{extract_nested_value("extra","badge","boolean")}} as extra_badge,
        {{extract_nested_value("extra","message_body","string")}} as extra_message_body,
        {{extract_nested_value("_extra","___customer_merge___","boolean")}} as _extra__customer_merge__,
        {% if target.type == 'snowflake' %}
        {{timezone_conversion("event_properties.value:timestamp")}} as event_properties_timestamp,
        {% else %}
        {{timezone_conversion("event_properties.value:timestamp")}} as event_properties_timestamp,
        {% endif %}
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
        {{ multi_unnesting("event_properties", "extra") }}
        {{ multi_unnesting("event_properties", "_extra") }}
        {{ unnesting("links") }}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_received_push_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
