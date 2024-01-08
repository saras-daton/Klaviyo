{% if var('KlaviyoOpenedPush') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_opened_push_tbl_ptrn','%klaviyo%opened_push','klaviyo_opened_push_tbl_exclude_ptrn','') %}
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
        {{extract_nested_value("event_properties","Subject","string")}} as event_properties_Subject,
        {{extract_nested_value("event_properties","Campaign_Name","string")}} as event_properties_Campaign_Name,
        {{extract_nested_value("event_properties","flow","string")}} as event_properties_flow,
        {{extract_nested_value("event_properties","message","string")}} as event_properties_message,
        {{extract_nested_value("event_properties","Email_Domain","string")}} as event_properties_Email_Domain,
        {{extract_nested_value("event_properties","cohort_message_send_cohort","string")}} as event_properties_cohort_message_send_cohort,
        {{extract_nested_value("event_properties","message_interaction","string")}} as event_properties_message_interaction,
        {{extract_nested_value("event_properties","ESP","numeric")}} as event_properties_ESP,
        {{extract_nested_value("event_properties","machine_open","string")}} as event_properties_machine_open,
        {{extract_nested_value("event_properties","group_ids","string")}} as event_properties_group_ids,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        timestamp_millis({{extract_nested_value("event_properties","timestamp","int64")}})as event_properties_timestamp,
        {{extract_nested_value("event_properties","x","string")}} as event_properties_x,
        {{extract_nested_value("event_properties","c","string")}} as event_properties_c,
        {{extract_nested_value("event_properties","t","numeric")}} as event_properties_t,
        {{extract_nested_value("event_properties","cr","string")}} as event_properties_cr,
        {{extract_nested_value("event_properties","cohort_variation_send_cohort","string")}} as event_properties_cohort_variation_send_cohort,
        {{extract_nested_value("event_properties","Message_Type","string")}} as event_properties_Message_type,
        {{extract_nested_value("event_properties","Message_Name","string")}} as event_properties_Message_Name,
        {% if target.type == 'snowflake' %}
        {{timezone_conversion("event_properties.value:timestamp_ts")}} as event_properties_timestamp_ts,
        {% else %}
        {{timezone_conversion("event_properties.timestamp_ts")}} as event_properties_timestamp_ts,
        {% endif %}
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("extra","Sound","boolean")}} as extra_Sound,
        {{extract_nested_value("extra","Badge","boolean")}} as extra_Badge,
        {{extract_nested_value("extra","___customer_merge___","boolean")}} as extra__customer_merge__,
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
        {{ multi_unnesting("event_properties", "attribution") }}
        {{ multi_unnesting("event_properties", "extra") }}
        {{ unnesting("links") }}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_opened_push_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}