{% if var('KlaviyoPlacedOrder') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_placed_order_tbl_ptrn','%klaviyo%placed_order','klaviyo_placed_order_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        coalesce({{extract_nested_value("_attribution","_attributed_event_id","string")}}) as _attribution__attributed_event_id,
        {{extract_nested_value("_attribution","_send_ts","numeric")}} as _attribution__send_ts,
        {{extract_nested_value("_attribution","_message","string")}} as _attribution__message,
        {{extract_nested_value("_attribution","_flow","string")}} as _attribution__flow,
        {{extract_nested_value("_attribution","_variation","string")}} as _attribution__variation,
        {{extract_nested_value("_attribution","_group_ids","string")}} as _attribution__group_ids,
        {{extract_nested_value("_attribution","_experiment","string")}} as _attribution__experiment,
        {{extract_nested_value("_attribution","_attributed_channel","string")}} as _attribution__attributed_channel,
        {{extract_nested_value("event_properties","items","string")}} as event_properties_items,
        {{extract_nested_value("event_properties","collections","string")}} as event_properties_collections,
        {{extract_nested_value("event_properties","item_count","numeric")}} as event_properties_item_count,
        {{extract_nested_value("event_properties","tags","string")}} as event_properties_tags,
        {{extract_nested_value("event_properties","total_discounts","numeric")}} as event_properties_total_discounts,
        {{extract_nested_value("event_properties","source_name","string")}} as event_properties_source_name,
        {{extract_nested_value("event_properties","_currency_code","string")}} as event_properties__currency_code,
        {{extract_nested_value("event_properties","_event_id","string")}} as event_properties__event_id,
        {{extract_nested_value("event_properties","_value","numeric")}} as event_properties__value,
        {{extract_nested_value("event_properties","shippingrate","string")}} as event_properties_shippingrate,
        {{extract_nested_value("event_properties","discount_codes","string")}} as event_properties_discount_codes,
        {{extract_nested_value("event_properties","currency_code","string")}} as event_properties_currency_code,
        {{extract_nested_value("event_properties","event_id","string")}} as event_properties_event_id,
        {{extract_nested_value("event_properties","value","numeric")}} as event_properties_value,
        {{extract_nested_value("event_properties","OptedInToSmsOrderUpdates","boolean")}} as event_properties_OptedInToSmsOrderUpdates,
        {{extract_nested_value("attribution","attributed_event_id","string")}} as attribution_attributed_event_id,
        {{extract_nested_value("attribution","send_ts","numeric")}} as attribution_send_ts,
        {{extract_nested_value("attribution","message","string")}} as attribution_message,
        {{extract_nested_value("attribution","flow","string")}} as attribution_flow,
        {{extract_nested_value("attribution","variation","string")}} as attribution_variation,
        {{extract_nested_value("attribution","group_ids","string")}} as attribution_group_ids,
        {{extract_nested_value("attribution","experiment","string")}} as attribution_experiment,
        {{extract_nested_value("attribution","attributed_channel","string")}} as attribution_attributed_channel,
        {% if target.type == 'snowflake' %}
        {{ currency_conversion('c.value', 'c.from_currency_code', 'event_properties.value:_currency_code') }},
        {% else %}
        {{ currency_conversion('c.value', 'c.from_currency_code', 'event_properties._currency_code') }},
        {% endif %}
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("attributes","uuid","string")}} as attributes_uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        {{timezone_conversion('replace(replace(left(datetime,19),"T"," "),"Z",":00")')}} as attributes_datetime,
        type,
        coalesce(id) as id,
        a._daton_user_id,
        a._daton_batch_runtime,
        a._daton_batch_id,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{ unnesting("attributes") }}
        {{ multi_unnesting("attributes", "event_properties") }}
        {{ multi_unnesting("event_properties", "_attribution") }}
        {{ multi_unnesting("event_properties", "attribution") }}
        {{ unnesting("links") }}
        {% if var('currency_conversion_flag') %}
        left join {{ref('ExchangeRates')}} c on {{timezone_conversion('replace(replace(left(datetime,19),"T"," "),"Z",":00")')}} = c.date 
        and event_properties._currency_code = c.to_currency_code                      
        {% endif %}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a._daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_placed_order_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_rank() over (partition by a.id, _attribution._attributed_event_id order by a._daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}