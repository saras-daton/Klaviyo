{% if var('KlaviyoPlacedOrder') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

-- depends_on: {{ref('ExchangeRates')}}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_placed_order_tbl_ptrn'),
exclude=var('klaviyo_placed_order_tbl_exclude_ptrn'),
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
        coalesce({{extract_nested_value("_attribution","_attributed_event_id","string")}}, 'NA') as _attribution__attributed_event_id,
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
        {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then {{extract_nested_value("event_properties","_currency_code","string")}} else c.from_currency_code end as exchange_currency_code,
        {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        {{extract_nested_value("event_properties","_currency_code","string")}} as exchange_currency_code, 
        {% endif %} 
        {{extract_nested_value("attributes","metric_id","string")}} as attributes_metric_id,
        {{extract_nested_value("attributes","profile_id","string")}} as attributes_profile_id,
        timestamp_millis({{extract_nested_value("attributes","timestamp","int64")}}) as attributes_timestamp,
        {{extract_nested_value("attributes","uuid","string")}} as attributes_uuid,
        {{extract_nested_value("links","self","string")}} as links_self,
        {% if var('timezone_conversion_flag') %}
           datetime(datetime_add(cast(datetime as timestamp), interval {{hr}} hour )) as attributes_datetime,
        {% else %}
           datetime(timestamp(datetime)) as attributes_datetime,
        {% endif %}   
        type,
        coalesce(id, 'NA') as id,
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
        left join {{ref('ExchangeRates')}} c on date(cast(attributes.datetime as timestamp)) = c.date 
        and event_properties._currency_code = c.to_currency_code                      
        {% endif %}
        
        
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a._daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_placed_order_lookback') }},0) from {{ this }})
            {% endif %}
    qualify dense_rank() over (partition by a.id, _attribution._attributed_event_id order by a._daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}