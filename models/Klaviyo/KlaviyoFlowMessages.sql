
{% if var('KlaviyoFlowMessages') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_flow_messages_tbl_ptrn'),
exclude=var('klaviyo_flow_messages_tbl_exclude_ptrn'),
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
        {{extract_nested_value("attributes","name","string")}} as attributes_name,
        {{extract_nested_value("attributes","channel","string")}} as attributes_channel,
        {{extract_nested_value("content","subject","string")}} as content_subject,
        {{extract_nested_value("content","preview_text","string")}} as content_preview_text,
        {{extract_nested_value("content","from_email","string")}} as content_from_email,
        {{extract_nested_value("content","from_name","string")}} as content_from_name,
        {{extract_nested_value("content","body","string")}} as content_body,
        {% if var('timezone_conversion_flag') %}
           datetime(datetime_add(cast({{extract_nested_value("attributes","created","string")}} as timestamp), interval {{hr}} hour )) as created_time,
        {% else %}
           datetime(timestamp({{extract_nested_value("attributes","created","string")}})) as created_time,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
           datetime(datetime_add(cast({{extract_nested_value("attributes","updated","string")}} as timestamp), interval {{hr}} hour )) as updated_time,
        {% else %}
           datetime(timestamp({{extract_nested_value("attributes","updated","string")}})) as updated_time,
        {% endif %}
        {% if var('timezone_conversion_flag') %}
           date(datetime_add(cast({{extract_nested_value("attributes","updated","string")}} as timestamp), interval {{hr}} hour )) as updated_date,
        {% else %}
           date(timestamp({{extract_nested_value("attributes","updated","string")}})) as updated_date,
        {% endif %}
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {% if var('timezone_conversion_flag') %}
           datetime_add(cast({{extract_nested_value("attributes","updated","string")}} as timestamp), interval {{hr}} hour ) as _edm_eff_strt_ts,
        {% else %}
           cast({{extract_nested_value("attributes","updated","string")}} as timestamp) as _edm_eff_strt_ts,
        {% endif %}
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","content")}}
        {{unnesting("links")}}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_flow_messages_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

