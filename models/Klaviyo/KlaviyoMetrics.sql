
{% if var('KlaviyoMetrics') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_metrics_tbl_ptrn'),
exclude=var('klaviyo_metrics_tbl_exclude_ptrn'),
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
        coalesce(a.id, 'NA') as id,
        {{extract_nested_value("attributes","name","string")}} as attributes_name,
        datetime(timestamp(case when regexp_contains(attributes.created, r'.*T.{5}Z') then REGEXP_EXTRACT(attributes.created,r'(.*T.{5})Z') ||':00' else attributes.created end  ),'America/New_York' ) as created_time,
        datetime(timestamp(case when regexp_contains(attributes.updated, r'.*T.{5}Z') then REGEXP_EXTRACT(attributes.updated,r'(.*T.{5})Z') ||':00' else attributes.updated end  ),'America/New_York' ) as updated,
        date(timestamp(case when regexp_contains(attributes.updated, r'.*T.{5}Z') then REGEXP_EXTRACT(attributes.updated,r'(.*T.{5})Z') ||':00' else attributes.updated end  ),'America/New_York' ) as updated_date,
        {{extract_nested_value("integration","object","string")}} as integration_object,
        {{extract_nested_value("integration","id","string")}} as integration_id,
        {{extract_nested_value("integration","name","string")}} as integration_name,
        {{extract_nested_value("integration","category","string")}} as integration_category ,
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        datetime(timestamp(case when regexp_contains(attributes.updated, r'.*T.{5}Z') then REGEXP_EXTRACT(attributes.updated,r'(.*T.{5})Z') ||':00' else attributes.updated end ),'America/New_York' ) as _edm_eff_strt_ts,
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","integration")}}
        {{unnesting("links")}}
    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_metrics_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by a.id order by _daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

