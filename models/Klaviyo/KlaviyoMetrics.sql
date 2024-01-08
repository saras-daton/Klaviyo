{% if var('KlaviyoMetrics') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_metrics_tbl_ptrn','%klaviyo%metrics','klaviyo_metrics_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(a.id) as id,
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

