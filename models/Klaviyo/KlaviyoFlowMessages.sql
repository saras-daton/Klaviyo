{% if var('KlaviyoFlowMessages') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_flow_messages_tbl_ptrn','%klaviyo%flow_messages','klaviyo_flow_messages_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(id) as id,
        {{extract_nested_value("attributes","name","string")}} as attributes_name,
        {{extract_nested_value("attributes","channel","string")}} as attributes_channel,
        {{extract_nested_value("content","subject","string")}} as content_subject,
        {{extract_nested_value("content","preview_text","string")}} as content_preview_text,
        {{extract_nested_value("content","from_email","string")}} as content_from_email,
        {{extract_nested_value("content","from_name","string")}} as content_from_name,
        {{extract_nested_value("content","body","string")}} as content_body,
        {{timezone_conversion("attributes.created")}} as created_time,
        {{timezone_conversion("attributes.updated")}} as updated_time,
        date({{timezone_conversion('attributes.updated')}}) as updated_date,
        {{extract_nested_value("links","self","string")}} as links_self,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {{timezone_conversion('attributes.updated')}} as _edm_eff_strt_ts,
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

