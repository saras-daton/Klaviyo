{% if var('KlaviyoFlowActions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('klaviyo_flow_actions_tbl_ptrn','%klaviyo%flow_actions','klaviyo_flow_actions_tbl_exclude_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        type,
        coalesce(id) as id,
        {{extract_nested_value("attributes","action_type","string")}} as attributes_action_type,
        {{extract_nested_value("attributes","status","string")}} as attributes_status,
        {{timezone_conversion("attributes.created")}} as created_time,
        {{timezone_conversion("attributes.updated")}} as updated_time,
        date({{timezone_conversion('attributes.updated')}}) as updated_date,
        {{extract_nested_value("tracking_options","add_utm","boolean")}} as tracking_options_add_utm,
        {{extract_nested_value("utm_params","name","string")}} as utm_params_name,
        {{extract_nested_value("utm_params","value","string")}} as utm_params_value,
        {{extract_nested_value("tracking_options","is_tracking_opens","boolean")}} as tracking_options_is_tracking_opens,
        {{extract_nested_value("tracking_options","is_tracking_clicks","boolean")}} as tracking_options_is_tracking_clicks,
        {{extract_nested_value("send_options","use_smart_sending","boolean")}} as send_options_use_smart_sending,
        {{extract_nested_value("send_options","is_transactional","boolean")}} as send_options_is_transactional,
        {{extract_nested_value("send_options","quiet_hours_enabled","boolean")}} as send_options_quiet_hours_enabled,
        {{extract_nested_value("render_options","shorten_links","boolean")}} as render_options_shorten_links,
        {{extract_nested_value("render_options","add_org_prefix","boolean")}} as render_options_add_org_prefix,
        {{extract_nested_value("render_options","add_info_link","boolean")}} as render_options_add_info_link,
        {{extract_nested_value("render_options","add_opt_out_language","boolean")}} as render_options_add_opt_out_language,
        {{extract_nested_value("links","self","string")}} as links_self,
        {{extract_nested_value("settings","days_of_week","string")}} as settings_days_of_week,
        {{extract_nested_value("settings","delay_seconds","numeric")}} as settings_delay_seconds,
        {{extract_nested_value("settings","is_joined","boolean")}} as settings_is_joined,
        {{extract_nested_value("profile_operations","operator","string")}} as profile_operations_operator,
        {{extract_nested_value("profile_operations","property_key","string")}} as profile_operations_property_key,
        {{extract_nested_value("profile_operations","property_type","string")}} as profile_operations_property_type,
        {{extract_nested_value("profile_operations","property_value","string")}} as profile_operations_property_value,
        {{extract_nested_value("settings","inventory_min","numeric")}} as settings_inventory_min,
        {{extract_nested_value("settings","subscriber_notification_rate","numeric")}} as settings_subscriber_notification_rate,
        {{extract_nested_value("settings","delay_value","numeric")}} as settings_delay_value,
        {{extract_nested_value("settings","delay_type","string")}} as settings_delay_type,
        _daton_user_id,
        _daton_batch_runtime,
        _daton_batch_id,
        {{timezone_conversion('attributes.updated')}} as _edm_eff_strt_ts,
        null as _edm_eff_end_ts,
        unix_micros(current_timestamp()) as _edm_runtime
        from {{i}} a
        {{unnesting("attributes")}}
        {{multi_unnesting("attributes","tracking_options")}}
        {{multi_unnesting("tracking_options","utm_params")}}
        {{multi_unnesting("attributes","send_options")}}
        {{multi_unnesting("attributes","render_options")}}
        {{multi_unnesting("attributes","settings")}}
        {{multi_unnesting("settings","profile_operations")}}
        {{unnesting("links")}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where _daton_batch_runtime  >= (select coalesce(max(_daton_batch_runtime) - {{ var('klaviyo_flow_actions_lookback') }},0) from {{ this }})
        {% endif %}
        qualify row_number() over (partition by id order by _daton_batch_runtime desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %}

