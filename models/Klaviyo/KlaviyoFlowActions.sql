
{% if var('KlaviyoFlowActions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('klaviyo_flow_actions_tbl_ptrn'),
exclude=var('klaviyo_flow_actions_tbl_exclude_ptrn'),
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
        {{extract_nested_value("attributes","action_type","string")}} as attributes_action_type,
        {{extract_nested_value("attributes","status","string")}} as attributes_status,
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
        {% if var('timezone_conversion_flag') %}
           datetime_add(cast({{extract_nested_value("attributes","updated","string")}} as timestamp), interval {{hr}} hour ) as _edm_eff_strt_ts,
        {% else %}
           cast({{extract_nested_value("attributes","updated","string")}} as timestamp) as _edm_eff_strt_ts,
        {% endif %}
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

