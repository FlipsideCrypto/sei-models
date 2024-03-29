{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
{{ create_udtf_get_base_table(
            schema = "streamline"
        ) }}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_bulk_json_rpc() }}

        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
