{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_sei_api AS {% if target.name == "prod" %}
        'https://XXX.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://fv6hblo1a5.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_get_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_get_json_rpc(
        json variant
    ) returns text api_integration = aws_sei_api AS {% if target.name == "prod" %}
        'https://XXX.execute-api.us-east-1.amazonaws.com/prod/bulk_get_json_rpc'
    {% else %}
        'https://fv6hblo1a5.execute-api.us-east-1.amazonaws.com/dev/bulk_get_json_rpc'
    {%- endif %};
{% endmacro %}