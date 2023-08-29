{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration =
    {% if target.name == "prod" %}
        aws_sei_api AS 'https://v19hb3dk4k.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        aws_sei_api_dev AS 'https://u1hda5gxml.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = {% if target.name == "prod" %}
        aws_sei_api AS 'https://v19hb3dk4k.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        aws_sei_api_dev AS 'https://u1hda5gxml.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}