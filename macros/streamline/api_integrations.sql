{% macro create_aws_sei_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_sei_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/sei-api-prod-rolesnowflakeudfsAF733095-CZPTGU57XEA0' api_allowed_prefixes = (
            'https://v19hb3dk4k.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_sei_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/sei-api-dev-rolesnowflakeudfsAF733095-4GCHXFFK8LJ7' api_allowed_prefixes = (
            'https://u1hda5gxml.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
