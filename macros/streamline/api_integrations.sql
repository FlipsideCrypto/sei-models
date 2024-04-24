{% macro create_aws_sei_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_sei_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/sei-api-prod-rolesnowflakeudfsAF733095-iooKxz0RazMg' api_allowed_prefixes = (
            'https://dbtc9lfp0k.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_sei_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/sei-api-stg-rolesnowflakeudfsAF733095-YX1gTAavoOYe' api_allowed_prefixes = (
            'https://ibj933oi6f.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
