{% macro create_aws_sei_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_sei_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/sei-api-dev-rolesnowflakeudfsAF733095-N2OQ6E4CMQQW' api_allowed_prefixes = (
            'https://XXX.execute-api.us-east-1.amazonaws.com/prod/',
            'https://fv6hblo1a5.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
