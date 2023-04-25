{% macro run_sp_create_prod_clone() %}
{% set clone_query %}
call sei._internal.create_prod_clone('sei', 'sei_dev', 'internal_dev');
{% endset %}

{% do run_query(clone_query) %}
{% endmacro %}