{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

SELECT
    get_select/dbt_snowflake_query_tags.get_query_comment('invocation_id') AS A
