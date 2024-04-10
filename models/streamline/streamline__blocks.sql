{{ config (
    materialized = "view"
) }}

{% if execute %}
    {% set height = run_query("SELECT live.udf_api( 'POST', '{Service}/{Authentication}', OBJECT_CONSTRUCT( 'Content-Type', 'application/json' ), OBJECT_CONSTRUCT( 'id', 0, 'jsonrpc', '2.0', 'method', 'status', 'params', [] ), 'vault/prod/sei/quicknode/arctic1' ):data:result:sync_info:latest_block_height::INT as block") %}
    {% set block_height = height.columns [0].values() [0] %}
{% else %}
    {% set block_height = 0 %}
{% endif %}

SELECT
    _id AS block_number
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id <= {{ block_height }}
