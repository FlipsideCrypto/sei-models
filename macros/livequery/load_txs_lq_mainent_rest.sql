{% macro load_txs_lq_mainnet_rest() %}
    {% set load_query %}
INSERT INTO
    bronze.lq_txs_rest WITH calls AS (
        SELECT
            '/cosmos/tx/v1beta1/txs?events=tx.height%3D' || block_id || '&pagination.limit=1000&pagination.offset=1' calls,
            block_id
        FROM
            (
                SELECT
                    block_id
                FROM
                    sei.silver_observability.transactions_completeness_rest
                EXCEPT
                SELECT
                    block_number
                FROM
                    bronze.lq_txs_rest A
            )
        LIMIT
            25
    ), results AS (
        SELECT
            ethereum.streamline.udf_api(
                'get',
                'url' || calls,{},{}
            ) DATA,
            block_id
        FROM
            calls
    )
SELECT
    DISTINCT NULL AS VALUE,
    ROUND(
        block_id,
        -3
    ) AS _PARTITION_BY_BLOCK_ID,
    block_id AS block_number,
    DATA :headers AS metadata,
    DATA,
    getdate() AS _inserted_timestamp
FROM
    results {% endset %}
    {% do run_query(load_query) %}
    {# {% set wait %}
    CALL system $ wait(10);
{% endset %}
    {% do run_query(wait) %}
    #}
{% endmacro %}
