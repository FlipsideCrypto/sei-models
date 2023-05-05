{% macro load_txs_lq() %}
    {% set load_query %}
INSERT INTO
    bronze.lq_txs WITH calls AS (
        SELECT
            ARRAY_AGG(
                { 'id': block_number,
                'jsonrpc': '2.0',
                'method': 'tx_search',
                'params': [ 'tx.height='||BLOCK_NUMBER::STRING , true, '1', '1000', 'asc' ] }
            ) calls
        FROM
            (
                SELECT
                    *,
                    NTILE (10) over(PARTITION BY getdate()
                ORDER BY
                    block_number) AS grp
                FROM
                    (
                        SELECT
                            DISTINCT block_number
                        FROM
                            bronze.lq_blocks
                        EXCEPT
                        SELECT
                            DISTINCT block_number
                        FROM
                            bronze.lq_txs A
                        LIMIT
                            250
                    )
                ORDER BY
                    1
            )
        GROUP BY
            grp
    ),
    results AS (
        SELECT
            ethereum.streamline.udf_json_rpc_call(
                'http://35.158.109.187:26657',{},
                calls
            ) DATA
        FROM
            calls
    )
SELECT
    NULL AS VALUE,
    ROUND(
        VALUE :id,
        -3
    ) AS _PARTITION_BY_BLOCK_ID,
    VALUE :id AS block_number,
    DATA :headers AS metadata,
    VALUE AS DATA,
    getdate() AS _inserted_timestamp
FROM
    results,
    LATERAL FLATTEN (
        DATA :data,
        outer => TRUE
    );
{% endset %}
    {% do run_query(load_query) %}
    {% set wait %}
    CALL system$wait(10);
{% endset %}
    {% do run_query(wait) %}
{% endmacro %}
