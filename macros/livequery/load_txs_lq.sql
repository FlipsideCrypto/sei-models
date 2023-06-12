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
                    NTILE (500) over(PARTITION BY getdate()
                ORDER BY
                    block_number) AS grp
                FROM
                    (
                        SELECT
                            DISTINCT block_number
                        FROM
                            bronze.lq_blocks
                        WHERE
                            block_number IS NOT NULL
                            AND block_number > 9839243
                            AND block_number NOT IN (
                                1835256,
                                2762450,
                                2762412,
                                2762410,
                                2762409,
                                2762408,
                                2762404,
                                2762335,
                                2762333,
                                2762330,
                                852652,
                                852649,
                                849173,
                                849150,
                                849138,
                                849137,
                                808189,
                                808188,
                                808187,
                                806084,
                                806079,
                                806074,
                                806073
                            )
                        EXCEPT
                        SELECT
                            block_number
                        FROM
                            bronze.lq_txs A
                        WHERE
                            block_number > 9839243
                        ORDER BY
                            1
                        LIMIT
                            50
                    )
            )
        GROUP BY
            grp
    ),
    results AS (
        SELECT
            ethereum.streamline.udf_json_rpc_call(
                'https://sei-testnet-rpc.polkachu.com/',{},
                calls
            ) DATA
        FROM
            calls
    )
SELECT
    DISTINCT NULL AS VALUE,
    ROUND(
        CASE
            WHEN DATA :data :id IS NOT NULL THEN DATA :data :id
            ELSE VALUE :id
        END,
        -3
    ) AS _PARTITION_BY_BLOCK_ID,
    CASE
        WHEN DATA :data :id IS NOT NULL THEN DATA :data :id
        ELSE VALUE :id
    END AS block_number,
    DATA :headers AS metadata,
    CASE
        WHEN DATA :data :id IS NOT NULL THEN DATA
        ELSE VALUE
    END AS DATA,
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
