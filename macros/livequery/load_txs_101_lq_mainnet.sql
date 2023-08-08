{% macro load_txs_101_lq_mainnet() %}
    {% set load_query %}
INSERT INTO
    bronze.lq_txs_101 WITH gen AS (
        SELECT
            ROW_NUMBER() over (
                ORDER BY
                    SEQ4()
            ) AS id
        FROM
            TABLE(GENERATOR(rowcount => 50))
    ),
    possible_perms AS (
        SELECT
            id,
            (
                id * 100
            ) - 99 min_count,
            id * 100 max_count
        FROM
            gen
    ),
    perms AS (
        SELECT
            block_id,
            id
        FROM
            silver.blocks A
            JOIN possible_perms
            ON CEIL(
                tx_count,
                -2
            ) >= max_count
        WHERE
            tx_count > 100
    ),
    calls AS (
        SELECT
            ARRAY_AGG(
                { 'id': block_id,
                'jsonrpc': '2.0',
                'method': 'tx_search',
                'params': [ 'tx.height='||block_id::STRING , true, ''||id||'', '100', 'asc' ] }
            ) calls
        FROM
            (
                SELECT
                    A.block_id,
                    p.id,
                    NTILE (1) over(PARTITION BY getdate()
                ORDER BY
                    A.block_id) AS grp
                FROM
                    (
                        (
                            SELECT
                                DISTINCT block_id
                            FROM
                                perms
                            WHERE
                                block_id IS NOT NULL
                        )
                        EXCEPT
                        SELECT
                            block_number
                        FROM
                            bronze.lq_txs_101 A
                        ORDER BY
                            1
                        LIMIT
                            1
                    ) A
                    JOIN perms p
                    ON A.block_id = p.block_id
            )
        GROUP BY
            grp
    ),
    results AS (
        SELECT
            ethereum.streamline.udf_json_rpc_call(
                'https://snapshotter-0.pacific-1.seinetwork.io/',{},
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
    {# {% set wait %}
    CALL system $ wait(10);
{% endset %}
    {% do run_query(wait) %}
    #}
{% endmacro %}
