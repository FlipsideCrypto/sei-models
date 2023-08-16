{% macro load_txs_lq_mainnet() %}
    {% set load_query %}
INSERT INTO
    bronze.lq_txs WITH gen AS (
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
            block_number,
            id
        FROM
            (
                SELECT
                    block_number,
                    COALESCE(ARRAY_SIZE(COALESCE(b.value, C.value) :data :txs) :: NUMBER, 100) AS tx_count
                FROM
                    bronze.lq_blocks A,
                    LATERAL FLATTEN(
                        input => A.data :result,
                        outer => TRUE
                    ) AS b,
                    LATERAL FLATTEN(
                        input => A.data :data :result,
                        outer => TRUE
                    ) AS C
            ) A
            JOIN possible_perms
            ON CEIL(
                tx_count,
                -2
            ) >= max_count
    ),
    calls AS (
        SELECT
            ARRAY_AGG(
                { 'id': block_number,
                'jsonrpc': '2.0',
                'method': 'tx_search',
                'params': [ 'tx.height='||BLOCK_NUMBER::STRING , true, ''||id||'', '100', 'asc' ] }
            ) calls
        FROM
            (
                SELECT
                    A.block_number,
                    p.id,
                    NTILE (5000) over(PARTITION BY getdate()
                ORDER BY
                    A.block_number) AS grp
                FROM
                    (
                        SELECT
                            DISTINCT block_number
                        FROM
                            bronze.lq_blocks
                        WHERE
                            block_number IS NOT NULL
                            AND block_number > 16500000
                        EXCEPT
                        SELECT
                            block_number
                        FROM
                            bronze.lq_txs A
                        WHERE
                            block_number > 16500000
                        ORDER BY
                            1
                        LIMIT
                            5000
                    ) A
                    JOIN perms p
                    ON A.block_number = p.block_number
            )
        GROUP BY
            grp
    ),
    results AS (
        SELECT
            ethereum.streamline.udf_json_rpc_call(
                {# (
                SELECT
                    url
                FROM
                    sei._internal.api_keys
                WHERE
                    provider = 'allthatnode'
            ),{},
            #}
            'https://sei-priv.kingnodes.com/',{ 'Referer': 'https://flipside.com' },
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
