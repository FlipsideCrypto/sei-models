{% macro load_blocks_lq() %}
    {%- call statement(
            'get_mb',
            fetch_result = True
        ) -%}
    SELECT
        DATA :data :result :response :last_block_height :: INT last_block_height
    FROM
        (
            SELECT
                ethereum.streamline.udf_json_rpc_call(
                    'http://35.158.109.187:26657',{},
                    [ { 'id': 1, 'jsonrpc': '2.0', 'method': 'abci_info' } ]
                ) DATA
        )
    {%- endcall -%}

    {%- set max_block = load_result('get_mb') ['data'] [0] [0] -%}
    {% set load_query %}
INSERT INTO
    bronze.lq_blocks WITH gen AS (
        SELECT
            ROW_NUMBER() over (
                ORDER BY
                    SEQ4()
            ) AS block_height
        FROM
            TABLE(GENERATOR(rowcount => 100000000))
    ),
    blocks AS (
        SELECT
            block_height
        FROM
            gen
        WHERE
            block_height <= {{ max_block }}
        ORDER BY
            1 DESC
    ),
    calls AS (
        SELECT
            ARRAY_AGG(
                { 'id': block_height,
                'jsonrpc': '2.0',
                'method': 'block',
                'params': [ block_height::STRING ] }
            ) calls
        FROM
            (
                SELECT
                    *,
                    NTILE (200) over(PARTITION BY getdate()
                ORDER BY
                    block_height) AS grp
                FROM
                    (
                        SELECT
                            block_height
                        FROM
                            blocks
                        EXCEPT
                        SELECT
                            block_number
                        FROM
                            bronze.lq_blocks A
                        ORDER BY
                            1 DESC
                        LIMIT
                            5000
                    )
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
    {# {% set wait %}
    CALL system$wait(10);
{% endset %}
    {% do run_query(wait) %} #}
{% endmacro %}
