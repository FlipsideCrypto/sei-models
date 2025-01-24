-- depends_on: {{ ref('bronze_evm__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    incremental_predicates = [fsc_evm.standard_predicate()],
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    full_refresh = false,
    tags = ['core']
) }}

{% if execute %}
    {% set max_inserted_query %}

    SELECT
        DATEADD('minute', -5, MAX(_inserted_timestamp))
    FROM
        {{ this }}

        {% endset %}
        {% set max_ins = run_query(max_inserted_query) [0] [0] %}
        {% if not max_ins or max_ins == 'None' %}
            {% set max_ins = '2099-01-01' %}
        {% endif %}

        {% set query_blocks %}
        CREATE
        OR REPLACE temporary TABLE silver_evm.silver_evm__traces2_intermediate_tmp AS
    SELECT
        block_number,
        partition_key,
        VALUE :array_index :: INT AS tx_position,
        DATA :result AS full_traces,
        DATA :txHash :: STRING AS tx_hash,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_evm__streamline_traces') }}
{% else %}
    {{ ref('bronze_evm__streamline_fr_traces') }}
{% endif %}
WHERE
    DATA :result IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= '{{max_ins}}'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_position, tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1 {% endset %}
    {% do run_query(
        query_blocks
    ) %}
{% endif %}

WITH flatten_traces AS (
    SELECT
        block_number,
        tx_hash,
        partition_key,
        IFF(
            path IN (
                'result',
                'result.value',
                'result.type',
                'result.to',
                'result.input',
                'result.gasUsed',
                'result.gas',
                'result.from',
                'result.output',
                'result.error',
                'result.revertReason',
                'result.time',
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'time',
                'revertReason'
            ),
            'ORIGIN',
            REGEXP_REPLACE(REGEXP_REPLACE(path, '[^0-9]+', '_'), '^_|_$', '')
        ) AS trace_address,
        _inserted_timestamp,
        OBJECT_AGG(
            key,
            VALUE
        ) AS trace_json,
        CASE
            WHEN trace_address = 'ORIGIN' THEN NULL
            WHEN POSITION(
                '_' IN trace_address
            ) = 0 THEN 'ORIGIN'
            ELSE REGEXP_REPLACE(
                trace_address,
                '_[0-9]+$',
                '',
                1,
                1
            )
        END AS parent_trace_address,
        SPLIT(
            trace_address,
            '_'
        ) AS trace_address_array
    FROM
        silver_evm.silver_evm__traces2_intermediate_tmp txs,
        TABLE(
            FLATTEN(
                input => PARSE_JSON(
                    txs.full_traces
                ),
                recursive => TRUE
            )
        ) f
    WHERE
        f.index IS NULL
        AND f.key != 'calls'
        AND f.path != 'result'
    GROUP BY
        block_number,
        tx_hash,
        partition_key,
        trace_address,
        _inserted_timestamp
)
SELECT
    block_number,
    tx_hash,
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number'] + ['tx_hash'] + ['trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
