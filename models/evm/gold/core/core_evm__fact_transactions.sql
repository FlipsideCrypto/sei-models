{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,from_address,to_address,origin_function_signature)",
    tags = ['core']
) }}

WITH base AS (

    SELECT
        block_number,
        tx_position,
        transaction_json
    FROM
        {{ ref('silver_evm__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
    ),
    transactions_fields AS (
        SELECT
            block_number,
            tx_position,
            transaction_json :blockHash :: STRING AS block_hash,
            transaction_json :blockNumber :: STRING AS block_number_hex,
            transaction_json :from :: STRING AS from_address,
            utils.udf_hex_to_int(
                transaction_json :gas :: STRING
            ) :: bigint AS gas_limit,
            TRY_TO_NUMBER(utils.udf_hex_to_int(
                transaction_json :gasPrice :: STRING
            )) AS gas_price,
            transaction_json :hash :: STRING AS tx_hash,
            transaction_json :input :: STRING AS input_data,
            LEFT(
                input_data,
                10
            ) AS origin_function_signature,
            utils.udf_hex_to_int(
                transaction_json :nonce :: STRING
            ) :: bigint AS nonce,
            transaction_json :r :: STRING AS r,
            transaction_json :s :: STRING AS s,
            transaction_json :to :: STRING AS to_address1,
            CASE
                WHEN to_address1 = '' THEN NULL
                ELSE to_address1
            END AS to_address,
            utils.udf_hex_to_int(
                transaction_json :transactionIndex :: STRING
            ) :: bigint AS transaction_index,
            utils.udf_hex_to_int(
                transaction_json :v :: STRING
            ) :: bigint AS v,
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_fee_per_gas,
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxPriorityFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_priority_fee_per_gas,
            utils.udf_hex_to_int(
                transaction_json :value :: STRING
            ) AS value_precise_raw,
            utils.udf_decimal_adjust(
                value_precise_raw,
                18
            ) AS value_precise,
            value_precise :: FLOAT AS VALUE
            ,utils.udf_hex_to_int(transaction_json :yParity :: STRING):: bigint AS y_parity
            ,transaction_json :accessList AS access_list
        FROM
            base
    ),
    new_transactions AS (
        SELECT
            txs.block_number,
            txs.block_hash,
            b.block_timestamp,
            txs.tx_hash,
            txs.from_address,
            txs.to_address,
            txs.origin_function_signature,
            txs.value,
            txs.value_precise_raw,
            txs.value_precise,
            txs.max_fee_per_gas,
            txs.max_priority_fee_per_gas,
            txs.y_parity,
            txs.access_list,
            txs.gas_price / pow(
                10,
                9
            ) AS gas_price,
            utils.udf_hex_to_int(
                r.receipts_json :gasUsed :: STRING
            ) :: bigint AS gas_used,
            txs.gas_limit,
            utils.udf_hex_to_int(
                r.receipts_json :cumulativeGasUsed :: STRING
            ) :: bigint AS cumulative_gas_used,
            utils.udf_hex_to_int(
                r.receipts_json :effectiveGasPrice :: STRING
            ) :: bigint AS effective_gas_price,
            utils.udf_decimal_adjust(
                txs.gas_price * utils.udf_hex_to_int(
                    r.receipts_json :gasUsed :: STRING
                ) :: bigint,
                18
            ) AS tx_fee_precise,
            COALESCE(
                tx_fee_precise :: FLOAT,
                0
            ) AS tx_fee,
            CASE
                WHEN r.receipts_json :status :: STRING = '0x1' THEN TRUE
                WHEN r.receipts_json :status :: STRING = '0x0' THEN FALSE
                ELSE NULL
            END AS tx_succeeded,
            utils.udf_hex_to_int(
                r.receipts_json :type :: STRING
            ) :: bigint AS tx_type,
            txs.nonce,
            txs.tx_position,
            txs.input_data,
            txs.r,
            txs.s,
            txs.v
        FROM
            transactions_fields txs
            LEFT JOIN {{ ref('core_evm__fact_blocks') }}
            b
            ON txs.block_number = b.block_number

{% if is_incremental() %}
AND b.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
LEFT JOIN {{ ref('silver_evm__receipts') }}
r
ON txs.block_number = r.block_number
AND txs.tx_hash = r.tx_hash

{% if is_incremental() %}
AND r.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp AS block_timestamp_heal,
        t.tx_hash,
        t.from_address,
        t.to_address,
        t.origin_function_signature,
        t.value,
        t.value_precise_raw,
        t.value_precise,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.y_parity,
        t.access_list,
        t.authorization_list,
        t.gas_price,
        utils.udf_hex_to_int(
            r.receipts_json :gasUsed :: STRING
        ) :: bigint AS gas_used_heal,
        t.gas_limit,
        utils.udf_hex_to_int(
            r.receipts_json :cumulativeGasUsed :: STRING
        ) :: bigint AS cumulative_gas_used_heal,
        utils.udf_hex_to_int(
            r.receipts_json :effectiveGasPrice :: STRING
        ) :: bigint AS effective_gas_price_heal,
        utils.udf_decimal_adjust(
            t.gas_price * utils.udf_hex_to_int(
                r.receipts_json :gasUsed :: STRING
            ) :: bigint, 
            9
        ) AS tx_fee_precise_heal,
        COALESCE(
            tx_fee_precise_heal :: FLOAT,
            0
        ) AS tx_fee_heal,
        CASE
            WHEN r.receipts_json :status :: STRING = '0x1' THEN TRUE
            WHEN r.receipts_json :status :: STRING = '0x0' THEN FALSE
            ELSE NULL
        END AS tx_succeeded_heal,
        utils.udf_hex_to_int(
                r.receipts_json :type :: STRING
            ) :: bigint AS tx_type_heal,
        t.nonce,
        t.tx_position,
        t.input_data,
        t.r,
        t.s,
        t.v
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core_evm__fact_blocks') }}
        b
        ON t.block_number = b.block_number
        LEFT JOIN {{ ref('silver_evm__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash
    WHERE
        t.block_timestamp IS NULL
        OR t.tx_succeeded IS NULL
)
{% endif %},
all_transactions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        origin_function_signature,
        VALUE,
        value_precise_raw,
        value_precise,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        y_parity,
        access_list,
        tx_fee,
        tx_fee_precise,
        tx_succeeded,
        tx_type,
        nonce,
        tx_position,
        input_data,
        gas_price,
        gas_used,
        gas_limit,
        cumulative_gas_used,
        effective_gas_price / pow(10, 9) as effective_gas_price,
        r,
        s,
        v
    FROM
        new_transactions

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    y_parity,
    access_list,
    tx_fee_heal AS tx_fee,
    tx_fee_precise_heal AS tx_fee_precise,
    tx_succeeded_heal AS tx_succeeded,
    tx_type_heal AS tx_type,
    nonce,
    tx_position,
    input_data,
    gas_price,
    gas_used_heal AS gas_used,
    gas_limit,
    cumulative_gas_used_heal AS cumulative_gas_used,
    effective_gas_price_heal / pow(10, 9) AS effective_gas_price,
    r,
    s,
    v
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    COALESCE(tx_fee_precise,'0') AS tx_fee_precise,
    tx_succeeded,
    tx_type,
    nonce,
    tx_position,
    input_data,
    gas_price,
    effective_gas_price,
    gas_used,
    gas_limit,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    access_list,
    r,
    s,
    v,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS fact_transactions_id,
    {% if is_incremental() %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    all_transactions qualify ROW_NUMBER() over (
        PARTITION BY fact_transactions_id
        ORDER BY
            block_number DESC,
            block_timestamp DESC nulls last,
            tx_succeeded DESC nulls last
    ) = 1