{% test missing_decoded_logs(model) %}
-- depends_on: {{ ref('silver_evm__logs') }}
SELECT
    l.block_number,
    l._log_id
FROM
    {{ ref('silver_evm__logs') }}
    l
    LEFT JOIN {{ model }}
    d
    ON l.block_number = d.block_number
    AND l._log_id = d._log_id
WHERE
    l.contract_address = lower('0xe30fedd158a2e3b13e9badaeabafc5516e95e8c7') -- WSEI
    AND l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer
    AND l.block_timestamp BETWEEN DATEADD('hour', -48, SYSDATE())
    AND DATEADD('hour', -6, SYSDATE())
    AND d._log_id IS NULL 
{% endtest %}