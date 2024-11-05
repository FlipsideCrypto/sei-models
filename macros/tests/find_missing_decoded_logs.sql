{% test find_missing_decoded_logs(model, fact_logs_model) %}
SELECT
    l.block_number,
    l.logs_id
FROM
    {{ fact_logs_model }}
    l
    LEFT JOIN {{ model }}
    d
    ON d.ez_decoded_event_logs_id = l.logs_id
WHERE
    l.contract_address = lower('0xE30feDd158A2e3b13e9badaeABaFc5516e95e8C7')
    AND l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer
    AND d.ez_decoded_event_logs_id IS NULL
{% endtest %}