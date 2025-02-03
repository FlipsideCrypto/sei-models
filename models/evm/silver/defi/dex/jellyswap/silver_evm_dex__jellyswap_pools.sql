SELECT 
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address as swapper,
    contract_address,
    event_name,
    decoded_log:poolId::STRING as pool_id,
    decoded_log:tokenIn::STRING as token_in,
    decoded_log:tokenOut::STRING as token_out,
    decoded_log:amountIn::NUMBER as amount_in,
    decoded_log:amountOut::NUMBER as amount_out
FROM sei.core_evm.ez_decoded_event_logs
WHERE contract_address = LOWER('0xfb43069f6d0473b85686a85f4ce4fc1fd8f00875')
AND event_name = 'Swap'
AND tx_hash = '0x5c1852f9396b8dbcf41e7f8e028257628a43d51c482b0df921870177ac027c9a'
ORDER BY event_index;